"""
Classes related to file transfer requests as used by COPY INTO ON CLIENT.
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


from abc import ABC, abstractmethod
import codecs
from importlib import import_module
from io import BufferedIOBase, BufferedWriter, TextIOBase, TextIOWrapper
from pathlib import Path
from shutil import copyfileobj
from typing import Optional, Union
from pymonetdb import mapi
from pymonetdb.exceptions import OperationalError, ProgrammingError


def handle_file_transfer(mapi, cmd: str):
    if cmd.startswith("r "):
        parts = cmd[2:].split(' ', 2)
        if len(parts) == 2:
            try:
                n = int(parts[0])
            except ValueError:
                pass
            return handle_upload(mapi, parts[1], True, n)
    elif cmd.startswith("rb "):
        return handle_upload(mapi, cmd[2:], False, 0)
    elif cmd.startswith("w "):
        return handle_download(mapi, cmd[2:], True)
    elif cmd.startswith("wb "):
        return handle_download(mapi, cmd[3:], False)
    else:
        pass
    # we only reach this if decoding the cmd went wrong:
    mapi._putblock(f"Invalid file transfer command: {cmd!r}")


def handle_upload(mapi, filename, text_mode, offset):
    if not mapi.uploader:
        mapi._putblock("No upload handler has been registered with pymonetdb\n")
        return
    skip_amount = offset - 1 if offset > 0 else 0
    upload = Upload(mapi)
    try:
        try:
            mapi.uploader.handle_upload(upload, filename, text_mode, skip_amount)
        except Exception as e:
            # We must make sure the server doesn't think this is a succesful upload.
            # The protocol does not allow us to flag an error after the upload has started,
            # so the only thing we can do is kill the connection
            upload.error = True
            mapi._sabotage()
            raise e
        if not upload.has_been_used():
            raise ProgrammingError("Upload handler didn't do anything")
    finally:
        upload.close()


def handle_download(mapi, filename, text_mode):
    if not mapi.downloader:
        mapi._putblock("No download handler has been registered with pymonetdb\n")
        return
    download = Download(mapi)
    try:
        mapi.downloader.handle_download(download, filename, text_mode)
    except Exception as e:
        # For consistency we also drop the connection on these exceptions.
        #
        # # Alternatively we might just discard the incoming data and allow
        # work to continue, but in 99% of the cases the application is about
        # to crash and it makes no sense to delay that by first reading all
        # the data.
        #
        # Also, if the download has not really started yet we might send
        # an error message to the server but then you get inconsistent
        # behaviour: if the download hadn't started yet, the transaction ends
        # up in an aborted state and must be ROLLed BACK, but if the download
        # has started we discard all data and allow it to continue without
        # error.
        #
        # Bottom line is that it's easier to understand if we just always
        # crash the connection.
        download._shutdown()
        mapi._sabotage()
        raise e
    finally:
        download.close()


class Upload:
    """
    Represents a request from the server to upload data to the server. It is
    passed to the Uploader registered by the application, which for example
    might retrieve the data from a file on the client system. See
    pymonetdb.Connection.set_uploader().

    Use the method send_error() to refuse the upload, binary_writer() to get a
    binary file object to write to, or text_writer() to get a text-mode file
    object to write to.

    Implementations should be VERY CAREFUL to validate the file name before
    opening any files on the client system!
    """

    def __init__(self, mapi):
        self.mapi = mapi
        self.error = False
        self.cancelled = False
        self.bytes_sent = 0
        self.chunk_size = 1024 * 1024
        self.chunk_used = 0
        self.rawio = None
        self.writer = None
        self.twriter = None

    def _check_usable(self):
        if self.error:
            raise ProgrammingError("Upload handle has had an error, cannot be used anymore")
        if not self.mapi:
            raise ProgrammingError("Upload handle has been closed, cannot be used anymore")

    def is_cancelled(self):
        """Returns true if the server has cancelled the upload."""
        return self.cancelled

    def has_been_used(self) -> bool:
        """Returns true if .send_error(), .text_writer() or .binary_writer() have been called."""
        return self.error or (self.rawio is not None)

    def set_chunk_size(self, size: int):
        """
        After every CHUNK_SIZE bytes, the server gets the opportunity to cancel
        the rest of the upload. Defaults to 1 MiB.
        """
        self.chunk_size = size

    def send_error(self, message: str):
        """
        Tell the server the requested upload has been refused
        """
        if self.cancelled:
            return
        self._check_usable()
        if self.bytes_sent:
            raise ProgrammingError("Cannot send error after data has been sent")
        if not message.endswith("\n"):
            message += "\n"
        self.error = True
        assert self.mapi
        self.mapi._putblock(message)
        self.mapi = None

    def _raw(self):
        if self.bytes_sent == 0:
            # send the magic newline indicating we're ok with the upload
            self._send(b'\n', False)
        if not self.rawio:
            self.rawio = UploadIO(self)
        return self.rawio

    def binary_writer(self) -> BufferedIOBase:
        """
        Returns a binary file-like object. All data written to it is uploaded
        to the server.
        """
        if not self.writer:
            self.writer = BufferedWriter(self._raw())
        return self.writer

    def text_writer(self) -> TextIOBase:
        r"""
        Returns a text-mode file-like object. All text written to it is uploaded
        to the server. DOS/Windows style line endings (CR LF, \\r \\n) are
        automatically rewritten to single \\n's.
        """
        if not self.twriter:
            w = self._raw()
            w = NormalizeCrLf(w)
            self.twriter = TextIOWrapper(w, encoding='utf-8', newline='\n')
        return self.twriter

    def _send_data(self, data: Union[bytes, memoryview]):
        if self.cancelled:
            return
        self._check_usable()
        assert self.mapi is not None
        pos = 0
        end = len(data)
        while pos < end:
            n = min(end - pos, self.chunk_size - self.chunk_used)
            chunk = memoryview(data)[pos:pos + n]
            if n == self.chunk_size - self.chunk_used and self.chunk_size > 0:
                server_wants_more = self._send_and_get_prompt(chunk)
                if not server_wants_more:
                    self.cancelled = True
                    self.mapi.uploader.cancel()
                    self.mapi = None
                    break
            else:
                self._send(chunk, False)
            pos += n

    def _send(self, data: Union[bytes, memoryview], finish: bool):
        assert self.mapi
        self.mapi._putblock_raw(data, finish)
        self.bytes_sent += len(data)
        self.chunk_used += len(data)

    def _send_and_get_prompt(self, data: Union[bytes, memoryview]) -> bool:
        assert self.mapi
        self._send(data, True)
        prompt = self.mapi._getblock()
        if prompt == mapi.MSG_MORE:
            self.chunk_used = 0
            return True
        elif prompt == mapi.MSG_FILETRANS:
            # server says stop
            return False
        else:
            raise ProgrammingError(f"Unexpected server response: {prompt[:50]!r}")

    def close(self):
        """
        End the upload succesfully
        """
        if self.error:
            return
        if self.twriter:
            self.twriter.close()
        if self.writer:
            self.writer.close()
        if self.mapi:
            server_wants_more = False
            if self.chunk_used != 0:
                # finish the current block
                server_wants_more = self._send_and_get_prompt(b'')
            if server_wants_more:
                # send empty block to indicate end of upload
                self.mapi._putblock('')
                # receive acknowledgement
                resp = self.mapi._getblock()
                if resp != mapi.MSG_FILETRANS:
                    raise ProgrammingError(f"Unexpected server response: {resp[:50]!r}")
            self.mapi = None


class UploadIO(BufferedIOBase):
    """IO adaptor for Upload. """

    def __init__(self, upload):
        self.upload = upload

    def writable(self):
        return True

    def write(self, b):
        n = len(b)
        if self.upload.is_cancelled():
            return n
        self.upload._send_data(b)
        return n


class Uploader(ABC):
    """
    Base class for upload hooks. Instances of subclasses of this class can be
    registered using pymonetdb.Connection.set_uploader(). Every time an upload
    request is received, an Upload object is created and passed to this objects
    .handle_upload() method.

    If the server cancels the upload halfway, the .cancel() methods is called
    and all further data written is ignored.
    """

    @abstractmethod
    def handle_upload(self, upload: Upload, filename: str, text_mode: bool, skip_amount: int):
        """
        Called when an upload request is received. Implementations should either
        send an error using upload.send_error(), or request a writer using
        upload.text_writer() or upload.binary_writer(). All data written to the
        writer will be sent to the server.

        Parameter 'filename' is the file name used in the COPY INTO statement.
        Parameter 'text_mode' indicates whether the server requested a text file
        or a binary file. In case of a text file, 'skip_amount' indicates the
        number of lines to skip. In binary mode, 'skip_amount' is always 0.

        SECURITY NOTE! Make sure to carefully validate the file name before
        opening files on the file system. Otherwise, if an adversary has taken
        control of the network connection or of the server, they can use file
        upload requests to read arbitrary files from your computer
        (../../)

        """
        pass

    def cancel(self):
        """Optional method called when the server cancels the upload."""
        pass


class Download:
    """
    Represents a request from the server to download data from the server. It is
    passed to the Downloader registered by the application, which for example
    might write the data to a file on the client system. See
    pymonetdb.Connection.set_downloader().

    Use the method send_error() to refuse the download, binary_reader() to get a
    binary file object to read bytes from, or text_reader() to get a text-mode
    file object to read text from.

    Implementations should be EXTREMELY CAREFUL to validate the file name before
    opening and writing to any files on the client system!
    """

    def __init__(self, mapi):
        self.mapi = mapi
        self.started = False
        buffer = bytearray(8190)
        self.buffer = memoryview(buffer)
        self.pos = 0
        self.len = 0
        self.reader = None
        self.treader = None

    def send_error(self, message: str):
        """
        Tell the server the requested download is refused
        """
        if self.started:
            raise ProgrammingError("Cannot send error anymore")
        if not self.mapi:
            return
        self.started = True
        if not message.endswith("\n"):
            message += "\n"
        self.mapi._putblock(message)
        self._shutdown()

    def binary_reader(self):
        """Returns a binary file-like object to read the downloaded data from."""
        if not self.reader:
            if not self.mapi:
                raise ProgrammingError("download has already been closed")
            self.started = True
            self.mapi._putblock("\n")
            self.reader = DownloadIO(self)
        return self.reader

    def text_reader(self):
        """Returns a text mode file-like object to read the downloaded data from."""
        if not self.treader:
            self.treader = TextIOWrapper(self.binary_reader(), encoding='utf-8', newline='\n')
        return self.treader

    def close(self):
        """End the download succesfully. Any unconsumed data will be discarded."""
        while self.mapi:
            self._fetch()
        assert not self.mapi

    def _available(self):
        return self.len - self.pos

    def _consume(self, n: int) -> memoryview:
        end = min(self.pos + n, self.len)
        ret = self.buffer[self.pos:end]
        self.pos = end
        return ret

    def _fetch(self) -> memoryview:
        self.pos = 0
        self.len = self._fetch_into(self.buffer)
        return self.buffer[0:self.len]

    def _fetch_into(self, buf: memoryview) -> int:
        assert len(buf) >= 8190
        # loop because the server *might* send empty blocks
        while True:
            if not self.mapi:
                return 0
            if not self._read_bytes(buf[:2]):
                # clean EOF
                return 0
            unpacked = buf[0] + 256 * buf[1]
            length = unpacked // 2
            if length > 0:
                if not self._read_bytes(buf[:length]):
                    self._shutdown()
                    raise OperationalError("incomplete packet")
            if unpacked & 1:
                self._shutdown()
            return length

    def _read_bytes(self, buf: memoryview) -> bool:
        assert self.mapi.socket
        pos = 0
        end = len(buf)
        while pos < end:
            n = self.mapi.socket.recv_into(buf[pos:end])
            if n == 0:
                self._shutdown()
                break
            pos += n
        if pos == end:
            return True
        elif pos == 0:
            return False
        else:
            raise OperationalError("incomplete packet")

    def _shutdown(self):
        self.started = True
        self.mapi = None


class DownloadIO(BufferedIOBase):

    def __init__(self, download):
        self.download = download

    def readable(self):
        return True

    def read(self, n=0):
        if self.download._available() == 0:
            self.download._fetch()
        return bytes(self.download._consume(n))

    def read1(self, n=0):
        return self.read(n)


class Downloader(ABC):
    """
    Base class for download hooks. Instances of subclasses of this class can be
    registered using pymonetdb.Connection.set_downloader(). Every time a
    download request arrives, a Download object is created and passed to this
    objects .handle_download() method.

    SECURITY NOTE! Make sure to carefully validate the file name before opening
    files on the file system. Otherwise, if an adversary has taken control of
    the network connection or of the server, they can use download requests to
    OVERWRITE ARBITRARY FILES on your computer
    """

    @abstractmethod
    def handle_download(self, download: Download, filename: str, text_mode: bool):
        pass


class NormalizeCrLf(BufferedIOBase):
    """
    Helper class used to normalize line endings before sending text to MonetDB.

    Existing normalization code mostly deals with normalizing after reading,
    not before writing.
    """

    def __init__(self, inner):
        self.inner = inner
        self.pending = False

    def writable(self):
        return True

    def write(self, data) -> int:
        if not data:
            return 0

        if self.pending:
            if data.startswith(b"\n"):
                # normalize by forgetting the pending \r
                pass
            else:
                # pending \r not followed by \n, write it
                self.inner.write(b"\r")
                # do not take the above write into account in the return value,
                # it was included last time

        normalized = data.replace(b"\r\n", b"\n")

        if normalized[-1] == 13:  # \r
            # not sure if it will be followed by \n, move it to pending
            self.pending = True
            normalized = memoryview(normalized)[:-1]
        else:
            self.pending = False

        n = self.inner.write(normalized)
        assert n == len(normalized)
        return len(data)

    def flush(self):
        return self.inner.flush()

    def close(self):
        if self.pending:
            self.inner.write(b"\r")
            self.pending = False
        return self.inner.close()


class DefaultHandler(Uploader, Downloader):
    """
    File transfer handler which uploads and downloads files from a given
    directory, taking care not to allow access to files outside that directory.
    Instances of this class can be registered using the pymonetb.Connection's
    set_uploader() and set_downloader() methods.

    The 'encoding' and 'newline' parameters are applied to text file transfers
    and ignored for binary transfers.

    As an optimization, if you set encoding to 'utf-8' and newline to '\\\\n',
    text mode transfers are performed as binary, which improves performance. For
    uploads, only do this if you are absolutely, positively sure that all files
    in the directory are actually valid UTF-8 encoded and have Unix line
    endings.

    If 'compression' is set to True, which is the default, the DefaultHandler will
    automatically compress and decompress files with extensions .gz, .bz2, .xz
    and .lz4. Note that the first three algorithms are built into Python, but LZ4
    only works if the lz4.frame module is available.
    """

    def __init__(self, dir, encoding: str = None, newline=None, compression=True):
        self.dir = Path(dir).resolve()
        self.encoding = encoding
        self.is_utf8 = (self.encoding and (codecs.lookup('utf-8') == codecs.lookup(self.encoding)))
        self.newline = newline
        self.compression = compression

    def secure_resolve(self, filename) -> Optional[Path]:
        p = self.dir.joinpath(filename).resolve()
        if str(p).startswith(str(self.dir)):
            return p
        else:
            return None

    def handle_upload(self, upload: Upload, filename: str, text_mode: bool, skip_amount: int):
        """:meta private:"""
        p = self.secure_resolve(filename)
        if not p:
            return upload.send_error("Forbidden")

        if self.is_utf8 and self.newline == "\n" and skip_amount == 0:
            # optimization
            text_mode = False

        # open
        if text_mode:
            mode = "rt"
            encoding = self.encoding
            newline = self.newline
        else:
            mode = "rb"
            encoding = None
            newline = None
        try:
            opener = lookup_compression_algorithm(filename) if self.compression else open
        except ModuleNotFoundError as e:
            return upload.send_error(str(e))
        try:
            f = opener(p, mode=mode, encoding=encoding, newline=newline)
        except IOError as e:
            return upload.send_error(str(e))

        with f:
            if text_mode:
                tw = upload.text_writer()
                for _ in range(skip_amount):
                    if not f.readline():
                        break
                self._upload_data(upload, f, tw)
            else:
                bw = upload.binary_writer()
                self._upload_data(upload, f, bw)

    def _upload_data(self, upload: Upload, src, dst):
        # Due to duck typing this method works equally well in text- and binary mode
        bufsize = 1024 * 1024
        while not upload.is_cancelled():
            data = src.read(bufsize)
            if not data:
                break
            dst.write(data)

    def handle_download(self, download: Download, filename: str, text_mode: bool):
        p = self.secure_resolve(filename)
        if not p:
            return download.send_error("Forbidden")

        if self.is_utf8 and self.newline == "\n":
            # optimization
            text_mode = False

        # open
        mode = "w" if text_mode else "wb"
        if text_mode:
            mode = "wt"
            encoding = self.encoding
            newline = self.newline
        else:
            mode = "wb"
            encoding = None
            newline = None
        try:
            opener = lookup_compression_algorithm(filename) if self.compression else open
        except ModuleNotFoundError as e:
            return download.send_error(str(e))
        try:
            f = opener(p, mode=mode, encoding=encoding, newline=newline)
        except IOError as e:
            return download.send_error(str(e))

        with f:
            if text_mode:
                tr = download.text_reader()
                copyfileobj(tr, f)
            else:
                br = download.binary_reader()
                copyfileobj(br, f)


def lookup_compression_algorithm(filename: str):
    lowercase = str(filename).lower()
    if lowercase.endswith('.gz'):
        mod = 'gzip'
    elif lowercase.endswith('.bz2'):
        mod = 'bz2'
    elif lowercase.endswith('.xz'):
        mod = 'lzma'
    elif lowercase.endswith('.lz4'):
        # not always available
        mod = 'lz4.frame'
    else:
        return open
    opener = import_module(mod).open
    return opener