"""
Classes related to file transfer requests as used by COPY INTO ON CLIENT.
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
import typing
from io import BufferedIOBase, BufferedWriter, RawIOBase, TextIOBase, TextIOWrapper
from abc import ABC, abstractmethod
from typing import Any, Optional, Union
from pymonetdb.mapi import MSG_MORE, MSG_FILETRANS
from pymonetdb.exceptions import ProgrammingError

if typing.TYPE_CHECKING:
    from pymonetdb.mapi import Connection as MapiConnection


class Upload:
    """
    Represents a request from the server to upload data to the server. It is
    passed to the Uploader registered by the application, which for example
    might retrieve the data from a file on the client system. See
    pymonetdb.sql.connections.Connection.set_uploader().

    Use the method send_error() to refuse the upload, binary_writer() to get a
    binary file object to write to, or text_writer() to get a text-mode file
    object to write to.

    Implementations should be VERY CAREFUL to validate the file name before
    opening any files on the client system!
    """

    mapi: Optional["MapiConnection"]
    error = False
    cancelled = False
    bytes_sent = 0
    chunk_size = 1024 * 1024
    chunk_used = 0
    rawio: Optional["UploadIO"] = None
    writer: Optional[BufferedWriter] = None
    twriter: Optional[TextIOBase] = None

    def __init__(self, mapi: "MapiConnection"):
        self.mapi = mapi

    def _check_usable(self):
        if self.error:
            raise ProgrammingError("Upload handle has had an error, cannot be used anymore")
        if not self.mapi:
            raise ProgrammingError("Upload handle has been closed, cannot be used anymore")

    def is_cancelled(self) -> bool:
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

    def send_error(self, message: str) -> None:
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

    def _raw(self) -> "UploadIO":
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
            # Without the Any annotation there is no way I can convince the
            # type checker that TextIOWrapper can accept a NormalizeCrLf
            # object. Apparently being a subclass of BufferedIOBase is not enough.
            w: Any = NormalizeCrLf(self._raw())
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
        if prompt == MSG_MORE:
            self.chunk_used = 0
            return True
        elif prompt == MSG_FILETRANS:
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
                if resp != MSG_FILETRANS:
                    raise ProgrammingError(f"Unexpected server response: {resp[:50]!r}")
            self.mapi = None


class UploadIO(RawIOBase):
    """IO adaptor for Upload. """

    def __init__(self, upload: Upload):
        self.upload = upload

    def writable(self):
        return True

    def write(self, b):
        n = len(b)
        if self.upload.is_cancelled():
            return n
        self.upload._send_data(b)
        return n


class NormalizeCrLf(BufferedIOBase):
    """
    Helper class used to normalize line endings before sending text to MonetDB.

    Existing normalization code mostly deals with normalizing after reading,
    this one normalizes before writing.
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
    def handle_upload(self, upload: Upload, filename: str, text_mode: bool,
                      skip_amount: int):
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
