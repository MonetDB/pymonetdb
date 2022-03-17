"""
This is the python implementation of the mapi protocol.
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


from abc import ABC, abstractmethod
import socket
import logging
import struct
import hashlib
import os
from typing import Optional, Union
from io import SEEK_SET, BufferedIOBase, BufferedWriter, BytesIO, TextIOBase, TextIOWrapper
from urllib.parse import urlparse, parse_qs

from pymonetdb.exceptions import OperationalError, DatabaseError, \
    ProgrammingError, NotSupportedError, IntegrityError

logger = logging.getLogger(__name__)

MAX_PACKAGE_LENGTH = (1024 * 8) - 2

MSG_PROMPT = ""
MSG_MORE = "\1\2\n"
MSG_FILETRANS = "\1\3\n"
MSG_FILETRANS_B = b"\1\3\n"
MSG_INFO = "#"
MSG_ERROR = "!"
MSG_Q = "&"
MSG_QTABLE = "&1"
MSG_QUPDATE = "&2"
MSG_QSCHEMA = "&3"
MSG_QTRANS = "&4"
MSG_QPREPARE = "&5"
MSG_QBLOCK = "&6"
MSG_HEADER = "%"
MSG_TUPLE = "["
MSG_TUPLE_NOSLICE = "="
MSG_REDIRECT = "^"
MSG_OK = "=OK"

STATE_INIT = 0
STATE_READY = 1

# MonetDB error codes
errors = {
    '42S02': OperationalError,  # no such table
    '40002': IntegrityError,  # INSERT INTO: UNIQUE constraint violated
    '2D000': IntegrityError,  # COMMIT: failed
    '40000': IntegrityError,  # DROP TABLE: FOREIGN KEY constraint violated
    'M0M29': IntegrityError,  # The code monetdb emitted before Jun2020
}


def handle_error(error):
    """Return exception matching error code.

    args:
        error (str): error string, potentially containing mapi error code

    returns:
        tuple (Exception, formatted error): returns OperationalError if unknown
            error or no error code in string

    """

    if error[:13] == 'SQLException:':
        idx = str.index(error, ':', 14)
        error = error[idx + 10:]
    if len(error) > 5 and error[:5] in errors:
        return errors[error[:5]], error
    else:
        return OperationalError, error


# noinspection PyExceptionInherit
class Connection(object):
    """
    MAPI (low level MonetDB API) connection
    """

    def __init__(self):
        self.state = STATE_INIT
        self._result = None
        self.socket: Optional[socket.socket] = None
        self.unix_socket = None
        self.hostname = ""
        self.port = 0
        self.username = ""
        self.password = ""
        self.database = ""
        self.language = ""
        self.handshake_options = None
        self.connect_timeout = socket.getdefaulttimeout()
        self.uploader = None
        self.downloader = None

    def connect(self, database, username, password, language, hostname=None,
                port=None, unix_socket=None, connect_timeout=-1, handshake_options=None):
        """ setup connection to MAPI server

        unix_socket is used if hostname is not defined.
        """

        if ':' in database:
            if not database.startswith('mapi:monetdb:'):
                raise DatabaseError("colon not allowed in database name, except as part of mapi:monetdb://<hostname>[:<port>]/<database> URI")
            parsed = urlparse(database[5:])
            # parse basic settings
            if parsed.hostname or parsed.port:
                # connect over tcp
                if not parsed.path.startswith('/'):
                    raise DatabaseError('invalid mapi url')
                database = parsed.path[1:]
                if '/' in database:
                    raise DatabaseError('invalid mapi url')
                username = parsed.username or username
                password = parsed.password or password
                hostname = parsed.hostname or hostname
                port = parsed.port or port
            else:
                # connect over unix domain socket
                unix_socket = parsed.path or unix_socket
                username = parsed.username or username
                password = parsed.password or password
                database = ''  # must be set in uri parameter
            # parse uri parameters
            if parsed.query:
                parms = parse_qs(parsed.query)
                if 'database' in parms:
                    if database == '':
                        database = parms['database'][-1]
                    else:
                        raise DatabaseError('database= query parameter is only allowed with unix domain sockets')
                # Future work: parse other parameters such as reply_size.

        if hostname and hostname[:1] == '/' and not unix_socket:
            unix_socket = '%s/.s.monetdb.%d' % (hostname, port)
            hostname = None
        if not unix_socket and os.path.exists("/tmp/.s.monetdb.%i" % port):
            unix_socket = "/tmp/.s.monetdb.%i" % port
        elif not unix_socket and not hostname:
            hostname = 'localhost'

        # None and zero are allowed values
        if connect_timeout != -1:
            assert connect_timeout is None or connect_timeout >= 0
            self.connect_timeout = connect_timeout

        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.language = language
        self.unix_socket = unix_socket
        self.handshake_options = handshake_options or []
        if hostname:
            if self.socket:
                self.socket.close()
                self.socket = None
            for af, socktype, proto, canonname, sa in socket.getaddrinfo(hostname, port,
                                                                         socket.AF_UNSPEC, socket.SOCK_STREAM):
                try:
                    self.socket = socket.socket(af, socktype, proto)
                    # For performance, mirror MonetDB/src/common/stream.c socket settings.
                    self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    self.socket.settimeout(self.connect_timeout)
                except socket.error as msg:
                    logger.debug(f"'{msg}' for af {af} with socktype {socktype}")
                    self.socket = None
                    continue
                try:
                    self.socket.connect(sa)
                except socket.error as msg:
                    logger.info(msg.strerror)
                    self.socket.close()
                    self.socket = None
                    continue
                break
            if self.socket is None:
                raise socket.error("Connection refused")
        else:
            self.socket = socket.socket(socket.AF_UNIX)
            self.socket.settimeout(self.connect_timeout)
            self.socket.connect(unix_socket)
            if self.language != 'control':
                # don't know why, but we need to do this
                self.socket.send('0'.encode())

        if not (self.language == 'control' and not self.hostname):
            # control doesn't require authentication over socket
            self._login()

        self.socket.settimeout(socket.getdefaulttimeout())
        self.state = STATE_READY

    def _login(self, iteration=0):
        """ Reads challenge from line, generate response and check if
        everything is okay """

        challenge = self._getblock()
        response = self._challenge_response(challenge)
        self._putblock(response)
        prompt = self._getblock().strip()

        if len(prompt) == 0:
            # Empty response, server is happy
            pass
        elif prompt == MSG_OK:
            pass
        elif prompt.startswith(MSG_INFO):
            logger.info("%s" % prompt[1:])

        elif prompt.startswith(MSG_ERROR):
            logger.error(prompt[1:])
            raise DatabaseError(prompt[1:])

        elif prompt.startswith(MSG_REDIRECT):
            # a redirect can contain multiple redirects, for now we only use
            # the first
            redirect = prompt.split()[0][1:].split(':')
            if redirect[1] == "merovingian":
                logger.debug("restarting authentication")
                if iteration <= 10:
                    self._login(iteration=iteration + 1)
                else:
                    raise OperationalError("maximal number of redirects "
                                           "reached (10)")

            elif redirect[1] == "monetdb":
                self.hostname = redirect[2][2:]
                self.port, self.database = redirect[3].split('/')
                self.port = int(self.port)
                logger.info("redirect to monetdb://%s:%s/%s" %
                            (self.hostname, self.port, self.database))
                self.socket.close()
                self.connect(hostname=self.hostname, port=self.port,
                             username=self.username, password=self.password,
                             database=self.database, language=self.language)

            else:
                raise ProgrammingError("unknown redirect: %s" % prompt)

        else:
            raise ProgrammingError("unknown state: %s" % prompt)

    def disconnect(self):
        """ disconnect from the monetdb server """
        logger.info("disconnecting from database")
        self.state = STATE_INIT
        self.socket.close()

    def _sabotage(self):
        """ Kill the connection in a way that the server is sure to recognize as an error"""
        self.state = STATE_INIT
        if not self.socket:
            return
        bad_header = struct.pack('<H', 2 * 8193 + 0)  # too large, and not the final message
        bad_body = b"ERROR\x80ERROR"  # invalid utf-8
        try:
            self.socket.send(bad_header + bad_body)
            self.socket.close()
        except Exception:
            # whatever
            pass
        self.socket = None

    def cmd(self, operation):
        """ put a mapi command on the line"""
        logger.debug("executing command %s" % operation)

        if self.state != STATE_READY:
            raise ProgrammingError("Not connected")

        self._putblock(operation)
        response = self._getblock_and_transfer_files()
        if not len(response):
            return ""
        elif response.startswith(MSG_OK):
            return response[3:].strip() or ""
        if response == MSG_MORE:
            # tell server it isn't going to get more
            return self.cmd("")

        # If we are performing an update test for errors such as a failed
        # transaction.

        # We are splitting the response into lines and checking each one if it
        # starts with MSG_ERROR. If this is the case, find which line records
        # the error and use it to call handle_error.
        if response[:2] == MSG_QUPDATE:
            lines = response.split('\n')
            if any([line.startswith(MSG_ERROR) for line in lines]):
                index = next(i for i, v in enumerate(lines) if v.startswith(MSG_ERROR))
                exception, msg = handle_error(lines[index][1:])
                raise exception(msg)

        if response[0] in [MSG_Q, MSG_HEADER, MSG_TUPLE]:
            return response
        elif response[0] == MSG_ERROR:
            exception, msg = handle_error(response[1:])
            raise exception(msg)
        elif response[0] == MSG_INFO:
            logger.info("%s" % (response[1:]))
        elif self.language == 'control' and not self.hostname:
            if response.startswith("OK"):
                return response[2:].strip() or ""
            else:
                return response
        else:
            raise ProgrammingError("unknown state: %s" % response)

    def _challenge_response(self, challenge):
        """ generate a response to a mapi login challenge """

        challenges = challenge.split(':')
        if challenges[-1] != '' or len(challenges) < 7:
            raise OperationalError("Server sent invalid challenge")
        challenges.pop()

        salt, identity, protocol, hashes, endian = challenges[:5]
        password = self.password

        if protocol == '9':
            algo = challenges[5]
            try:
                h = hashlib.new(algo)
                h.update(password.encode())
                password = h.hexdigest()
            except ValueError as e:
                raise NotSupportedError(str(e))
        else:
            raise NotSupportedError("We only speak protocol v9")

        for h in hashes.split(","):
            try:
                s = hashlib.new(h)
            except ValueError:
                pass
            else:
                s.update(password.encode())
                s.update(salt.encode())
                pwhash = "{" + h + "}" + s.hexdigest()
                break
        else:
            raise NotSupportedError("Unsupported hash algorithms required"
                                    " for login: %s" % hashes)

        response = ":".join(["BIG", self.username, pwhash, self.language, self.database]) + ":"

        if len(challenges) >= 7:
            response += "FILETRANS:"
            options_level = 0
            for part in challenges[6].split(","):
                if part.startswith("sql="):
                    try:
                        options_level = int(part[4:])
                    except ValueError:
                        raise OperationalError("invalid sql options level in server challenge: " + part)
            options = []
            for opt in self.handshake_options:
                if opt.level < options_level:
                    options.append(opt.name + "=" + str(int(opt.value)))
                    opt.sent = True
            response += ",".join(options) + ":"

        return response

    def _getblock_and_transfer_files(self):
        """ read one mapi encoded block and take care of any file transfers the server requests"""
        prev = b''
        prev_end = 0
        # This loop only iterates more than once if
        # file transfers occur.
        while True:
            # preserve results from the previous iteration
            writer = BytesIO(memoryview(prev)[:prev_end])
            writer.seek(prev_end, SEEK_SET)
            self._getblock_raw(writer)
            buf = writer.getvalue()    # unavoidable, we cannot scan the buffer efficiently
            if not buf:
                break
            # a FILETRANSFER request looks like this:
            #     ... optional other contents (usually none) \n
            #     \1\3\n
            #     file transfer command\n
            # the i below is the position of the second-last \n,
            # separating the prompt from the file transfer command
            i = buf.rfind(b'\n', 0, len(buf) - 1)
            if i < 2:
                break
            if buf[i - 2:i + 1] != MSG_FILETRANS_B:
                break
            cmd = buf[i + 1:].decode()
            prev = buf
            prev_end = i - 2    # discard the file transfer prompt and command

            self._handle_transfer(cmd)

        return buf.decode()

    def _handle_transfer(self, cmd: str):
        if cmd.startswith("r "):
            parts = cmd[2:-1].split(' ', 2)
            if len(parts) == 2:
                try:
                    n = int(parts[0])
                except ValueError:
                    pass
                return self._handle_upload(parts[1], True, n)
        elif cmd.startswith("rb "):
            return self._handle_upload(cmd[2:-1], False, 0)
        elif cmd.startswith("w "):
            return self._handle_download(cmd[2:-1], True)
        elif cmd.startswith("wb "):
            return self._handle_download(cmd[3:-1], False)
        else:
            pass
        # we only reach this if decoding the cmd went wrong:
        self._putblock(f"Invalid file transfer command: {cmd!r}")

    def _handle_upload(self, filename, text_mode, offset):
        if not self.uploader:
            self._putblock("No upload handler has been registered with pymonetdb\n")
            return
        skip_amount = offset - 1 if offset > 0 else 0
        upload = Upload(self)
        try:
            try:
                self.uploader.handle_upload(upload, filename, text_mode, skip_amount)
            except Exception as e:
                # We must make sure the server doesn't think this is a succesful upload.
                # The protocol does not allow us to flag an error after the upload has started,
                # so the only thing we can do is kill the connection
                upload.error = True
                self._sabotage()
                raise e
            if not upload.has_been_used():
                raise ProgrammingError("Upload handler didn't do anything")
        finally:
            upload.close()

    def _handle_download(self, filename, text_mode):
        if not self.downloader:
            self._putblock("No download handler has been registered with pymonetdb\n")
            return
        download = Download(self)
        try:
            self.downloader.handle_download(download, filename, text_mode)
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
            self._sabotage()
            raise e
        finally:
            download.close()

    def _getblock(self):
        """ read one mapi encoded block """
        buf = BytesIO()
        self._getblock_raw(buf)
        return buf.getvalue().decode()

    def _getblock_raw(self, buffer: BytesIO):
        """ read one mapi encoded block and append it to the buf"""
        if self.language == 'control' and not self.hostname:
            self._getblock_socket(buffer)  # control doesn't do block splitting when using a socket
        else:
            self._getblock_inet(buffer)

    def _getblock_inet(self, buffer):
        last = 0
        while not last:
            flag_buf = BytesIO()
            self._getbytes(flag_buf, 2)
            flag = flag_buf.getvalue()
            unpacked = struct.unpack('<H', flag)[0]  # little endian short
            length = unpacked >> 1
            last = unpacked & 1
            self._getbytes(buffer, length)

    def _getblock_socket(self, buffer):
        while True:
            x = self.socket.recv(1)
            if len(x):
                buffer.write(x)
            else:
                break

    def _getbytes(self, buffer, count):
        """Read 'count' bytes from the socket into 'buffer'"""
        while count > 0:
            recv = self.socket.recv(count)
            if len(recv) == 0:
                raise BrokenPipeError("Server closed connection")
            count -= len(recv)
            buffer.write(recv)

    def _putblock(self, block):
        """ wrap the line in mapi format and put it into the socket """
        if self.language == 'control' and not self.hostname:
            return self.socket.send(block.encode())  # control doesn't do block splitting when using a socket
        else:
            self._putblock_inet_raw(block.encode(), True)

    def _putblock_raw(self, block, finish: bool):
        """ put the data into the socket """
        assert self.language != 'control' or self.hostname
        self._putblock_inet_raw(block, finish)

    def _putblock_inet_raw(self, block, finish):
        pos = 0
        last = 0
        while not last:
            data = memoryview(block)[pos:pos + MAX_PACKAGE_LENGTH]
            length = len(data)
            if length < MAX_PACKAGE_LENGTH:
                last = 1
            flag = struct.pack('<H', (length << 1) + (last if finish else 0))
            self.socket.send(flag)
            self.socket.send(data)
            pos += length

    def __del__(self):
        if self.socket:
            self.socket.close()

    def set_reply_size(self, size):
        # type: (int) -> None
        """
        Set the amount of rows returned by the server.

        args:
            size: The number of rows
        """

        self.cmd("Xreply_size %s" % size)

    def set_uploader(self, uploader):
        """Register the given Uploader, or None to deregister"""
        assert isinstance(uploader, Uploader)
        self.uploader = uploader

    def set_downloader(self, downloader):
        """Register the given Downloader, or None to deregister"""
        assert isinstance(downloader, Downloader)
        self.downloader = downloader


# When all supported Python versions support it we can enable @dataclass here.
class HandshakeOption:
    """
    Option that can be set during the MAPI handshake

    Should be sent as <name>=<val>, where <val> is `value` converted to int.
    The `level` is used to determine if the server supports this option.
    The `fallback` is a function-like object that can be called with the
    value (not converted to an integer) as a parameter.
    Field `sent` can be used to keep track of whether the option has been sent.
    """
    def __init__(self, level, name, fallback, value):
        self.level = level
        self.name = name
        self.value = value
        self.fallback = fallback
        self.sent = False


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
        the rest of the update. Defaults to 1 MiB.
        """
        self.chunk_size = size

    def send_error(self, message: str):
        """
        Tell the server the requested upload is refused
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
        assert self.mapi is not None
        if self.cancelled:
            return
        self._check_usable()
        if self.bytes_sent == 0:
            # send the magic newline indicating we're ok with the upload
            self._send(b'\n', False)
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


class UploadIO(BufferedIOBase):

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
    and all further data is ignored.
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
    Represents a request from the server to download daa from the server. It is
    passed to the Downloader registered by the application, which for example
    might write the data to a file on the client system. See
    pymonetdb.Connection.set_downloader().

    Use the method send_error() to refuse the download, binary_reader() to get a
    binary file object to read bytes from, or text_reader() to get a text-mode
    file object to read text from.

    Implementations should be EXTREMELY CAREFUL to validate the file name before
    opening any files on the client system!
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
            if length == 0:
                # valid but unlikely
                continue
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
    """Helper class used to normalize line ending before sending text to MonetDB."""

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

        return int(self.pending) + self.inner.write(normalized)

    def flush(self):
        return self.inner.flush()

    def close(self):
        if self.pending:
            self.inner.write(b"\r")
            self.pending = False
        return self.inner.close()