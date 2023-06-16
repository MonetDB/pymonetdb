"""
This is the python implementation of the mapi protocol.
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


import socket
import logging
import struct
import hashlib
import os
import typing
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlparse

from pymonetdb.exceptions import OperationalError, DatabaseError, \
    ProgrammingError, NotSupportedError, IntegrityError

if typing.TYPE_CHECKING:
    from pymonetdb.filetransfer.downloads import Downloader
    from pymonetdb.filetransfer.uploads import Uploader

logger = logging.getLogger(__name__)

MAX_PACKAGE_LENGTH = (1024 * 8) - 2

MSG_PROMPT = ""
MSG_MORE = "\1\2\n"
MSG_FILETRANS = "\1\3\n"
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

MSG_ERROR_B = bytes(MSG_ERROR, 'ascii')
MSG_FILETRANS_B = bytes(MSG_FILETRANS, 'ascii')

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

    socket: Optional[socket.socket]

    def __init__(self):
        self.state = STATE_INIT
        self._result = None
        self.socket = None
        self.unix_socket = None
        self.hostname = ""
        self.port = 0
        self.username = ""
        self.database = ""
        self.language = ""
        self.handshake_options_callback = None
        self.remaining_handshake_options = []
        self.connect_timeout = socket.getdefaulttimeout()
        self.uploader = None
        self.downloader = None
        self.stashed_buffer = None

    def connect(self, database: str, username: str, password: str, language: str,  # noqa: C901
                hostname: Optional[str] = None, port: Optional[int] = None, unix_socket=None, connect_timeout=-1,
                handshake_options_callback: Callable[[bool], List['HandshakeOption']] = lambda x: []):
        """ setup connection to MAPI server

        unix_socket is used if hostname is not defined.
        """

        url_options = {}
        if ':' in database:
            if not database.startswith('mapi:monetdb:'):
                raise DatabaseError("colon not allowed in database name, except as part of "
                                    "mapi:monetdb://<hostname>[:<port>]/<database> URI")
            parsed = urlparse(database[5:])
            url_options = dict(parse_qsl(parsed.query))
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
            if 'database' in url_options:
                if database == '':
                    database = url_options['database']
                else:
                    raise DatabaseError('database= query parameter is only allowed with unix domain sockets')

        if hostname and hostname[:1] == '/' and not unix_socket:
            unix_socket = f'{hostname}/.s.monetdb.{port}'
            hostname = None
        if not unix_socket and os.path.exists(f"/tmp/.s.monetdb.{port}"):
            unix_socket = f"/tmp/.s.monetdb.{port}"
        elif not unix_socket and not hostname:
            hostname = 'localhost'

        # None and zero are allowed values
        if connect_timeout != -1:
            assert connect_timeout is None or connect_timeout >= 0
            self.connect_timeout = connect_timeout

        self.hostname = hostname
        self.port = port
        self.username = username
        self.database = database
        self.language = language
        self.unix_socket = unix_socket
        self.handshake_options_callback = handshake_options_callback
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
            self._login(password=password, url_options=url_options)

        self.socket.settimeout(socket.getdefaulttimeout())
        self.state = STATE_READY

        for opt in self.remaining_handshake_options:
            opt.fallback(opt.value)

    def _login(self, password: str, url_options: Dict[str, str], iteration=0):
        """ Reads challenge from line, generate response and check if
        everything is okay """

        assert self.socket
        self.socket.sendall(b'\x00\x00\x00\x00\x00\x00\x00\x00')

        challenge = self._getblock()
        response = self._challenge_response(challenge, password, url_options)
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
                    self._login(iteration=iteration + 1, password=password, url_options={})
                else:
                    raise OperationalError("maximal number of redirects "
                                           "reached (10)")

            elif redirect[1] == "monetdb":
                self.hostname = redirect[2][2:]
                self.port, self.database = redirect[3].split('/')
                self.port = int(self.port)
                logger.info("redirect to monetdb://%s:%s/%s" %
                            (self.hostname, self.port, self.database))
                if self.socket:
                    self.socket.close()
                self.connect(hostname=self.hostname, port=self.port,
                             username=self.username, password=password,
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
        sock = self.socket
        self.socket = None
        self.state = STATE_INIT
        if not sock:
            return
        bad_header = struct.pack('<H', 2 * 8193 + 0)  # larger than allowed, and not the final message
        bad_body = b"ERROR\x80ERROR"  # invalid utf-8, and too small
        try:
            sock.send(bad_header + bad_body)
            # and then we hang up
            sock.close()
        except Exception:
            # don't care
            pass

    def cmd(self, operation: str):  # noqa: C901
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

    def binary_cmd(self, operation: str) -> memoryview:
        """ put a mapi command on the line, with a binary response.

        returns a memoryview that can only be used until the next
        operation on this Connection object.
        """
        logger.debug("executing binary command %s" % operation)

        if self.state != STATE_READY:
            raise ProgrammingError("Not connected")

        self._putblock(operation)
        buffer = self._get_buffer()
        n = self._getblock_raw(buffer, 0)
        view = memoryview(buffer)[:n]
        self._stash_buffer(buffer)

        # Handle !Error message
        if view[0:len(MSG_ERROR_B)] == MSG_ERROR_B:
            msg_bytes = bytes(view)
            idx = msg_bytes.find(b'\n')
            if idx > 0:
                msg_bytes = msg_bytes[1:idx + 1]
            exception, msg = handle_error(str(msg_bytes, 'utf-8'))
            raise exception(msg)

        return view

    def _challenge_response(self, challenge: str, password: str, url_options: Dict[str, str]):  # noqa: C901
        """ generate a response to a mapi login challenge """

        challenges = challenge.split(':')
        if challenges[-1] != '' or len(challenges) < 7:
            raise OperationalError("Server sent invalid challenge")
        challenges.pop()

        salt, identity, protocol, hashes, endian = challenges[:5]

        if endian == 'LIT':
            self.server_endian = 'little'
        elif endian == 'BIG':
            self.server_endian = 'big'
        else:
            raise NotSupportedError('Unknown byte order: ' + endian)

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

        for i in hashes.split(","):
            try:
                s = hashlib.new(i)
            except ValueError:
                pass
            else:
                s.update(password.encode())
                s.update(salt.encode())
                pwhash = "{" + i + "}" + s.hexdigest()
                break
        else:
            raise NotSupportedError("Unsupported hash algorithms required"
                                    " for login: %s" % hashes)

        response = ":".join(["BIG", self.username, pwhash, self.language, self.database]) + ":"

        self.binexport_level = 0
        if len(challenges) >= 8:
            part = challenges[7]
            assert part.startswith('BINARY=')
            self.binexport_level = int(part[7:])

        handshake_options = self.handshake_options_callback(self.binexport_level)

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
            for opt in handshake_options:
                if opt.level < options_level:
                    options.append(opt.name + "=" + str(int(opt.value)))
                    opt.sent = True
            response += ",".join(options) + ":"

        self.remaining_handshake_options = [opt for opt in handshake_options if not opt.sent]

        return response

    def _getblock_and_transfer_files(self) -> str:
        """ read one mapi encoded block and take care of any file transfers the server requests"""
        if self.language == 'control' and not self.hostname:
            # control connections do not use the blocking protocol and do not transfer files
            return self._recv_to_end()

        buffer = self._get_buffer()
        offset = 0

        # import this here to solve circular import
        from pymonetdb.filetransfer import handle_file_transfer

        while True:
            old_offset = offset
            offset = self._getblock_raw(buffer, old_offset)
            i = buffer.rfind(b'\n', old_offset, offset - 1)
            if i >= old_offset + 2 and buffer[i - 2: i + 1] == MSG_FILETRANS_B:
                # File transfer request. Chop the cmd off the buffer by lowering the offset
                cmd = str(buffer[i + 1: offset - 1], 'utf-8')
                offset = i - 2
                handle_file_transfer(self, cmd)
                continue
            else:
                break
        self._stash_buffer(buffer)
        return str(memoryview(buffer)[:offset], 'utf-8')

    def _getblock(self) -> str:
        """ read one mapi encoded block """
        if self.language == 'control' and not self.hostname:
            # control connections do not use the blocking protocol
            return self._recv_to_end()
        buf = self._get_buffer()
        end = self._getblock_raw(buf, 0)
        ret = str(memoryview(buf)[:end], 'utf-8')
        self._stash_buffer(buf)
        return ret

    def _getblock_raw(self, buffer: bytearray, offset: int) -> int:
        """
        Read one mapi block into 'buffer' starting at 'offset', enlarging the buffer
        as necessary and returning offset plus the number of bytes read.
        """
        last = False
        while not last:
            offset, last = self._get_minor_block(buffer, offset)
        return offset

    def _get_minor_block(self, buffer: bytearray, offset: int) -> Tuple[int, bool]:
        self._getbytes(buffer, offset, 2)
        unpacked = buffer[offset] + 256 * buffer[offset + 1]
        length = unpacked >> 1
        last = unpacked & 1
        if length:
            offset = self._getbytes(buffer, offset, length)
        return (offset, bool(last))

    def _getbytes(self, buffer: bytearray, offset: int, count: int) -> int:
        """
        Read 'count' bytes from the socket into 'buffer' starting at 'offset'.
        Enlarge buffer if necessary.
        Return offset + count if all goes well.
        """
        assert self.socket
        end = count + offset
        if len(buffer) < end:
            # enlarge
            nblocks = 1 + (end - len(buffer)) // 8192
            buffer += bytes(nblocks * 8192)
        while offset < end:
            view = memoryview(buffer)[offset:end]
            n = self.socket.recv_into(view)
            if n == 0:
                raise BrokenPipeError("Server closed connection")
            offset += n
        return end

    def _recv_to_end(self) -> str:
        """
        Read bytes from the socket until the server closes the connection
        """
        parts = []
        while True:
            assert self.socket
            received = self.socket.recv(4096)
            if not received:
                break
            parts.append(received)
        return str(b"".join(parts).strip(), 'utf-8')

    def _get_buffer(self) -> bytearray:
        """Retrieve a previously stashed buffer for reuse, or create a new one"""
        if self.stashed_buffer:
            buffer = self.stashed_buffer
            self.stashed_buffer = None
        else:
            buffer = bytearray(8192)
        return buffer

    def _stash_buffer(self, buffer):
        """Stash a used buffer for future reuse"""
        if self.stashed_buffer is None or len(self.stashed_buffer) < len(buffer):
            self.stashed_buffer = buffer

    def _putblock(self, block):
        """ wrap the line in mapi format and put it into the socket """
        data = block.encode('utf-8')
        if self.language == 'control' and not self.hostname:
            # control does not use the blocking protocol
            return self._send_all_and_shutdown(data)
        else:
            self._putblock_raw(block.encode(), True)

    def _putblock_raw(self, block, finish):
        """ put the data into the socket """
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

    def _send_all_and_shutdown(self, block):
        """ put the data into the socket """
        pos = 0
        end = len(block)
        block = memoryview(block)
        while pos < end:
            data = block[pos:pos + 8192]
            nsent = self.socket.send(data)
            pos += nsent
        try:
            self.socket.shutdown(socket.SHUT_WR)
        except OSError:
            pass

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

    def set_uploader(self, uploader: "Uploader"):
        """Register the given Uploader, or None to deregister"""
        self.uploader = uploader

    def set_downloader(self, downloader: "Downloader"):
        """Register the given Downloader, or None to deregister"""
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


def mapi_url_options(possible_mapi_url: str) -> Dict[str, str]:
    """Try to parse the argument as a MAPI URL and return a Dict of url options

    Return empty dict if it's not a MAPI URL.
    """
    if not possible_mapi_url.startswith('mapi:monetdb:'):
        return {}
    url = possible_mapi_url[5:]
    return dict(parse_qsl(urlparse(url).query))
