"""
This is the python implementation of the mapi protocol.
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


import os
import platform
import re
import socket
import logging
import struct
import hashlib
import ssl
import sys
import typing
from typing import Callable, Dict, List, Optional, Tuple, Union

# import pymonetdb
from pymonetdb.exceptions import OperationalError, DatabaseError, \
    ProgrammingError, NotSupportedError, IntegrityError
from pymonetdb.target import Target
from pymonetdb import __version__

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

    state: int = STATE_INIT
    target: Target = Target()
    socket: Optional[Union['socket.socket', ssl.SSLSocket]] = None
    is_tcp: Optional[bool] = None
    is_raw_control: Optional[bool] = None
    handshake_options_callback: Optional[Callable[[int], List['HandshakeOption']]] = None
    remaining_handshake_options: List['HandshakeOption'] = []
    clientinfo: Optional[Dict[str, Optional[str]]] = None
    uploader: Optional['Uploader'] = None
    downloader: Optional['Downloader'] = None
    stashed_buffer: Optional[bytearray] = None

    def connect(self, database: Optional[Union[Target, str]] = None, *args, **kwargs):  # noqa C901
        """ setup connection to MAPI server
        """

        # Ideally we'd just take the Target as a parameter, but we want to
        # provide some backward compatibility so we first deal with the legacy
        # arguments.
        callback = kwargs.get('handshake_options_callback')
        if callback is not None:
            self.handshake_options_callback = callback
            del kwargs['handshake_options_callback']

        # Create Target or use given
        if isinstance(database, Target):
            self.target = database.clone()
            assert not args and not kwargs
        else:
            self.target = construct_target_from_args(database, *args, **kwargs)

        # Validate the target parameters
        try:
            self.target.validate()
        except ValueError as e:
            raise DatabaseError(str(e))

        if self.target.connect_scan:
            self.scan_sockdir()
            return

        # Close any remainders of previous attempts
        if self.socket:
            try:
                self.socket.close()
            except OSError:
                pass
            self.socket = None

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Connecting to {self.target.summary_url()}")
        # Enter a loop to deal with redirects.
        try:
            self.connect_loop()
        except Exception as e:
            logger.error(f"Could not connect to {self.target.summary_url()}: {e}")
            raise
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Established connection to {self.target.summary_url()}")

        # We have a working connection now. Take care of the options we couldn't
        # handle during the handshake
        self.state = STATE_READY

        if self.clientinfo:
            lang = self.target.language
            if lang == 'sql':
                cmd = "Xclientinfo " + "".join(
                    f"{k}={v or ''}\n"
                    for k, v in self.clientinfo.items()
                    if v is None or '\n' not in v
                )
            elif lang == 'mal' or lang == 'msql':
                cmd = "\n".join(
                    f'clients.setinfo("{mal_escape(k)}", "{mal_escape(v or "")}");'
                    for k, v in self.clientinfo.items()
                    if v is None or '\n' not in v
                )
            else:
                cmd = None
            if cmd:
                try:
                    self.cmd(cmd)
                except OperationalError as e:
                    logger.warning(f"Server rejected clientinfo: {e}")

        for opt in self.remaining_handshake_options:
            opt.fallback(opt.value)

    def connect_loop(self):
        for i in range(10):
            # maybe the previous attempt left an open socket that just needs an
            # additional login attempt
            if self.socket is None:
                # No, we need to make a new connection
                self.try_connect()
                assert self.socket is not None

                # Once connected, deal with the file handle passing protocol,
                # AND with TLS. Note that these are necessarily exclusive, we
                # can't do TLS over unix domain sockets.
                self.is_raw_control = False
                if self.is_tcp:
                    self.prime_or_wrap_connection()
                elif self.target.language == 'control':
                    self.is_raw_control = True
                else:
                    # Send a '0' (0x48) to let the other side know we're not
                    # going to try to pass a file handle.
                    self.socket.sendall(b'0')

            # We have a connection now. Try to log in. If it succeeds, we're
            # done. If it fails, _login should either
            # 1) close the socket and update self.target for a new attempt, or
            # 2) leave the socket open for another login attempt.
            if self.is_raw_control:
                # no login needed, we're done
                break
            elif self._login():
                break
            else:
                # _login has determined that we need another round
                continue
        else:
            raise OperationalError("too many redirects")

    def try_connect(self):  # noqa C901
        err = None

        default_timeout = socket.getdefaulttimeout()
        if default_timeout == 0:
            raise ProgrammingError('Global socket timeout set to non-blocking, pymonetdb does not support that')
        t = self.target.connect_timeout
        if t == -1:
            # This is the only negative value allowed by Target.validate().
            # It means 'leave it alone'
            set_timeout = False
        else:
            set_timeout = True
            # Our settings and and socket.settimeout() assign different meanings to
            # value '0'
            custom_timeout = None if t == 0 else t

        sock = self.target.connect_unix
        if sock and hasattr(socket, 'AF_UNIX'):
            s = socket.socket(socket.AF_UNIX)
            try:
                if set_timeout:
                    s.settimeout(custom_timeout)
                s.connect(sock)
                # it worked!
                logger.debug(f"Connected to {sock}")
                if set_timeout:
                    s.settimeout(default_timeout)
                self.socket = s
                self.is_tcp = False
                return
            except OSError as e:
                s.close()
                err = e

        host = self.target.connect_tcp
        if host:
            port = self.target.connect_port
            addrs = socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
            for fam, typ, proto, cname, addr in addrs:
                s = socket.socket(fam, typ, proto)
                try:
                    if set_timeout:
                        s.settimeout(custom_timeout)
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    s.connect(addr)
                    # it worked!
                    logger.debug(f"Connected to {addr[0]} port {addr[1]}")
                    if set_timeout:
                        s.settimeout(default_timeout)
                    self.socket = s
                    self.is_tcp = True
                    return
                except OSError as e:
                    s.close()
                    err = e

        if err is not None:
            raise err
        raise DatabaseError("endpoint not found")

    def prime_or_wrap_connection(self):  # noqa: C901
        if not self.target.tls:
            # Prime the connection with some NUL bytes.
            # We expect the remote server to be a MAPI server, in which
            # case it will ignore them.
            # But if it is accidentally a TLS server, the NUL bytes tend
            # to force an error, avoiding a hang.
            # Also, unexpectedly, in some situations sending the NUL bytes
            # appear to make connection setup a little faster rather than slower.
            self.socket.sendall(b'\x00\x00\x00\x00\x00\x00\x00\x00')
            return

        target = self.target
        verification_mode = target.connect_tls_verify

        if target.dangerous_tls_nocheck:
            disabled_checks = set(target.dangerous_tls_nocheck.split(','))
        else:
            disabled_checks = set()

        # Set up the SSL context. How to do that depends on the verification mode
        if verification_mode == 'system':
            ssl_context = ssl.create_default_context()
        elif verification_mode == 'cert':
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.load_verify_locations(target.cert)
        else:
            assert verification_mode == 'hash'
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            disabled_checks.add('host')
            disabled_checks.add('cert')

        # The following is common between all verification modes
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
        ssl_context.set_alpn_protocols(["mapi/9"])
        if target.clientkey:
            certfile = (target.clientcert or target.clientkey)
            keyfile = target.clientkey
            ssl_context.load_cert_chain(certfile, keyfile)
        if 'host' in disabled_checks:
            ssl_context.check_hostname = False
        if 'cert' in disabled_checks:
            ssl_context.verify_mode = ssl.CERT_NONE

        # Perform the SSL handshake and switch to the encrypted connection
        self.socket = ssl_context.wrap_socket(self.socket, server_hostname=target.connect_tcp)

        # In hash mode, verify the identity of the server. Otherwise, just log a message about it
        if verification_mode == 'hash':
            self._verify_fingerprint(target.certhash)
            logger.debug(f"TLS certificate matches hash {target.certhash}")
        else:
            peercert = self.socket.getpeercert()
            if peercert:
                logger.debug("Valid TLS certificate")
            else:
                logger.debug("TLS certificate check was disabled")

    def _login(self) -> bool:  # noqa: C901
        """ Reads challenge from line, generate response and check if
        everything is okay """

        assert self.socket

        challenge = self._getblock()
        response = self._challenge_response(challenge)
        self._putblock(response)
        prompt = self._getblock().strip()

        if len(prompt) == 0 or prompt == MSG_OK:
            # server is happy
            return True
        elif prompt.startswith(MSG_INFO):
            # is this right?
            logger.info("%s" % prompt[1:])
            return True
        elif prompt.startswith(MSG_ERROR):
            logger.error(prompt[1:])
            raise DatabaseError(prompt[1:])
        elif prompt.startswith(MSG_REDIRECT):
            # a redirect can contain multiple redirects, we only use the first
            redirect = prompt.split('\n', 1)[0][1:]
            self._handle_redirect(redirect)
            return False
        else:
            raise ProgrammingError("unknown state: %s" % prompt)
        assert False and "unreachable"

    def _handle_redirect(self, redirect: str):
        if redirect.startswith('mapi:merovingian:'):
            if not redirect.startswith('mapi:merovingian://proxy'):
                raise DatabaseError(f"Invalid redirect: {redirect}")
            logger.debug("Local redirect, restarting authentication")
        else:
            logger.debug("Redirected to " + redirect)
            try:
                self.target.parse(redirect)
            except ValueError as e:
                raise DatabaseError(str(e))
            # close the socket so the next iteration will reconnect based on the
            # updated target.
            if self.socket:
                self.socket.close()
                self.socket = None

    def _verify_fingerprint(self, fingerprint: str):
        assert self.socket and isinstance(self.socket, ssl.SSLSocket)

        m = re.match(r'sha256:([0-9a-fA-F:]+)$', fingerprint)
        if not m:
            raise ssl.SSLError(f"invalid certificate hash {fingerprint!r}")
        digits = m.group(1).lower().replace(':', '')

        der = self.socket.getpeercert(binary_form=True)
        if not der:
            raise ssl.SSLError("server has no certificate")

        digest = hashlib.sha256(der).hexdigest()
        if not digest.startswith(digits):
            raise ssl.SSLError(f"wrong server certificate hash: {fingerprint}")

    def scan_sockdir(self):   # noqa C901
        try:
            my_uid = os.getuid()
        except AttributeError:
            # Windows
            my_uid = -1

        # Scan the sockdir and put candidate sockets in
        # my_socks if they are owned by me and strange_socks otherwise
        my_socks = []
        strange_socks = []
        prefix = self.target.sockprefix
        sockdir = self.target.connect_sockdir
        try:
            logger.debug(f"scanning {sockdir!r} for Unix domain sockets")
            entries = [e for e in os.scandir(sockdir) if e.name.startswith(prefix)]
        except OSError:
            entries = []
        for entry in entries:
            try:
                portno = int(entry.name[len(prefix):])
                if portno < 1 or portno > 65535:
                    continue
            except ValueError:
                continue

            try:
                st = entry.stat()
            except OSError:
                continue
            if st.st_uid == my_uid:
                my_socks.append(entry.path)
            else:
                strange_socks.append(entry.path)

        # Try to connect to each of them
        for sock in my_socks + strange_socks:

            self.target.sock = sock
            try:
                logger.debug(f"Trying {sock!r}")
                self.connect(self.target)
                # If it works, use this
                return
            except Exception:
                pass
        self.target.sock = ''

        # last resort
        logger.debug("Trying a TCP connection to localhost")
        self.target.host = 'localhost'
        self.connect(self.target)

    def disconnect(self):
        """ disconnect from the monetdb server """
        logger.info("Closing connection")
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
        elif self.is_raw_control:
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

    def _challenge_response(self, challenge: str):  # noqa: C901
        """ generate a response to a mapi login challenge """

        challenges = challenge.split(':')
        if challenges[-1] != '' or len(challenges) < 7:
            raise OperationalError("Server sent invalid challenge")
        challenges.pop()

        salt, server_type, protocol, hashes, endian = challenges[:5]

        if server_type == 'merovingian' and self.target.language != 'control':
            # we want to be forwarded, hide real credentials
            user = 'merovingian'
            password = ''
        else:
            user = self.target.user or ''
            password = self.target.password or ''

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

        response = ":".join([
            "BIG",
            user,
            pwhash,
            self.target.language,
            self.target.database or ''
        ]) + ":"

        self.binexport_level = 0
        if len(challenges) >= 8:
            part = challenges[7]
            assert part.startswith('BINARY=')
            self.binexport_level = int(part[7:])

        if len(challenges) >= 10:
            part = challenges[9]
            assert part == "CLIENTINFO"
            if self.target.client_info:
                application_name = self.target.client_application
                if not application_name and sys.argv:
                    application_name = os.path.basename(sys.argv[0])
                self.clientinfo = dict(
                    ClientHostname=platform.node() or None,
                    ApplicationName=application_name or None,
                    ClientLibrary=f"pymonetdb {__version__}",
                    ClientRemark=self.target.client_remark or None,
                    ClientPid=str(os.getpid()),
                )

        callback = self.handshake_options_callback
        handshake_options = callback(self.binexport_level) if callback else []

        response += "FILETRANS:"
        if len(challenges) >= 7:
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
        if self.is_raw_control:
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
        if self.is_raw_control:
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
        if self.is_raw_control:
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


def mal_escape(s):
    mapping = {'\n': r'\n', '\t': r'\t', '"': r'\"', '\\': r'\\'}
    return "".join(mapping.get(c, c) for c in s)


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


def construct_target_from_args(database: Optional[str], username: str, password: str, language: str,  # noqa: C901
                               hostname: Optional[str] = None, port: Optional[int] = None, unix_socket: Optional[str] = None,
                               connect_timeout: Optional[Union[float, int]] = None,
                               **kwargs):
    """Construct a Target from the other args"""

    target = Target()

    if database is not None:
        assert isinstance(database, str)
        target.database = database
    if username is not None:
        target.user = username
    if password is not None:
        target.password = password
    if language is not None:
        target.language = language
    if hostname is not None:
        target.host = hostname
    if port is not None:
        target.port = port
    if unix_socket is not None:
        target.sock = unix_socket
    if connect_timeout is not None:
        target.connect_timeout = connect_timeout
    for key, value in kwargs.items():
        try:
            target.set(key, value)
        except ValueError as e:
            raise DatabaseError(f"{key}={value}: {e}")

    return target
