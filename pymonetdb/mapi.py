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
from typing import Optional
from io import BytesIO
from urllib.parse import urlparse, parse_qs

from pymonetdb.exceptions import OperationalError, DatabaseError, \
    ProgrammingError, NotSupportedError, IntegrityError

logger = logging.getLogger(__name__)

MAX_PACKAGE_LENGTH = (1024 * 8) - 2

MSG_PROMPT = ""
MSG_MORE = "\1\2\n"
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

    def cmd(self, operation):
        """ put a mapi command on the line"""
        logger.debug("executing command %s" % operation)

        if self.state != STATE_READY:
            raise (ProgrammingError, "Not connected")

        self._putblock(operation)
        response = self._getblock()
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

    def _getblock(self):
        """ read one mapi encoded block """
        if self.language == 'control' and not self.hostname:
            return self._getblock_socket()  # control doesn't do block splitting when using a socket
        else:
            return self._getblock_inet()

    def _getblock_inet(self):
        result = BytesIO()
        last = 0
        while not last:
            flag = self._getbytes(2)
            unpacked = struct.unpack('<H', flag)[0]  # little endian short
            length = unpacked >> 1
            last = unpacked & 1
            result.write(self._getbytes(length))
        return result.getvalue().decode()

    def _getblock_socket(self):
        buffer = BytesIO()
        while True:
            x = self.socket.recv(1)
            if len(x):
                buffer.write(x)
            else:
                break
        return buffer.getvalue().strip().decode()

    def _getbytes(self, bytes_):
        """Read an amount of bytes from the socket"""
        result = BytesIO()
        count = bytes_
        while count > 0:
            recv = self.socket.recv(count)
            if len(recv) == 0:
                raise BrokenPipeError("Server closed connection")
            count -= len(recv)
            result.write(recv)
        return result.getvalue()

    def _putblock(self, block):
        """ wrap the line in mapi format and put it into the socket """
        if self.language == 'control' and not self.hostname:
            return self.socket.send(block.encode())  # control doesn't do block splitting when using a socket
        else:
            self._putblock_inet(block)

    def _putblock_inet(self, block):
        pos = 0
        last = 0
        block = block.encode()
        while not last:
            data = block[pos:pos + MAX_PACKAGE_LENGTH]
            length = len(data)
            if length < MAX_PACKAGE_LENGTH:
                last = 1
            flag = struct.pack('<H', (length << 1) + last)
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
