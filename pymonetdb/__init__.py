"""
This is a MonetDB Python API.

To set up a connection use pymonetdb.connect()

"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import pkg_resources
from pymonetdb import sql
from pymonetdb import mapi
from pymonetdb import exceptions

from pymonetdb.profiler import ProfilerConnection
from pymonetdb.sql.connections import Connection
from pymonetdb.sql.pythonize import BINARY, Binary, DATE, Date, Time, Timestamp, DateFromTicks, TimestampFromTicks, \
    TimeFromTicks, NUMBER, ROWID, STRING, TIME, types, DATETIME, TimeTzFromTicks, TimestampTzFromTicks
from pymonetdb.exceptions import Error, DataError, DatabaseError, IntegrityError, InterfaceError, InternalError, \
    NotSupportedError, OperationalError, ProgrammingError, Warning
from pymonetdb.filetransfer.downloads import Download, Downloader
from pymonetdb.filetransfer.uploads import Upload, Uploader
from pymonetdb.filetransfer.directoryhandler import SafeDirectoryHandler

try:
    __version__ = pkg_resources.require("pymonetdb")[0].version
except pkg_resources.DistributionNotFound:
    __version__ = "1.0rc"

apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"

__all__ = ['sql', 'mapi', 'exceptions', 'BINARY', 'Binary', 'connect', 'Connection', 'DATE', 'Date', 'Time',
           'Timestamp', 'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
           'IntegrityError', 'InterfaceError', 'InternalError', 'NUMBER', 'NotSupportedError', 'OperationalError',
           'ProgrammingError', 'ROWID', 'STRING', 'TIME', 'Warning', 'apilevel', 'connect', 'paramstyle',
           'threadsafety', 'Download', 'Downloader', 'Upload', 'Uploader', 'SafeDirectoryHandler', 'types', 'DATETIME',
           'TimeTzFromTicks', 'TimestampTzFromTicks']


def connect(*args, **kwargs) -> Connection:
    """ Set up a connection to a MonetDB SQL database.

    database (str)
        name of the database, or MAPI URI (see below)
    hostname (str)
        Hostname where MonetDB is running
    port (int)
        port to connect to (default: 50000)
    username (str)
        username for connection (default: "monetdb")
    password (str)
        password for connection (default: "monetdb")
    unix_socket (str)
        socket to connect to. used when hostname not set (default: "/tmp/.s.monetdb.50000")
    autocommit (bool)
        enable/disable auto commit (default: false)
    connect_timeout (int)
        the socket timeout while connecting
    binary (int)
        enable binary result sets when possible if > 0 (default: 1)
    replysize(int)
        number of rows to retrieve immediately after query execution (default: 100, -1 means everything)
    maxprefetch(int)
        max. number of rows to prefetch during Cursor.fetchone() or Cursor.fetchmany()
    use_tls (bool)
        whether to secure (encrypt) the connection
    server_cert (str)
        optional path to TLS certificate to verify the server with
    client_key (str)
        optional path to TLS key to present to server for authentication
    client_cert (str)
        optional path to TLS cert to present to server for authentication.
        the certificate file can also be appended to the key file.
    client_key_password (str)
        optional password to decrypt client_key with
    server_fingerprint (str)
        if given, only verify that server certificate has this fingerprint, implies dangerous_tls_nocheck=host,cert.
        format: {hashname}hexdigits,{hashname}hexdigits,... hashname defaults to sha1
    dangerous_tls_nocheck (str)
        optional comma separated list of security checks to disable. possible values: 'host' and 'cert'

    **MAPI URI Syntax**:

    tcp socket
        mapi:monetdb://[<username>[:<password>]@]<host>[:<port>]/<database>
    unix domain socket
        mapi:monetdb:///[<username>[:<password>]@]path/to/socket?database=<database>
    """

    return Connection(*args, **kwargs)


def profiler_connection(*args, **kwargs):
    c = ProfilerConnection()
    c.connect(*args, **kwargs)
    return c


profiler_connection.__doc__ = ProfilerConnection.__init__.__doc__
