"""
This is a MonetDB Python API.

To set up a connection use pymonetdb.connect()

"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

# Set __version__ first, so the imported modules can access it.
__version__ = '1.8.2'


from typing import Optional
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
from pymonetdb.target import Target, looks_like_url


apilevel = "2.0"
threadsafety = 1

paramstyle = "pyformat"   # with sufficiently recent MonetDB versions you can override this to 'named'

__all__ = ['sql', 'mapi', 'exceptions', 'BINARY', 'Binary', 'connect', 'Connection', 'DATE', 'Date', 'Time',
           'Timestamp', 'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
           'IntegrityError', 'InterfaceError', 'InternalError', 'NUMBER', 'NotSupportedError', 'OperationalError',
           'ProgrammingError', 'ROWID', 'STRING', 'TIME', 'Warning', 'apilevel', 'connect', 'paramstyle',
           'threadsafety', 'Download', 'Downloader', 'Upload', 'Uploader', 'SafeDirectoryHandler', 'types', 'DATETIME',
           'TimeTzFromTicks', 'TimestampTzFromTicks']


def connect(    # noqa C901
        database: str,
        hostname: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        unix_socket: Optional[str] = None,
        autocommit: Optional[bool] = None,
        host: Optional[str] = None,
        user: Optional[str] = None,
        connect_timeout: Optional[int] = None,
        binary: Optional[int] = None,
        replysize: Optional[str] = None,
        maxprefetch: Optional[str] = None,
        *,
        sock: Optional[str] = None,
        sockdir: Optional[str] = None,
        tls: Optional[bool] = None,
        cert: Optional[str] = None,
        certhash: Optional[str] = None,
        clientkey: Optional[str] = None,
        clientcert: Optional[str] = None,
        schema: Optional[str] = None,
        timezone: Optional[int] = None,
        client_info: Optional[bool] = None,
        client_application: Optional[str] = None,
        client_remark: Optional[str] = None,
        dangerous_tls_nocheck: Optional[str] = None,
):
    """Set up a connection to a MonetDB SQL database

    Can connect using TCP or using a Unix Domain socket.

    database : str
        name of the database, or a
        `MonetDB URL <https://www.monetdb.org/documentation/user-guide/client-interfaces/monetdb-urls/>`_
    hostname : str
        host name to make TCP connection to
    port : int
        port number to connect to (default: 50000), also used to
        construct default Unix Domain socket name
    username : str
        user name to authenticate as (default: monetdb)
    password : str
        password to authenticate with (default: monetdb)
    unix_socket : str
        Unix Domain socket to connect to when host name is not set
        (default: /tmp/.s.monetdb.PORTNUMBER)
    autocommit : bool
        enable/disable auto commit (default: false)
    host : str
        alias for hostname
    user : str
        alias for username
    connect_timeout : float
        socket timeout when connecting, in seconds, 0=block indefinitely,
        -1=system default which usually also is 'block indefinitely'
        (default: -1)
    binary : int
        enable binary result sets when possible if > 0 (default: 1)
    replysize : str
        number of rows to retrieve immediately after query execution,
        the rest will be fetched on demand (default: 100, -1 means everything)
    maxprefetch : str
        max. number of additional rows to fetch during Cursor.fetchone() or Cursor.fetchmany()
    sock : str
        alias for unix_socket
    sockdir: str
        directory where Unix Domain sockets are searched (default: /tmp)
    tls : bool
        whether to secure (encrypt) the connection
    cert : str
        optional path to TLS certificate to verify the server with
    certhash : str
        if given, only verify that server certificate has this fingerprint, implies dangerous_tls_nocheck=host,cert.
        format: sha256:digits
    clientkey : str
        optional path to TLS key to present to server for authentication
    clientcert : str
        optional path to TLS cert to present to server for authentication,
        this can also be appended to the key file.
    schema : str
        the schema to select after connecting
    timezone : int
        the time zone to use, in minutes east of UTC
    client_info : bool
        whether to send client details when connecting
    client_application : str
        application name to send in the client details
    client_remark : str
        additional info to send in the client details
    dangerous_tls_nocheck : str
        comma-separated list of TLS certificate checks to skip during connecting:
        'host': ignore host name mismatch,
        'cert': ignore bad certificate chain
    """

    target = Target()

    if hostname is not None:
        target.host = hostname
    if port is not None:
        target.port = port
    if username is not None:
        target.user = username
    if password is not None:
        target.password = password
    if unix_socket is not None:
        target.sock = unix_socket
    if autocommit is not None:
        target.autocommit = autocommit
    if host is not None:
        target.host = host
    if user is not None:
        target.user = user
    if connect_timeout is not None:
        target.connect_timeout = connect_timeout
    if binary is not None:
        target.binary = binary
    if replysize is not None:
        target.replysize = replysize
    if maxprefetch is not None:
        target.maxprefetch = maxprefetch
    if sock is not None:
        target.sock = sock
    if sockdir is not None:
        target.sockdir = sockdir
    if tls is not None:
        target.tls = tls
    if cert is not None:
        target.cert = cert
    if certhash is not None:
        target.certhash = certhash
    if clientkey is not None:
        target.clientkey = clientkey
    if clientcert is not None:
        target.clientcert = clientcert
    if schema is not None:
        target.schema = schema
    if timezone is not None:
        target.timezone = timezone
    if client_info is not None:
        target.client_info = client_info
    if client_application is not None:
        target.client_application = client_application
    if client_remark is not None:
        target.client_remark = client_remark
    if dangerous_tls_nocheck is not None:
        target.dangerous_tls_nocheck = dangerous_tls_nocheck

    if looks_like_url(database):
        target.boundary()
        target.parse(database)
    else:
        target.database = database

    return Connection(target)


def profiler_connection(*args, **kwargs):
    c = ProfilerConnection()
    c.connect(*args, **kwargs)
    return c


profiler_connection.__doc__ = ProfilerConnection.__init__.__doc__
