"""
This is a MonetDB Python API.

To set up a connection use pymonetdb.connect()

"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

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

__version__ = '1.7.1'

apilevel = "2.0"
threadsafety = 1

paramstyle = "pyformat"   # with sufficiently recent MonetDB versions you can override this to 'named'

__all__ = ['sql', 'mapi', 'exceptions', 'BINARY', 'Binary', 'connect', 'Connection', 'DATE', 'Date', 'Time',
           'Timestamp', 'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
           'IntegrityError', 'InterfaceError', 'InternalError', 'NUMBER', 'NotSupportedError', 'OperationalError',
           'ProgrammingError', 'ROWID', 'STRING', 'TIME', 'Warning', 'apilevel', 'connect', 'paramstyle',
           'threadsafety', 'Download', 'Downloader', 'Upload', 'Uploader', 'SafeDirectoryHandler', 'types', 'DATETIME',
           'TimeTzFromTicks', 'TimestampTzFromTicks']


def connect(*args, **kwargs) -> Connection:
    return Connection(*args, **kwargs)


def profiler_connection(*args, **kwargs):
    c = ProfilerConnection()
    c.connect(*args, **kwargs)
    return c


connect.__doc__ = Connection.__init__.__doc__
profiler_connection.__doc__ = ProfilerConnection.__init__.__doc__
