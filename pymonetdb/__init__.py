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

from pymonetdb.sql.connections import Connection
from pymonetdb.sql.pythonize import *
from pymonetdb.exceptions import *

try:
    __version__ = pkg_resources.require("pymonetdb")[0].version
except pkg_resources.DistributionNotFound:
    __version__ = "1.0rc"

apilevel = "2.0"
threadsafety = 0
paramstyle = "pyformat"

__all__ = ['sql', 'mapi', 'exceptions', 'BINARY', 'Binary', 'connect', 'Connection', 'DATE', 'Date', 'Time',
           'Timestamp', 'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
           'IntegrityError', 'InterfaceError', 'InternalError', 'NUMBER', 'NotSupportedError', 'OperationalError',
           'ProgrammingError', 'ROWID', 'STRING', 'TIME', 'Warning', 'apilevel', 'connect', 'paramstyle',
           'threadsafety']


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)


connect.__doc__ = Connection.__init__.__doc__
