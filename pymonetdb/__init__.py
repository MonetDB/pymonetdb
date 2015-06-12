# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2014 MonetDB B.V.
# All Rights Reserved.

"""
This is a MonetDB Python API.

To set up a connection use pymonetdb.connect()

"""
from pymonetdb import sql
from pymonetdb import mapi
from pymonetdb import exceptions

from pymonetdb.sql.connections import Connection
from pymonetdb.sql.pythonize import *
from pymonetdb.exceptions import *


__version__ = '0.1'
__all__ = ["sql", "mapi", "exceptions"]
apilevel = "2.0"
threadsafety = 0
paramstyle = "pyformat"

__all__ = ['BINARY', 'Binary', 'connect', 'Connection', 'DATE',
           'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
           'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
           'FIELD_TYPE', 'IntegrityError', 'InterfaceError', 'InternalError',
           'MySQLError', 'NULL', 'NUMBER', 'NotSupportedError', 'DBAPISet',
           'OperationalError', 'ProgrammingError', 'ROWID', 'STRING', 'TIME',
           'TIMESTAMP', 'Set', 'Warning', 'apilevel', 'connect',
           'connections', 'constants', 'cursors', 'debug', 'escape',
           'escape_dict', 'escape_sequence', 'escape_string',
           'get_client_info', 'paramstyle', 'string_literal', 'threadsafety',
           'version_info']


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)

connect.__doc__ = Connection.__init__.__doc__
