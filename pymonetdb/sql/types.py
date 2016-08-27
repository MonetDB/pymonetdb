# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

"""
definition of MonetDB column types, for more info:
http://www.monetdb.org/Documentation/Manuals/SQLreference/Datatypes
"""

CHAR = 'char'                      # (L) character string with length L
VARCHAR = 'varchar'                # (L) string with atmost length L
CLOB = 'clob'
BLOB = 'blob'

TINYINT = 'tinyint'                # 8 bit integer
SMALLINT = 'smallint'              # 16 bit integer
INT = 'int'                        # 32 bit integer
BIGINT = 'bigint'                  # 64 bit integer
HUGEINT = 'hugeint'                # 128 bit integer
REAL = 'real'                      # 32 bit floating point
DOUBLE = 'double'                  # 64 bit floating point
DECIMAL = 'decimal'                # (P,S)
BOOLEAN = 'boolean'

DATE = 'date'
TIME = 'time'                      # (T) time of day
TIMESTAMP = 'timestamp'            # (T) date concatenated with unique time
MONTH_INTERVAL = 'month_interval'
SEC_INTERVAL = 'sec_interval'

URL = 'url'
INET = 'inet'
UUID = 'uuid'
JSON = 'json'
GEOMETRY = 'geometry'
GEOMETRYA = 'geometrya'

# Not on the website:
OID = 'oid'
WRD = 'wrd'                        # 64 bit integer
TIMESTAMPTZ = 'timestamptz'        # with timezone
TIMETZ = 'timetz'                  # with timezone
