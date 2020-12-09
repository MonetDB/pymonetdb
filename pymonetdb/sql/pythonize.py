# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

"""
functions for converting monetdb SQL fields to Python objects
"""

import json
import time
import datetime
import re
import uuid
from decimal import Decimal
from datetime import timedelta

from pymonetdb.sql import types
from pymonetdb.exceptions import ProgrammingError


def _extract_timezone(data):
    sign_symbol = data[-6]

    if sign_symbol == '+':
        sign = 1
    elif sign_symbol == '-':
        sign = -1
    else:
        raise ProgrammingError("no + or - in %s" % data)

    return data[:-6], datetime.timedelta(hours=sign * int(data[-5:-3]),
                                         minutes=sign * int(data[-2:]))


def strip(data):
    """ returns a python string, with chopped off quotes,
    and replaced escape characters"""
    return ''.join([w.encode('utf-8').decode('unicode_escape')
                    if '\\' in w else w
                    for w in re.split('([\000-\200]+)', data[1:-1])])


def py_bool(data):
    """ return python boolean """
    return data == "true"


def py_time(data):
    """ returns a python Time
    """
    if '.' in data:
        return datetime.datetime.strptime(data, '%H:%M:%S.%f').time()
    else:
        return datetime.datetime.strptime(data, '%H:%M:%S').time()


def py_timetz(data):
    """ returns a python Time where data contains a tz code
    """
    t, timezone_delta = _extract_timezone(data)
    if '.' in t:
        return (datetime.datetime.strptime(t, '%H:%M:%S.%f') + timezone_delta).time()
    else:
        return (datetime.datetime.strptime(t, '%H:%M:%S') + timezone_delta).time()


def py_date(data):
    """ Returns a python Date
    """
    return datetime.datetime.strptime(data, '%Y-%m-%d').date()


def py_timestamp(data):
    """ Returns a python Timestamp
    """
    if '.' in data:
        return datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S.%f')
    else:
        return datetime.datetime.strptime(data, '%Y-%m-%d %H:%M:%S')


def py_timestamptz(data):
    """ Returns a python Timestamp where data contains a tz code
    """
    dt, timezone_delta = _extract_timezone(data)
    if '.' in dt:
        return datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S.%f') + timezone_delta
    else:
        return datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S') + timezone_delta


def py_sec_interval(data: str) -> timedelta:
    """ Returns a python TimeDelta where data represents a value of MonetDB's INTERVAL SECOND type
    which resembles a stringified decimal.
    """
    return timedelta(seconds=int(Decimal(data)))


def py_day_interval(data: str) -> int:
    """ Returns a python number of days where data represents a value of MonetDB's INTERVAL DAY type
    which resembles a stringified decimal.
    """
    return timedelta(seconds=int(Decimal(data))).days


def py_bytes(data):
    """Returns a bytes (py3) or string (py2) object representing the input blob."""
    return Binary(data)


def oid(data):
    """represents an object identifier

    For now we will just return the string representation just like mclient does.
    """
    return oid


mapping = {
    types.CHAR: strip,
    types.VARCHAR: strip,
    types.CLOB: strip,
    types.BLOB: py_bytes,
    types.TINYINT: int,
    types.SMALLINT: int,
    types.INT: int,
    types.BIGINT: int,
    types.HUGEINT: int,
    types.SERIAL: int,
    types.SHORTINT: int,
    types.MEDIUMINT: int,
    types.LONGINT: int,
    types.OID: oid,
    types.WRD: int,
    types.REAL: float,
    types.FLOAT: float,
    types.DOUBLE: float,
    types.DECIMAL: Decimal,
    types.BOOLEAN: py_bool,
    types.DATE: py_date,
    types.TIME: py_time,
    types.TIMESTAMP: py_timestamp,
    types.TIMETZ: py_timetz,
    types.TIMESTAMPTZ: py_timestamptz,
    types.MONTH_INTERVAL: int,
    types.SEC_INTERVAL: py_sec_interval,
    types.DAY_INTERVAL: py_day_interval,
    types.URL: strip,
    types.INET: str,
    types.UUID: uuid.UUID,
    types.JSON: json.loads,
    types.GEOMETRY: strip,
    types.GEOMETRYA: strip,
    types.MBR: strip,
    types.XML: str,
}


def convert(data, type_code):
    """
    Calls the appropriate convertion function based upon the python type
    """

    # null values should always be replaced by None type
    if data == "NULL":
        return None
    try:
        return mapping[type_code](data)
    except KeyError:
        raise ProgrammingError("type %s is not supported" % type_code)


# below stuff required by the DBAPI

def Binary(data):
    """returns binary encoding of data"""
    return bytes.fromhex(data)


def DateFromTicks(ticks):
    """Convert ticks to python Date"""
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    """Convert ticks to python Time"""
    return Time(*time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    """Convert ticks to python Timestamp"""
    return Timestamp(*time.localtime(ticks)[:6])


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
STRING = types.VARCHAR
BINARY = types.BLOB
NUMBER = types.DECIMAL
DATE = types.DATE
TIME = types.TIME
DATETIME = types.TIMESTAMP
ROWID = types.INT
