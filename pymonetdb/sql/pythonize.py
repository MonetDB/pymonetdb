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

    hours = sign * int(data[-5:-3])
    minutes = sign * int(data[-2:])
    delta = timedelta(hours=hours, minutes=minutes)
    timezone = datetime.timezone(delta)

    return data[:-6], timezone


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
    hour, min, sec_usec = data.split(':', 3)
    sec_parts = sec_usec.split('.', 2)
    sec = sec_parts[0]
    if len(sec_parts) == 2:
        usec = int((sec_parts[1] + '000000')[:6])
    else:
        usec = 0
    return datetime.time(int(hour), int(min), int(sec), usec)


def py_timetz(data):
    """ returns a python Time where data contains a tz code
    """
    t, timezone_delta = _extract_timezone(data)
    return py_time(t).replace(tzinfo=timezone_delta)


def py_date(data):
    """ Returns a python Date
    """
    try:
        year, month, day = data.split('-', 3)
    except ValueError:
        if data.startswith('-'):
            raise ValueError("year out of range, must be positive")
        else:
            raise
    return datetime.date(int(year), int(month), int(day))


def py_timestamp(data):
    """ Returns a python Timestamp
    """
    date_part, time_part = data.split(' ', 2)
    try:
        year, month, day = date_part.split('-', 3)
    except ValueError:
        if date_part.startswith('-'):
            raise ValueError("year out of range, must be positive")
        else:
            raise
    hour, min, sec_usec = time_part.split(':', 3)
    sec_parts = sec_usec.split('.', 2)
    sec = sec_parts[0]
    if len(sec_parts) == 2:
        usec = int((sec_parts[1] + '000000')[:6])
    else:
        usec = 0
    return datetime.datetime(int(year), int(month), int(day), int(hour), int(min), int(sec), usec)


def py_timestamptz(data):
    """ Returns a python Timestamp where data contains a tz code
    """
    dt, timezone_delta = _extract_timezone(data)
    return py_timestamp(dt).replace(tzinfo=timezone_delta)


def py_sec_interval(data: str) -> timedelta:
    """ Returns a python TimeDelta where data represents a value of MonetDB's INTERVAL SECOND type
    which resembles a stringified decimal.
    """
    # It comes in as a decimal but we use Pythons float parser to parse it.
    # That's ok because the precision of the decimal is only three decimal digits
    # so far coarser than the rounding errors introduced by the float.
    return timedelta(seconds=float(data))


def py_day_interval(data: str) -> int:
    """ Returns a python number of days where data represents a value of MonetDB's INTERVAL DAY type
    which resembles a stringified decimal.
    """
    # It comes in as a decimal but we use Pythons float parser to parse it.
    # That's ok because the precision of the decimal is only three decimal digits
    # so far coarser than the rounding errors introduced by the float.
    return timedelta(seconds=float(data)).days


def py_bytes(data: str):
    """Returns a bytes (py3) or string (py2) object representing the input blob."""
    return bytes.fromhex(data)


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
    """Convert to wraps binary data"""
    assert isinstance(data, bytes) or isinstance(data, bytearray)
    return data


def DateFromTicks(ticks):
    """Convert ticks to python Date"""
    return Date(*time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    """Convert ticks to python Time"""
    return Time(*time.localtime(ticks)[3:6])


def TimeTzFromTicks(ticks):
    """Convert ticks to python Time"""
    return _make_localtime(Time(*time.localtime(ticks)[3:6]))


def TimestampFromTicks(ticks):
    """Convert ticks to python Timestamp"""
    return Timestamp(*time.localtime(ticks)[:6])


def TimestampTzFromTicks(ticks):
    """Convert ticks to python Timestamp"""
    return _make_localtime(Timestamp(*time.localtime(ticks)[:6]))


_local_tzinfo = datetime.datetime.now().astimezone().tzinfo


def _make_localtime(t):
    return t.replace(tzinfo=_local_tzinfo)


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
