# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

"""
functions for converting python objects to monetdb SQL format. If you want
to add support for a specific type you should add a function as a value to
the mapping dict and the datatype as key.
"""

import datetime
import decimal

from pymonetdb.exceptions import ProgrammingError


def monet_none(_):
    """
    returns a NULL string
    """
    return "NULL"


def monet_bool(data):
    """
    returns "true" or "false"
    """
    return ["false", "true"][bool(data)]


def monet_escape(data):
    """
    returns an escaped string
    """
    data = str(data).replace("\\", "\\\\")
    data = data.replace("\'", "\\\'")
    return "'%s'" % str(data)


def monet_bytes(data):
    """
    converts bytes to string
    """
    return "'%s'" % data.hex()


def monet_datetime(data):
    """
    returns a casted timestamp
    """
    return "TIMESTAMP %s" % monet_escape(data)


def monet_date(data):
    """
    returns a casted date
    """
    return "DATE %s" % monet_escape(data)


def monet_time(data):
    """
    returns a casted time
    """
    return "TIME %s" % monet_escape(data)


def monet_timedelta(data):
    """
    returns timedelta casted to interval seconds
    """
    return "INTERVAL %s SECOND" % monet_escape(int(data.total_seconds()))


def monet_unicode(data):
    return monet_escape(data.encode('utf-8'))


mapping = [

    (str, monet_escape),
    (bytes, monet_bytes),
    (int, str),
    (complex, str),
    (float, str),
    (decimal.Decimal, str),
    (datetime.datetime, monet_datetime),
    (datetime.time, monet_time),
    (datetime.date, monet_date),
    (datetime.timedelta, monet_timedelta),
    (bool, monet_bool),
    (type(None), monet_none),
]

mapping_dict = dict(mapping)


def convert(data):
    """
    Return the appropriate convertion function based upon the python type.
    """
    if type(data) in mapping_dict:
        return mapping_dict[type(data)](data)
    else:
        for type_, func in mapping:
            if issubclass(type(data), type_):
                return func(data)
    raise ProgrammingError("type %s not supported as value" % type(data))
