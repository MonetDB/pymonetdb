# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import unittest

import pymonetdb

# regular import doesn't work, don't know why
DATE = pymonetdb.types.DATE
TIME = pymonetdb.types.TIME
TIMESTAMP = pymonetdb.types.TIMESTAMP
TIMETZ = pymonetdb.types.TIMETZ
TIMESTAMPTZ = pymonetdb.types.TIMESTAMPTZ


class TestPythonizeTemporal(unittest.TestCase):

    def verify(self, val, typ, expected=None):
        dt = pymonetdb.sql.pythonize.convert(val, typ)
        iso = dt.isoformat()
        if expected is None:
            expected = val.replace(' ', 'T')
        self.assertEqual(iso, expected)

    def test_date(self):
        self.verify('2015-02-14', DATE)
        self.verify('1970-02-14', DATE)
        self.verify('1950-02-14', DATE)
        self.verify('1676-02-14', DATE)
        self.verify('2-02-14', DATE, '0002-02-14')

    def test_time(self):
        self.verify('00:00:00', TIME)
        self.verify('00:00:00.00', TIME, '00:00:00')
        self.verify('00:00:00.12', TIME, '00:00:00.120000')
        self.verify('12:13:14.1516', TIME, '12:13:14.151600')
        self.verify('23:59:59.999999', TIME)
        with self.assertRaises(ValueError):
            # hello, leap second
            self.verify('23:59:60.123456', TIME)

    def test_timetz(self):
        self.verify('00:00:00+01:30', TIMETZ, '00:00:00+01:30')
        self.verify('12:34:56-04:00', TIMETZ, '12:34:56-04:00')

    def test_timestamp(self):
        self.verify('2015-02-14 20:50:12', TIMESTAMP)
        self.verify('2015-02-14 20:50:12.34', TIMESTAMP, '2015-02-14T20:50:12.340000')
        self.verify('15-02-14 20:50:12.34', TIMESTAMP, '0015-02-14T20:50:12.340000')

    def test_timestamptz(self):
        self.verify('2015-02-14 20:50:12-04:30', TIMESTAMPTZ)
        self.verify('2015-02-14 20:50:12.34+04:30', TIMESTAMPTZ, '2015-02-14T20:50:12.340000+04:30')
        self.verify('15-02-14 20:50:12.34-04:30', TIMESTAMPTZ, '0015-02-14T20:50:12.340000-04:30')
