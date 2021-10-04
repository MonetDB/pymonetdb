# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import datetime
import unittest
import uuid
from pymonetdb.sql.monetize import convert
from pymonetdb.exceptions import ProgrammingError


class TestMonetize(unittest.TestCase):
    def test_str_subclass(self):
        class StrSubClass(str):
            pass
        x = StrSubClass('test')
        csub = convert(x)
        cstr = convert('test')
        self.assertEqual(csub, cstr)

    def test_unknown_type(self):
        class Unknown:
            pass
        x = Unknown()
        self.assertRaises(ProgrammingError, convert, x)

    def test_datetime(self):
        x = datetime.datetime(2017, 12, 6, 12, 30)
        self.assertEqual(convert(x), "TIMESTAMP '2017-12-06 12:30:00'")

    def test_datetime_tz(self):
        x = datetime.datetime(2017, 12, 6, 12, 30).replace(tzinfo=datetime.timezone(datetime.timedelta(hours=3)))
        self.assertEqual(convert(x), "TIMESTAMPTZ '2017-12-06 12:30:00+03:00'")

    def test_date(self):
        x = datetime.date(2017, 12, 6)
        self.assertEqual(convert(x), "DATE '2017-12-06'")

    def test_time(self):
        x = datetime.time(12, 5)
        self.assertEqual(convert(x), "TIME '12:05:00'")

    def test_time_tz(self):
        x = datetime.time(12, 5).replace(tzinfo=datetime.timezone(datetime.timedelta(hours=3)))
        self.assertEqual(convert(x), "TIMETZ '12:05:00+03:00'")

    def test_timedelta(self):
        x = datetime.timedelta(days=5, hours=2, minutes=10)
        self.assertEqual(convert(x), "INTERVAL '439800' SECOND")

    def test_uuids(self):
        x = uuid.UUID('334e6185-dd64-33d8-a052-d93371d3d20d')
        self.assertEqual(convert(x), "'334e6185-dd64-33d8-a052-d93371d3d20d'")
