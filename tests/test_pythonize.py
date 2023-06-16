# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

from datetime import datetime, timedelta, timezone
import unittest
import pymonetdb.sql.pythonize
import pymonetdb
from tests.util import test_args


class TestPythonize(unittest.TestCase):
    TEST_TIMEZONE = -4

    def setUp(self):
        db = pymonetdb.connect(autocommit=False, **test_args)
        db.set_timezone(self.TEST_TIMEZONE * 3600)
        self.connection = db
        self.cursor = db.cursor()

    def tearDown(self):
        self.connection.close()

    def test_Binary(self):
        input1 = bytes(range(256)).hex()
        output1 = bytes(range(256))
        result1 = pymonetdb.sql.pythonize.convert(input1, pymonetdb.types.BLOB)
        self.assertEqual(output1, result1)

        input2 = b'\tdharma'.hex()
        output2 = b'\tdharma'

        result2 = pymonetdb.sql.pythonize.convert(input2, pymonetdb.types.BLOB)
        self.assertEqual(output2, result2)

    def test_month_interval(self):
        self.cursor.execute('CREATE TEMPORARY TABLE foo (i INTERVAL MONTH)')
        self.cursor.execute('INSERT INTO foo VALUES (INTERVAL \'2\' YEAR)')
        self.cursor.execute('SELECT * from FOO')
        row = self.cursor.fetchone()
        self.assertEqual(row[0], 24)

    def test_timestamptz(self):
        tz = timezone(timedelta(hours=self.TEST_TIMEZONE))
        now = datetime.now(tz)
        self.cursor.execute('SELECT now()')
        row = self.cursor.fetchone()
        ts = row[0]

        # ts is timezone-aware, see the datetime docs
        self.assertIsNotNone(ts.tzinfo)
        self.assertIsNotNone(ts.tzinfo.utcoffset(ts))

        # ts is correct, allowing for fairly large clock skew between client and server
        self.assertAlmostEqual(60 * ts.hour + ts.minute, 60 * now.hour + now.minute, delta=12)

    def test_roundtrip_datetime(self):
        dt = datetime(2020, 2, 14, 20, 50)
        tz = timezone(timedelta(hours=self.TEST_TIMEZONE))
        dtz = dt.replace(tzinfo=tz)

        self.cursor.execute('SELECT %s, %s', [dt, dtz])
        row = self.cursor.fetchone()

        self.assertEqual(row[0].isoformat(), dt.isoformat())
        self.assertEqual(row[1].isoformat(), dtz.isoformat())

    def test_date_year0(self):
        with self.assertRaisesRegex(ValueError, "out of range"):
            self.cursor.execute("SELECT DATE '0-1-1'")

    def test_date_negative_year(self):
        with self.assertRaisesRegex(ValueError, "out of range"):
            self.cursor.execute("SELECT DATE '-1-1-1'")

    def test_timestamp_year0(self):
        with self.assertRaisesRegex(ValueError, "out of range"):
            self.cursor.execute("SELECT TIMESTAMP '0-1-1 11:12:13'")

    def test_timestamp_negative_year(self):
        with self.assertRaisesRegex(ValueError, "out of range"):
            self.cursor.execute("SELECT TIMESTAMP '-1-1-1 11:12:13'")

    def test_roundtrip_binary(self):
        raw = b'BLUB\x00BLOB'
        wrapped = pymonetdb.Binary(raw)
        self.cursor.execute('SELECT %s, %s', [raw, wrapped])
        row = self.cursor.fetchone()

        self.assertEqual(row[0], raw)
        self.assertEqual(row[1], raw)
