# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

from unittest import TestCase
import pymonetdb
from tests.util import test_args


class ExecuteTests(TestCase):
    def test_execute_return_value(self):
        # The return value of Cursor.execute() is not specified by PEP 249
        # but we want it to behave as follows:

        conn = pymonetdb.connect(**test_args)
        c = conn.cursor()

        ret = c.execute("DROP TABLE IF EXISTS foo")
        self.assertIsNone(ret)

        ret = c.execute("CREATE TABLE foo(i INT)")
        self.assertIsNone(ret)

        ret = c.execute("INSERT INTO foo SELECT * FROM sys.generate_series(0,10)")
        self.assertEqual(ret, 10)

        ret = c.execute("DELETE FROM foo WHERE i % 3 = 0")
        self.assertEqual(ret, 4)  # 0 3 6 9

        ret = c.execute("SELECT * FROM foo")
        self.assertEqual(ret, 6)  # 1 2 4 5 7 8

        ret = c.execute("DROP TABLE foo")
        self.assertIsNone(ret)

        conn.rollback()
