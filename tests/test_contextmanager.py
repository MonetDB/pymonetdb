"""
This is the python implementation of the mapi protocol.
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


from unittest import TestCase

import pymonetdb
from pymonetdb.exceptions import OperationalError
from tests.util import test_args


class TestContextManager(TestCase):

    def connect(self):
        return pymonetdb.connect(**test_args)

    def test_context_manager(self):
        """Test using a Connection in a with-clause"""

        # This is just a quick example of how connections and cursors
        # would typically be used as a context manager.
        # More detailed test cases are given below.

        with self.connect() as conn, conn.cursor() as c:
            c.execute("SELECT 42")
            self.assertEqual(c.fetchone()[0], 42)

    def test_connection_closes_when_ok(self):
        x = self.connect()
        self.assertIsNotNone(x.mapi)
        try:
            with x as conn:
                y = conn.cursor()
                self.assertIsNotNone(y.connection)
                try:
                    with y as c:
                        c.execute("SELECT 42")
                        self.assertEqual(c.fetchone()[0], 42)
                finally:
                    # check cursor is closed
                    self.assertIsNone(y.connection)
        finally:
            # check connection is closed
            self.assertIsNone(x.mapi)

    def test_connection_closes_when_sql_error(self):
        x = self.connect()
        y = x.cursor()
        self.assertIsNotNone(x.mapi)
        self.assertIsNotNone(y.connection)
        with self.assertRaisesRegex(OperationalError, expected_regex="Unexpected symbol"):
            with x as conn:
                # suppress warning about unused 'conn'
                if conn:
                    pass
                with y as cursor:
                    cursor.execute("SELECT 42zzz")   # This fails
                    self.fail("the statement above should have raised an exception")
        # check cursor and conn have been closed
        self.assertIsNone(y.connection)
        self.assertIsNone(x.mapi)

    def test_connection_closes_when_other_error(self):
        x = self.connect()
        y = x.cursor()
        self.assertIsNotNone(x.mapi)
        self.assertIsNotNone(y.connection)
        with self.assertRaises(ZeroDivisionError):
            with x as conn:
                # suppress warning about unused 'conn'
                if conn:
                    pass
                with y as cursor:
                    # suppress warning about unused 'conn' and 'cursor'
                    if conn or cursor:
                        pass
                    # create a ZeroDivisionError without static analyzers noticing it
                    one = 1
                    zero = 0
                    one / zero
                    self.fail("the statement above should have raised an exception")
        # check cursor and conn have been closed
        self.assertIsNone(y.connection)
        self.assertIsNone(x.mapi)
