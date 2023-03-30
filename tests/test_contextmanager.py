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
from tests.util import test_args


class TestContextManager(TestCase):

    def connect(self):
        return pymonetdb.connect(**test_args)

    def test_context_manager(self):
        """Test using a Connection in a with-clause"""
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
        try:
            x = self.connect()
            self.assertIsNotNone(x.mapi)
            try:
                with x as conn:
                    y = conn.cursor()
                    self.assertIsNotNone(y.connection)
                    try:
                        with y as c:
                            c.execute("SELECT 42x")   # This fails
                            self.assertEqual(c.fetchone()[0], 42)
                    finally:
                        # check cursor is closed
                        self.assertIsNone(y.connection)
            finally:
                # check connection is closed
                self.assertIsNone(x.mapi)
        except pymonetdb.exceptions.OperationalError as e:
            if "Unexpected symbol x" in str(e):
                pass
            else:
                raise e

    def test_connection_closes_when_other_error(self):
        try:
            x = self.connect()
            self.assertIsNotNone(x.mapi)
            try:
                with x as conn:
                    y = conn.cursor()
                    self.assertIsNotNone(y.connection)
                    try:
                        with y as c:
                            one = len([c])
                            zero = 0
                            one / zero
                    finally:
                        # check cursor is closed
                        self.assertIsNone(y.connection)
            finally:
                # check connection is closed
                self.assertIsNone(x.mapi)
        except ZeroDivisionError:
            pass
