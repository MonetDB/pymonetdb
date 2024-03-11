# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

from typing import List
import pymonetdb
import unittest
from tests.util import test_args


class ConnectionTest(unittest.TestCase):

    connections_to_close: List[pymonetdb.Connection] = []
    schemas_to_drop: List[str] = []

    def tearDown(self):
        with self.connect() as conn, conn.cursor() as c:
            for s in self.schemas_to_drop:
                try:
                    c.execute("DROP SCHEMA %s", (s,))
                except pymonetdb.OperationalError:
                    pass
        for conn in self.connections_to_close:
            try:
                conn.close()
            except pymonetdb.exceptions.Error:
                pass
        self.connections_to_close = []
        super().tearDown()

    def connect(self, **more_args) -> pymonetdb.Connection:
        args = {**test_args, **more_args}
        conn = pymonetdb.connect(**args)
        self.connections_to_close.append(conn)
        return conn

    def test_autocommit_off(self):
        conn = self.connect(autocommit=False)
        # Verify that the connection works at all
        with conn.cursor() as c:
            c.execute("SELECT 42")
            # In autocommit mode, rollbacks fail
            conn.rollback()

    def test_autocommit_on(self):
        conn = self.connect(autocommit=True)
        # Verify that the connection works at all
        with conn.cursor() as c:
            c.execute("SELECT 42")
            # In autocommit mode, rollbacks fail
            with self.assertRaisesRegex(pymonetdb.OperationalError, "auto commit"):
                conn.rollback()

    def test_timezone(self):
        minutes_east = 60 + 23
        conn = self.connect(timezone=minutes_east)
        with conn.cursor() as c:
            c.execute("SELECT now()")
            now = c.fetchone()[0]
            tz = now.tzname()
            self.assertIn("+01:23", tz)

    def test_replysize(self):
        conn = self.connect(replysize=99)
        with conn.cursor() as c:
            self.assertEqual(c.replysize, 99)

    def test_schema(self):
        conn = self.connect()
        with conn.cursor() as c:
            c.execute("SELECT current_schema")
            cur = c.fetchone()[0]
            c.execute("SELECT name FROM sys.schemas")
            existing_schemas = [row[0] for row in c.fetchall()]
            for s in existing_schemas:
                schema_to_connect_to = s
                if schema_to_connect_to not in [cur, 'tmp']:
                    break
            else:
                self.fail(f"no suitable schema in {existing_schemas!r}")

        # no try to connect with this schema set
        conn = self.connect(schema=schema_to_connect_to)
        with conn.cursor() as c:
            c.execute("SELECT current_schema")
            actual_schema = c.fetchone()[0]
            self.assertEqual(actual_schema, schema_to_connect_to)
