# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


from unittest import TestCase

import pymonetdb

import tests.util


class TestAutocommit(TestCase):

    def setUp(self):
        with self.connect() as conn, conn.cursor() as c:
            c.execute('DROP TABLE IF EXISTS foo')
            c.execute('CREATE TABLE foo(i INT)')
            c.execute('INSERT INTO foo VALUES (0)')
            conn.commit()

    def connect(self, **connect_parms) -> pymonetdb.Connection:
        args = {**tests.util.test_args, **connect_parms}
        return pymonetdb.connect(**args)

    def verify_autocommit(self, conn: pymonetdb.Connection, expected_autocommit: bool):
        # Check what the connection believes
        self.assertEqual(expected_autocommit, conn.autocommit)

        # Check how the server behaves
        with conn.cursor() as c:
            c.execute('SELECT * FROM foo')
            initial_value = c.fetchone()[0]
            modified_value = initial_value + 1
            c.execute('UPDATE foo SET i = %s', [modified_value])
            with self.connect() as conn2, conn2.cursor() as c2:
                c2.execute('SELECT * FROM foo')
                observed = c2.fetchone()[0]
                if expected_autocommit:
                    self.assertEqual(observed, modified_value)
                else:
                    self.assertEqual(observed, initial_value)

    def test_autocommit(self):
        with self.connect() as conn, conn.cursor() as c:
            # with DBAPI, default is off
            self.verify_autocommit(conn, False)

            # turn it on with the property
            conn.set_autocommit(True)
            self.verify_autocommit(conn, True)

            # Now we 'manually' start a transaction.
            # Check if conn.autocommit changes with it
            c.execute('START TRANSACTION')
            self.verify_autocommit(conn, False)
            c.execute('ROLLBACK')
            self.verify_autocommit(conn, True)

    def test_multi(self):
        with self.connect(autocommit=True) as conn, conn.cursor() as c:
            self.verify_autocommit(conn, True)
            c.execute('START TRANSACTION; UPDATE foo SET i = 42; COMMIT')
            self.verify_autocommit(conn, True)
