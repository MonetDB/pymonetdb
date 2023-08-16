# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


from unittest import SkipTest, TestCase

from pymonetdb import OperationalError
import pymonetdb
from tests.util import test_args


class TestNamed(TestCase):

    def connect(self):
        return pymonetdb.connect(**test_args)

    def setUp(self):
        with self.connect() as conn, conn.cursor() as c:
            # does it work at all?
            c.execute("SELECT 42")
            # does it support named arguments?
            try:
                c.execute("SELECT :fortytwo : ( fortytwo 42 )")
            except OperationalError:
                raise SkipTest("Named parameter syntax not supported by this MonetDB")

        # if we get here, temporarily change paramstyle
        assert pymonetdb.paramstyle == 'pyformat'
        pymonetdb.paramstyle = 'named'

    def tearDown(self):
        pymonetdb.paramstyle = 'pyformat'

    def test_named_parameters(self):
        with self.connect() as conn, conn.cursor() as c:
            parms = dict(foo=42, bar="banana")
            c.execute("SELECT :foo AS foo, :bar AS bar", parms)
            row = c.fetchone()
            self.assertEqual(row, (42, "banana"))
