# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import unittest
import pymonetdb
from tests.util import test_args


class TestExceptions(unittest.TestCase):
    def setUp(self):
        self.con = pymonetdb.connect(**test_args)
        cursor = self.con.cursor()
        cursor.execute('create table exceptions (s VARCHAR(1000) UNIQUE)')

    def tearDown(self):
        self.con.rollback()

    def test_unique_contraint_violated(self):
        """
        M0M29!INSERT INTO: UNIQUE constraint violated
        """
        cursor = self.con.cursor()
        x = u"something"
        cursor.execute(u'insert into exceptions VALUES (%s)', (x,))
        with self.assertRaises(pymonetdb.exceptions.IntegrityError):
            cursor.execute(u'insert into exceptions VALUES (%s)', (x,))

    def test_unique_contraint_violated2(self):
        """
        '42S02!' no such table
        """
        cursor = self.con.cursor()

        self.assertRaises(pymonetdb.exceptions.OperationalError,
                          cursor.execute,
                          u'select id from thistableshouldnotexist limit 1')
