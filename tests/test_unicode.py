# coding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import unittest
import pymonetdb
from tests.util import test_args


class TestUnicode(unittest.TestCase):
    def setUp(self):
        self.con = pymonetdb.connect(**test_args)
        cursor = self.con.cursor()
        cursor.execute('create table bla (s VARCHAR(1000))')

    def tearDown(self):
        cursor = self.con.cursor()
        cursor.execute('drop table bla')

    def test_unicode(self):
        cursor = self.con.cursor()
        x = u"drôle de  m’a réveillé. « S’il  plaît… dessine-moi un»"
        cursor.execute(u'insert into bla VALUES (%s)', (x,))
        cursor.execute(u'select * from bla')
        self.assertEqual(x, cursor.fetchone()[0])


if __name__ == '__main__':
    unittest.main()
