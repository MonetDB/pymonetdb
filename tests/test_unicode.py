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
    table_prefix = 'dbapi20test_'  # If you need to specify a prefix for tables

    ddl1 = 'create table %sbooze (name varchar(60))' % table_prefix
    ddl2 = 'create table %sbarflys (name varchar(60))' % table_prefix
    xddl1 = 'drop table %sbooze' % table_prefix
    xddl2 = 'drop table %sbarflys' % table_prefix

    lowerfunc = 'lower'  # Name of stored procedure to convert string->lowercase

    # Some drivers may need to override these helpers, for example adding
    # a 'commit' after the execute.
    def executeDDL1(self, cursor):
        cursor.execute(self.ddl1)

    def executeDDL2(self, cursor):
        cursor.execute(self.ddl2)

    def setUp(self):
        pass

    def tearDown(self):
        con = self._connect()
        try:
            cur = con.cursor()
            for ddl in (self.xddl1, self.xddl2):
                try:
                    cur.execute(ddl)
                    con.commit()
                except pymonetdb.Error:
                    # Assume table didn't exist. Other tests will check if
                    # execute is busted.
                    pass
        finally:
            con.close()

    def _connect(self):
        try:
            return pymonetdb.connect(**test_args)
        except AttributeError:
            self.fail("No connect method found in pymonetdb module")

    def test_unicode_string(self):
        con = self._connect()
        cursor = con.cursor()
        self.executeDDL1(cursor)
        x = u"ô  ’a élé.«S’ilît… de-mun»"
        cursor.execute("insert into %sbooze VALUES ('%s')" % (self.table_prefix, x))
        cursor.execute('select name from %sbooze' % self.table_prefix)
        self.assertEqual(x, cursor.fetchone()[0])
        con.close()

    def test_utf8(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL1(cur)
            args = {'beer': '\xc4\xa5'}
            cur.execute('insert into %sbooze values (%%(beer)s)' % self.table_prefix, args)
            cur.execute('select name from %sbooze' % self.table_prefix)
            res = cur.fetchall()
            beer = res[0][0]
            self.assertEqual(beer, args['beer'], 'incorrect data retrieved')
        finally:
            con.close()

    def test_unicode(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL1(cur)
            args = {'beer': '\N{latin small letter a with acute}'}
            encoded = args['beer']

            cur.execute('insert into %sbooze values (%%(beer)s)' % self.table_prefix, args)
            cur.execute('select name from %sbooze' % self.table_prefix)
            res = cur.fetchall()
            beer = res[0][0]
            self.assertEqual(beer, encoded, 'incorrect data retrieved')
        finally:
            con.close()

    def test_substring(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL1(cur)
            args = {'beer': '"" \"\'\",\\"\\"\"\'\"'}
            cur.execute('insert into %sbooze values (%%(beer)s)' % self.table_prefix, args)
            cur.execute('select name from %sbooze' % self.table_prefix)
            res = cur.fetchall()
            beer = res[0][0]
            self.assertEqual(beer, args['beer'],
                             'incorrect data retrieved, got %s, should be %s' % (beer, args['beer']))
        finally:
            con.close()

    def test_escape(self):
        teststrings = [
            'abc\ndef',
            'abc\\ndef',
            'abc\\\ndef',
            'abc\\\\ndef',
            'abc\\\\\ndef',
            'abc"def',
            'abc""def',
            'abc\'def',
            'abc\'\'def',
            "abc\"def",
            "abc\"\"def",
            "abc'def",
            "abc''def",
            "abc\tdef",
            "abc\\tdef",
            "abc\\\tdef",
            "\\x"
        ]

        con = self._connect()
        cur = con.cursor()
        self.executeDDL1(cur)
        for i in teststrings:
            args = {'beer': i}
            cur.execute('insert into %sbooze values (%%(beer)s)' % self.table_prefix, args)
            cur.execute('select * from %sbooze' % self.table_prefix)
            row = cur.fetchone()
            cur.execute('delete from %sbooze where name=%%s' % self.table_prefix, i)
            self.assertEqual(i, row[0], 'newline not properly converted, got %s, should be %s' % (row[0], i))
        con.close()

    def test_non_ascii_string(self):
        con = self._connect()
        cur = con.cursor()
        self.executeDDL1(cur)
        input_ = '中文 zhōngwén'
        args = {'beer': input_}
        cur.execute('insert into %sbooze values (%%(beer)s)' % self.table_prefix,
                    args)
        cur.execute('select name from %sbooze' % self.table_prefix)
        res = cur.fetchall()
        returned = res[0][0]
        self.assertEqual(returned, input_)
        self.assertEqual(type(returned), str)
        con.close()

    def test_query_ending_with_comment(self):
        con = self._connect()
        cur = con.cursor()
        self.executeDDL1(cur)
        cur.execute("insert into %sbooze values ('foo')" % self.table_prefix)
        cur.execute('select * from %sbooze --This is a SQL comment' % self.table_prefix)
        # the above line should execute without problems
        self.assertEqual(1, cur.rowcount,
                         'queries ending in comments should be executed correctly')
        con.close()


if __name__ == '__main__':
    unittest.main()
