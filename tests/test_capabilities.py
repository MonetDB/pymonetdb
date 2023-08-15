""" Script to test database capabilities and the DB-API interface
    for functionality and memory leaks.

    Adapted from a script by M-A Lemburg and taken from the MySQL python driver.

"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


import datetime
from time import time
import unittest

from pymonetdb.exceptions import ProgrammingError
import pymonetdb
from pymonetdb.sql import monetize
from tests.util import test_args


class DatabaseTest(unittest.TestCase):
    create_table_extra = ''
    rows = 10

    leak_test = False

    local_tzinfo = datetime.datetime.now().astimezone().tzinfo

    def setUp(self):
        db = pymonetdb.connect(autocommit=False, **test_args)
        self.connection = db
        self.cursor = db.cursor()
        self.BLOBText = ''.join([chr(i) for i in range(33, 127)] * 100)
        self.BLOBBinary = bytes(range(256)) * 16
        self.BLOBUText = ''.join([chr(i) for i in range(1, 16384)])

    def tearDown(self):
        self.connection.close()

    def table_exists(self, name):
        try:
            self.cursor.execute('select * from %s where 1=0' % name)
        except pymonetdb.OperationalError:
            self.connection.rollback()
            return False
        else:
            return True

    @staticmethod
    def quote_identifier(ident):
        return '"%s"' % ident

    def new_table_name(self):
        i = id(self.cursor)
        while True:
            name = self.quote_identifier('tb%08x' % i)
            if not self.table_exists(name):
                return name
            i = i + 1

    def create_table(self, columndefs):
        """ Create a table using a list of column definitions given in
            columndefs.

            generator must be a function taking arguments (row_number,
            col_number) returning a suitable data object for insertion
            into the table.

        """
        self.table = self.new_table_name()
        self.cursor.execute('CREATE TABLE %s (%s) %s' %
                            (self.table,
                             ',\n'.join(columndefs),
                             self.create_table_extra))

    def check_data_integrity(self, columndefs, generator, drop=True):
        self.create_table(columndefs)
        insert_statement = ('INSERT INTO %s VALUES (%s)' %
                            (self.table,
                             ','.join(['%s'] * len(columndefs))))
        data = [[generator(i, j) for j in range(len(columndefs))]
                for i in range(self.rows)]
        self.cursor.executemany(insert_statement, data)
        self.connection.commit()

        self.cursor.execute('select * from %s' % self.table)
        rows = self.cursor.fetchall()
        self.assertEqual(len(rows), self.rows)
        try:
            for i in range(self.rows):
                for j in range(len(columndefs)):
                    self.assertEqual(rows[i][j], generator(i, j))
        finally:
            if drop:
                self.cursor.execute('drop table %s' % self.table)

    def test_transactions(self):
        columndefs = ('col1 INT', 'col2 VARCHAR(255)')

        def generator(row, col):
            if col == 0:
                return row
            else:
                return ('%i' % (row % 10)) * 255

        self.check_data_integrity(columndefs, generator, drop=False)
        delete_statement = 'delete from %s where col1=%%s' % self.table
        self.cursor.execute(delete_statement, (0,))
        self.cursor.execute('select col1 from %s where col1=%s' % (self.table, 0))
        rows = self.cursor.fetchall()
        self.assertFalse(rows, "DELETE didn't work")
        self.connection.rollback()
        self.cursor.execute('select col1 from %s where col1=%s' % (self.table, 0))
        rows = self.cursor.fetchall()
        self.assertTrue(len(rows) == 1, "ROLLBACK didn't work")
        self.cursor.execute('drop table %s' % self.table)

    def test_truncation(self):  # noqa: C901
        columndefs = ('col1 INT', 'col2 VARCHAR(255)')

        def generator(row, col):
            if col == 0:
                return row
            else:
                return ('%i' % (row % 10)) * (int(255 - self.rows / 2) + row)

        self.create_table(columndefs)
        insert_statement = ('INSERT INTO %s VALUES (%s)' %
                            (self.table, ','.join(['%s'] * len(columndefs))))
        try:
            self.cursor.execute(insert_statement, (0, '0' * 256))
        except Warning:
            pass
        except self.connection.Error:
            pass
        else:
            self.fail("Over-long column did not generate warnings/exception with single insert")

        self.connection.rollback()
        self.create_table(columndefs)

        try:
            for i in range(self.rows):
                data = []
                for j in range(len(columndefs)):
                    data.append(generator(i, j))
                self.cursor.execute(insert_statement, tuple(data))
        except Warning:
            pass
        except self.connection.Error:
            pass
        else:
            self.fail("Over-long columns did not generate warnings/exception with execute()")

        self.connection.rollback()
        self.create_table(columndefs)

        try:
            data = [[generator(i, j) for j in range(len(columndefs))]
                    for i in range(self.rows)]
            self.cursor.executemany(insert_statement, data)
        except Warning:
            pass
        except self.connection.Error:
            pass
        else:
            self.fail("Over-long columns did not generate warnings/exception with executemany()")

        self.connection.rollback()

    def test_CHAR(self):
        # Character data
        def generator(row, col):
            return ('%i' % ((row + col) % 10)) * 255

        self.check_data_integrity(
            ('col1 char(255)', 'col2 char(255)'),
            generator)

    def test_INT(self):
        # Number data
        def generator(row, _):
            return row * row

        self.check_data_integrity(
            ('col1 INT',),
            generator)

    def test_DECIMAL(self):
        def generator(row, col):
            from decimal import Decimal
            return Decimal("%d.%02d" % (row, col))

        self.check_data_integrity(
            ('col1 DECIMAL(5,2)',),
            generator)

    def test_REAL(self):
        def generator(row, _):
            return row * 1000.0

        self.check_data_integrity(
            ('col1 REAL',),
            generator)

    def test_DOUBLE(self):
        def generator(row, _):
            return row / 1e-99

        self.check_data_integrity(
            ('col1 DOUBLE',),
            generator)

    def test_DATE(self):
        ticks = time()

        def generator(row, col):
            return pymonetdb.DateFromTicks(ticks + row * 86400 - col * 1313)

        self.check_data_integrity(
            ('col1 DATE',),
            generator)

    def test_TIME(self):
        ticks = time()

        self.assertIsNone(pymonetdb.TimeFromTicks(ticks).tzinfo)

        def generator(row, col):
            return pymonetdb.TimeFromTicks(ticks + row * 86400 - col * 1313)

        self.check_data_integrity(
            ('col1 TIME',),
            generator)

    def test_TIMETZ(self):
        ticks = time()

        self.assertEqual(self.local_tzinfo, pymonetdb.TimeTzFromTicks(ticks).tzinfo)

        def generator(row, col):
            return pymonetdb.TimeTzFromTicks(ticks + row * 86400 - col * 1313)

        self.check_data_integrity(
            ('col1 TIMETZ',),
            generator)

    def test_DATETIME(self):
        ticks = time()

        self.assertIsNone(pymonetdb.TimestampFromTicks(ticks).tzinfo)

        def generator(row, col):
            return pymonetdb.TimestampFromTicks(ticks + row * 86400 - col * 1313)

        self.check_data_integrity(
            ('col1 TIMESTAMP',),
            generator)

    def test_TIMESTAMP(self):
        ticks = time()

        self.assertIsNone(pymonetdb.TimestampFromTicks(ticks).tzinfo)

        def generator(row, col):
            return pymonetdb.TimestampFromTicks(ticks + row * 86400 - col * 1313)

        self.check_data_integrity(
            ('col1 TIMESTAMP',),
            generator)

    def test_TIMESTAMPTZ(self):
        ticks = time()

        self.assertEqual(self.local_tzinfo, pymonetdb.TimestampTzFromTicks(ticks).tzinfo)

        def generator(row, col):
            return pymonetdb.TimestampTzFromTicks(ticks + row * 86400 - col * 1313)

        self.check_data_integrity(
            ('col1 TIMESTAMPTZ',),
            generator)

    def test_fractional_TIMESTAMP(self):
        ticks = time()

        def generator(row, col):
            return pymonetdb.TimestampFromTicks(ticks + row * 86400 - col * 1313 + row * 0.7 * col / 3.0)

        self.check_data_integrity(
            ('col1 TIMESTAMP',),
            generator)

    def test_SEC_INTERVAL(self):

        def generator(row, col):
            return datetime.timedelta(seconds=row * 86400 - col * 1313)

        self.check_data_integrity(
            ('col1 INTERVAL SECOND',),
            generator)

    def test_TEXT(self):
        def generator(_, __):
            return self.BLOBText  # 'BLOB Text ' * 1024

        self.check_data_integrity(
            ('col2 TEXT',),
            generator)

    def test_BLOB(self):
        def generator(row, col):
            if col == 0:
                return row
            else:
                return self.BLOBBinary  # 'BLOB\000Binary ' * 1024

        self.check_data_integrity(
            ('col1 INT', 'col2 BLOB'),
            generator)

    def test_TINYINT(self):
        # Number data
        def generator(row, _):
            v = (row * row) % 256
            if v > 127:
                v = v - 256
            return v

        self.check_data_integrity(
            ('col1 TINYINT',),
            generator)

    def test_small_CHAR(self):
        # Character data
        def generator(row, col):
            i = (row * col + 62) % 256
            if i == 62:
                return ''
            if i == 63:
                return None
            return chr(i)

        self.check_data_integrity(
            ('col1 char(1)', 'col2 char(1)'),
            generator)

    def test_BOOL(self):
        def generator(row, _):
            return bool(row % 2)

        self.check_data_integrity(
            ('col1 BOOL',),
            generator)

    def test_URL(self):
        def generator(_, __):
            return "http://example.org/something"

        self.check_data_integrity(
            ('col1 URL',),
            generator)

    def test_INET(self):
        def generator(_, __):
            return "192.168.254.101"

        self.check_data_integrity(
            ('col1 INET',),
            generator)

    def test_description(self):
        self.table = self.new_table_name()
        shouldbe = [
            ('c', 'varchar', None, 1024, None, None, None),
            ('d', 'decimal', None, 9, 9, 4, None),
            ('n', 'varchar', None, 1, None, None, None),
        ]
        try:
            self.cursor.execute(
                "create table %s (c VARCHAR(1024), d DECIMAL(9,4), n VARCHAR(1) NOT NULL)" % self.table)
            self.cursor.execute("insert into %s VALUES ('test', 12345.1234, 'x')" % self.table)
            self.cursor.execute('select * from %s' % self.table)
            self.assertEqual(self.cursor.description, shouldbe, "cursor.description is incorrect")
        finally:
            self.cursor.execute('drop table %s' % self.table)

    def test_bigresult(self):
        self.cursor.execute('select count(*) from types')
        r = self.cursor.fetchone()
        n = r[0]
        self.cursor.arraysize = 100000
        self.cursor.execute('select * from types t1, types t2')
        r = self.cursor.fetchall()
        self.assertEqual(len(r), n ** 2)

    def test_closecur(self):
        self.cursor.close()
        self.assertRaises(ProgrammingError, self.cursor.execute, "select * from tables")
        self.cursor = self.connection.cursor()

    def test_customtype(self):
        t = ["list", "test"]
        self.assertRaises(ProgrammingError, monetize.convert, t)
        monetize.mapping_dict[list] = str
        self.assertEqual(monetize.convert(t), "['list', 'test']")

    def test_multiple_queries(self):
        table1 = self.new_table_name()
        table2 = table1[:-1] + 'bla"'
        self.cursor.execute("create table %s (a int)" % table1)
        self.cursor.execute("create table %s (a int, b int)" % table2)
        self.cursor.execute("insert into %s VALUES (100)" % table1)
        self.cursor.execute("insert into %s VALUES (50, 50)" % table2)
        self.cursor.execute('select * from %s; select * from %s;' %
                            (table1, table2))
        result = self.cursor.fetchall()
        self.assertEqual(result, [(50, 50)])

    def test_temporal_operations(self):
        dt = datetime.datetime(2017, 12, 6, 12, 30)
        self.cursor.execute("SELECT %(dt)s - INTERVAL '1' DAY", {'dt': dt})
        expected = datetime.datetime(2017, 12, 5, 12, 30)
        self.assertEqual(self.cursor.fetchone()[0], expected)

        d = datetime.date(2017, 12, 6)
        self.cursor.execute("SELECT %(d)s - INTERVAL '1' DAY", {'d': d})
        expected = datetime.date(2017, 12, 5)
        self.assertEqual(self.cursor.fetchone()[0], expected)

        t = datetime.time(12, 5)
        self.cursor.execute("SELECT %(t)s - INTERVAL '30' MINUTE", {'t': t})
        expected = datetime.time(11, 35)
        self.assertEqual(self.cursor.fetchone()[0], expected)

        td = datetime.timedelta(days=5, hours=2, minutes=10)
        self.cursor.execute("SELECT %(dt)s - %(td)s", {'dt': dt, 'td': td})
        expected = datetime.datetime(2017, 12, 1, 10, 20)
        self.assertEqual(self.cursor.fetchone()[0], expected)
