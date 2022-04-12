"""
Python DB API 2.0 driver compliance unit test suite.
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
#
#
# this code is modified version of (public domain):
#
# https://github.com/psycopg/psycopg2/blob/master/tests/dbapi20.py
#
import unittest
import time
import pymonetdb
from tests.util import test_args


class DatabaseAPI20Test(unittest.TestCase):
    """ Test pymonetdb for DB API 2.0 compatibility.
    """

    table_prefix = 'dbapi20test_'  # If you need to specify a prefix for tables

    ddl1 = 'create table %sbooze (name varchar(20))' % table_prefix
    ddl2 = 'create table %sbarflys (name varchar(20))' % table_prefix
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

    def test_connect(self):
        con = self._connect()
        con.close()

    def test_apilevel(self):
        try:
            # Must exist
            apilevel = pymonetdb.apilevel
            # Must equal 2.0
            self.assertEqual(apilevel, '2.0')
        except AttributeError:
            self.fail("Driver doesn't define apilevel")

    def test_threadsafety(self):
        try:
            # Must exist
            threadsafety = pymonetdb.threadsafety
            # Must be a valid value
            self.assertTrue(threadsafety in (0, 1, 2, 3))
        except AttributeError:
            self.fail("Driver doesn't define threadsafety")

    def test_paramstyle(self):
        try:
            # Must exist
            paramstyle = pymonetdb.paramstyle
            # Must be a valid value
            self.assertTrue(paramstyle in (
                'qmark', 'numeric', 'named', 'format', 'pyformat'
            ))
        except AttributeError:
            self.fail("Driver doesn't define paramstyle")

    def test_Exceptions(self):
        # Make sure required exceptions exist, and are in the
        # defined hierarchy.
        # under Python 3 StardardError no longer exist, replaced with Exception
        self.assertTrue(issubclass(pymonetdb.Warning, Exception))
        self.assertTrue(issubclass(pymonetdb.Error, Exception))

        self.assertTrue(
            issubclass(pymonetdb.InterfaceError, pymonetdb.Error)
        )
        self.assertTrue(
            issubclass(pymonetdb.DatabaseError, pymonetdb.Error)
        )
        self.assertTrue(
            issubclass(pymonetdb.OperationalError, pymonetdb.Error)
        )
        self.assertTrue(
            issubclass(pymonetdb.IntegrityError, pymonetdb.Error)
        )
        self.assertTrue(
            issubclass(pymonetdb.InternalError, pymonetdb.Error)
        )
        self.assertTrue(
            issubclass(pymonetdb.ProgrammingError, pymonetdb.Error)
        )
        self.assertTrue(
            issubclass(pymonetdb.NotSupportedError, pymonetdb.Error)
        )

    def test_ExceptionsAsConnectionAttributes(self):
        # OPTIONAL EXTENSION
        # Test for the optional DB API 2.0 extension, where the exceptions
        # are exposed as attributes on the Connection object
        # I figure this optional extension will be implemented by any
        # driver author who is using this test suite, so it is enabled
        # by default.
        con = self._connect()
        drv = pymonetdb
        self.assertTrue(con.Warning is drv.Warning)
        self.assertTrue(con.Error is drv.Error)
        self.assertTrue(con.InterfaceError is drv.InterfaceError)
        self.assertTrue(con.DatabaseError is drv.DatabaseError)
        self.assertTrue(con.OperationalError is drv.OperationalError)
        self.assertTrue(con.IntegrityError is drv.IntegrityError)
        self.assertTrue(con.InternalError is drv.InternalError)
        self.assertTrue(con.ProgrammingError is drv.ProgrammingError)
        self.assertTrue(con.NotSupportedError is drv.NotSupportedError)

    def test_commit(self):
        con = self._connect()
        try:
            # Commit must work, even if it doesn't do anything
            con.commit()
        finally:
            con.close()

    def test_rollback(self):
        con = self._connect()
        # If rollback is defined, it should either work or throw
        # the documented exception
        if hasattr(con, 'rollback'):
            try:
                con.rollback()
            except pymonetdb.NotSupportedError:
                pass

    def test_cursor(self):
        con = self._connect()
        try:
            _ = con.cursor()
        finally:
            con.close()

    def test_cursor_isolation(self):
        con = self._connect()
        try:
            # Make sure cursors created from the same connection have
            # the documented transaction isolation level
            cur1 = con.cursor()
            cur2 = con.cursor()
            self.executeDDL1(cur1)
            cur1.execute("insert into %sbooze values ('Victoria Bitter')" % (
                self.table_prefix
            ))
            cur2.execute("select name from %sbooze" % self.table_prefix)
            booze = cur2.fetchall()
            self.assertEqual(len(booze), 1)
            self.assertEqual(len(booze[0]), 1)
            self.assertEqual(booze[0][0], 'Victoria Bitter')
        finally:
            con.close()

    def test_description(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL1(cur)
            self.assertEqual(cur.description, None,
                             'cursor.description should be none after executing a '
                             'statement that can return no rows (such as DDL)'
                             )
            cur.execute('select name from %sbooze' % self.table_prefix)
            self.assertEqual(len(cur.description), 1,
                             'cursor.description describes too many columns'
                             )
            self.assertEqual(len(cur.description[0]), 7,
                             'cursor.description[x] tuples must have 7 elements'
                             )
            self.assertEqual(cur.description[0][0].lower(), 'name',
                             'cursor.description[x][0] must return column name'
                             )
            self.assertEqual(cur.description[0][1], pymonetdb.STRING,
                             'cursor.description[x][1] must return column type. Got %r'
                             % cur.description[0][1]
                             )

            # Make sure self.description gets reset
            self.executeDDL2(cur)
            self.assertEqual(cur.description, None,
                             'cursor.description not being set to None when executing '
                             'no-result statements (eg. DDL)'
                             )
        finally:
            con.close()

    def test_rowcount(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL1(cur)
            self.assertEqual(cur.rowcount, -1,
                             'cursor.rowcount should be -1 after executing no-result '
                             'statements'
                             )
            cur.execute("insert into %sbooze values ('Victoria Bitter')" % (
                self.table_prefix
            ))
            self.assertTrue(cur.rowcount in (-1, 1),
                            'cursor.rowcount should == number or rows inserted, or '
                            'set to -1 after executing an insert statement'
                            )
            cur.execute("select name from %sbooze" % self.table_prefix)
            self.assertTrue(cur.rowcount in (-1, 1),
                            'cursor.rowcount should == number of rows returned, or '
                            'set to -1 after executing a select statement'
                            )
            self.executeDDL2(cur)
            self.assertEqual(cur.rowcount, -1,
                             'cursor.rowcount not being reset to -1 after executing '
                             'no-result statements'
                             )
        finally:
            con.close()

    lower_func = 'lower'

    def test_callproc(self):
        con = self._connect()
        try:
            cur = con.cursor()
            if self.lower_func and hasattr(cur, 'callproc'):
                r = cur.callproc(self.lower_func, ('FOO',))
                self.assertEqual(len(r), 1)
                self.assertEqual(r[0], 'FOO')
                r = cur.fetchall()
                self.assertEqual(len(r), 1, 'callproc produced no result set')
                self.assertEqual(len(r[0]), 1,
                                 'callproc produced invalid result set'
                                 )
                self.assertEqual(r[0][0], 'foo',
                                 'callproc produced invalid results'
                                 )
        finally:
            con.close()

    def test_close(self):
        con = self._connect()
        try:
            cur = con.cursor()
        finally:
            con.close()

        # cursor.execute should raise an Error if called after connection
        # closed
        self.assertRaises(pymonetdb.Error, self.executeDDL1, cur)

        # connection.commit should raise an Error if called after connection'
        # closed.'
        self.assertRaises(pymonetdb.Error, con.commit)

        # connection.close should raise an Error if called more than once
        self.assertRaises(pymonetdb.Error, con.close)

    def test_execute(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self._paraminsert(cur)
        finally:
            con.close()

    def _paraminsert(self, cur):
        self.executeDDL1(cur)
        cur.execute("insert into %sbooze values ('Victoria Bitter')" % (
            self.table_prefix
        ))
        self.assertTrue(cur.rowcount in (-1, 1))

        if pymonetdb.paramstyle == 'qmark':
            cur.execute(
                'insert into %sbooze values (?)' % self.table_prefix,
                ("Cooper's",)
            )
        elif pymonetdb.paramstyle == 'numeric':
            cur.execute(
                'insert into %sbooze values (:1)' % self.table_prefix,
                ("Cooper's",)
            )
        elif pymonetdb.paramstyle == 'named':
            cur.execute(
                'insert into %sbooze values (:beer)' % self.table_prefix,
                {'beer': "Cooper's"}
            )
        elif pymonetdb.paramstyle == 'format':
            cur.execute(
                'insert into %sbooze values (%%s)' % self.table_prefix,
                ("Cooper's",)
            )
        elif pymonetdb.paramstyle == 'pyformat':
            cur.execute(
                'insert into %sbooze values (%%(beer)s)' % self.table_prefix,
                {'beer': "Cooper's"}
            )
        else:
            self.fail('Invalid paramstyle')
        self.assertTrue(cur.rowcount in (-1, 1))

        cur.execute('select name from %sbooze' % self.table_prefix)
        res = cur.fetchall()
        self.assertEqual(len(res), 2, 'cursor.fetchall returned too few rows')
        beers = [res[0][0], res[1][0]]
        beers.sort()
        self.assertEqual(beers[0], "Cooper's",
                         'cursor.fetchall retrieved incorrect data, or data inserted '
                         'incorrectly'
                         )
        self.assertEqual(beers[1], "Victoria Bitter",
                         'cursor.fetchall retrieved incorrect data, or data inserted '
                         'incorrectly'
                         )

    def test_executemany(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL1(cur)
            largs = [("Cooper's",), ("Boag's",)]
            margs = [{'beer': "Cooper's"}, {'beer': "Boag's"}]
            if pymonetdb.paramstyle == 'qmark':
                cur.executemany(
                    'insert into %sbooze values (?)' % self.table_prefix,
                    largs
                )
            elif pymonetdb.paramstyle == 'numeric':
                cur.executemany(
                    'insert into %sbooze values (:1)' % self.table_prefix,
                    largs
                )
            elif pymonetdb.paramstyle == 'named':
                cur.executemany(
                    'insert into %sbooze values (:beer)' % self.table_prefix,
                    margs
                )
            elif pymonetdb.paramstyle == 'format':
                cur.executemany(
                    'insert into %sbooze values (%%s)' % self.table_prefix,
                    largs
                )
            elif pymonetdb.paramstyle == 'pyformat':
                cur.executemany(
                    'insert into %sbooze values (%%(beer)s)' % (
                        self.table_prefix
                    ),
                    margs
                )
            else:
                self.fail('Unknown paramstyle')
            self.assertTrue(cur.rowcount in (-1, 2),
                            'insert using cursor.executemany set cursor.rowcount to '
                            'incorrect value %r' % cur.rowcount
                            )
            cur.execute('select name from %sbooze' % self.table_prefix)
            res = cur.fetchall()
            self.assertEqual(len(res), 2,
                             'cursor.fetchall retrieved incorrect number of rows'
                             )
            beers = [res[0][0], res[1][0]]
            beers.sort()
            self.assertEqual(beers[0], "Boag's", 'incorrect data retrieved')
            self.assertEqual(beers[1], "Cooper's", 'incorrect data retrieved')
        finally:
            con.close()

    def test_fetchone(self):
        con = self._connect()
        try:
            cur = con.cursor()

            # cursor.fetchone should raise an Error if called before
            # executing a select-type query
            self.assertRaises(pymonetdb.Error, cur.fetchone)

            # cursor.fetchone should raise an Error if called after
            # executing a query that cannot return rows
            self.executeDDL1(cur)
            self.assertRaises(pymonetdb.Error, cur.fetchone)

            cur.execute('select name from %sbooze' % self.table_prefix)
            self.assertEqual(cur.fetchone(), None,
                             'cursor.fetchone should return None if a query retrieves '
                             'no rows'
                             )
            self.assertTrue(cur.rowcount in (-1, 0))

            # cursor.fetchone should raise an Error if called after
            # executing a query that cannot return rows
            cur.execute("insert into %sbooze values ('Victoria Bitter')" % (
                self.table_prefix
            ))
            self.assertRaises(pymonetdb.Error, cur.fetchone)

            cur.execute('select name from %sbooze' % self.table_prefix)
            r = cur.fetchone()
            self.assertEqual(len(r), 1,
                             'cursor.fetchone should have retrieved a single row'
                             )
            self.assertEqual(r[0], 'Victoria Bitter',
                             'cursor.fetchone retrieved incorrect data'
                             )
            self.assertEqual(cur.fetchone(), None,
                             'cursor.fetchone should return None if no more rows available'
                             )
            self.assertTrue(cur.rowcount in (-1, 1))
        finally:
            con.close()

    samples = [
        'Carlton Cold',
        'Carlton Draft',
        'Mountain Goat',
        'Redback',
        'Victoria Bitter',
        'XXXX'
    ]

    def _populate(self):
        """ Return a list of sql commands to setup the DB for the fetch
            tests.
        """
        populate = [
            "insert into %sbooze values ('%s')" % (self.table_prefix, s)
            for s in self.samples
        ]
        return populate

    def test_cursor_next(self):
        con = self._connect()
        try:
            cur = con.cursor()

            self.executeDDL1(cur)
            for sql in self._populate():
                cur.execute(sql)

            cur.execute('select name from %sbooze' % self.table_prefix)

            for _ in cur:
                pass

        except TypeError:
            self.fail("Cursor iterator not implemented correctly")

        finally:
            con.close()

    def test_fetchmany(self):
        con = self._connect()
        try:
            cur = con.cursor()

            # cursor.fetchmany should raise an Error if called without
            # issuing a query
            self.assertRaises(pymonetdb.Error, cur.fetchmany, 4)

            self.executeDDL1(cur)
            for sql in self._populate():
                cur.execute(sql)

            cur.execute('select name from %sbooze' % self.table_prefix)
            # our default is not one since that is slow
            cur.arraysize = 1
            r = cur.fetchmany()
            self.assertEqual(len(r), 1,
                             'cursor.fetchmany retrieved incorrect number of rows, '
                             'default of arraysize is one.'
                             )
            cur.arraysize = 10
            r = cur.fetchmany(3)  # Should get 3 rows
            self.assertEqual(len(r), 3,
                             'cursor.fetchmany retrieved incorrect number of rows'
                             )
            r = cur.fetchmany(4)  # Should get 2 more
            self.assertEqual(len(r), 2,
                             'cursor.fetchmany retrieved incorrect number of rows'
                             )
            r = cur.fetchmany(4)  # Should be an empty sequence
            self.assertEqual(len(r), 0,
                             'cursor.fetchmany should return an empty sequence after '
                             'results are exhausted'
                             )
            self.assertTrue(cur.rowcount in (-1, 6))

            # Same as above, using cursor.arraysize
            cur.arraysize = 4
            cur.execute('select name from %sbooze' % self.table_prefix)
            r = cur.fetchmany()  # Should get 4 rows
            self.assertEqual(len(r), 4,
                             'cursor.arraysize not being honoured by fetchmany'
                             )
            r = cur.fetchmany()  # Should get 2 more
            self.assertEqual(len(r), 2)
            r = cur.fetchmany()  # Should be an empty sequence
            self.assertEqual(len(r), 0)
            self.assertTrue(cur.rowcount in (-1, 6))

            cur.arraysize = 6
            cur.execute('select name from %sbooze' % self.table_prefix)
            rows = cur.fetchmany()  # Should get all rows
            self.assertTrue(cur.rowcount in (-1, 6))
            self.assertEqual(len(rows), 6)
            self.assertEqual(len(rows), 6)
            rows = [r[0] for r in rows]
            rows.sort()

            # Make sure we get the right data back out
            for i in range(0, 6):
                self.assertEqual(rows[i], self.samples[i],
                                 'incorrect data retrieved by cursor.fetchmany'
                                 )

            rows = cur.fetchmany()  # Should return an empty list
            self.assertEqual(len(rows), 0,
                             'cursor.fetchmany should return an empty sequence if '
                             'called after the whole result set has been fetched'
                             )
            self.assertTrue(cur.rowcount in (-1, 6))

            self.executeDDL2(cur)
            cur.execute('select name from %sbarflys' % self.table_prefix)
            r = cur.fetchmany()  # Should get empty sequence
            self.assertEqual(len(r), 0,
                             'cursor.fetchmany should return an empty sequence if '
                             'query retrieved no rows'
                             )
            self.assertTrue(cur.rowcount in (-1, 0))

        finally:
            con.close()

    def test_fetchall(self):
        con = self._connect()
        try:
            cur = con.cursor()
            # cursor.fetchall should raise an Error if called
            # without executing a query that may return rows (such
            # as a select)
            self.assertRaises(pymonetdb.Error, cur.fetchall)

            self.executeDDL1(cur)
            for sql in self._populate():
                cur.execute(sql)

            # cursor.fetchall should raise an Error if called
            # after executing a a statement that cannot return rows
            self.assertRaises(pymonetdb.Error, cur.fetchall)

            cur.execute('select name from %sbooze' % self.table_prefix)
            rows = cur.fetchall()
            self.assertTrue(cur.rowcount in (-1, len(self.samples)))
            self.assertEqual(len(rows), len(self.samples),
                             'cursor.fetchall did not retrieve all rows'
                             )
            rows = [r[0] for r in rows]
            rows.sort()
            for i in range(0, len(self.samples)):
                self.assertEqual(rows[i], self.samples[i],
                                 'cursor.fetchall retrieved incorrect rows'
                                 )
            rows = cur.fetchall()
            self.assertEqual(
                len(rows), 0,
                'cursor.fetchall should return an empty list if called '
                'after the whole result set has been fetched'
            )
            self.assertTrue(cur.rowcount in (-1, len(self.samples)))

            self.executeDDL2(cur)
            cur.execute('select name from %sbarflys' % self.table_prefix)
            rows = cur.fetchall()
            self.assertTrue(cur.rowcount in (-1, 0))
            self.assertEqual(len(rows), 0,
                             'cursor.fetchall should return an empty list if '
                             'a select query returns no rows'
                             )

        finally:
            con.close()

    def test_mixedfetch(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL1(cur)
            for sql in self._populate():
                cur.execute(sql)

            cur.execute('select name from %sbooze' % self.table_prefix)
            rows1 = cur.fetchone()
            rows23 = cur.fetchmany(2)
            rows4 = cur.fetchone()
            rows56 = cur.fetchall()
            self.assertTrue(cur.rowcount in (-1, 6))
            self.assertEqual(len(rows23), 2,
                             'fetchmany returned incorrect number of rows'
                             )
            self.assertEqual(len(rows56), 2,
                             'fetchall returned incorrect number of rows'
                             )

            rows = [rows1[0]]
            rows.extend([rows23[0][0], rows23[1][0]])
            rows.append(rows4[0])
            rows.extend([rows56[0][0], rows56[1][0]])
            rows.sort()
            for i in range(0, len(self.samples)):
                self.assertEqual(rows[i], self.samples[i],
                                 'incorrect data retrieved or inserted'
                                 )
        finally:
            con.close()

    def test_arraysize(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.assertTrue(hasattr(cur, 'arraysize'),
                            'cursor.arraysize must be defined'
                            )
        finally:
            con.close()

    def test_setinputsizes(self):
        con = self._connect()
        try:
            cur = con.cursor()
            cur.setinputsizes((25,))
            self._paraminsert(cur)  # Make sure cursor still works
        finally:
            con.close()

    def test_setoutputsize_basic(self):
        # Basic test is to make sure setoutputsize doesn't blow up
        con = self._connect()
        try:
            cur = con.cursor()
            cur.setoutputsize(1000)
            cur.setoutputsize(2000, 0)
            self._paraminsert(cur)  # Make sure the cursor still works
        finally:
            con.close()

    def test_None(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL1(cur)
            cur.execute('insert into %sbooze values (NULL)' % self.table_prefix)
            cur.execute('select name from %sbooze' % self.table_prefix)
            r = cur.fetchall()
            self.assertEqual(len(r), 1)
            self.assertEqual(len(r[0]), 1)
            self.assertEqual(r[0][0], None, 'NULL value not returned as None')
        finally:
            con.close()

    def test_Date(self):
        d1 = pymonetdb.Date(2002, 12, 25)
        d2 = pymonetdb.DateFromTicks(time.mktime((2002, 12, 25, 0, 0, 0, 0, 0, 0)))
        self.assertEqual(str(d1), str(d2))

    def test_Time(self):
        t1 = pymonetdb.Time(13, 45, 30)
        t2 = pymonetdb.TimeFromTicks(time.mktime((2001, 1, 1, 13, 45, 30, 0, 0, 0)))
        self.assertEqual(str(t1), str(t2))

    def test_Timestamp(self):
        t1 = pymonetdb.Timestamp(2002, 12, 25, 13, 45, 30)
        t2 = pymonetdb.TimestampFromTicks(
            time.mktime((2002, 12, 25, 13, 45, 30, 0, 0, 0))
        )
        self.assertEqual(str(t1), str(t2))

    def test_Binary(self):
        b1 = b'\x00\x01\x02\x03'
        b2 = pymonetdb.Binary(b1)
        self.assertEqual(b1, b2)

    def test_STRING(self):
        self.assertTrue(hasattr(pymonetdb, 'STRING'), 'module.STRING must be defined')

    def test_BINARY(self):
        self.assertTrue(hasattr(pymonetdb, 'BINARY'), 'module.BINARY must be defined.')

    def test_NUMBER(self):
        self.assertTrue(hasattr(pymonetdb, 'NUMBER'), 'module.NUMBER must be defined.')

    def test_DATETIME(self):
        self.assertTrue(hasattr(pymonetdb, 'DATETIME'), 'module.DATETIME must be defined.')

    def test_ROWID(self):
        self.assertTrue(hasattr(pymonetdb, 'ROWID'), 'module.ROWID must be defined.')
