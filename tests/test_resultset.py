# coding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

from abc import abstractmethod
import datetime
from decimal import ROUND_HALF_UP, Decimal
from random import Random
from typing import Any, Callable, List, Optional, Tuple
from unittest import SkipTest, TestCase
from uuid import UUID
import pymonetdb
from tests.util import have_monetdb_version_at_least, test_args

QUERY_TEMPLATE = """\
WITH resultset AS (
    SELECT
        %(exprs)s,
        42 AS dummy
    FROM sys.generate_series(0, %(count)s - 1)
),
t AS (SELECT 42 AS dummy UNION SELECT 43)
--
SELECT * FROM
    resultset RIGHT OUTER JOIN t
    ON resultset.dummy = t.dummy;
"""


def decimal_column(p, s):
    dec = f"DECIMAL({p}, {s})"
    expr = f"CAST(CAST(value AS {dec}) * 1.5 AS {dec})"
    # MonetDB rounds (N + 0.5) away from zero
    quantum = Decimal('10') ** (-s)

    def verifier(n):
        return (Decimal(n) * 3 / 2).quantize(quantum, rounding=ROUND_HALF_UP)

    return (expr, verifier)


test_uuid = UUID('{12345678-1234-5678-1234-567812345678}')
test_blobs = {
    0: b'MONETDB',
    1: b'',
    2: None,
}


def seconds_timedelta_helper(value, multiplier):
    t = Decimal(multiplier) * value
    millis = int(1000 * t)
    return datetime.timedelta(milliseconds=millis)


TEST_COLUMNS = dict(
    int_col=("CAST(value AS int)", lambda n: n),
    tinyint_col=("CAST(value % 128 AS tinyint)", lambda n: n % 128),
    smallint_col=("CAST(value AS smallint)", lambda n: n),
    bigint_col=("CAST(value AS bigint)", lambda n: n),
    # hugeint_col=("CAST(value AS hugeint)", lambda n: n),    text_col=("'v' || value", lambda n: f"v{n}"),
    text_col=("'v' || value", lambda n: f"v{n}"),
    varchar_col=("CAST('v' || value AS VARCHAR(10))", lambda n: f"v{n}"),
    bool_col=("(value % 2 = 0)", lambda n: (n % 2) == 0),
    decimal_col=decimal_column(5, 2),
    real_col=("CAST(value AS REAL) / 2", lambda x: x / 2),
    float_col=("CAST(value AS FLOAT) / 2", lambda x: x / 2),
    double_col=("CAST(value AS DOUBLE) / 2", lambda x: x / 2),
    f32_col=("CAST(value AS float(24)) / 2", lambda x: x / 2),
    f53_col=("CAST(value AS float(53)) / 2", lambda x: x / 2),
    blob_col=(
        "CAST((CASE WHEN value % 3 = 0 THEN '4d4f4e45544442' WHEN value % 3 = 1 THEN '' ELSE NULL END) AS BLOB)",
        lambda x: test_blobs[x % 3]),
    months_col=("CAST(CAST(value AS TEXT) AS INTERVAL MONTH)", lambda x: x),
    days_col=("CAST(CAST(value AS TEXT) AS INTERVAL DAY) * 1.007", lambda x: int(x * Decimal('1.007'))),
    seconds_col=("CAST(CAST(value AS TEXT) AS INTERVAL SECOND) * 1.007", lambda x: seconds_timedelta_helper(x, '1.007')),
    # not a very dynamic example:
    uuid_col=("CAST('12345678-1234-5678-1234-567812345678' AS UUID)", lambda x: test_uuid)
)

# Some versions of MonetDB have a bug where the output is wrong if more than one
# of the following is present in the result set at the same time.
# We'll test them separately
BLACKLIST = set(['months_col', 'days_col', 'seconds_col'])


class BaseTestCases(TestCase):
    _server_binexport_level: Optional[int] = None
    _server_has_huge: Optional[bool] = None
    conn: Optional[pymonetdb.Connection] = None
    cursor: Optional[pymonetdb.sql.cursors.Cursor] = None
    cur: int = 0
    rowcount: int = 0
    verifiers: List[Tuple[str, Callable[[int], Any]]] = []
    colnames: List[str] = []

    def probe_server(self):
        if self._server_binexport_level is not None:
            return
        conn = self.connect_with_args()
        self._server_binexport_level = conn.mapi.binexport_level
        cursor = conn.cursor()
        cursor.execute("SELECT sqlname FROM sys.types WHERE sqlname = 'hugeint'")
        self._server_has_huge = cursor.rowcount > 0
        cursor.close()
        conn.close()

    def have_binary(self, at_least=1):
        self.probe_server()
        return self._server_binexport_level >= at_least

    def server_has_new_time_conversion(self):
        return have_monetdb_version_at_least(11, 50, 0)

    def have_huge(self):
        self.probe_server()
        return self._server_has_huge

    def skip_unless_have_binary(self):
        if not self.have_binary():
            raise SkipTest("need server with support for binary")

    def skip_unless_have_huge(self):
        if not self.have_huge():
            raise SkipTest("need server with support for hugeint")

    def setUp(self):
        self.cur = 0
        self.rowcount = 0
        self.to_close = []

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None

    def tearDown(self):
        if self.cursor:
            self.cursor.execute("ROLLBACK")
        for c in self.to_close:
            try:
                c.close()
            except Exception:
                pass

    def connect_with_args(self, **kw_args) -> pymonetdb.Connection:
        try:
            args = dict()
            args.update(test_args)
            args.update(kw_args)
            conn = pymonetdb.connect(**args)   # type: ignore
        except AttributeError:
            self.fail("No connect method found in pymonetdb module")
        self.to_close.append(conn)
        with conn.cursor() as c:
            c.execute("SELECT %s", f"This connection is for test {self.id()!r}")
        return conn

    @abstractmethod
    def setup_connection(self) -> Tuple[pymonetdb.Connection, Optional[int]]:
        assert False

    def assertAtEnd(self):
        self.assertEqual(self.rowcount, self.cur, f"expected to be at end ({self.rowcount} rows), not {self.cur}")

    def do_connect(self):
        if self.conn is None:
            assert self.cursor is None
            self.conn, self.expect_binary_after = self.setup_connection()
            self.cursor = self.conn.cursor()

    def construct_query(self, n, cols):
        if isinstance(cols, dict):
            test_columns = cols
        else:
            test_columns = dict()
            for col in cols:
                test_columns[col] = TEST_COLUMNS[col]

        exprs = []
        verifiers = []
        colnames = []
        for col, (expr, verifier) in test_columns.items():
            exprs.append(f"{expr} AS {col}")
            verifiers.append(verifier)
            colnames.append(col)
        query = QUERY_TEMPLATE % dict(
            exprs=",\n        ".join(exprs),
            count=n
        )

        return query, colnames, verifiers

    def set_expected_results(self, rowcount, colnames, verifiers):
        self.cur = 0
        self.rowcount = rowcount
        self.colnames = colnames
        self.verifiers = verifiers

    def do_query(self, n, cols=('int_col',)):
        query, colnames, verifiers = self.construct_query(n, cols)

        self.do_connect()
        self.cursor.execute(query)
        self.set_expected_results(n, colnames, verifiers)

        self.assertEqual(n, self.cursor.rowcount)

    def verifyRow(self, n, row):
        # two dummy columns because of the outer join
        self.assertEqual(len(row) - 2, len(self.verifiers))

        if n == self.rowcount - 1:
            expected = len(self.verifiers) * (None,)
        else:
            expected = tuple(verifier(n) for verifier in self.verifiers)

        found = row[:len(self.verifiers)]
        if found == expected:
            return

        for i in range(len(self.verifiers)):
            if found[i] == expected[i]:
                continue
            self.assertEqual(expected[i], found[i], f"Mismatch at row {n}, col {i} '{self.colnames[i]}'")

        self.assertEqual(expected, found, f"Mismatch at row {n}")

    def verifyBinary(self):
        if not self.have_binary():
            return
        if self.expect_binary_after is None:
            return
        if self.cur <= self.expect_binary_after:
            return
        # with this many rows, binary should have been used
        self.assertTrue(self.cursor.used_binary_protocol(), "Expected binary result sets to be used")

    def do_fetchone(self):
        row = self.cursor.fetchone()
        if self.cur < self.rowcount:
            self.assertIsNotNone(row)
            self.verifyRow(self.cur, row)
            self.cur += 1
        else:
            self.assertIsNone(row)
        self.verifyBinary()

    def do_fetchmany(self, n):
        rows = self.cursor.fetchmany(n)
        if n is not None:
            expectedRows = min(n, self.rowcount - self.cur)
            self.assertEqual(expectedRows, len(rows))
        for i, row in enumerate(rows):
            self.verifyRow(self.cur + i, row)
        self.cur += len(rows)
        self.verifyBinary()

    def do_fetchall(self) -> int:
        oldcur = self.cur
        assert self.cursor
        rows = self.cursor.fetchall()
        expectedRows = self.rowcount - self.cur
        self.assertEqual(expectedRows, len(rows))
        for i, row in enumerate(rows):
            self.verifyRow(self.cur + i, row)
        self.cur += len(rows)
        self.verifyBinary()
        self.assertAtEnd()
        return self.cur - oldcur

    def do_scroll(self, n, mode):
        self.cursor.scroll(n, mode)
        if mode == 'absolute':
            self.cur = n
        elif mode == 'relative':
            self.cur += n
        else:
            raise self.failureException(f"unexpected mode {mode}")

    def test_fetchone(self, n=1000):
        self.do_query(n)
        for i in range(self.rowcount + 1):
            self.do_fetchone()
        self.assertAtEnd()

    def test_fetchmany(self, n=1000):
        self.do_query(n)
        for i in range(n):
            self.do_fetchmany(None)
        self.assertAtEnd()

    def test_fetchmany42(self, n=1000):
        self.do_query(n)
        for i in range(self.rowcount // 42 + 1):
            self.do_fetchmany(42)
        self.assertAtEnd()

    def test_fetchmany120(self, n=1000):
        self.do_query(n)
        for i in range(self.rowcount // 120 + 1):
            self.do_fetchmany(120)
        self.assertAtEnd()

    def test_fetchall(self, n=1000):
        self.do_query(n)
        self.do_fetchall()
        self.assertAtEnd()

    def test_fetchone_large(self):
        self.test_fetchone(25_000)

    def test_fetchmany_large(self):
        self.test_fetchmany(100_000)

    def test_fetchmany42_large(self):
        self.test_fetchmany42(100_000)

    def test_fetchmany120_large(self):
        self.test_fetchmany120(100_000)

    def test_fetchall_large(self):
        self.test_fetchall(100_000)

    def test_scroll(self):
        rng = Random()
        rng.seed(42)
        self.do_query(1000)
        for _ in range(500):
            x = rng.randrange(0, self.rowcount)
            y = rng.randrange(0, self.rowcount)
            if x > y:
                (x, y) = (y, x)
            if rng.randrange(0, 10) >= 2:
                y = rng.randrange(x, min(y, self.rowcount))
            if rng.randrange(0, 2) > 0:
                self.do_scroll(x, 'absolute')
            else:
                self.do_scroll(x - self.cur, 'relative')
            self.do_fetchmany(y - x)

    def test_multiple(self):
        self.do_connect()

        nrows = 20
        query1, colnames1, verifiers1 = self.construct_query(nrows, ['int_col'])
        query2, colnames2, verifiers2 = self.construct_query(2 * nrows, ['tinyint_col', 'text_col'])

        def access_pattern(self, n):
            self.do_fetchone()
            self.do_fetchmany(n // 2)
            self.do_scroll(n // 4, 'absolute')
            remaining = self.do_fetchall()
            self.assertEqual(remaining, n // 4 * 3)
            self.verifyBinary()
            self.assertAtEnd()

        query = f"{query1};\n{query2}"
        self.cursor.execute(query)

        self.set_expected_results(nrows, colnames1, verifiers1)
        access_pattern(self, nrows)

        self.assertTrue(self.cursor.nextset())
        self.set_expected_results(2 * nrows, colnames2, verifiers2)
        access_pattern(self, 2 * nrows)

        self.assertFalse(self.cursor.nextset())

    def test_huge(self):
        self.skip_unless_have_huge()
        max_value = (1 << 127) - 1
        min_value = - max_value
        columns = dict(
            up_from_0=("CAST(value AS hugeint)", lambda n: n),
            down_from_0=("-CAST(value AS hugeint)", lambda n: -n),
            up_from_min=(f"{min_value} + CAST(value AS hugeint)", lambda n: min_value + n),
            down_from_max=(f"{max_value} - CAST(value AS hugeint)", lambda n: max_value - n),
        )
        self.do_query(5, columns)
        self.do_fetchall()
        self.verifyBinary()

    def test_data_types(self):
        self.do_query(250, TEST_COLUMNS.keys() - BLACKLIST)
        self.do_fetchall()
        # no self.verifyBinary()

    def test_binary_data_types(self):
        self.skip_unless_have_binary()
        self.do_query(250, TEST_COLUMNS.keys() - BLACKLIST)
        self.do_fetchall()
        self.verifyBinary()

    def test_decimal_types(self):
        cases = set()
        widest = 39 if self.have_huge() else 18
        for p in range(1, widest):
            cases.add((p, 0))
            cases.add((p, min(3, p - 1)))

        test_columns = dict()
        for p, s in sorted(cases):
            name = f"dec_{p}_{s}"
            maker = decimal_column(p, s)
            test_columns[name] = maker

        # only get 6 rows because 1.5 * n must fit in DECIMAL(1,0)
        self.do_query(6, test_columns)
        self.do_fetchall()
        self.verifyBinary()

    def prepare_temporal_tests(self, col):
        self.do_connect()
        minutes_east = 60 + 30  # easily recognizable
        self.conn.set_timezone(60 * minutes_east)
        self.conn.set_autocommit(False)

        our_timezone = datetime.timezone(datetime.timedelta(hours=1, minutes=30))

        self.cursor.execute("DROP TABLE IF EXISTS foo")
        cols = [
            "name TEXT",
            "ts_with TIMESTAMPTZ",
            "ts_without TIMESTAMP",
            "d DATE",
            "t_with TIMETZ",
            "t_without TIME",
        ]
        create_statement = f"CREATE TABLE foo ({(', '.join(cols))})"
        self.cursor.execute(create_statement)
        interesting_times = [
            ('dummy', "NULL"),

            ('null', "NULL"),
            # MonetDB will normalize the following two into the exact same thing
            ('apollo13_utc', "TIMESTAMPTZ '1970-04-17 18:07:41+00:00'"),
            ('apollo13_pacific', "TIMESTAMPTZ '1970-04-17 10:07:41-08:00'"),
        ]
        insert_statement = (
            "INSERT INTO foo(name, ts_with) VALUES "
            + ", ".join(f"('{name}', {expr})" for name, expr in interesting_times)
        )
        self.cursor.execute(insert_statement)
        self.cursor.execute("UPDATE foo set ts_without = ts_with, d = ts_with, t_with = ts_with, t_without = ts_with")

        self.cursor.execute(f"SELECT name, {col} FROM foo")
        rows = self.cursor.fetchall()

        # needed by verifyBinary
        # update cur manually because it is set by do_fetchall but not
        # by cursor.fetchall
        self.cur = self.cursor.rowcount

        values = dict()
        for row in rows:
            name = row[0]
            # make REALLY REALLY sure the indices match the order in the SELECT clause!
            values[name] = row[1]

        self.verifyBinary()
        return (our_timezone, values)

    def test_temporal_timestamp_with_timezone(self):
        (tz, values) = self.prepare_temporal_tests('ts_with')
        self.assertIsNone(values['null'])

        # apollo13_utc was given as 1970-04-17 18:07:41+00:00,
        # stored in ts_with as 1970-04-17 18:07:41 UTC,
        # rendered on the wire as 1970-04-17 19:37+01:30,
        # converted to a DateTime of 19:37 in the +01:30 time zone
        x = values['apollo13_utc']
        self.assertEqual('1970-04-17T19:37:41+01:30', x.isoformat())
        self.assertEqual(tz, x.tzinfo)

        # apollo13_pacific was given as 1970-04-17 10:07:41-08:00,
        # stored in ts_with as 1970-04-17 18:07:41 UTC
        # rendered on the wire as 1970-04-17 19:37+01:30
        # converted to a DateTime of 19:37 in the +01:30 time zone
        x = values['apollo13_pacific']
        self.assertEqual('1970-04-17T19:37:41+01:30', x.isoformat())
        self.assertEqual(tz, x.tzinfo)

    def test_temporal_timestamp_without_timezone_old(self):
        # the difference between _old and _new is marked with an arrow <---
        if self.server_has_new_time_conversion():
            raise SkipTest("test only applies to old MonetDB versions")

        (tz, values) = self.prepare_temporal_tests('ts_without')
        self.assertIsNone(values['null'])

        # apollo13_utc was given as 1970-04-17 18:07:41+00:00,
        # stored in ts_with as 1970-04-17 18:07:41 UTC,
        # then stored in ts_without as 1970-04-17 18:07:41,   <---
        # rendered on the wire as 1970-04-17 18:07:41,
        # converted to a DateTime of 18:07 without timezone
        x = values['apollo13_utc']
        self.assertEqual('1970-04-17T18:07:41', x.isoformat())
        self.assertIsNone(x.tzinfo)

        # apollo13_pacific was given as 1970-04-17 10:07:41-08:00,
        # stored in ts_with as 1970-04-17 18:07:41 UTC,
        # then stored in ts_without as 1970-04-17 18:07:41,   <---
        # rendered on the wire as 1970-04-17 18:07:41,
        # converted to a DateTime of 18:07 without timezone
        x = values['apollo13_pacific']
        self.assertEqual('1970-04-17T18:07:41', x.isoformat())
        self.assertIsNone(x.tzinfo)

    def test_temporal_timestamp_without_timezone_new(self):
        # the difference between _old and _new is marked with an arrow <---
        if not self.server_has_new_time_conversion():
            raise SkipTest("test only applies to new MonetDB versions")

        (tz, values) = self.prepare_temporal_tests('ts_without')
        self.assertIsNone(values['null'])

        # apollo13_utc was given as 1970-04-17 18:07:41+00:00,
        # stored in ts_with as 1970-04-17 18:07:41 UTC,
        # then stored in ts_without as 1970-04-17 19:37:41,   <---
        # rendered on the wire as 1970-04-17 19:37:41,
        # converted to a DateTime of 19:37 without timezone
        x = values['apollo13_utc']
        self.assertEqual('1970-04-17T19:37:41', x.isoformat())
        self.assertIsNone(x.tzinfo)

        # apollo13_pacific was given as 1970-04-17 10:07:41-08:00,
        # stored in ts_with as 1970-04-17 18:07:41 UTC,
        # then stored in ts_without as 1970-04-17 19:37:41,   <---
        # rendered on the wire as 1970-04-17 19:37:41,
        # converted to a DateTime of 19:37 without timezone
        x = values['apollo13_pacific']
        self.assertEqual('1970-04-17T19:37:41', x.isoformat())
        self.assertIsNone(x.tzinfo)

    def test_temporal_date(self):
        (tz, values) = self.prepare_temporal_tests('d')
        self.assertIsNone(values['null'])

        # apollo13_utc was given as 1970-04-17 18:07:41+00:00,
        # stored in ts_with as 1970-04-17 18:07:41 UTC,
        # then stored in d as 1970-04-17,
        # rendered on the wire as 1970-04-17,
        # converted to a Date of 1970-04-17
        x = values['apollo13_utc']
        self.assertEqual('1970-04-17', x.isoformat())

        # apollo13_pacific was given as 1970-04-17 10:07:41-08:00,
        # stored in ts_with as 1970-04-17 18:07:41 UTC,
        # then stored in d as 1970-04-17,
        # rendered on the wire as 1970-04-17,
        # converted to a Date of 1970-04-17
        x = values['apollo13_pacific']
        self.assertEqual('1970-04-17', x.isoformat())

    def test_temporal_time_with_timezone(self):
        (tz, values) = self.prepare_temporal_tests('t_with')
        self.assertIsNone(values['null'])

        # apollo13_utc was given as 1970-04-17 18:07:41+00:00,
        # stored in ts_with as 1970-04-17 18:07:41 UTC,
        # then stored in t_with as 18:07:41 UTC,
        # rendered on the wire as 19:37+01:30,
        # converted to a DateTime of 19:37 in the +01:30 time zone
        x = values['apollo13_utc']
        self.assertEqual('19:37:41+01:30', x.isoformat())
        self.assertEqual(tz, x.tzinfo)

        # apollo13_pacific was given as 1970-04-17 10:07:41-08:00,
        # stored in ts_with as 1970-04-17 18:07:41 UTC
        # then stored in t_with as 18:07:41 UTC,
        # rendered on the wire as 19:37+01:30,
        # converted to a DateTime of 19:37 in the +01:30 time zone
        x = values['apollo13_pacific']
        self.assertEqual('19:37:41+01:30', x.isoformat())
        self.assertEqual(tz, x.tzinfo)

    def test_temporal_time_without_timezone_old(self):
        # the difference between _old and _new is marked with an arrow <---
        if self.server_has_new_time_conversion():
            raise SkipTest("test only applies to old MonetDB versions")

        (tz, values) = self.prepare_temporal_tests('t_without')
        self.assertIsNone(values['null'])

        # apollo13_utc was given as 1970-04-17 18:07:41+00:00,
        # stored in ts_with as 1970-04-17 18:07:41 UTC,
        # then stored in t_without as 18:07:41,          <---
        # rendered on the wire as 18:07:41,
        # converted to a DateTime of 18:07 without timezone
        x = values['apollo13_utc']
        self.assertEqual('18:07:41', x.isoformat())
        self.assertIsNone(x.tzinfo)

        # apollo13_pacific was given as 1970-04-17 10:07:41-08:00,
        # stored in ts_with as 1970-04-17 18:07:41 UTC,
        # then stored in t_without as 18:07:41,          <---
        # rendered on the wire as 18:07:41,
        # converted to a DateTime of 18:07 without timezone
        x = values['apollo13_pacific']
        self.assertEqual('18:07:41', x.isoformat())
        self.assertIsNone(x.tzinfo)

    def test_temporal_time_without_timezone_new(self):
        # the difference between _old and _new is marked with an arrow <---
        if not self.server_has_new_time_conversion():
            raise SkipTest("test only applies to new MonetDB versions")

        (tz, values) = self.prepare_temporal_tests('t_without')
        self.assertIsNone(values['null'])

        # apollo13_utc was given as 1970-04-17 18:07:41+00:00,
        # stored in ts_with as 1970-04-17 18:07:41 UTC,
        # then stored in t_without as 19:37:41,          <---
        # rendered on the wire as 19:37:41,
        # converted to a DateTime of 19:37 without timezone
        x = values['apollo13_utc']
        self.assertEqual('19:37:41', x.isoformat())
        self.assertIsNone(x.tzinfo)

        # apollo13_pacific was given as 1970-04-17 10:07:41-08:00,
        # stored in ts_with as 1970-04-17 18:07:41 UTC,
        # then stored in t_without as 19:37:41,          <---
        # rendered on the wire as 19:37:41,
        # converted to a DateTime of 19:37 without timezone
        x = values['apollo13_pacific']
        self.assertEqual('19:37:41', x.isoformat())
        self.assertIsNone(x.tzinfo)

    def test_interval_second(self):
        if not self.server_has_new_time_conversion():
            raise SkipTest("test only applies to newer MonetDB versions")
        self.do_query(3, ['seconds_col'])
        self.do_fetchall()
        self.verifyBinary()

    def test_interval_day(self):
        if not self.server_has_new_time_conversion():
            raise SkipTest("test only applies to newer MonetDB versions")
        self.do_query(250, ['days_col'])
        self.do_fetchall()
        self.verifyBinary()

    def test_interval_month(self):
        self.do_query(250, ['months_col'])
        self.do_fetchall()
        self.verifyBinary()

    def do_test_wide(self, ncols):
        i = 0
        cols = []
        while len(cols) < ncols:
            cols += [
                (f'int{i}', TEST_COLUMNS['int_col']),
                (f'text{i}', TEST_COLUMNS['varchar_col']),
                (f'dbl{i}', TEST_COLUMNS['double_col']),
            ]
            i += 1
        coldict = dict(cols[:ncols])
        self.do_query(10, coldict)
        self.do_fetchall()
        self.verifyBinary()

    def test_wide300(self):
        self.do_test_wide(300)

    def test_wide3000(self):
        self.do_test_wide(3000)


class TestResultSet(BaseTestCases):
    def setup_connection(self):
        # no special connect parameters
        conn = self.connect_with_args()
        # if binary is enabled we expect to see it after row 100
        binary_after = 100
        return (conn, binary_after)


class TestResultSetNoBinary(BaseTestCases):
    def setup_connection(self):
        self.skip_unless_have_binary()  # test is not interesting if server does not support binary anyway
        conn = self.connect_with_args(binary=0)
        binary_after = None
        # we do not expect to see any binary
        return (conn, binary_after)


class TestResultSetForceBinary(BaseTestCases):
    def setup_connection(self):
        self.skip_unless_have_binary()
        # replysize 1 switches to binary protocol soonest, at the cost of more batches.
        conn = self.connect_with_args(binary=1, replysize=1)
        binary_after = 1
        return (conn, binary_after)


class TestResultSetFetchAllBinary(BaseTestCases):
    def setup_connection(self):
        self.skip_unless_have_binary()
        conn = self.connect_with_args(binary=1, replysize=-1)
        binary_after = 10
        return (conn, binary_after)


class TestResultSetFetchAllNoBinary(BaseTestCases):
    def setup_connection(self):
        conn = self.connect_with_args(binary=0, replysize=-1)
        binary_after = None
        return (conn, binary_after)


class TestResultSetNoPrefetch(BaseTestCases):
    def setup_connection(self):
        conn = self.connect_with_args(maxprefetch=0)
        binary_after = 100
        return (conn, binary_after)


# Make sure the abstract base class doesn't get executed
del BaseTestCases
