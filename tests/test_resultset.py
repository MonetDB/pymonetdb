# coding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

from abc import abstractmethod
from random import Random
from typing import Any, Callable, List, Optional, Tuple
from unittest import SkipTest, TestCase
import pymonetdb
from tests.util import test_args

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

TEST_COLUMNS = dict(
    int_col=("CAST(value AS int)", lambda n: n),
    tinyint_col=("CAST(value % 128 AS tinyint)", lambda n: n % 128),
    smallint_col=("CAST(value AS smallint)", lambda n: n),
    bigint_col=("CAST(value AS bigint)", lambda n: n),
    # hugeint_col=("CAST(value AS hugeint)", lambda n: n),    text_col=("'v' || value", lambda n: f"v{n}"),
    text_col=("'v' || value", lambda n: f"v{n}"),
    bool_col=("(value % 2 = 0)", lambda n: (n % 2) == 0),
)


class BaseTestCases(TestCase):
    _server_has_binary: Optional[bool] = None
    conn: Optional[pymonetdb.Connection] = None
    cursor: Optional[pymonetdb.sql.cursors.Cursor] = None
    cur: int = 0
    rowcount: int = 0
    verifiers: List[Tuple[str, Callable[[int], Any]]] = []
    colnames: List[str] = []

    def have_binary(self):
        if self._server_has_binary is None:
            conn = self.connect()
            self._server_has_binary = conn.mapi.supports_binexport
        return self._server_has_binary

    def skip_unless_binary(self):
        if not self.have_binary():
            raise SkipTest("need server with support for binary")

    def setUp(self):
        self.cur = 0
        self.rowcount = 0
        self.cursor = None
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.conn:
            self.conn.close()
            self.conn = None

    def tearDown(self):
        pass

    def connect(self, **kw_args) -> pymonetdb.Connection:
        try:
            args = dict()
            args.update(test_args)
            args.update(kw_args)
            conn = pymonetdb.connect(**args)
        except AttributeError:
            self.fail("No connect method found in pymonetdb module")
        return conn

    @abstractmethod
    def do_connect(self) -> Tuple[pymonetdb.Connection, Optional[int]]:
        assert False

    def assertAtEnd(self):
        self.assertEqual(self.rowcount, self.cur, f"expected to be at end ({self.rowcount} rows), not {self.cur}")

    def do_query(self, n, cols=('int_col',), column_descriptions=TEST_COLUMNS):
        exprs = []
        verifiers = []
        colnames = []
        for col in cols:
            expr, verifier = column_descriptions[col]
            exprs.append(f"{expr} AS {col}")
            verifiers.append(verifier)
            colnames.append(col)
        query = QUERY_TEMPLATE % dict(
            exprs=",\n        ".join(exprs),
            count=n
        )

        if self.conn is None:
            self.conn, self.expect_binary_after = self.do_connect()
            assert self.cursor is None
        self.cursor = cursor = self.conn.cursor()
        cursor.execute(query)

        self.assertEqual(n, cursor.rowcount)

        self.rowcount = n
        self.colnames = colnames
        self.verifiers = verifiers

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
        self.assertIsNotNone(
            self.cursor._can_bindecode,
            f"Expected binary result sets to be used after row {self.expect_binary_after}, am at {self.cur}")

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

    def do_fetchall(self):
        rows = self.cursor.fetchall()
        expectedRows = self.rowcount - self.cur
        self.assertEqual(expectedRows, len(rows))
        for i, row in enumerate(rows):
            self.verifyRow(self.cur + i, row)
        self.cur += len(rows)
        self.verifyBinary()
        self.assertAtEnd()

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

    def test_data_types(self):
        self.do_query(250, TEST_COLUMNS.keys())
        self.do_fetchall()
        # no self.verifyBinary()

    def test_binary_data_types(self):
        self.skip_unless_binary()
        blacklist = set()
        cols = [k for k in TEST_COLUMNS.keys() if k not in blacklist]
        self.do_query(250, cols)
        self.do_fetchall()
        self.verifyBinary()


class TestResultSet(BaseTestCases):
    def do_connect(self):
        # no special connect parameters
        conn = self.connect()
        # if binary is enabled we expect to see it after row 100
        binary_after = 100
        return (conn, binary_after)


class TestResultSetNoBinary(BaseTestCases):
    def do_connect(self):
        self.skip_unless_binary()  # test is not interesting if server does not support binary anyway
        conn = self.connect(binary=0)
        binary_after = None
        # we do not expect to see any binary
        return (conn, binary_after)


class TestResultSetForceBinary(BaseTestCases):
    def do_connect(self):
        self.skip_unless_binary()
        # replysize 1 switches to binary protocol soonest, at the cost of more batches.
        conn = self.connect(binary=1, replysize=1)
        binary_after = 1
        return (conn, binary_after)


class TestResultSetFetchAllBinary(BaseTestCases):
    def do_connect(self):
        self.skip_unless_binary()
        conn = self.connect(binary=1, replysize=-1)
        binary_after = 10
        return (conn, binary_after)


class TestResultSetFetchAllNoBinary(BaseTestCases):
    def do_connect(self):
        conn = self.connect(binary=0, replysize=-1)
        binary_after = None
        return (conn, binary_after)


class TestResultSetNoPrefetch(BaseTestCases):
    def do_connect(self):
        conn = self.connect(maxprefetch=0)
        binary_after = 100
        return (conn, binary_after)


# Make sure the abstract base class doesn't get executed
del BaseTestCases
