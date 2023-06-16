# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


from typing import List, Optional
from unittest import TestCase
from urllib.parse import parse_qsl, urlencode, urlparse
import pymonetdb
from pymonetdb.policy import BatchPolicy
from tests.util import test_args, test_url


def run_scenario(rowcount: int,
                 server_binexport_level: int,
                 pattern: List[int],
                 binary: Optional[bool] = None,
                 replysize: Optional[int] = None,
                 maxprefetch: Optional[int] = None,
                 ):

    # simulate connect
    policy = BatchPolicy()
    if binary is not None:
        policy.binary_level = binary
    if replysize is not None:
        policy.replysize = replysize
    if maxprefetch is not None:
        policy.maxprefetch = maxprefetch
    policy.server_binexport_level = server_binexport_level

    handshake_reply_size = policy.handshake_reply_size()

    # simulate new cursor
    pol = policy.clone()
    array_size = pol.decide_arraysize()

    # simulate execute
    query_reply_size = pol.new_query()
    pos = 0
    cache_start = 0
    if query_reply_size > 0:
        cache_end = query_reply_size
    else:
        cache_end = rowcount
    intervals = [(cache_start, cache_end)]

    # simulate fetch*()
    for size in pattern:
        if size < 0:
            # simulate fetchall
            size = rowcount - pos
        requested_end = min(pos + size, rowcount)

        # take existing rows
        end = min(requested_end, cache_end)
        existing_rows = end - pos
        pos = end

        if pos == requested_end:
            continue

        fetch_size = pol.batch_size(existing_rows, pos, requested_end, rowcount)
        end = pos + fetch_size
        intervals.append((pos, end))
        cache_start = pos
        cache_end = end
        pos = requested_end

    return Scenario(handshake_reply_size, array_size, query_reply_size, intervals)


class Scenario:
    handshake_reply_size: int
    array_size: int
    query_reply_size: int
    intervals: List[int]

    def __init__(self, handshake_reply_size, array_size, query_reply_size, intervals):
        self.handshake_reply_size = handshake_reply_size
        self.array_size = array_size
        self.query_reply_size = query_reply_size
        self.intervals = intervals

    def __eq__(self, other):
        return (self.handshake_reply_size == other.handshake_reply_size
                and self.array_size == other.array_size
                and self.query_reply_size == other.query_reply_size
                and self.intervals == other.intervals)

    def __repr__(self):
        return (
            "Scenario("
            f"handshake_reply_size={self.handshake_reply_size}, "
            f"array_size={self.array_size}, "
            f"query_reply_size={self.query_reply_size}, "
            f"intervals={self.intervals})"
        )


class TestBatchPolicy(TestCase):
    """Test the BatchPolicy in isolation"""

    def setUp(self):
        self.addTypeEqualityFunc(Scenario, self.compare_scenarios)

    def compare_scenarios(self, left, right, msg=None):
        if left.handshake_reply_size != right.handshake_reply_size:
            self.failureException(
                "scenario differs in handshake_reply_size: "
                f"{left.handshake_reply_size} vs. {right.handshake_reply_size}")
        if left.array_size != right.array_size:
            self.failureException(
                "scenario differs in array_size: "
                f"{left.array_size} vs. {right.array_size}")
        if left.query_reply_size != right.query_reply_size:
            self.failureException(
                "scenario differs in query_reply_size: "
                f"{left.query_reply_size} vs. {right.query_reply_size}")
        if left.intervals != right.intervals:
            try:
                self.assertListEqual(left.intervals, right.intervals)
            except AssertionError as e:
                raise self.failureException(f"scenario differs in intervals: {e}")
            raise self.failureException(f"scenario differs in intervals: {left.intervals} vs. {right.intervals}")

    def run_both(self,
                 rowcount: int,
                 pattern: List[int],
                 binary: Optional[bool] = None,
                 replysize: Optional[int] = None,
                 maxprefetch: Optional[int] = None,
                 ):
        without_binary = run_scenario(
            rowcount, 0, pattern,
            binary=binary, replysize=replysize, maxprefetch=maxprefetch)
        with_binary = run_scenario(
            rowcount, 1, pattern,
            binary=binary, replysize=replysize, maxprefetch=maxprefetch)
        with_binary_but_disabled = run_scenario(
            rowcount, 1, pattern,
            binary=False, replysize=replysize, maxprefetch=maxprefetch)

        try:
            self.assertEqual(without_binary, with_binary)
        except AssertionError as e:
            raise AssertionError("Scenarios differ between binary=False and binary=True") from e

        try:
            self.assertEqual(without_binary, with_binary_but_disabled)
        except AssertionError as e:
            raise AssertionError("Behavior of binary=False and server_support_binary=False differs") from e

        return without_binary

    def test_defaults_fetchall(self):
        # the first 100 rows have already been fetched, fetchall fetches the rest.
        scen = self.run_both(1000, [-1])
        self.assertEqual(100, scen.handshake_reply_size)
        self.assertEqual(100, scen.array_size)
        self.assertEqual(100, scen.query_reply_size)
        self.assertEqual(
            [(0, 100), (100, 1000)],
            scen.intervals)

    def test_defaults_fetchone(self):
        # if we request the rows one by one we get the typical doubling batch size.
        scen = self.run_both(1000, 1000 * [1])
        self.assertEqual(100, scen.handshake_reply_size)
        self.assertEqual(100, scen.array_size)
        self.assertEqual(100, scen.query_reply_size)
        self.assertEqual(
            [(0, 100), (100, 300), (300, 700), (700, 1000)],
            scen.intervals)

    def test_fetchmany_aligned(self):
        # if the stride fits the reply size there is no difference with fetchone
        scen = self.run_both(1000, 200 * [50])
        self.assertEqual(100, scen.handshake_reply_size)
        self.assertEqual(100, scen.array_size)
        self.assertEqual(100, scen.query_reply_size)
        self.assertEqual(
            [(0, 100), (100, 300), (300, 700), (700, 1000)],
            scen.intervals)

    def test_stride(self):
        # with fetchmany, the batch sizes adjust to the stride.
        scen = self.run_both(1000, 8 * [40])
        self.assertEqual(100, scen.handshake_reply_size)
        self.assertEqual(100, scen.array_size)
        self.assertEqual(100, scen.query_reply_size)
        self.assertEqual([
            (0, 100),       # initial fetch
            (100, 320),     # 320 instead of 300
        ], scen.intervals)

    def test_fetchmany_unaligned(self):
        scen = self.run_both(1000, 100 * [75])
        self.assertEqual(100, scen.handshake_reply_size)
        self.assertEqual(100, scen.array_size)
        self.assertEqual(100, scen.query_reply_size)
        self.assertEqual([
            (0, 100),  # initial fetch
            (100, 300),  # 300 is a multiple of 75
            (300, 750),  # 700 is not, but 750 is
            (750, 1000)
        ], scen.intervals)

    def test_small_reply_size(self):
        # fetchall behaves as expected
        scen = self.run_both(1000, [-1], replysize=20)
        self.assertEqual(20, scen.handshake_reply_size)
        self.assertEqual(20, scen.array_size)
        self.assertEqual(20, scen.query_reply_size)
        self.assertEqual(
            [(0, 20), (20, 1000)],
            scen.intervals)

        # batches still double but they start smaller
        scen = self.run_both(1000, 1000 * [1], replysize=20)
        self.assertEqual(20, scen.handshake_reply_size)
        self.assertEqual(20, scen.array_size)
        self.assertEqual(20, scen.query_reply_size)
        self.assertEqual([
            (0, 20),
            (20, 60),
            (60, 140),
            (140, 300),
            (300, 620),
            (620, 1000)
        ], scen.intervals)

    def test_small_prefetch(self):
        # batch does not grow larger than stride + prefetch = 100 + 100 = 200
        scen = self.run_both(1000, 100 * [100], maxprefetch=100)
        self.assertEqual(100, scen.handshake_reply_size)
        self.assertEqual(100, scen.array_size)
        self.assertEqual(100, scen.query_reply_size)
        self.assertEqual([
            (0, 100),
            (100, 300),
            (300, 500),
            (500, 700),
            (700, 900),
            (900, 1000)
        ], scen.intervals)

    def test_small_prefetch_large_stride(self):
        # if maxprefetch is small and does not match the stride it ends
        # up not really being used
        scen = self.run_both(1000, 30 * [250], maxprefetch=100)
        self.assertEqual(100, scen.handshake_reply_size)
        self.assertEqual(100, scen.array_size)
        self.assertEqual(100, scen.query_reply_size)
        self.assertEqual([
            (0, 100),
            (100, 350),  # steps of 250, but shifted
            (350, 600),
            (600, 850),
            (850, 1000),
        ], scen.intervals)

    def test_no_prefetch_large_stride(self):
        # without prefetch it goes 100, 150, 250, 250, ...
        scen = self.run_both(1000, 30 * [250], maxprefetch=0)
        self.assertEqual(100, scen.handshake_reply_size)
        self.assertEqual(100, scen.array_size)
        self.assertEqual(100, scen.query_reply_size)
        self.assertEqual([
            (0, 100),
            (100, 250),  # complete the first step
            (250, 500),  # fetch the remaining steps individually
            (500, 750),
            (750, 1000),
        ], scen.intervals)

    def test_unlimited_prefetch(self):
        # if we turn off the prefetch limit it grows without bound
        scen = self.run_both(1_000_000, 10_000 * [100], maxprefetch=-1)
        self.assertEqual(100, scen.handshake_reply_size)
        self.assertEqual(100, scen.array_size)
        self.assertEqual(100, scen.query_reply_size)
        self.assertEqual([
            (0, 100),
            (100, 300),
            (300, 700),
            (700, 1500),
            (1500, 3100),
            (3100, 6300),
            (6300, 12700),
            (12700, 25500),
            (25500, 51100),
            (51100, 102300),
            (102300, 204700),
            (204700, 409500),
            (409500, 819100),
            (819100, 1000000)
        ], scen.intervals)

    def test_unlimited_replysize_no_binary(self):
        # everything comes in the initial response
        scen = run_scenario(1000, 0, 10 * [100], replysize=-1)
        self.assertEqual(-1, scen.handshake_reply_size)
        self.assertEqual(100, scen.array_size)
        self.assertEqual(-1, scen.query_reply_size)
        self.assertEqual([(0, 1000)], scen.intervals)

        # same if binary is possible but disabled
        scen = run_scenario(1000, 1, 10 * [100], replysize=-1, binary=False)
        self.assertEqual(-1, scen.handshake_reply_size)
        self.assertEqual(100, scen.array_size)
        self.assertEqual(-1, scen.query_reply_size)
        self.assertEqual([(0, 1000)], scen.intervals)

    def test_unlimited_replysize_with_binary(self):
        # we set replysize to -1 but it still sends 10 to the server
        # to keep the initial response small and retrieve
        # rest using the binary protocol
        scen = run_scenario(1000, 1, 10 * [100], replysize=-1)
        self.assertEqual(10, scen.handshake_reply_size)
        self.assertEqual(100, scen.array_size)
        self.assertEqual(10, scen.query_reply_size)
        self.assertEqual([(0, 10), (10, 1000)], scen.intervals)

    def test_arraysize(self):
        # arraysize follows the replysize of the connection
        # at the time the cursor was created
        master = BatchPolicy()
        master.replysize = 50
        pol = master.clone()
        self.assertEqual(50, pol.decide_arraysize())

        master.replysize = -1
        pol = master.clone()
        self.assertEqual(100, pol.decide_arraysize())


class TestPolicySetting(TestCase):
    _server_has_binary: Optional[bool]
    _conns: List[pymonetdb.Connection]

    def setUp(self):
        self._conns = []

    def tearDown(self):
        for conn in self._conns:
            try:
                conn.close()
            except pymonetdb.exceptions.Error:
                pass
        self._conns = []

    def _connect(self, **kw_args):
        try:
            args = dict()
            args.update(test_args)
            for key in ['replysize', 'maxprefetch', 'binary']:
                if key in args:
                    del args[key]
            args.update(kw_args)
            conn = pymonetdb.connect(**args)
            self._conns.append(conn)
            return conn
        except AttributeError:
            self.fail("No connect method found in pymonetdb module")

    def check_more(self, conn: pymonetdb.Connection):
        self.assertEqual(conn.binary > 0, conn._policy.binary_level)
        self.assertEqual(conn.replysize, conn._policy.replysize)
        self.assertEqual(conn.maxprefetch, conn._policy.maxprefetch)

        cursor = conn.cursor()

        self.assertEqual(cursor.binary > 0, conn.binary)
        self.assertEqual(cursor.replysize, conn.replysize)
        self.assertEqual(cursor.maxprefetch, conn.maxprefetch)
        if cursor.replysize < 0:
            self.assertEqual(100, cursor.arraysize)
        else:
            self.assertEqual(cursor.replysize, cursor.arraysize)

        self.assertEqual(cursor.binary > 0, cursor._policy.binary_level)
        self.assertEqual(cursor.replysize, cursor._policy.replysize)
        self.assertEqual(cursor.maxprefetch, cursor._policy.maxprefetch)

    def test_defaults(self):
        conn = self._connect()
        self.assertEqual(1, conn._policy.binary_level)   # by default we WANT binary
        self.assertEqual(100, conn._policy.replysize)
        self.assertEqual(2_500, conn._policy.maxprefetch)

        self.check_more(conn)

    def test_constructor_parameters1(self):
        conn = self._connect(replysize=99, maxprefetch=333, binary=0)
        self.assertEqual(0, conn._policy.binary_level)
        self.assertEqual(99, conn._policy.replysize)
        self.assertEqual(333, conn._policy.maxprefetch)

        self.check_more(conn)

    def test_constructor_parameters2(self):
        conn = self._connect(replysize=-1, maxprefetch=-1, binary=1)
        self.assertEqual(1, conn._policy.binary_level)
        self.assertEqual(-1, conn._policy.replysize)
        self.assertEqual(-1, conn._policy.maxprefetch)

        self.check_more(conn)

    def test_conn_attr_updates(self):
        conn = self._connect()
        conn.replysize = 99
        conn.maxprefetch = 333
        conn.binary = 0
        self.assertEqual(0, conn._policy.binary_level)
        self.assertEqual(99, conn._policy.replysize)
        self.assertEqual(333, conn._policy.maxprefetch)

        self.check_more(conn)

    def test_cursor_attr_updates(self):
        conn = self._connect()
        cursor = conn.cursor()
        cursor.replysize = 99
        cursor.maxprefetch = 333
        cursor.binary = 0
        self.assertEqual(0, cursor._policy.binary_level)
        self.assertEqual(99, cursor._policy.replysize)
        self.assertEqual(333, cursor._policy.maxprefetch)

    def update_url(self, replysize, maxprefetch, binary) -> str:
        u = urlparse(test_url)
        opts = dict(parse_qsl(u.query))
        opts['replysize'] = replysize
        opts['maxprefetch'] = maxprefetch
        opts['binary'] = binary
        u = u._replace(query=urlencode(opts))
        return u.geturl()

    def test_url_parameters1(self):
        updated_url = self.update_url(replysize='99', maxprefetch='444', binary='1')
        conn = pymonetdb.connect(updated_url)
        self.assertEqual(1, conn._policy.binary_level)
        self.assertEqual(99, conn._policy.replysize)
        self.assertEqual(444, conn._policy.maxprefetch)

    def test_url_parameters2(self):
        updated_url = self.update_url(replysize='999', maxprefetch='44', binary='0')
        conn = pymonetdb.connect(updated_url)
        self.assertEqual(0, conn._policy.binary_level)
        self.assertEqual(999, conn._policy.replysize)
        self.assertEqual(44, conn._policy.maxprefetch)
