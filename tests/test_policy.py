from dataclasses import dataclass
from typing import List
from unittest import TestCase
from pymonetdb.policy import FetchPolicy


@dataclass
class Scenario:
    policy: FetchPolicy
    handshake_reply_size: int
    initial_arraysize: int
    positions: List[int]


def run_scenario(
        server_supports_binary: bool,
        total_rows: int,
        fetchsize=None,
        maxfetchsize=None,
        binary=None,
        override_arraysize=None
):
    pol = FetchPolicy(fetchsize, maxfetchsize, binary)
    pol.set_server_supports_binary(server_supports_binary)
    handshake_reply_size = pol.decide_connect_reply_size()
    # record initial arraysize before overriding it
    initial_arraysize = pol.decide_arraysize()
    az = initial_arraysize if override_arraysize is None else override_arraysize
    # now simulate retrieving the full result set
    positions = []
    pos = pol.decide_cursor_reply_size(az)
    if pos < 0:
        pos = total_rows
    else:
        pos = min(pos, total_rows)
    adjacent = 1
    positions.append(pos)
    while pos < total_rows:
        n = pol.decide_fetch_amount(az, adjacent, pos, total_rows)
        if n < 0:
            n = total_rows - pos
        adjacent += 1
        pos += n
        positions.append(pos)

    return Scenario(
        policy=pol,
        handshake_reply_size=handshake_reply_size,
        initial_arraysize=initial_arraysize,
        positions=positions
    )


class TestPolicy(TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    def test_default(self):
        scen = run_scenario(False, 355)
        self.assertEqual(scen.handshake_reply_size, 100)
        self.assertEqual(scen.initial_arraysize, 100)
        # fetch 100, 200, the rest
        self.assertEqual(scen.positions, [100, 300, 355])

        # note that binary makes no difference here
        scen = run_scenario(True, 355)
        self.assertEqual(scen.handshake_reply_size, 100)
        self.assertEqual(scen.initial_arraysize, 100)
        self.assertEqual(scen.positions, [100, 300, 355])

    def test_override_arraysize(self):
        scen = run_scenario(False, 355, override_arraysize=50)
        self.assertEqual(scen.handshake_reply_size, 100)  # not affected yet
        self.assertEqual(scen.initial_arraysize, 100)  # not affected either
        # Before sending the query, arraysize gets set to 50.
        # This influences the fetch behavior: 50, +100, +200, +400
        self.assertEqual(scen.positions, [50, 150, 350, 355])

        # note that binary makes no difference here
        scen = run_scenario(True, 355, override_arraysize=50)
        self.assertEqual(scen.handshake_reply_size, 100)
        self.assertEqual(scen.initial_arraysize, 100)
        self.assertEqual(scen.positions, [50, 150, 350, 355])

    def test_fetchsize(self):
        scen = run_scenario(False, 1234, fetchsize=200)
        self.assertEqual(scen.handshake_reply_size, 200)  # set from arraysize
        self.assertEqual(scen.initial_arraysize, 200)  # set from fetchsize
        # batch sizes: 200, +400, +800
        self.assertEqual(scen.positions, [200, 600, 1234])

        # note that binary makes no difference here
        scen = run_scenario(False, 1234, fetchsize=200)
        self.assertEqual(scen.handshake_reply_size, 200)
        self.assertEqual(scen.initial_arraysize, 200)
        # batch sizes: 200, +400, +800
        self.assertEqual(scen.positions, [200, 600, 1234])

    def test_negative_fetchsize(self):
        scen = run_scenario(False, 3355, fetchsize=-1)
        self.assertEqual(scen.handshake_reply_size, -1)  # immediately -1 because not binary
        self.assertEqual(scen.initial_arraysize, 100)  # default because never negative
        self.assertEqual(scen.positions, [3355])  # everything at once

        # binary is different:
        scen = run_scenario(True, 3355, fetchsize=-1)
        n = scen.handshake_reply_size       # should be small positive number:
        self.assertGreater(n, 0)
        self.assertLess(n, 100)  # but less than the default
        self.assertEqual(scen.initial_arraysize, 100)
        # first Xexportbin fetches the remainder
        self.assertEqual(scen.positions, [n, 3355])

    def test_negative_fetchsize_override_arraysize(self):
        # When fetchsize is negative, changes to arraysize by the client
        # are ignored. Maybe that's not such a good idea.
        scen = run_scenario(False, 3355, fetchsize=-1, override_arraysize=42)
        # Override happens here but it is ignored, we still fetch everything at once
        self.assertEqual(scen.positions, [3355])  # everything at once

        # Same with binary.
        scen = run_scenario(True, 3355, fetchsize=-1, override_arraysize=42)
        n = scen.handshake_reply_size
        # driver uses its own default, not the 42 we set the arraysize to:
        self.assertEqual(scen.positions, [n, 3355])

    def test_binary(self):
        def make_pol(server_supports_binary, binary_parameter):
            policy = FetchPolicy(
                fetchsize_parameter=None,
                maxfetchsize_parameter=None,
                binary_parameter=binary_parameter,
            )
            policy.set_server_supports_binary(server_supports_binary)
            return policy

        pol = make_pol(server_supports_binary=False, binary_parameter=False)
        self.assertFalse(pol.use_binary(), "binary should be off when server does not support it")

        pol = make_pol(server_supports_binary=False, binary_parameter=True)
        self.assertFalse(pol.use_binary(), "binary should be off when server does not support it")

        pol = make_pol(server_supports_binary=True, binary_parameter=False)
        self.assertFalse(pol.use_binary(), "binary should be off when user disables it")

        pol = make_pol(server_supports_binary=True, binary_parameter=True)
        self.assertTrue(pol.use_binary(), "binary should be on when possible")
