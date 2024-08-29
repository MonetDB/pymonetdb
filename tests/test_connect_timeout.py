# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

# Test pymonetdb.connect(..., connect_timeout=)
# by running it in a separate process which returns sucessfully
# when a timeout happens.
# If the process runs too long, it is killed. (This is why we need
# a process rather than a thread.)

import logging
import multiprocessing
import socket
import subprocess
import sys
import unittest

import pymonetdb

from tests.util import dead_address, test_args

log_format = '%(levelname)s:t=%(relativeCreated).1f:proc=%(processName)s:%(name)s:%(message)s'
logging.basicConfig(level=logging.DEBUG, format=log_format)


# prefer 'spawn' over 'fork' or 'forkserver'.
SPAWN_CONTEXT = multiprocessing.get_context('spawn')


class NotDeadError(Exception):
    """The given address was expected to be dead but a connection could be established"""
    pass


VERIFIED_DEAD = set()


def verify_dead_address(host, port):
    addr = (host, port)
    if addr in VERIFIED_DEAD:
        return
    try:
        logging.debug(f'checking if {addr} is dead')
        socket.create_connection(addr, timeout=1.0)
    except socket.timeout:
        logging.debug("it's dead")
        VERIFIED_DEAD.add(addr)
        return
    logging.error(f'connection to supposedly dead {addr} succeeded')
    raise NotDeadError()


class ConnectTimeoutTests(unittest.TestCase):
    addr: str
    host: str
    port: int

    def setUp(self):
        if dead_address is None:
            raise unittest.SkipTest("TSTDEADADDRESS not set")
        host, port = dead_address.split(':', 1)
        port = int(port)
        self.host = host
        self.port = port
        self.addr = dead_address
        verify_dead_address(host, port)

    def run_isolated(self, socket_timeout, global_timeout, expected_exception, expected_duration, use_tls=False):  # noqa C901
        epsilon = 0.5

        kill_timeout = max(0, socket_timeout or 0, global_timeout or 0)
        if not kill_timeout:
            kill_timeout = 3.0
        kill_timeout += 1

        scheme = 'monetdbs' if use_tls else 'monetdb'
        url = f'{scheme}://{self.addr}/'
        if socket_timeout is not None:
            url += f'?connect_timeout={socket_timeout}'
        cmd = [sys.executable, '-m', 'tests.timeout_helper', url]
        if global_timeout is not None:
            cmd += ['-g', str(global_timeout)]
        if expected_exception:
            cmd += ['-e', expected_exception]
        logging.info(cmd)

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.DEVNULL,
            text=True, encoding='utf-8'
        )
        try:
            out, err = proc.communicate(None, timeout=kill_timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            if expected_duration is not None:
                self.fail(f'still running after {kill_timeout}s, expected timeout at {expected_duration}s')
            return
        if out:
            duration = round(float(out), 2)

        if proc.returncode == 0:
            self.fail(f'subprocess unexpectedly ran to completion after {duration}s')
        elif proc.returncode == 1:
            if expected_duration is None:
                self.fail(f'subprocess timed out after {duration}s seconds but was expected to block')
            duration = float(out)
            if abs(duration - expected_duration) > epsilon:
                self.fail(
                    f'subprocess timed out after {duration}s, '
                    f'not close enough to expected {expected_duration}s (epsilon={epsilon})')
        else:
            msg = f'subprocess exited with exit code {proc.returncode}'
            if err:
                msg += f'\n ---- helper stderr ({url})----\n' + err + '\n---- end stderr ----\n'
            self.fail(msg)

    # Test cases:
    #
    # | TIMEOUT PASSED | SYSDEFLT = None     | SYSDEFLT = 0 | SYSDEFLT = 2.0      |
    # |----------------+---------------------+--------------+---------------------|
    # |             -2 | (1) error           | (1) error    | (1) error           |
    # |              0 | (3) block           | (2) error    | (3) block           |
    # |              1 | (4) timeout at 1s   | (2) error    | (4) timeout at 1s   |
    # |           None | (5) block           | (2) error    | (5) timeout at 2s   |
    # |             -1 | (5) block           | (2) error    | (5) timeout at 2s   |
    #
    # (1) timeout -2 is rejected during validation.
    # (2) sysdefault 0 is detected and rejected during connect
    # (3) timeout=0 always blocks
    # (4) timeout=1 always times out
    # (5) timeout None and -1 follow sysdefault

    def test_local_minus2_sys_none(self):
        # Rule 1.
        self.run_isolated(
            socket_timeout=-2, global_timeout=None,
            expected_exception='DatabaseError', expected_duration=0.0,
        )

    def test_local_minus2_sys_0(self):
        # Rule 1.
        self.run_isolated(
            socket_timeout=-2, global_timeout=0,
            expected_exception='DatabaseError', expected_duration=0.0,
        )

    def test_local_minus2_sys_2(self):
        # Rule 1.
        self.run_isolated(
            socket_timeout=-2, global_timeout=2,
            expected_exception='DatabaseError', expected_duration=0.0,
        )

    def test_local_0_sys_0(self):
        # Rule 2.
        self.run_isolated(
            socket_timeout=0, global_timeout=0,
            expected_exception='ProgrammingError', expected_duration=0.0,
        )

    def test_local_1_sys_0(self):
        # Rule 2.
        self.run_isolated(
            socket_timeout=1, global_timeout=0,
            expected_exception='ProgrammingError', expected_duration=0.0,
        )

    def test_local_none_sys_0(self):
        # Rule 2.
        self.run_isolated(
            socket_timeout=None, global_timeout=0,
            expected_exception='ProgrammingError', expected_duration=0.0,
        )

    def test_local_minus1_sys_0(self):
        # Rule 2.
        self.run_isolated(
            socket_timeout=-1, global_timeout=0,
            expected_exception='ProgrammingError', expected_duration=0.0,
        )

    def test_local_0_sys_none(self):
        # Rule 3.
        self.run_isolated(
            socket_timeout=0, global_timeout=None,
            expected_exception='timed out', expected_duration=None,
        )

    def test_local_1_sys_none(self):
        # Rule 4.
        self.run_isolated(
            socket_timeout=1, global_timeout=None,
            expected_exception='timed out', expected_duration=1.0,
        )

    def test_local_0_sys_2(self):
        # Rule 3.
        self.run_isolated(
            socket_timeout=0, global_timeout=2,
            expected_exception=None, expected_duration=None,
        )

    def test_local_1_sys_2(self):
        # Rule 4.
        self.run_isolated(
            socket_timeout=1, global_timeout=2,
            expected_exception='timed out', expected_duration=1.0,
        )

    def test_local_none_sys_none(self):
        # Rule 5.
        self.run_isolated(
            socket_timeout=None, global_timeout=None,
            expected_exception=None, expected_duration=None,
        )

    def test_local_none_sys_2(self):
        # Rule 5.
        self.run_isolated(
            socket_timeout=None, global_timeout=2,
            expected_exception='timed out', expected_duration=2.0,
        )

    def test_local_minus1_sys_none(self):
        # Rule 5.
        self.run_isolated(
            socket_timeout=-1, global_timeout=None,
            expected_exception='timed out', expected_duration=None,
        )

    def test_local_minus1_sys_2(self):
        # Rule 5.
        self.run_isolated(
            socket_timeout=-1, global_timeout=2,
            expected_exception='timed out', expected_duration=2.0,
        )

    def test_tls_timeout(self):
        self.run_isolated(
            socket_timeout=1.0, global_timeout=None,
            expected_exception='timed out', expected_duration=1.0,
            use_tls=True
        )

    def connect_with_timeout(self, timeout):
        args = {** test_args}
        args['connect_timeout'] = timeout
        return pymonetdb.connect(**args)

    def test_connect_timeout_does_not_apply_later(self):
        # test the test: check that our timeout is not being ignored
        with self.assertRaises(expected_exception=ValueError):
            with self.connect_with_timeout('invalid timeout'):
                pass

        # now test that the timeout is cleared when the connection has been established
        default_timeout = socket.getdefaulttimeout()
        our_timeout = 3.1415
        self.assertNotEqual(default_timeout, our_timeout)
        with self.connect_with_timeout(our_timeout) as conn:
            sock = conn.mapi.socket
            self.assertEqual(default_timeout, sock.gettimeout())
