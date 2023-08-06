# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
import unittest
from unittest.mock import patch
from typing import Optional
from pymonetdb.profiler import ProfilerConnection
from tests import util


class ProfilerTest(unittest.TestCase):
    """Test the profiler connection."""

    conn: Optional[ProfilerConnection] = None

    @classmethod
    def setUpClass(cls):
        cls.conn = ProfilerConnection()
        cls.conn.connect(**util.test_mapi_args)

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.conn:
            cls.conn.close()

    @patch('pymonetdb.mapi.Connection._getblock')
    def test_profiler_connection(self, mock_getblock):
        response = '{"key":"value"}\n'  # A random JSON object
        mock_getblock.return_value = response
        self.assertEqual(self.conn.read_object(), response[:-1])
