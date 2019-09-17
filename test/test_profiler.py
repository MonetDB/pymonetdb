# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
import unittest
from mock import patch
import pymonetdb
from test import util


class ProfilerTest(unittest.TestCase):
    """Test the profiler connection."""

    #@patch('pymonetdb.mapi.Connection._getblock')
    def test_profiler_connection(self):
        response = '{"key":"value"}\n'  # A random JSON object
        #mock_getblock.return_value = response
        c = pymonetdb.profiler.ProfilerConnection()
        c.connect(**util.test_args)
        self.assertEqual(c.read_object(), response[:-1])
