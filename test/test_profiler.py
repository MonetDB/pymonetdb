import unittest
from mock import patch
import pymonetdb


class ProfilerTest(unittest.TestCase):
    """Test the profiler connection."""

    @patch('pymonetdb.mapi.Connection._getblock')
    def test_profiler_connection(self, mock_getblock):
        response = '{"key":"value"}\n'  # A random JSON object
        mock_getblock.return_value = response
        c = pymonetdb.profiler.ProfilerConnection()
        c.connect()
        self.assertEqual(c.read_object(), response[:-1])
