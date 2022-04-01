import unittest
from unittest.mock import patch
import pymonetdb


class MultilineResponseTest(unittest.TestCase):
    """MonetDB sometimes sends back multi-line responses. Most notably when there
       are more than one concurrent update transactions due to the Optimistic
       Concurrency Control that MonetDB is implementing, only one of them will
       succeed. In auto-commit mode the failed transactions will get something
       like the following response:

       &2 1 -1\n!4000!COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead\n

       The first line is emitted by the MonetDB server when the update is
       actually executed inside the MAL plan while the second is emitted
       outside the MAL plan during the commit that fails. We should take this
       into account.

       Another case is comments that start with the '#' char and should be
       ignored. These are useful for debugging the server.

       This class is intended to create a test suite for this kind of cases.
       Since there is no deterministic way of forcing transaction failure, the
       MonetDB server will be mocked using https://pypi.python.org/pypi/mock

    """

    @patch('pymonetdb.mapi.Connection._putblock')
    @patch('pymonetdb.mapi.Connection._getblock_raw')
    def test_failed_transactions(self, mock_getblock_raw, _):
        """This test mocks two low level methods in the mapi.Connection class:
           mapi.Connection._getblock_raw
           mapi.Connection._putblock

           and tests mapi.Connection.cmd. Specifically we test for the event
           that a transaction has failed due to concurrency conflicts.
        """
        query_text = 'sINSERT INTO tbl VALUES (1)'
        response = b"&2 1 -1\n!40000!COMMIT: transaction is aborted " \
                   b"because of concurrency conflicts, will ROLLBACK instead\n"

        def mocked_getblock_raw(buf, off):
            buf[off:] = response
            return off + len(response)

        mock_getblock_raw.side_effect = mocked_getblock_raw
        c = pymonetdb.mapi.Connection()

        # Simulate a connection
        c.state = pymonetdb.mapi.STATE_READY

        # Make sure that cmd raises the correct exception
        self.assertRaises(pymonetdb.IntegrityError, c.cmd, [query_text])
