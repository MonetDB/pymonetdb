from unittest import TestCase
from pymonetdb import connect
from tests.util import test_args


class TestPrepare(TestCase):
    def setUp(self):
        self.connection = connect(autocommit=False, **test_args)
        self.cursor = self.connection.cursor()

    def test_prepare(self):
        # It would be nice to have a prepare API but in the mean time we can
        # invoke PREPARE and EXEC ourselves.
        #
        # Note how the id of the newly prepared statement is stashed in
        # cursor.lastrowid.

        prepare = "PREPARE SELECT value, 10 * value, 100 * value FROM sys.generate_series(0, ?)"
        self.cursor.execute(prepare)
        exec_id = self.cursor.lastrowid

        n = 3
        expected = [
            (value, 10 * value, 100 * value)
            for value in range(n)
        ]
        self.cursor.execute("EXEC %s(%s)", (exec_id, n))
        received = self.cursor.fetchall()
        self.assertEqual(expected, received)
