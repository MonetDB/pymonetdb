from unittest import TestCase
from pymonetdb import connect
from tests.util import test_args


class TestPrepare(TestCase):
    def setUp(self):
        self.connection = connect(autocommit=False, **test_args)
        self.cursor = self.connection.cursor()

    def tearDown(self):
        self.connection.rollback()
        self.connection.close()

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

    def test_prepare_wide(self):
        self.connection.set_replysize(3)
        self.cursor = self.connection.cursor()
        n = 150
        colnames = [f'c{i + 1}' for i in range(n)]

        col_defs = ', '.join(f'{name} VARCHAR(10)' for name in colnames)
        table_def = f'DROP TABLE IF EXISTS foo; CREATE TABLE foo({col_defs})'
        self.cursor.execute(table_def)

        prep = "PREPARE INSERT INTO foo VALUES (" + ", ".join('?' for n in colnames) + ")"
        self.cursor.execute(prep)
        nrows = len(self.cursor.fetchall())
        self.assertEqual(n, nrows)
