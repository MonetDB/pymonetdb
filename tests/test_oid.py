from unittest import TestCase
from pymonetdb import connect
from tests.util import test_args


class TestOid(TestCase):
    def setUp(self):
        self.connection = connect(autocommit=False, **test_args)
        self.cursor = self.connection.cursor()

    def test_oid(self):
        q = "select tag from sys.queue()"
        self.cursor.execute(q)
        self.cursor.fetchall()
