from unittest import TestCase
from pymonetdb import connect
from tests.util import test_args


class TestOid(TestCase):
    def setUp(self):
        self.connection = connect(autocommit=False, **test_args)
        self.cursor = self.connection.cursor()

    def test_oid(self):
        q = "SELECT CAST(10 AS OID)"
        self.cursor.execute(q)
        x = self.cursor.fetchone()[0]
        self.assertEqual("10@0", x)
