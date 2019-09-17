import unittest
from pymonetdb import Connection
from tests.util import test_args


class TestNamedTuple(unittest.TestCase):
    def test_namedtuple(self):
        con = Connection(autocommit=False, **test_args)

        cur = con.cursor()

        cur.execute('select 1.0 as floatje')
        cur.fetchone()
        descr = cur.description[0]
        self.assertTrue(descr.name in 'floatje')
        self.assertEqual(descr.type_code, 'decimal')
        self.assertEqual(descr.display_size, None)
        self.assertEqual(type(descr.internal_size), int)
        self.assertEqual(type(descr.precision), int)
        self.assertEqual(type(descr.scale), int)
        self.assertEqual(descr.null_ok, None)

        cur.execute('select 1 as intje')
        cur.fetchone()
        descr = cur.description[0]
        self.assertTrue(descr.name in 'intje')
        self.assertEqual(descr.type_code, 'tinyint')
        self.assertEqual(descr.display_size, None)
        self.assertEqual(type(descr.internal_size), int)
        self.assertEqual(descr.precision, None)
        self.assertEqual(descr.scale, None)
        self.assertEqual(descr.null_ok, None)
