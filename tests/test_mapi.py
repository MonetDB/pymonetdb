from unittest import TestCase
from tests.util import test_mapi_args
from pymonetdb.mapi import Connection


class TestMapi(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = Connection()
        cls.conn.connect(language='sql', **test_mapi_args)

    def test_set_size(self):
        query = 'sselect * from tables t1, tables t2;'
        for size in (2, 10):
            self.conn.set_reply_size(size)
            data = self.conn.cmd(query)
            cleaned = [i for i in data.split('\n') if i and not i[0] in '%&']
            self.assertEqual(len(cleaned), size)
