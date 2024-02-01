from unittest import TestCase
from tests.util import test_mapi_args
from pymonetdb.mapi import Connection


class TestMapi(TestCase):
    def setUp(self):
        self.conn = Connection()
        self.conn.connect(language='sql', **test_mapi_args)

    def tearDown(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def test_set_size(self):
        query = 'sselect * from tables t1, tables t2;'
        for size in (2, 10):
            self.conn.set_reply_size(size)
            data = self.conn.cmd(query)
            cleaned = [i for i in data.split('\n') if i and not i[0] in '%&']
            self.assertEqual(len(cleaned), size)
