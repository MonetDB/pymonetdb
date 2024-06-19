# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

from unittest import TestCase, skipUnless
import pymonetdb

from tests.util import have_monetdb_version_at_least, test_url

QUERY = """\
SELECT language, peer, hostname, application, client, clientpid, remark
FROM sys.sessions
WHERE sessionid = sys.current_sessionid()
"""

SERVER_HAS_CLIENTINFO = have_monetdb_version_at_least(11, 51, 0)


@skipUnless(SERVER_HAS_CLIENTINFO, "server does not support clientinfo")
class TestClientInfo(TestCase):

    def get_clientinfo(self, **connect_args):
        with pymonetdb.connect(test_url, **connect_args) as conn, conn.cursor() as c:
            nrows = c.execute(QUERY)
            self.assertEqual(nrows, 1)
            row = c.fetchone()
            d = dict(
                (descr.name, v) for descr, v in zip(c.description, row)
            )
            return d

    # 'application': 'python3 -m unittest'
    # 'client': 'pymonetdb 1.8.2a0'
    # 'clientpid': 2762097
    # 'hostname': 'totoro'
    # 'language': 'sql'
    # 'peer': '<UNIX SOCKET>'
    # 'remark': None

    def test_default_clientinfo(self):
        d = self.get_clientinfo()
        self.assertEqual(d['language'], 'sql')
        self.assertIsNotNone(d['peer'])
        self.assertIsNotNone(d['hostname'])
        self.assertIsNotNone(d['application'])
        self.assertIn('pymonetdb', d['client'])
        self.assertIn(pymonetdb.__version__, d['client'])
        self.assertGreater(d['clientpid'], 0)
        self.assertIsNone(d['remark'])

    def test_suppressed_default_clientinfo(self):
        d = self.get_clientinfo(client_info=False)
        self.assertEqual(d['language'], 'sql')
        self.assertIsNotNone(d['peer'])
        self.assertIsNone(d['hostname'])
        self.assertIsNone(d['application'])
        self.assertIsNone(d['client'])
        self.assertIsNone(d['clientpid'])
        self.assertIsNone(d['remark'])

    def test_set_application_name(self):
        d = self.get_clientinfo(client_application='banana')
        self.assertEqual(d['application'], 'banana')
        d = self.get_clientinfo(client_info=False, client_application='banana')
        self.assertIsNone(d['application'])

    def test_set_remark(self):
        d = self.get_clientinfo(client_remark='banana')
        self.assertEqual(d['remark'], 'banana')
        d = self.get_clientinfo(client_info=False, client_remark='banana')
        self.assertIsNone(d['remark'])

    def test_suppressed_configured_clientinfo(self):
        d = self.get_clientinfo(client_info=False, client_application='a', client_remark='b')
        self.assertEqual(d['language'], 'sql')
        self.assertIsNotNone(d['peer'])
        self.assertIsNone(d['hostname'])
        self.assertIsNone(d['application'])
        self.assertIsNone(d['client'])
        self.assertIsNone(d['clientpid'])
        self.assertIsNone(d['remark'])
