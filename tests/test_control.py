# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import unittest
from pymonetdb.control import Control
from pymonetdb.exceptions import OperationalError
from tests.util import test_hostname, test_port, test_passphrase, test_control

database_prefix = 'controltest_'
database_name = database_prefix + 'other'

test_control_tcp = 'tcp' in test_control.split(',')
test_control_local = 'local' in test_control.split(',')


def do_without_fail(function):
    try:
        function()
    except OperationalError:
        pass


class TestControl(unittest.TestCase):
    """
    These tests require control access to monetdb. This is enabled with:

    $ monetdbd set control=yes /var/lib/monetdb
    $ monetdbd set passphrase=testdb /var/lib/monetdb

    Where /var/lib/monetdb is the path to your dbfarm. Don't forget to restart the db after setting the credentials.
    """

    def setUpControl(self):
        # use tcp
        if not test_control_tcp:
            raise unittest.SkipTest("Skipping 'tcp' Control test")
        return Control(hostname=test_hostname, port=test_port, passphrase=test_passphrase)

    def setUp(self):
        self.control = self.setUpControl()
        self.ensure_destroyed(database_name)
        self.control.create(database_name)

    def tearDown(self):
        self.ensure_destroyed(database_name)

    def ensure_destroyed(self, database_name):
        do_without_fail(lambda: self.control.stop(database_name))
        do_without_fail(lambda: self.control.destroy(database_name))

    def test_create(self):
        create_name = database_prefix + "create"
        self.ensure_destroyed(create_name)
        self.control.create(create_name)
        self.assertRaises(OperationalError, self.control.create, create_name)
        self.ensure_destroyed(create_name)

    def test_destroy(self):
        destroy_name = database_prefix + "destroy"
        self.ensure_destroyed(destroy_name)
        self.control.create(destroy_name)
        self.control.destroy(destroy_name)
        self.assertRaises(OperationalError, self.control.destroy, destroy_name)

    def test_lock(self):
        do_without_fail(lambda: self.control.release(database_name))
        self.assertEqual(self.control.status(database_name)['locked'], False)
        self.control.lock(database_name)
        self.assertEqual(self.control.status(database_name)['locked'], True)
        with self.assertRaises(OperationalError):
            self.control.lock(database_name)
        self.control.release(database_name)
        self.assertEqual(self.control.status(database_name)['locked'], False)

    def test_release(self):
        do_without_fail(lambda: self.control.release(database_name))
        self.assertEqual(self.control.status(database_name)['locked'], False)
        do_without_fail(lambda: self.control.lock(database_name))
        self.assertEqual(self.control.status(database_name)['locked'], True)
        self.assertTrue(self.control.release(database_name))
        self.assertEqual(self.control.status(database_name)['locked'], False)
        self.assertRaises(OperationalError, self.control.release, database_name)

    def test_status(self):
        status = self.control.status(database_name)
        self.assertEqual(status["name"], database_name)

    def test_statuses(self):
        status1 = database_prefix + "status1"
        status2 = database_prefix + "status2"
        self.ensure_destroyed(status1)
        self.ensure_destroyed(status2)
        self.control.create(status1)
        self.control.rename(status1, status2)
        statuses = self.control.status()
        self.assertFalse(status1 in [status["name"] for status in statuses])
        self.assertTrue(status2 in [status["name"] for status in statuses])
        self.assertRaises(OperationalError, self.control.destroy, status1)
        do_without_fail(lambda: self.control.destroy(status2))

    def test_start(self):
        do_without_fail(lambda: self.control.stop(database_name))
        self.assertTrue(self.control.start(database_name))

    def test_stop(self):
        self.ensure_destroyed(database_name)
        self.assertNotIn(database_name, [st['name'] for st in self.control.status()])

    def test_kill(self):
        do_without_fail(lambda: self.control.start(database_name))
        self.assertTrue(self.control.kill(database_name))

    def test_set(self):
        property_ = "readonly"
        value = "yes"
        self.control.set(database_name, property_, value)
        properties = self.control.get(database_name)
        self.assertEqual(properties[property_], value)

    def test_get(self):
        self.control.get(database_name)

    def test_inherit(self):
        self.control.set(database_name, "readonly", "yes")
        self.assertTrue("readonly" in self.control.get(database_name))
        self.control.inherit(database_name, "readonly")
        # TODO: False on OSX, True on travis?
        # self.assertTrue("readonly" in self.control.get(database_name))

    def test_rename(self):
        old = database_prefix + "old"
        new = database_prefix + "new"
        self.ensure_destroyed(old)
        self.ensure_destroyed(new)
        self.control.create(old)
        self.control.rename(old, new)
        names = [st['name'] for st in self.control.status()]
        self.assertIn(new, names)
        self.assertNotIn(old, names)
        self.ensure_destroyed(new)
        self.assertRaises(OperationalError, self.control.destroy, old)

    def test_defaults(self):
        defaults = self.control.defaults()
        self.assertTrue("readonly" in defaults)

    def test_neighbours(self):
        self.control.neighbours()


class TestLocalControl(TestControl):
    def setUpControl(self):
        # use unix domain socket
        if not test_control_local:
            raise unittest.SkipTest("Skipping 'local' Control test")
        return Control(port=test_port, passphrase=test_passphrase)
