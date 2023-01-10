import os
import unittest
from pymonetdb.exceptions import DatabaseError
from socket import gethostbyname
from unittest import TestCase
from pymonetdb import connect
from tests.util import test_args


class TestMapiUri(TestCase):

    def setUp(self):
        self.hostname = test_args['hostname']
        self.port = test_args['port']
        self.database = test_args['database']
        self.username = test_args['username']
        self.password = test_args['password']

    def attempt_connect(self, uri, **kwargs):
        connection = connect(uri, **kwargs)
        cursor = connection.cursor()
        q = "select tag from sys.queue()"
        cursor.execute(q)
        cursor.fetchall()

    def test_no_uri(self):
        # test setUp and attempt_connect themselves
        self.attempt_connect(self.database, port=self.port, hostname=self.hostname,
                             username=self.username, password=self.password)

    def test_full_mapi_uri(self):
        self.attempt_connect(f"mapi:monetdb://{self.hostname}:{self.port}/{self.database}",
                             username=self.username, password=self.password)

    def test_without_port(self):
        if self.port != 50000:
            raise unittest.SkipTest("test_without_port only makes sense on the default port")

        self.attempt_connect(f"mapi:monetdb://{self.hostname}/{self.database}",
                             username=self.username, password=self.password)

    def test_username_component(self):
        try:
            self.attempt_connect(f"mapi:monetdb://{self.hostname}:{self.port}/{self.database}",
                                 username="not" + self.username, password="not" + self.password)
        except DatabaseError:
            # expected to fail, username and password incorrect
            pass
        # override username and password parameters from within url
        s = f"mapi:monetdb://{self.username}:{self.password}@{self.hostname}:{self.port}/{self.database}"
        self.attempt_connect(s, username="not" + self.username, password="not" + self.password)

    def test_ipv4_address(self):
        try:
            # gethostbyname only resolves ipv4
            ip = gethostbyname(self.hostname)
        except Exception:
            raise unittest.SkipTest(f"host '{self.hostname}' doesn't resolve to an ipv4 address")

        self.attempt_connect(f"mapi:monetdb://{ip}:{self.port}/{self.database}",
                             username=self.username, password=self.password)

    def test_unix_domain_socket(self):
        sock_path = "/tmp/.s.monetdb.%i" % self.port
        if not os.path.exists(sock_path):
            raise unittest.SkipTest("Unix domain socket does not exist")
        uri = f"mapi:monetdb://{sock_path}?database={self.database}"
        self.attempt_connect(uri, username=self.username, password=self.password)

    def test_unix_domain_socket_username(self):
        sock_path = "/tmp/.s.monetdb.%i" % self.port
        if not os.path.exists(sock_path):
            raise unittest.SkipTest("Unix domain socket does not exist")
        uri = f"mapi:monetdb://{self.username}:{self.password}@{sock_path}?database={self.database}"
        self.attempt_connect(uri, username="not" + self.username, password="not" + self.password)
