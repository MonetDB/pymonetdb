import socket
import unittest
from pymonetdb.exceptions import DatabaseError
from socket import gethostbyname
from unittest import SkipTest, TestCase
from pymonetdb import connect
from tests.util import test_args


class TestMapiUri(TestCase):

    def setUp(self):
        if test_args.get('tls'):
            raise SkipTest("mapi:monetdb: URI's do not support TLS")
        self.hostname = test_args['hostname']
        self.port = test_args['port']
        self.database = test_args['database']
        self.username = test_args['username']
        self.password = test_args['password']
        self.to_close = []

    def tearDown(self):
        for c in self.to_close:
            try:
                c.close()
            except Exception:
                pass

    def attempt_connect(self, uri, **kwargs):
        with connect(uri, **kwargs) as connection:
            self.to_close.append(connection)
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
        uri = self.unix_domain_socket_uri()
        self.attempt_connect(uri, username=self.username, password=self.password)

    def test_unix_domain_socket_username(self):
        uri = self.unix_domain_socket_uri()
        self.attempt_connect(uri, username="not" + self.username, password="not" + self.password)

    def unix_domain_socket_uri(self):
        if not hasattr(socket, 'AF_UNIX'):
            raise unittest.SkipTest("Unix domain sockets are not supported on this platform")
        sock_path = "/tmp/.s.monetdb.%i" % self.port
        try:
            with socket.socket(socket.AF_UNIX) as sock:
                sock.settimeout(0.1)
                sock.connect(sock_path)
        except FileNotFoundError:
            raise unittest.SkipTest(f"Unix domain socket {sock_path} does not exist")
        except ConnectionRefusedError:
            raise unittest.SkipTest(f"Unix domain socket {sock_path} is stale")

        return f"mapi:monetdb://{self.username}:{self.password}@{sock_path}?database={self.database}"
