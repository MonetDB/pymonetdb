# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


from ssl import SSLError
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Optional, Union
from unittest import SkipTest, TestCase, skipUnless
from urllib.parse import quote as urlquote
import urllib.request

import pymonetdb
from pymonetdb.exceptions import DatabaseError

from tests.util import (
    test_tls_tester_host,
    test_tls_tester_port,
    test_tls_tester_sys_store,
)


class TestTLS(TestCase):
    _name: Optional[str]
    _cache: Dict[str, str]
    _files: Dict[str, Any]  # they are NamedTemporaryFile's, but mypy hates those

    def __init__(self, methodName):
        self._name = methodName
        self._cache = dict()
        self._files = dict()
        super().__init__(methodName)

    def setUp(self) -> None:
        # validate the config
        have_host = test_tls_tester_host is not None
        have_port = test_tls_tester_port is not None
        have_sys = test_tls_tester_sys_store
        if have_host != have_port:
            raise Exception(
                "Either pass both TSTTLSTESTERHOST and TSTTLSTESTERPORT, or neither"
            )
        if have_sys and not have_host:
            raise Exception(
                "Setting TSTTLSTESTERSYSSTORE does not make sense without TSTTLSTESTERHOST and TSTTLSTESTERPORT"
            )

        if not have_host:
            raise SkipTest("TSTTLSTESTERHOST and TSTTLSTESTERPORT not set")

    def try_connect(self, port_name: str, use_tls=True, server_cert=None, **kwargs):
        """Try to connect to the named port, looking it up in tlstester.py's portmap.

        Returns succesfully if pymonetdb.connect raised a DatabaseError containing
        tlstester's signature message. This indicates that a MAPI dialogue took
        place. Otherwise, raise whatever exception raised by pymonetdb.connect.
        """

        port = self.port(port_name)

        try:
            pymonetdb.connect(
                "banana",
                hostname=test_tls_tester_host,
                port=port,
                use_tls=use_tls,
                server_cert=server_cert,
                **kwargs,
            )
            self.fail("Expected connection to tlstester.py to fail but it didn't")
        except DatabaseError as e:
            if "Sorry, this is not a real MonetDB instance" not in str(e):
                raise

    def port(self, port_name: str) -> int:
        portmap = dict()
        url = f"/?test={urlquote(self._name)}" if self._name else "/"
        ports = self.download(url, encoding="utf-8")
        assert isinstance(ports, str)   # silence mypy
        for line in ports.splitlines():
            name_field, port_field = line.split(":", 1)
            portmap[name_field] = int(port_field)
        port = portmap.get(port_name)
        if port is None:
            names = ", ".join(repr(n) for n in portmap.keys())
            raise Exception(
                f"tlstester.py didn't offer port '{port_name}', only {names}"
            )
        return port

    def download(self, path, encoding=None) -> Union[bytes, str]:
        if path in self._cache:
            content = self._cache[path]
        else:
            url = f"http://{test_tls_tester_host}:{test_tls_tester_port}{path}"
            with urllib.request.urlopen(url) as resp:
                content = resp.read()
            self._cache[path] = content

        assert isinstance(content, bytes)
        if encoding:
            return str(content, encoding=encoding)
        else:
            return content

    def download_file(self, path: str) -> str:
        if path in self._files:
            file = self._files[path]
        else:
            content = self.download(path, encoding=None)
            file = NamedTemporaryFile()
            file.write(content)
            file.flush()
            self._files[path] = file
        return file.name

    def test_connect_plain(self):
        self.try_connect("plain", use_tls=False)

    def test_connect_tls(self):
        self.try_connect("server1", server_cert=self.download_file("/ca1.crt"))

    def test_refuse_no_cert(self):
        with self.assertRaisesRegex(
            SSLError, "self[- ]signed certificate in certificate chain"
        ):
            self.try_connect("server1", server_cert=None)

    def test_refuse_wrong_cert(self):
        with self.assertRaisesRegex(
            SSLError, "self[- ]signed certificate in certificate chain"
        ):
            self.try_connect("server1", server_cert=self.download_file("/ca2.crt"))

    def test_refuse_tls12(self):
        with self.assertRaisesRegex(SSLError, "version"):
            self.try_connect("tls12", server_cert=self.download_file("/ca1.crt"))

    def test_refuse_expired(self):
        with self.assertRaisesRegex(SSLError, "has expired"):
            self.try_connect("expiredcert", server_cert=self.download_file("/ca1.crt"))

    def test_connect_client_auth(self):
        self.try_connect(
            "clientauth",
            server_cert=self.download_file("/ca1.crt"),
            client_key=self.download_file("/client2.key"),
            client_cert=self.download_file("/client2.crt"),
        )

    def test_connect_client_auth_combined(self):
        self.try_connect(
            "clientauth",
            server_cert=self.download_file("/ca1.crt"),
            client_key=self.download_file("/client2.keycrt"),
        )

    def test_fail_plain_to_tls(self):
        with self.assertRaises(SSLError):
            self.try_connect("plain")

    def test_fail_tls_to_plain(self):
        with self.assertRaises(ConnectionError):
            self.try_connect("server1", use_tls=False)

    @skipUnless(test_tls_tester_sys_store, "TSTTLSTESTERSYSSTORE not set")
    def test_connect_trusted(self):
        self.try_connect("server3", server_cert=None)
