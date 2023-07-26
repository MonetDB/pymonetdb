"""
Utilities for parsing MonetDB URLs
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


import copy
import re
from typing import Any, Callable, Dict, Optional, Union
from urllib.parse import parse_qsl, urlparse


def looks_like_url(text: str) -> bool:
    return (
        text.startswith("mapi:")
        or text.startswith("monetdb:")
        or text.startswith("monetdbs:")
        or text.startswith("monetdbe:")
    )


def valid_database_name(text: str) -> bool:
    return not not re.match("^[-a-zA-Z0-9_]+", text)


BOOL_NAMES: dict[str, bool] = dict(
    true=True, yes=True, on=True, false=False, no=False, off=False
)


def parse_bool(s: str) -> bool:
    b = BOOL_NAMES.get(s.lower())
    if b is None:
        raise ValueError("invalid bool: " + s)
    return b


def parse_int_bool(s: str) -> Union[int, bool]:
    try:
        return parse_bool(s)
    except ValueError:
        pass
    try:
        return int(s, 10)
    except ValueError:
        bools = "/".join(BOOL_NAMES.keys())
        raise ValueError(f"expected int or {bools}, not: " + s)


_UNQUOTE_PATTERN = re.compile(b"[%](.?.?)")


def _unquote_fun(m: re.Match) -> bytes:
    digits = m.group(1)
    if len(digits) != 2:
        raise ValueError()
    return bytes([int(digits, 16)])


def strict_percent_decode(text: str) -> str:
    try:
        return str(_UNQUOTE_PATTERN.sub(_unquote_fun, bytes(text, "ascii")), "utf-8")
    except ValueError as e:
        raise ValueError("invalid percent escape") from e


PARSE_PARAM: Dict[str, Optional[Callable[[str], Any]]] = dict(
    sock=None,
    cert=None,
    fingerprint=None,
    clientkey=None,
    clientcert=None,
    clientkeypassword=None,
    user=None,
    password=None,
    language=None,
    autocommit=parse_bool,
    schema=None,
    timezone=int,
    replysize=int,
    fetchsize=int,
    maxprefetch=int,
    binary=parse_int_bool,
    # extensions
    connect_timeout=int,
    dangerous_tls_nocheck=None,
)


PARSE_OTHER: Dict[str, Optional[Callable[[str], Any]]] = dict(
    use_tls=parse_bool,
    host=None,
    port=int,
    database=None,
)


IGNORE_PARAM = set(["debug", "logfile"])


ALL_FIELDS = set([*PARSE_OTHER.keys(), *PARSE_PARAM.keys()])


class Target:
    _password_set: bool = False
    use_tls = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    sock: Optional[str] = None
    cert: Optional[str] = None
    fingerprint: Optional[str] = None
    clientkey: Optional[str] = None
    clientcert: Optional[str] = None
    clientkeypassword: Optional[str] = None
    _user: Optional[str] = None
    _password: Optional[str] = None
    language: Optional[str] = None
    autocommit: Optional[bool] = None
    schema: Optional[str] = None
    timezone: Optional[str] = None
    replysize: Optional[int] = None
    maxprefetch: Optional[int] = None
    binary: Optional[Union[int, bool]] = None
    # extensions:
    connect_timeout: Optional[int] = None
    dangerous_tls_nocheck: Optional[str] = None

    def get_user(self) -> Optional[str]:
        return self._user

    def set_user(self, new_user: Optional[str]):
        if self._user is not None and self._user != new_user and not self._password_set:
            self._password = None
        self._user = new_user

    user = property(get_user, set_user)

    def get_password(self) -> Optional[str]:
        return self._password

    def set_password(self, new_password: Optional[str]):
        self._password = new_password
        self._password_set = True

    password = property(get_password, set_password)

    def user_password_barrier(self):
        self._password_set = False

    def set_fetchsize(self, fetchsize: int):
        self.replysize = fetchsize

    fetchsize = property(None, set_fetchsize)

    @property
    def effective_use_tls(self) -> bool:
        return not not self.use_tls

    @property
    def effective_tcp_host(self) -> Optional[str]:
        if self.host is not None:
            return self.host
        if self.sock is not None:
            return None
        return "localhost"

    @property
    def effective_port(self) -> Optional[int]:
        if self.port is not None:
            return self.port
        return 50_000

    @property
    def effective_unix_sock(self) -> Optional[str]:
        if self.use_tls is True:
            return None
        if self.sock is not None:
            return self.sock
        if self.host is None or self.host == "localhost":
            return f"/tmp/.s.monetdb.{self.effective_port}"
        return None

    @property
    def effective_language(self) -> str:
        return self.language if self.language is not None else "sql"

    @property
    def effective_binary(self) -> int:
        huge_number = 2 << 64
        b = self.binary
        if b is None or b is True:
            return huge_number
        elif b is False:
            return 0
        else:
            return min(b, huge_number)

    @property
    def effective_connect_timeout(self):
        t = self.connect_timeout
        if t is not None and t >= 0:
            return t
        else:
            return None

    def validate(self):
        """
        Raise a ValueError if the combination of fields in this Target object is not valid.
        """
        # V1
        if self.sock is not None and self.host is not None and self.host != "localhost":
            raise ValueError("sock= is only valid for localhost")
        # V2
        if self.port is not None and not (1 <= self.port <= 65535):
            raise ValueError("port must be between 1 and 65535 (inclusive)")
        # V3
        if self.clientcert is not None and self.clientkey is None:
            raise ValueError("clientcert= does not make sense without clientkey=")
        if self.clientkeypassword is not None and self.clientkey is None:
            raise ValueError(
                "clientkeypassword= does not make sense without clientkey="
            )
        # V4
        if self.password is not None and self.user is None:
            raise ValueError("cannot have password= without user=")
        # V5
        # not implemented yet

        # Also check range of basically every int
        b = self.binary
        if b is not None and isinstance(b, int) and b < 0:
            raise ValueError("binary= must not be negative")

        if self.sock is not None and self.use_tls is True:
            raise ValueError("can't do TLS on Unix Domain sockets")

        if self.database is not None and not valid_database_name(self.database):
            raise ValueError("invalid database name")

    def clone(self):
        return copy.copy(self)

    def parse_url(self, url: str):
        self.user_password_barrier()
        self.use_tls = None
        self.host = None
        self.port = None
        self.database = None
        if url.startswith("mapi:"):
            self._parse_mapi_monetdb_url(url)
        else:
            self._parse_monetdb_url(url)
        self.user_password_barrier()

    def _parse_monetdb_url(self, url: str):
        parsed = urlparse(url, allow_fragments=True)

        if parsed.scheme == "monetdb":
            self.use_tls = False
        elif parsed.scheme == "monetdbs":
            self.use_tls = True
        else:
            raise ValueError("invalid URL scheme: " + parsed.scheme)

        self.host = parsed.hostname
        self.port = parsed.port
        path = parsed.path
        if path:
            parts = path.split("/", 3)
            # 0: always empty
            # 1: database name
            # 2: schema name
            # 3: table name
            assert parts[0] == ""
            if len(parts) >= 4 and "/" in parts[3]:
                raise ValueError("invalid table name: " + parts[3])
            database = parts[1]
            try:
                database = strict_percent_decode(database)
            except ValueError as e:
                raise ValueError("database: invalid percent encoding") from e
            self.database = database or None

        for name, value in parse_qsl(parsed.query):
            self.set_from_text(name, value, only_params=True)

    def _parse_mapi_monetdb_url(self, url: str, allow_override_database=False):  # noqa: C901
        # mapi urls have no percent encoding at all.
        parsed = urlparse(url[5:])
        if parsed.scheme != "monetdb":
            raise ValueError("invalid scheme: " + parsed.scheme)
        self.use_tls = False
        if parsed.username is not None:
            self.user = parsed.username
        if parsed.password is not None:
            self.password = parsed.password
        self.host = parsed.hostname
        self.port = parsed.port

        path = parsed.path
        if path is not None:
            if self.host is None and self.port is None:
                self.sock = path
            else:
                path = path[1:]
                if "/" in path:
                    raise ValueError("invalid database name")
                self.database = path

        # do it manually, the library functions perform percent decoding
        if parsed.query:
            self.parse_mapi_query(parsed.query)

    def parse_mapi_merovingian_url(self, url: str):
        if not url.startswith('mapi:merovingian://proxy'):
            raise ValueError("invalid mapi:merovingian URL: " + url)
        if len(url) == 24:
            # prefix is all there was
            return
        if url[24] != '?':
            raise ValueError("invalid mapi:merovingian URL: " + url)
        self.parse_mapi_query(url[25:])

    def parse_mapi_query(self, query: str):
        for part in query.split("&"):
            if part.startswith("language="):
                self.language = part[9:]
            elif part.startswith("database="):
                self.database = part[9:]
            elif part.startswith('user=') or part.startswith('password='):
                # ignore
                pass
            elif part.startswith('binary='):
                self.set_from_text('binary', part[7:])
            elif part.startswith('replysize='):
                self.set_from_text('replysize', part[10:])
            elif part.startswith('maxprefetch='):
                self.set_from_text('maxprefetch', part[12:])
            else:
                part = part.split("=", 1)[0]
                raise ValueError("illegal parameter: " + part)

    def set_from_text(self, name: str, text: Optional[str], only_params=True):
        if name in PARSE_PARAM:
            parser = PARSE_PARAM[name]
        elif name in IGNORE_PARAM:
            return
        elif name in PARSE_OTHER:
            if only_params:
                raise ValueError(f"field '{name}' cannot be set as a query parameter")
            parser = PARSE_OTHER[name]
        else:
            raise ValueError(f"invalid settings '{name}'")

        try:
            val = None if text is None else parser(text) if parser else text
        except ValueError as e:
            raise ValueError(f"invalid value for {name}: {e}")
        setattr(self, name, val)

    def get_as_text(self, name: str) -> Optional[str]:
        if name in ALL_FIELDS or name.startswith("effective_"):
            val = getattr(self, name)
        elif name == "valid":
            try:
                self.validate()
                val = True
            except ValueError:
                val = False
        else:
            raise ValueError(f"field '{name}' does not exist")

        if val is None:
            return None
        elif val is True:
            return "true"
        elif val is False:
            return "false"
        else:
            return str(val)

    def apply_connect_kwargs(  # noqa C901
        self,
        database=None,
        hostname=None,
        port=None,
        username=None,
        password=None,
        unix_socket=None,
        language=None,
        autocommit=None,
        host=None,
        user=None,
        connect_timeout=None,
        binary=None,
        replysize=None,
        maxprefetch=None,
        use_tls=False,
        server_cert=None,
        server_fingerprint=None,
        client_key=None,
        client_cert=None,
        client_key_password=None,
        dangerous_tls_nocheck=None,
    ):
        """
        Apply kwargs such as taken by pymonetdb.connect().
        If 'database' is a URL it is parsed after the other parameters have been
        processed.
        Calls user_password_barrier() before and after.
        """

        # Aliases for host=hostname, user=username, the DB API spec is not specific about this
        if host:
            hostname = host
        if user:
            username = user

        self.user_password_barrier()

        if hostname is not None:
            self.host = hostname
        if port is not None:
            self.port = port
        if username is not None:
            self.user = username
        if password is not None:
            self.password = password
        if unix_socket is not None:
            self.sock = unix_socket
        if language is not None:
            self.language = language
        if autocommit is not None:
            self.autocommit = autocommit
        if connect_timeout is not None:
            self.connect_timeout = connect_timeout
        if binary is not None:
            self.binary = binary
        if replysize is not None:
            self.replysize = replysize
        if maxprefetch is not None:
            self.maxprefetch = maxprefetch
        if use_tls is not None:
            self.use_tls = use_tls
        if server_cert is not None:
            self.cert = server_cert
        if server_fingerprint is not None:
            self.fingerprint = server_fingerprint
        if client_key is not None:
            self.clientkey = client_key
        if client_cert is not None:
            self.clientcert = client_cert
        if client_key_password is not None:
            self.clientkeypassword = client_key_password
        if dangerous_tls_nocheck is not None:
            self.dangerous_tls_nocheck = dangerous_tls_nocheck

        if database is not None:
            if looks_like_url(database):
                self.parse_url(database)
            else:
                self.database = database

        self.user_password_barrier()
