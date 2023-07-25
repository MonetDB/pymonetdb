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
    clientkey=None,
    clientcert=None,
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
    use_tls: Optional[bool] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    sock: Optional[str] = None
    cert: Optional[str] = None
    clientkey: Optional[str] = None
    clientcert: Optional[str] = None
    _user: Optional[str] = None
    _password: Optional[str] = None
    language: Optional[str] = None
    autocommit: Optional[bool] = None
    schema: Optional[str] = None
    timezone: Optional[str] = None
    replysize: Optional[int] = None
    maxprefetch: Optional[int] = None
    binary: Optional[Union[int, bool]] = None
    _password_set: bool = False

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

    def clone(self):
        return copy.copy(self)

    def parse_url(self, url: str):
        self.user_password_barrier()
        self.use_tls = None
        self.host = None
        self.port = None
        self.database = None
        if url.startswith("mapi:"):
            self._parse_mapi_url(url)
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

    def _parse_mapi_url(self, url: str):  # noqa: C901
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
            for part in parsed.query.split("&"):
                if part.startswith("language="):
                    self.language = part[9:]
                elif part.startswith("database="):
                    if self.sock is not None:
                        self.database = part[9:]
                    else:
                        raise ValueError(
                            "database parameter only allowed with unix domain sockets"
                        )
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
        if name == "binary":
            val = self.binary
            if val is True:
                return "true"
            elif val is False:
                return "false"
        if name not in ALL_FIELDS:
            raise ValueError(f"field '{name}' does not exist")
        val = getattr(self, name)
        if val is None:
            return None
        return str(val)
