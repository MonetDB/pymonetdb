"""
Utilities for parsing MonetDB URLs
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


import re
from typing import Any, Callable, Union
from urllib.parse import parse_qsl, urlparse, quote as urlquote


def looks_like_url(text: str) -> bool:
    return (
        text.startswith("mapi:")
        or text.startswith("monetdb:")
        or text.startswith("monetdbs:")
        or text.startswith("monetdbe:")
    )


# Note that 'valid' is not in VIRTUAL:
CORE = set(['tls', 'host', 'port', 'database', 'tableschema', 'table'])
KNOWN = set([
    'tls', 'host', 'port', 'database', 'tableschema', 'table',
    'sock', 'sockdir', 'sockprefix', 'cert', 'certhash', 'clientkey', 'clientcert',
    'user', 'password', 'language', 'autocommit', 'schema', 'timezone',
    'binary', 'replysize', 'fetchsize', 'maxprefetch',
    'connect_timeout',
    'client_info', 'client_application', 'client_remark',
])
IGNORED = set(['hash', 'debug', 'logfile'])
VIRTUAL = set([
    'connect_scan', 'connect_sockdir',
    'connect_unix', 'connect_tcp', 'connect_port',
    'connect_tls_verify', 'connect_certhash_digits',
    'connect_binary', 'connect_clientkey', 'connect_clientcert',
])

_BOOLEANS = dict(
    true=True,
    false=False,
    yes=True,
    no=False,
    on=True,
    off=False
)

_DEFAULTS = dict(
    tls=False,
    host="",
    port=-1,
    database="",
    tableschema="",
    table="",
    sock="",
    sockdir="",
    sockprefix=".s.monetdb.",
    cert="",
    certhash="",
    clientkey="",
    clientcert="",
    user="monetdb",
    password="monetdb",
    language="sql",
    autocommit=False,
    schema="",
    timezone=None,
    binary="on",
    replysize=None,
    fetchsize=None,
    maxprefetch=None,
    connect_timeout=-1,
    client_info=True,
    client_application="",
    client_remark="",
    dangerous_tls_nocheck="",
)


def parse_bool(x: Union[str, bool]):
    if isinstance(x, bool):
        return x
    try:
        return _BOOLEANS[x.lower()]
    except KeyError:
        raise ValueError("invalid boolean value")


class urlparam:
    """Decorator to create getter/setter for url parameter on a Target instance"""

    field: str
    parser: Callable[[Union[str, Any]], Any]

    def __init__(self, name, typ, doc):
        self.field = name
        if typ == 'string' or typ == 'path':
            self.parser = str
        elif typ == 'integer':
            self.parser = int
        elif typ == 'bool':
            self.parser = parse_bool
        elif typ == 'float':
            self.parser = float
        else:
            raise ValueError(f"invalid type '{typ}'")
        self.__doc__ = doc

    def __get__(self, instance, owner):
        # don't know the meaning of the owner parameter; irrelevant?
        return instance._VALUES.get(self.field)

    def __set__(self, instance, value):
        parsed = (self.parser)(value)
        instance._VALUES[self.field] = parsed
        if self.field in instance._TOUCHED:
            instance._TOUCHED[self.field] = True

    def __delete__(self, instance):
        raise Exception("cannot delete url parameter")


class Target:
    """Holds all parameters needed to connect to MonetDB."""
    __slots__ = [
        '_VALUES',
        '_OTHERS',
        '_TOUCHED',
    ]

    def __init__(self, *, prototype=None):
        if prototype:
            self._VALUES = {**prototype._VALUES}
            self._OTHERS = {**prototype._OTHERS}
            self._TOUCHED = {**prototype._TOUCHED}
        else:
            self._VALUES = dict(**_DEFAULTS)
            self._OTHERS = {}
            self._TOUCHED = dict(user=False, password=False)

    def clone(self):
        return Target(prototype=self)

    tls = urlparam('tls', 'bool', 'secure the connection using TLS')
    host = urlparam(
        'host', 'string', 'IP number, domain name or one of the special values `localhost` and `localhost.`')
    port = urlparam('port', 'integer',
                    'TCP port, also used to pick Unix Domain socket path')
    database = urlparam('database', 'string', 'name of database to connect to')
    tableschema = urlparam('tableschema', 'string', 'only used for REMOTE TABLE, otherwise unused')
    table = urlparam('table', 'string', 'only used for REMOTE TABLE, otherwise unused')
    sock = urlparam('sock', 'path', 'path to Unix Domain socket to connect to')
    sockdir = urlparam('sockdir', 'path', 'directory where implicit Unix domain sockets are created')
    sockprefix = urlparam('sockprefix', 'string', 'prefix for implicit Unix domain sockets')
    cert = urlparam(
        'cert', 'path', 'path to TLS certificate to authenticate server with')
    certhash = urlparam(
        'certhash', 'string', 'hash of server TLS certificate must start with these hex digits; overrides cert')
    clientkey = urlparam(
        'clientkey', 'path', 'path to TLS key (+certs) to authenticate with as client')
    clientcert = urlparam(
        'clientcert', 'path', "path to TLS certs for 'clientkey', if not included there")
    user = urlparam('user', 'string', 'user name to authenticate as')
    password = urlparam('password', 'string', 'password to authenticate with')
    language = urlparam('language', 'string',
                        'for example, "sql", "mal", "msql", "profiler"')
    autocommit = urlparam('autocommit', 'bool', 'initial value of autocommit')
    schema = urlparam('schema', 'string', 'initial schema')
    timezone = urlparam('timezone', 'integer',
                        'client time zone as minutes east of UTC')
    binary = urlparam(
        'binary', 'string', 'whether to use binary result set format (number or bool)')
    replysize = urlparam('replysize', 'integer',
                         'rows beyond this limit are retrieved on demand, <1 means unlimited')
    maxprefetch = urlparam('maxprefetch', 'integer', 'specific to pymonetdb')
    connect_timeout = urlparam('connect_timeout', 'float',
                               'abort if connect takes longer than this; 0=block indefinitely; -1=system default')
    client_info = urlparam('client_info', 'bool', 'whether to send client details when connecting')
    client_application = urlparam('client_application', 'string', 'application name to send in client details')
    client_remark = urlparam('client_remark', 'string', 'application name to send in client details')
    dangerous_tls_nocheck = urlparam(
        'dangerous_tls_nocheck', 'bool',
        'comma separated certificate checks to skip, host: do not verify host, cert: do not verify certificate chain')

    # alias
    fetchsize = replysize

    def set(self, key: str, value: str):
        if key in KNOWN:
            setattr(self, key, value)
        elif key in IGNORED or '_' in key:
            self._OTHERS[key] = value
        else:
            raise ValueError(f"unknown parameter {key!r}")

    def get(self, key: str):
        if key in KNOWN or key in VIRTUAL:
            return getattr(self, key)
        elif key in IGNORED or '_' in key:
            return self._OTHERS[key]
        else:
            raise KeyError(key)

    def boundary(self):
        """If user was set and password wasn't, clear password"""
        if self._TOUCHED['user'] and not self._TOUCHED['password']:
            self.password = ''
        self._TOUCHED['user'] = False
        self._TOUCHED['password'] = False

    def summary_url(self):
        db = self.database or ''
        if self.sock:
            return f"monetdb://localhost/{db}?sock={urlquote(self.sock)}"
        scheme = "monetdbs" if self.tls else "monetdb"
        host = self.host or "localhost"
        if self.port and self.port > 0 and self.port != 50_000:
            return f"{scheme}://{host}:{self.port}/{db}"
        else:
            return f"{scheme}://{host}/{db}"

    def parse(self, url: str):
        self.boundary()
        if url.startswith("monetdb://") or url.startswith("monetdbs://"):
            self._set_core_defaults()
            self._parse_monetdb_url(url)
        elif url.startswith("mapi:monetdb://"):
            self._set_core_defaults()
            self._parse_mapi_monetdb_url(url)
        else:
            raise ValueError("URL must start with monetdb://, monetdbs:// or mapi:monetdb://")
        self.boundary()

    def _set_core_defaults(self):
        self.tls = False
        self.host = ''
        self.port = _DEFAULTS['port']
        self.database = ''

    def _parse_monetdb_url(self, url):    # noqa C901
        parsed = urlparse(url, allow_fragments=True)

        if parsed.scheme == 'monetdb':
            self.tls = False
        elif parsed.scheme == 'monetdbs':
            self.tls = True
        else:
            raise ValueError(f"Invalid URL scheme: {parsed.scheme}")

        if parsed.hostname is not None:
            host = strict_percent_decode('host name', parsed.hostname)
            if host == 'localhost':
                host = ''
            elif host == 'localhost.':
                host = 'localhost'
            self.host = host
        if parsed.port is not None:
            port = parsed.port
            if port is not None and not 1 <= port <= 65535:
                raise ValueError(f"Invalid port number: {port}")
            self.port = port

        path = parsed.path
        if path:
            parts = path.split("/")
            # 0: before leading slash, always empty
            # 1: database name
            # 2: schema name, ignored
            # 3: table name, ignored
            # more: error
            assert parts[0] == ""
            if len(parts) > 4:
                raise ValueError("invalid table name: " + '/'.join(parts[3:]))
            self.database = strict_percent_decode('database name', parts[1])
            if len(parts) > 2:
                self.tableschema = strict_percent_decode('schema name', parts[2])
            if len(parts) > 3:
                self.table = strict_percent_decode('table name', parts[3])

        if not parsed.query:
            return
        for key, value in parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True):
            if not key:
                raise ValueError("empty key is not allowed")
            key = strict_percent_decode(repr(key), key)
            value = strict_percent_decode(f"value of {key!r}", value)
            if key in CORE:
                raise ValueError(
                    "key {key!r} is not allowed in the query parameters")
            self.set(key, value)

    def _parse_mapi_monetdb_url(self, url):    # noqa C901
        # mapi urls have no percent encoding at all
        parsed = urlparse(url[5:])
        if parsed.scheme != 'monetdb':
            raise ValueError(f"Invalid scheme {parsed.scheme!r}")
        self.tls = False
        if parsed.username is not None:
            self.user = parsed.username
        if parsed.password is not None:
            self.password = parsed.password
        if parsed.hostname is not None:
            self.host = parsed.hostname
        if parsed.port is not None:
            self.port = parsed.port

        path = parsed.path
        if path is not None:
            if parsed.hostname is None and parsed.port is None:
                self.sock = path
            else:
                path = path[1:]
                self.database = path  # validation will happen later

        # parse query manually, the library functions perform percent decoding
        if not parsed.query:
            return
        for part in parsed.query.split('&'):
            # language
            if part.startswith('language='):
                self.language = part[9:]
            elif part.startswith('database='):
                self.database = part[9:]
            elif part.startswith('user=') or part.startswith('password='):
                # ignored because libmapi does so
                pass
            elif part.startswith('binary='):
                # pymonetdb-only, backward compat
                self.binary = part[7:]
            elif part.startswith('replysize='):
                # pymonetdb-only, backward compat
                self.replysize = part[10:]
            elif part.startswith('maxprefetch='):
                # pymonetdb-only, backward compat
                self.maxprefetch = part[12:]
            else:
                # unknown parameters are ignored
                pass

    def _parse_mapi_merovingian_url(self, url):    # noqa C901
        # mapi urls have no percent encoding at all
        parsed = urlparse(url[5:])
        if parsed.scheme != 'merovingian':
            raise ValueError(f"Invalid scheme {parsed.scheme!r}")
        if parsed.username is not None:
            self.user = parsed.username
        if parsed.password is not None:
            self.password = parsed.password
        if parsed.hostname is not None:
            self.host = parsed.hostname
        if parsed.port is not None:
            self.port = parsed.port

        path = parsed.path
        if path is not None:
            if parsed.hostname is None and parsed.port is None:
                self.sock = path
            else:
                path = path[1:]
                self.database = path  # validation will happen later

        # parse query manually, the library functions perform percent decoding
        if not parsed.query:
            return
        for part in parsed.query.split('&'):
            # language
            if part.startswith('language='):
                self.language = part[9:]
            elif part.startswith('database='):
                self.database = part[9:]
            elif part.startswith('user=') or part.startswith('password='):
                # ignored because libmapi does so
                pass
            elif part.startswith('binary='):
                # pymonetdb-only, backward compat
                self.binary = part[7:]
            elif part.startswith('replysize='):
                # pymonetdb-only, backward compat
                self.replysize = part[10:]
            elif part.startswith('maxprefetch='):
                # pymonetdb-only, backward compat
                self.maxprefetch = part[12:]
            else:
                # unknown parameters are ignored
                pass

    def validate(self):    # noqa C901
        # 1. The parameters have the types listed in the table in [Section
        #    Parameters](#parameters).
        #
        # This has already been checked by the url_param magic.

        # 2. At least one of **sock** and **host** must be empty.
        if self.sock and self.host:
            raise ValueError("With sock=, host must be empty or 'localhost'")

        # 3. The string parameter **binary** must either parse as a boolean or as a
        #    non-negative integer.
        #
        # Let connect_binary do all the work.
        if self.connect_binary(1) < 0:
            raise ValueError("Parameter 'binary' must be ≥ 0")

        # 4. If **sock** is not empty, **tls** must be 'off'.
        if self.sock and self.tls:
            raise ValueError("TLS cannot be used with Unix domain sockets")

        # 5. If **certhash** is not empty, it must be of the form `{sha256}hexdigits`
        #    where hexdigits is a non-empty sequence of 0-9, a-f, A-F and colons.
        if self.certhash and not _HASH_PATTERN.match(self.certhash):
            raise ValueError(f"invalid certhash: {self.certhash}")

        # 6. If **tls** is 'off', **cert** and **certhash** must be 'off' as well.
        if not self.tls and (self.cert or self.certhash):
            raise ValueError("'cert' and 'certhash' can only be used with monetdbs:")

        # 7. Parameters **database**, **tableschema** and **table** must consist only of
        #    upper- and lowercase letters, digits, dashes and underscores. They must not
        #    start with a dash.
        if self.database and not _DATABASE_PATTERN.match(self.database):
            raise ValueError(f"invalid database name {self.database!r}")
        if self.tableschema and not _DATABASE_PATTERN.match(self.tableschema):
            raise ValueError(f"invalid schema name {self.tableschema!r}")
        if self.table and not _DATABASE_PATTERN.match(self.table):
            raise ValueError(f"invalid table name {self.table!r}")

        # 8. Parameter **port**, if present, must be in the range 1-65535.
        if self.port != -1 and not 1 <= self.port <= 65535:
            raise ValueError(f"Invalid port number: {self.port}")

        # 9. If **clientcert** is set, **clientkey** must also be set.
        if self.clientcert and not self.clientkey:
            raise ValueError("clientcert can only be used together with clientkey")

        # 10. pymonetdb-specific
        if self.connect_timeout < 0 and self.connect_timeout != -1:
            raise ValueError("connection_timeout must either be >= 0 or -1")

    @property
    def connect_scan(self):
        if not self.database:
            return False
        if self.sock:
            return False
        if self.host and not self.host.startswith('/'):
            return False
        if self.port != -1:
            return False
        if self.tls:
            return False
        return True

    @property
    def connect_sockdir(self):
        if self.sockdir:
            return self.sockdir
        elif self.host and self.host.startswith('/'):
            return self.host
        else:
            return "/tmp"

    @property
    def connect_unix(self):
        if self.sock:
            return self.sock
        if self.tls:
            return ""
        if self.host == "" or self.host.startswith('/'):
            return f"{self.connect_sockdir}/{self.sockprefix}{self.connect_port}"
        return ""

    @property
    def connect_tcp(self):
        if self.sock:
            return ""
        if self.host and not self.host.startswith('/'):
            return self.host
        return "localhost"

    @property
    def connect_port(self):
        assert self.port == -1 or 1 <= self.port <= 65535
        if self.port == -1:
            return 50000
        else:
            return self.port

    @property
    def connect_tls_verify(self):
        if not self.tls:
            return ""
        if self.certhash:
            return "hash"
        if self.cert:
            return "cert"
        return "system"

    @property
    def connect_clientkey(self):
        return self.clientkey

    @property
    def connect_clientcert(self):
        return self.clientcert or self.clientkey

    def connect_binary(self, max: int):
        try:
            return int(self.binary)
        except ValueError:
            try:
                return max if parse_bool(self.binary) else 0
            except ValueError:
                raise ValueError("invalid value for 'binary': {self.binary}, must be int or bool")

    @property
    def connect_certhash_digits(self):
        m = _HASH_PATTERN.match(self.certhash)
        if m:
            return m.group(1).lower().replace(':', '')
        else:
            return None


_UNQUOTE_PATTERN = re.compile(b"[%](.?.?)")
_DATABASE_PATTERN = re.compile("^[A-Za-z0-9_][-A-Za-z0-9_.]*$")
_HASH_PATTERN = re.compile(r"^sha256:([0-9a-fA-F:]+)$")


def _unquote_fun(m) -> bytes:
    digits = m.group(1)
    if len(digits) != 2:
        raise ValueError()
    return bytes([int(digits, 16)])


def strict_percent_decode(context: str, text: str) -> str:
    try:
        return str(_UNQUOTE_PATTERN.sub(_unquote_fun, bytes(text, "ascii")), "utf-8")
    except (ValueError, UnicodeDecodeError) as e:
        raise ValueError("invalid percent escape in {context}") from e
