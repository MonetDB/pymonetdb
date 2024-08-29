"""
Module containing shared utilities only used for testing MonetDB
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import pymonetdb

from importlib import import_module
from os import environ
from urllib.parse import urlencode


def _delete_none_values(d):
    for k in [k for k, v in d.items() if v is None]:
        del d[k]
    return d


def _parse_mapi_env():
    """Read the environment and return a dict that can be passed to mapi.Connection.connect()"""

    # this one is an int and it does not follow the 'TST*' scheme.
    port = int(environ.get('MAPIPORT', '50000'))

    args = {
        'port': port,
        'database': environ.get('TSTDB', 'demo'),
        'hostname': environ.get('TSTHOSTNAME', 'localhost'),
        'username': environ.get('TSTUSERNAME', 'monetdb'),
        'password': environ.get('TSTPASSWORD', 'monetdb'),
        'tls': environ.get('TSTTLS'),
        'cert': environ.get('TSTSERVERCERT'),
    }
    return _delete_none_values(args)


def _parse_control_env():
    """Read the environment and return a dict that can be passed to control.Control()"""

    args = _parse_mapi_env()
    try:
        del args['password']
    except Exception:
        pass
    args['passphrase'] = environ.get('TSTPASSPHRASE', 'testdb')
    return _delete_none_values(args)


def _parse_sql_env():
    """Read the environment and return a dict that can be passed to pymonetdb.connect()"""

    args = _parse_mapi_env()
    args['replysize'] = environ.get('TSTREPLYSIZE')
    args['maxprefetch'] = environ.get('TSTMAXPREFETCH')
    args['binary'] = environ.get('TSTBINARY')
    return _delete_none_values(args)


def _construct_url():
    """Construct a URL version of the result of _parse_sql_env()"""

    # This code is crude. We could use the more sophisticated code of
    # the Target class but we're trying to test that so we'd better not use it
    # for the test itself.
    args = _parse_sql_env()
    for old, new in dict(username='user', hostname='host').items():
        if old in args:
            if new not in args:
                args[new] = args[old]
            del args[old]

    if ':' in args.get('database', ''):
        url = args.get('database', '')
    else:
        bool_strings = dict(
            true=True, false=False,
            yes=True, no=False,
            on=True, off=False)
        tls_arg = args.get('tls', 'off').lower()
        if tls_arg not in bool_strings:
            raise ValueError("TSTTLS must be yes/no/true/false/on/off")
        url = 'monetdbs://' if bool_strings[tls_arg] else 'monetdb://'
        url += args.get('host', args.get('hostname', 'localhost'))
        if args.get('port'):
            url += f":{args['port']}"
        url += '/'
        url += args.get('database', '')
    parms = dict(
        (k, v)
        for k, v in args.items()
        if k not in ['tls', 'host', 'port', 'database']
    )
    if parms:
        sep = '?' if '?' not in url else '&'
        url += sep
        url += urlencode(parms)
    return url


# These are used by the tests to connect to MonetDB:
test_mapi_args = _parse_mapi_env()
control_test_args = _parse_control_env()
test_args = _parse_sql_env()
test_url = _construct_url()


# Control the selection of tests
test_full = environ.get('TSTFULL', 'false').lower() == 'true'
test_control = environ.get('TSTCONTROL', 'tcp,local')

# The timeout tests need a 'dead' socket address
dead_address = environ.get('TSTDEADADDRESS', None)


# Configuration for tlstester.py:
#
# Hostname to connect to, must match exactly what tsltester.py is signing the
# certificates for.
test_tls_tester_host = environ.get('TSTTLSTESTERHOST')
# Main port to connect to, tlstester.py will redirect the test cases to other
# ports as well.
test_tls_tester_port = environ.get('TSTTLSTESTERPORT')
# Set to true if tlstester.py's ca3.crt has been inserted into the system
# trusted root certificate store.
test_tls_tester_sys_store = environ.get('TSTTLSTESTERSYSSTORE', 'false').lower() == 'true'


_MONETDB_VERSION = None


def have_monetdb_version_at_least(major, minor, patch):
    global _MONETDB_VERSION
    if _MONETDB_VERSION is None:
        with pymonetdb.connect(**test_args) as conn, conn.cursor() as cursor:
            cursor.execute("SELECT value FROM environment WHERE name = 'monet_version'")
            _MONETDB_VERSION = tuple(int(part) for part in cursor.fetchone()[0].split('.'))

    return _MONETDB_VERSION >= (major, minor, patch)


try:
    import_module('lz4.frame')
    test_have_lz4 = True
except ModuleNotFoundError:
    test_have_lz4 = False


# Debug the debugging
if __name__ == "__main__":
    print(f'test_mapi_args = {test_mapi_args!r}')
    print(f'control_test_args = {control_test_args!r}')
    print(f'test_args = {test_args!r}')
    print(f'test_url = {test_url!r}')
    print(f'test_full = {test_full!r}')
    print(f'test_control = {test_control!r}')
    print(f'test_tls_tester_host = {test_tls_tester_host!r}')
    print(f'test_tls_tester_port = {test_tls_tester_port!r}')
    print(f'test_tls_tester_sys_store = {test_tls_tester_sys_store!r}')
    print(f'test_have_lz4 = {test_have_lz4!r}')
    try:
        print()
        have_monetdb_version_at_least(0, 0, 0)
        print(f'_MONETDB_VERSION = {_MONETDB_VERSION!r}')
    except Exception as e:
        print(f'Error while retrieving MonetDB version number: {e}')
