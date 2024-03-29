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
from urllib.parse import ParseResult, urlencode

test_port = int(environ.get('MAPIPORT', 50000))
test_database = environ.get('TSTDB', 'demo')
test_hostname = environ.get('TSTHOSTNAME', 'localhost')
test_username = environ.get('TSTUSERNAME', 'monetdb')
test_password = environ.get('TSTPASSWORD', 'monetdb')
test_passphrase = environ.get('TSTPASSPHRASE', 'testdb')
test_full = environ.get('TSTFULL', 'false').lower() == 'true'
test_control = environ.get('TSTCONTROL', 'tcp,local')
test_replysize = environ.get('TSTREPLYSIZE')
test_maxprefetch = environ.get('TSTMAXPREFETCH')
test_binary = environ.get('TSTBINARY')

test_use_tls = environ.get('TSTTLS', 'false').lower() == 'true'
test_tls_server_cert = environ.get('TSTSERVERCERT')

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

test_mapi_args = {
    'port': test_port,
    'database': test_database,
    'hostname': test_hostname,
    'username': test_username,
    'password': test_password,
    'tls': test_use_tls,
    # 'cert': test_tls_server_cert,
}
if test_tls_server_cert:
    test_mapi_args['cert'] = test_tls_server_cert


control_test_args = {**test_mapi_args}
del control_test_args['password']
control_test_args['passphrase'] = test_passphrase

test_args = test_mapi_args.copy()
if test_replysize is not None:
    test_args['replysize'] = int(test_replysize)
if test_maxprefetch is not None:
    test_args['maxprefetch'] = int(test_maxprefetch)
if test_binary is not None:
    test_args['binary'] = int(test_binary)

# construct the corresponding mapi url
if ':' in test_database:
    test_url = test_database
else:
    assert ':' not in test_password
    assert ':' not in test_username
    assert '@' not in test_password
    assert '@' not in test_username
    query = dict()
    if test_replysize is not None:
        query['replysize'] = test_replysize
    if test_maxprefetch is not None:
        query['maxprefetch'] = test_maxprefetch
    if test_binary is not None:
        query['binary'] = test_binary

    test_url = ParseResult(
        scheme="mapi:monetdb",
        netloc=f"{test_username}:{test_password}@{test_hostname}:{test_port}",
        path=f"/{test_database}",
        params="",
        query=urlencode(query),
        fragment=""
    ).geturl()


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
