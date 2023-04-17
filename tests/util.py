"""
Module containing shared utilities only used for testing MonetDB
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

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

test_mapi_args = {
    'port': test_port,
    'database': test_database,
    'hostname': test_hostname,
    'username': test_username,
    'password': test_password,
}
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

try:
    import_module('lz4.frame')
    test_have_lz4 = True
except ModuleNotFoundError:
    test_have_lz4 = False
