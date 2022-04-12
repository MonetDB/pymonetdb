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

test_port = int(environ.get('MAPIPORT', 50000))
test_database = environ.get('TSTDB', 'demo')
test_hostname = environ.get('TSTHOSTNAME', 'localhost')
test_username = environ.get('TSTUSERNAME', 'monetdb')
test_password = environ.get('TSTPASSWORD', 'monetdb')
test_passphrase = environ.get('TSTPASSPHRASE', 'testdb')
test_full = environ.get('TSTFULL', 'false').lower() == 'true'

test_args = {
    'port': test_port,
    'database': test_database,
    'hostname': test_hostname,
    'username': test_username,
    'password': test_password,
}


try:
    import_module('lz4.frame')
    have_lz4 = True
except ModuleNotFoundError:
    have_lz4 = False
