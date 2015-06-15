#!/usr/bin/env python

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2014 MonetDB B.V.
# All Rights Reserved.

import os
from setuptools import setup
from pymonetdb import __version__


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(name='pymonetdb',
    version=__version__,
    description='Native MonetDB client Python API',
    long_description=read('README.rst'),
    author='MonetDB BV',
    author_email='info@monetdb.org',
    url='http://www.monetdb.org/',
    packages=['pymonetdb', 'pymonetdb.sql'],
    download_url='https://github.com/gijzelaerr/pymonetdb',
    classifiers=[
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: Other/Proprietary License",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: Implementation :: PyPy",
    ]
)


