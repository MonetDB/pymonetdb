#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import os
from setuptools import setup

tests_require = ['pytest', 'mypy', 'pycodestyle']

extras_require = {
    'test': tests_require,
    'doc': ['sphinx', 'sphinx_rtd_theme'],

}


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


__version__ = '1.4.1'

setup(
    name='pymonetdb',
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
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    install_requires=['future'],
    tests_require=tests_require,
    test_suite="tests",
    extras_require=extras_require,
)
