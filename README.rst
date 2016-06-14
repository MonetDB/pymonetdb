.. This Source Code Form is subject to the terms of the Mozilla Public
.. License, v. 2.0.  If a copy of the MPL was not distributed with this
.. file, You can obtain one at http://mozilla.org/MPL/2.0/.
..
.. Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

.. This document is written in reStructuredText (see
   http://docutils.sourceforge.net/ for more information).
   Use ``rst2html.py`` to convert this file to HTML.


Introduction
============

pymonetdb is a native python client API for monetDB. This API is cross-platform,
and doesn't depend on any monetdb libraries. It has support for python 2.5+,
including Python 3 and PyPy and is Python DBAPI 2.0 compatible.

Please note, this is now the official MonetDB Python API. It should be a
drop-in replacement for python-monetdb. Just change `import monetdb` statements
into `import pymonetdb` in your project.

.. image:: https://img.shields.io/travis/gijzelaerr/pymonetdb.svg
  :target: https://travis-ci.org/gijzelaerr/pymonetdb

.. image:: https://img.shields.io/coveralls/gijzelaerr/pymonetdb.svg
  :target: https://coveralls.io/github/gijzelaerr/pymonetdb?branch=master

.. image:: https://img.shields.io/pypi/v/pymonetdb.svg
  :target: https://pypi.python.org/pypi/pymonetdb

.. image:: https://img.shields.io/pypi/pyversions/pymonetdb.svg
  :target: https://pypi.python.org/pypi/pymonetdb


Installation
============

The quickest way to get started is to install pymonetdb from pypi::

    $ pip install pymonetdb


Documentation
=============

You can find the online pymonetdb documentation at 
http://pymonetdb.readthedocs.io/


License
=======

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
