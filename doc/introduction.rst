==========================================
The MonetDB MAPI and SQL client Python API
==========================================


Introduction
============

pymonetdb is a native Python client API for monetDB. This API is cross-platform
and does not depend on any MonetDB libraries.  It supports
Python 3.6+ and PyPy and is Python DBAPI 2.0 compatible.

.. Note:: Since June 2016 pymonetdb has become the official MonetDB Python API. It
  replaces the old python-monetdb code. pymonetdb should be a drop-in
  replacement for python-monetdb. The only thing that changes is the module
  name; change `import monetdb` into `import pymonetdb`.


Installation
============

To install the MonetDB Python API, run the following command from the
Python source directory::

 $ python setup.py install

pymonetdb is also available on PyPI::

 $ pip install pymonetdb

That's all, and now you are ready to start using the API.
