==========================================
The MonetDB MAPI and SQL client python API
==========================================


Introduction
============

pymonetdb is a native python client API for monetDB. This API is cross-platform,
and doesn't depend on any monetdb libraries.  It has support for
python 2.5+, including Python 3 and PyPy and is Python DBAPI 2.0 compatible.

.. Note:: Since June 2016 pymonetdb is now the official MonetDB Python API. It
  replaces the old python-monetdb code. pymonetdb should be a drop-in
  replacement for python-monetdb. The only thing that changes is the module
  name; change `import monetdb` into `import pymonetdb`.


Installation
============

To install the MonetDB python API run the following command from the
python source directory::

 $ python setup.py install
 
pymonetdb is also available on pypi::

 $ pip install pymonetdb

That's all, now you are ready to start using the API.
