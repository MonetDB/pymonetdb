===============
Getting Started
===============


Installation
============

pymonetdb is available on PyPI and can be installed with the following command::

 $ pip install pymonetdb

It can also be installed from its source directory by running::

 $ python setup.py install


Connecting
==========

In its simplest form, the function :func:`pymonetdb.connect` takes a single
parameter, the database name::

    conn = pymonetdb.connect('demo')

Usually, you have to pass more::

    conn = pymonetdb.connect(
      'demo',
      hostname='dbhost', port=50001,
      username='yours', password='truly')

There are also some options you can set, for example :code:`autocommit=True`.

It is also possible to combine everything in a URL::

  url = 'mapi:monetdb://yours:truly@dbhost:50001/demo?autocommit=true'
  conn = pymonetdb.connect(url)

For more details see the documentation of :func:`pymonetdb.connect`.