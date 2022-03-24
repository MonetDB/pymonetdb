Examples
========

examples usage below::

 > # import the SQL module
 > import pymonetdb
 >
 > # set up a connection. arguments below are the defaults
 > connection = pymonetdb.connect(username="monetdb", password="monetdb",
 >                                    hostname="localhost", database="demo")
 >
 > # create a cursor
 > cursor = connection.cursor()
 >
 > # increase the rows fetched to increase performance (optional)
 > cursor.arraysize = 100
 >
 > # execute a query (return the number of rows to fetch)
 > cursor.execute('SELECT * FROM tables')
 26
 >
 > # fetch only one row
 > cursor.fetchone()
 [1062, 'schemas', 1061, None, 0, True, 0, 0]
 >
 > # fetch the remaining rows
 > cursor.fetchall()
 [[1067, 'types', 1061, None, 0, True, 0, 0],
  [1076, 'functions', 1061, None, 0, True, 0, 0],
  [1085, 'args', 1061, None, 0, True, 0, 0],
  [1093, 'sequences', 1061, None, 0, True, 0, 0],
  [1103, 'dependencies', 1061, None, 0, True, 0, 0],
  [1107, 'connections', 1061, None, 0, True, 0, 0],
  [1116, '_tables', 1061, None, 0, True, 0, 0],
  ...
  [4141, 'user_role', 1061, None, 0, True, 0, 0],
  [4144, 'auths', 1061, None, 0, True, 0, 0],
  [4148, 'privileges', 1061, None, 0, True, 0, 0]]
 >
 > # Show the table meta data
 > cursor.description
 [('id', 'int', 4, 4, None, None, None),
  ('name', 'varchar', 12, 12, None, None, None),
  ('schema_id', 'int', 4, 4, None, None, None),
  ('query', 'varchar', 168, 168, None, None, None),
  ('type', 'smallint', 1, 1, None, None, None),
  ('system', 'boolean', 5, 5, None, None, None),
  ('commit_action', 'smallint', 1, 1, None, None, None),
  ('temporary', 'tinyint', 1, 1, None, None, None)]


If you would like to communicate with the database at a lower level
you can use the MAPI library::

 > from pymonetdb import mapi
 > server = mapi.Connection()
 > server.connect(hostname="localhost", port=50000, username="monetdb",
                  password="monetdb", database="demo", language="sql")
 > server.cmd("sSELECT * FROM tables;")
 ...


File Transfers
==============

Here is an example script that uploads some data from the local file system::

    #!/usr/bin/env python3

    import os
    import pymonetdb

    # Create the data directory and the CSV file
    try:
        os.mkdir("datadir")
    except FileExistsError:
        pass
    f = open("datadir/data.csv", "w")
    for i in range(10):
        print(f"{i},item{i + 1}", file=f)
    f.close()

    # Connect to MonetDB and register the upload handler
    conn = pymonetdb.connect('demo')
    handler = pymonetdb.DefaultHandler("datadir")
    conn.set_uploader(handler)
    cursor = conn.cursor()

    # Set up the table
    cursor.execute("DROP TABLE foo")
    cursor.execute("CREATE TABLE foo(i INT, t TEXT)")
    conn.commit()

    # Upload the data, this will call the handler to upload data.csv
    cursor.execute("COPY INTO foo FROM 'data.csv' ON CLIENT USING DELIMITERS ','")

    # Check that it has loaded
    cursor.execute("SELECT t FROM foo WHERE i = 9")
    row = cursor.fetchone()
    assert row[0] == 'item10'

    # Goodbye
    conn.commit()
    cursor.close()
    conn.close()

You can also write your own up- and download handler. The DefaultHandler used
above is a good example of how to do that:

.. literalinclude:: ../pymonetdb/filetransfer.py
   :pyobject: DefaultHandler