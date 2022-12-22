Result set batch size
=====================

When a query produces a large result set, pymonetdb will often not retrieve the
full result set at once. Instead it will fetch it in batches. The default
behavior for the batch size is to start fairly small and increase rapidly but if
necessary this can be configured by the application.

==============  ==============  ==========================  ======================
Setting name    Defined by      Range                       Default
==============  ==============  ==========================  ======================
`replysize`     pymonetdb       positive integer or -1 [*]  100
`maxprefetch`   pymonetdb       positive integer or -1 [*]  10000
`arraysize`     DBAPI 2.0       positive integer            `Connection.replysize`
==============  ==============  ==========================  ======================

[*] The value -1 means unlimited.

The `replysize` and `maxprefetch` settings can be set as attributes of both
`Connection` and `Cursor`. They can also be passed as parameters in the
connection url. The `arraysize` setting only exists on `Cursor`.  It defaults to
the `replysize` of the connection when the cursor was created if that is
positive, or 100 otherwise.


Batching behavior
-----------------

When the server has finished executing the query it includes the first rows of
the result set in its response to `Cursor.execute()`. The exact number of rows
it includes can be configured using the `replysize` setting.

How the rest of the rows are retrieved depends on the way they are accessed.
`Cursor.fetchone()` and `Cursor.fetchmany()` retrieve the remainder of the rows
in batches of increasing size. Every batch is twice as large as the previous
batch until the prefetch limit `maxprefetch` has been reached. This setting
controls the maximum number of rows that are fetched but are not immediately used.
With `Cursor.fetchall()` all rows are retrieved at once.

When `Cursor.fetchmany()` is used, the batch sizes are adjusted to the requested
stride. For example, if we repeatedly call `fetchmany(40)` while `replysize` was
100, the first two calls will return rows from the cache and the third call
needs to fetch more rows. With `fetchone()` it would have retrieved rows 101-300
but with `fetchmany(40)` it will enlarge the window to rows 101-320 in order to
reach a multiple of 40.


Tweaking the behavior
---------------------

Usually the batching behavior does not need to be tweaked.

To reduce the amount of prefetching, set `maxprefetch` to a lower value or even
0. Setting it to -1 has the opposite effect, it allows the prefetch size to
increase without bound. This is valid but usually it's better to set `replysize`
to -1 instead.

If you expect the size of the individual rows to be extremely large it may be a
good idea to set both `replysize` and `maxprefetch` to a small value. For
example, 10 and 20, or even 1 and 0. This reduces the number of rows that will
be in memory at a time.

If you are going to use `Cursor.fetchall()` exclusively it may be beneficial to
set `replysize` to -1 so all data is returned from the server immediately. As a
quick rule of thumb for the memory requirements assume that pymonetdb may need
up to three times the size of the result set. Also remember that if MonetDB is
running on the same host, the server will also need at least that amount of
memory.

Generally it does not make sense to make `replysize` larger than the default.
Because of prefetching the batch sizes quickly become large anyway, and with
newer versions of MonetDB and pymonetdb it has advantages to keep the size of
the initial response fairly small. This is because starting from version
VERSION, MonetDB supports a binary result set protocol which is much more
efficient to parse. However, this protocol cannot be used in the first response,
only for the subsequent batches. Setting a large `replysize` causes more rows to
be transferred in the less efficient protocol. If you set `replysize` to -1 when
binary is enabled, pymonetdb will automatically keep the initial transfer small
and retrieve the rest using the binary protocol.


Arraysize
---------

The batching behavior of pymonetdb is mostly governed by `replysize` and
`maxprefetch` but the Python DBAPI also specifies `arraysize`. The relationship
between these three is as follows:

1. The `replysize` and `maxprefetch` settings are specific to pymonetdb,
   `arraysize` comes from the Python DBAPI.

2. The DBAPI only uses `arraysize` as the default value for `fetchmany()`, and
   says that it *may* influence the efficiency of `fetchall()`. It does not use
   `arraysize` anywhere else.

3. In pymonetdb, the batching behavior is only influenced by `arraysize` if
   `fetchmany()` is used without an explicit size, because then it's used as the
   default size, and `fetchmany()` tries to round the batches to this size. It
   has no effect on `fetchall()` because that always fetches everything at once.

4. The DBAPI says that the default value for the `arraysize` of a newly created
   cursor is 1. Pymonetdb deviates from that, similar to for example
   python-oracledb_. Pymonetdb uses the `replysize` of the connection instead.
   If `replysize` is not positive, the default is 100.

In general all this means that `arraysize` needs no tweaking.

.. _python-oracledb: https://python-oracledb.readthedocs.io/en/latest/api_manual/cursor.html#Cursor.arraysize

