Result set batch size
=====================

When a query produces a large result set, pymonetdb will often only retrieve
part of the result set, retrieving the rest later, one batch at a time.
The default behavior is to start with a reasonably small batch size but
increase it rapidly. However, if necessary the application can configure this
behavior.  In the table below you can see the settings that control the behavior
of large transfers.

==============  ==============  ==========================  ======================
Setting name    Defined by      Range                       Default
==============  ==============  ==========================  ======================
`replysize`     pymonetdb       positive integer or -1 [*]  100
`maxprefetch`   pymonetdb       positive integer or -1 [*]  2500
`arraysize`     DBAPI 2.0       positive integer            `Connection.replysize`
==============  ==============  ==========================  ======================

[*] The value -1 means unlimited.

The `replysize` and `maxprefetch` settings can be set as attributes of both
`Connection` and `Cursor`. They can also be passed as parameters in the
connection URL. The `arraysize` setting only exists for `Cursor`. It defaults to
the `replysize` of the connection when the cursor was created if that is
positive, or 100 otherwise.


Batching behavior
-----------------

When MonetDB has finished executing a query, the server includes the first rows of
the result set in its response to `Cursor.execute()`. The exact number of rows
it includes can be configured using the `replysize` setting.

How the rest of the rows are retrieved depends on how they are accessed.
`Cursor.fetchone()` and `Cursor.fetchmany()` retrieve the remaining rows
in batches of increasing size. Every batch is twice as large as the previous
one until the prefetch limit `maxprefetch` has been reached. This setting
controls the maximum number of fetched rows that are not immediately used.
With `Cursor.fetchall()`, all rows are retrieved at once.

When `Cursor.fetchmany()` is used, the batch sizes are adjusted to the requested
stride. For example, if we repeatedly call `fetchmany(40)` while `replysize` was
100, the first two calls will return rows from the cache and the third call
needs to fetch more rows. With `fetchone()`, it would have retrieved rows 101-300,
but with `fetchmany(40)`, it will enlarge the window to rows 101-320 to
reach a multiple of 40.


New result set format
---------------------

Starting at the next major release after Sep2022[*], MonetDB will support a new,
binary result set format that is much more efficient to parse. This format
cannot be used in the initial transfer of `replysize` rows but it makes the
subsequent batches much more efficient. By default, pymonetdb will automatically
use it when possible unless configured otherwise using the ‘binary’ setting.

[*] At the time of writing, the name of that release has not yet been determined.

Note that with the reply size set to -1, all data would be transferred in the
initial response so the binary protocol could be used. As a special case, when
binary transfers are possible but the reply size is set to -1, pymonetdb will
override the replysize. It will keep the initial transfer small and then
transfer the rest of the result set in one large binary batch instead.


Tweaking the behavior
---------------------

Usually, the batching behavior does not need to be tweaked.

To reduce the amount of prefetching, set `maxprefetch` to a lower value or even
to 0. Value 0 disables prefetch entirely, only ever fetching the rows needed right
now. Setting it to -1 has the opposite effect: it allows the prefetch size to
increase without bound.

If you expect the size of the individual rows to be huge, it may be a
good idea to set both `replysize` and `maxprefetch` to small values, for
example, 10 and 20, respectively, or even 1 and 0. These small batch sizes limit
the memory each batch consumes. As a
quick rule of thumb for the memory requirements, assume that pymonetdb may need
up to three times the size of the result set. Also, remember that if MonetDB is
running on the same host, the server will also need at least that amount of
memory.

Generally, it does not make sense to make `replysize` larger than the default.
The batch sizes grow quickly anyway, and with
newer versions of MonetDB and pymonetdb it is better to keep the size of
the initial response fairly small so the binary result set format can be used
more.


Arraysize
---------

The batching behavior of pymonetdb is governed mainly by `replysize` and
`maxprefetch`, but the Python DBAPI also specifies the setting `arraysize`_.
The relationship between these three is as follows:

1. The `replysize` and `maxprefetch` settings are specific to pymonetdb,
   `arraysize` comes from the Python DBAPI.

2. The DBAPI only uses `arraysize` as the default value for `fetchmany()` and
   says that it may influence the efficiency of `fetchall()`. It does not mention
   `arraysize` anywhere else.

3. In pymonetdb, the batching behavior is only influenced by `arraysize` if
   `fetchmany()` is used without an explicit size because then `arraysize` is used as the
   default size, and `fetchmany()` tries to round the batches to this size. It
   has no effect on `fetchall()` because that always fetches everything at once.

4. The DBAPI says that the default value for the `arraysize` of a newly created
   cursor is 1. Pymonetdb deviates from that, similar to, for example,
   python-oracledb_. Pymonetdb uses the replysize of the connection instead.
   If `replysize` is not a positive integer, the default is 100.

In general, all this means that `arraysize` needs no tweaking.

.. _python-oracledb: https://python-oracledb.readthedocs.io/en/latest/api_manual/cursor.html#Cursor.arraysize

.. _arraysize: https://peps.python.org/pep-0249/#arraysize