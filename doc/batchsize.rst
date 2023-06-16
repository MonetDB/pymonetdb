.. _batch_size:

Result set batch size
=====================

When a query produces a large result set, pymonetdb will often only retrieve
part of the result set, retrieving the rest later, one batch at a time.
The default behavior is to start with a reasonably small batch size but
increase it rapidly. However, if necessary, the application can configure this
behavior.  In the table below, you can see the settings controlling the behavior
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

For instance, with `replysize = 100`, the first 100 `fetchone()` calls
immediately return the next row from the cache. For the 101-st `fetchone()`,
pymonetdb will first double the `replysize` and retrieve rows 101-300 before
returning row 101. When `Cursor.fetchmany()` is used, pymonetdb also adjusts
the `replysize` to the requested stride. For example, for `fetchmany(40)`, the
first two calls will return rows from the cache. However, for the third call,
pymonetdb will first retrieve rows 101-320, i.e. double the `replysize` and
enlarge it to reach a multiple of 40, before returning rows 81 - 120.

With `Cursor.fetchall()`, all rows are retrieved at once.

New result set format
---------------------

Version Jun2023 of MonetDB introduces a new,
binary result set format that is much more efficient to parse. The initial
transfer of `replysize` rows still uses the existing text-based format;
however, the subsequent batches can be transferred much more efficiently with
the binary format. By default, pymonetdb will automatically use it when
possible unless configured otherwise using the `binary` setting, e.g.
`pymonetdb.connect('demo', binary=0)` or
`pymonetdb.connect('mapi:monetdb://localhost/demo?binary=0')`.

Normally, the binary result set transfer is transparent to the user
applications. The result set fetching functions automatically do the necessary
data conversion.  However, if you want to know explicitly if the binary format
has been used, you can use `Cursor.used_binary_protocol()`, e.g. after having
called a fetch function.

We have implemented a special case to benefit from the binary protocol even
when the `replysize` is set to -1. When pymonetdb knows that binary transfers
are possible (e.g. learnt when connecting with MoentDB) while `replysize` is
-1, it overrides the `replysize`. Pymonetdb will use a small size for the
initial transfer and then retrieve the rest of the result set in one large
binary batch.

Tweaking the behavior
---------------------

Usually, the batching behavior does not need to be tweaked.

When deciding which function to use to fetch the result sets,
`Cursor.fetchmany()` seems to be a few percent more efficient than
`Cursor.fetchall()`, while `Cursor.fetchone()` tends to be 10-15% slower.

To reduce the amount of prefetched data, set `maxprefetch` to a lower value or
even 0. The value 0 disables prefetch entirely, only fetching the requested
rows. Setting `maxprefetch` to -1 has the opposite effect: it allows the
prefetch size to increase without a bound.

If you expect the size of the individual rows to be huge, consider setting both
`replysize` and `maxprefetch` to small values, for example, 10 and 20,
respectively, or even 1 and 0. These small batch sizes limit the memory each
batch consumes. As a quick rule of thumb for the memory requirements, one can
assume that pymonetdb may need up to three times the size of the result set.
Also, remember that if MonetDB is running on the same host, the server will
also need at least that amount of memory.

Generally, one does not need to make `replysize` larger than the default
because it will grow rapidly. Furthermore, with the newer versions of MonetDB
and pymonetdb, it is better to keep the size of the initial response small to
transfer more data in the binary format.

Arraysize
---------

The batching behavior of pymonetdb is governed mainly by `replysize` and
`maxprefetch`, but the Python DBAPI also specifies the setting `arraysize`_.
The relationship between these three is as follows:

1. The `replysize` and `maxprefetch` settings are specific to pymonetdb,
   while `arraysize` comes from the Python DBAPI.

2. The DBAPI only uses `arraysize` as the default value for `fetchmany()` and
   says that it may influence the efficiency of `fetchall()`. It does not mention
   `arraysize` anywhere else.

3. In pymonetdb, the batching behavior is only influenced by `arraysize` if
   `fetchmany()` is used without an explicit size because then `arraysize` is used as the
   default size, and `fetchmany()` tries to round the batches to this size. It
   has no effect on `fetchall()` because that always fetches everything at once.

4. The DBAPI says that the default value for the `arraysize` of a newly created
   cursor is 1. Pymonetdb deviates from that, similar to, for example,
   python-oracledb_. Pymonetdb uses the `replysize` of the connection instead.
   If `replysize` is not a positive integer, the default is 100.

In general, all this means that `arraysize` needs no tweaking.

.. _python-oracledb: https://python-oracledb.readthedocs.io/en/latest/api_manual/cursor.html#Cursor.arraysize

.. _arraysize: https://peps.python.org/pep-0249/#arraysize
