Prepared Statements
===================

MonetDB offers a PREPARE_ statement, which precompiles a SQL statement for
later execution. If the statement is going to be executed frequently,
precompiling will save time.

The PREPARE statement yields a numeric prepared-sql-id which can then be passed
to the EXECUTE statement to execute it. For example,

::

> sql>PREPARE SELECT ? + 42;
> execute prepared statement using: EXEC 0(...)
> ...
> sql>EXECUTE 0(100);
> +------+
> | %2   |
> +======+
> |  142 |
> +------+
> 1 tuple
> sql>

When PREPARE is called from pymonetdb, the prepared-sql-id will be made available
in the `lastrowid` attribute of the cursor. For example,

::

    with pymonetdb.connect('demo') as conn, conn.cursor() as c:
        c.execute("PREPARE SELECT ? + 42")
        exec_id = c.lastrowid
        c.execute("EXECUTE %s(%s)", [exec_id, 100])
        result = c.fetchone()[0]
        assert result == 142

Note: MonetDB versions older than Dec2023 (11.49.x) drop all prepared statements whenever
the transaction fails. From Dec2023 onward, this has been corrected.


.. _PREPARE: https://www.monetdb.org/documentation/user-guide/sql-manual/data-manipulation/prepare-statement/