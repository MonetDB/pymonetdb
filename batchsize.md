Configuring the size of pymonetdb's result sets
===============================================

When a query has a large result set, only the first few rows are returned
immediately. The client can then retrieve the remaining rows using a special
command, `Xexport <resultset_id> <firstrow> <rowcount>`.

With the upcoming addition of binary result sets, we now also have `Xexportbin
<resultset_id> <firstrow> <rowcount>`.

There is a number of parameters to choose here, this document explores how to
choose them.


Definitions
-----------

First some terminology.

**REPLY SIZE**. The number of rows returned by the server in the initial
response to a query. The client can control this using the `Xreply_size`
command, and also during the initial connection handshake.

Reply size -1 means "give me everything". Mclient uses this.

Internally, the server sometimes uses reply size -2 to indicate some sort of
internal condition. I have not tried what happens if the client asks for reply
size -2 and I'm not sure I want to know.

It seems reply size 0 also means "give me everything" but I'm not sure if that
is by design. Pymonetdb uses this, perhaps unintentionally.

**ARRAY SIZE**. This is a concept from PEP-249, the Python DB API. Every cursor
has an array size which defaults to 1, except with Oracle where it defaults to
100, and in pymonetdb, where it unfortunately defaults to 0. It is the default
value for the number of rows returned by `cursor.fetchmany()`.

This is not necessarily equal to the number of rows fetched per round-trip.

Note that only positive values make sense for this parameter. In pymonetdb we
will change it from 0 to either 1 or to some larger value such as 100.

**FETCH SIZE**. A concept from JDBC (and possibly ODBC?). The number of rows to
retrieve per round trip. This is like the reply size, but for all batches of
rows, not just the initial one.  The MonetDB JDBC driver actually only uses this
setting for the reply size, after that the fetched size grows exponentially
until a certain maximum has been reached.


Pymonetdb
---------

Pymonetdb has to make the following operational choices:

1. The default setting of `cursor.arraysize` for newly created cursors.

2. The reply size to use when sending a query.

3. Whether or not to use Xexportbin, the binary version of Xexport.

4. When using Xexportbin or Xexport, how many rows to request per round-trip.


The current version of pymonetdb, 1.6.3, behaves as follows:

1. The arraysize is set to 0.

2. The reply size is set to 100 and never changes.

3. Version 1.6.3 does not support binary result sets

4. Pymonetdb requests 'arraysize' rows, which defaults to 0, which means that
   the server returns the remainder of the result set in one go. I'm not sure
   this is the intended behavior.


Proposed behavior
-----------------

The proposed default behavior for the new version is:

1. The array size is set to 100.

2. The reply size is the array size if that is positive, otherwise 100.

3. Binary result sets are used whenever possible.

4. The number of requested rows per round trip doubles every time up to a
   configurable maximum.

We will allow the user to configure the behavior through the following settings:

Setting **binary** controls whether or not to use the binary protocol, if the
server supports it. Value `0` means not to use it, value `1` means to use the
current implementation and other positive numeric values are reserved for
possible future variations of the protocol. Furthermore, `false` is equivalent
to `0` and `true` is (currently) equivalent to `1`.

Setting **fetchsize** defaults to 100 and controls the default array size and
through that, the reply size. Nonpositive values trigger special behavior, see
below.

Setting **maxfetchsize** defaults to 10'000 or another value empirically
determined to be 'high enough'. It bounds the number of requested rows per
round-trip. The setting is ignored if it is lower than the arraysize.

The effect of a negative **fetchsize** depends on whether binary is enabled or
not, which in turn depends on the **binary** setting and on whether the server
supports it.

If binary is not enabled, a negative **fetchsize** sets the reply size to -1 and
the array size to its default, 100. This causes the server to immediately send
the entire result set, without paging.

If binary is enabled, a negative **fetchsize** sets the reply size to a small
value, say 10, the array size to its default 100, and causes the first
`Xexport`/`Xexportbin` to immediately retrieve the whole remainder of the result
set.


Use cases
---------

The default behavior should be suitable for a large number of situations
and will generally not require tweaking.

If the user has large results sets and wants to minimize overhead but is quite
sure that the result sets will fit in memory, they can set the **fetchsize** to
-1. This will either retrieve the whole set in textual form or a small number of
rows as text and then the rest as binary, which should be faster.

When testing we may want force the binary protocol to kick in sooner, or the
opposite, disable it. Disabling can be done by setting **binary=false** on the
connection. Forcing it to kick in early can be done by setting **fetchsize=1**
on the connection or **arraysize** on an individual cursor.


WORRIES / CRITICISM / FOOD FOR THOUGHT / INVITATION FOR FEEDBACK
----------------------------------------------------------------

Worried I'm missing a point. Or making things too complicated.  Have not found
the 'natural solution' here.

Should we import the concept of fetchsize from JDBC at all?  It doesn't seem too
useful except for the default value and 1 to force binary during tests. Or
should we just have an **arraysize** connection parameter rather than fetchsize?
There is value in keeping the name the same across the variant MonetDB client
libraries but in practice, we ourselves are probably the only people who
regularly use multiple language bindings. Most programmers use only one binding
so staying closer to the native vocabulary might be more useful. On the other
hand, **maxfetchsize**, possibly by another name, is still very useful.

It would be great to be able to run Mtest with different settings for **binary**
and **fetchsize** (or whatever we will call it). But Mtest communicates through
environment variables MAPI_PORT and MAPI_HOST, etc, so we would need to adjust
all tests to switch to MAPI_URL instead and then teach Mtest about the binary
settings.. Overkill?

