# 1.7.1

changes since 1.7.0

* Bug fix: let TimeTzFromTicks and TimestampTzFromTicks use the correct time zone

* Feature: add support for the named parameter syntax that will be introduced in
  the next major version of MonetDB AFTER Jun2023. (Not Jun2023 itself.)

Example of the named parameters:

```python
import pymonetdb

# classic behavior: paramstyle pyformat
assert pymonetdb.paramstyle == 'pyformat'

with pymonetdb.connect('demo') as conn, conn.cursor() as cursor:
    parameters = dict(number=42, fruit="ban'ana")
    cursor.execute("SELECT %(number)s, %(fruit)s", parameters)
    assert cursor.fetchone() == (42, "ban'ana")

# enable named parameters
pymonetdb.paramstyle = 'named'

with pymonetdb.connect('demo') as conn, conn.cursor() as cursor:
    parameters = dict(number=42, fruit="ban'ana")
    cursor.execute("SELECT :number, :fruit", parameters)
    assert cursor.fetchone() == (42, "ban'ana")
```


# 1.7.0

changes since 1.6.4

* Add support for a new, binary result set format. This makes transferring large result sets much faster, often by a factor 3 or more. Only works in combination with MonetDB version Jun2023. This is enabled by default but can be controlled with the `binary` parameter.

* Automatically transfer large result sets in batches that grow progressively larger. This behavior can be controlled with the `replysize` and `maxprefetch` parameters. See the section on 'Result set batch size' in the documentation.

* Add optional `binary`, `replysize` and `maxprefetch` parameters to the MAPI URL syntax, equivalent to the `pymonetdb.connect()` parameters mentioned above.

* Allow to use Connection and Cursor as context managers, for example:

```python
    with pymonetdb.connect(db') as conn, conn.cursor() as cursor:
        cursor.execute("SELECT 42")
```
* Let `Cursor.execute()` return `None` for DDL statements such as `CREATE` and `DROP`, not -1. Note that PEP 249 leaves the return value of `Cursor.execute()` deliberately unspecified.

* Preserve precision of INTERVAL SECOND values, do not round them to whole seconds.

* Raise a more readable exception on DATE and TIMESTAMP values whose year is zero or negative.

* At connect time, prime the connection with a number of NUL bytes. This serves two purposes: it prevents a hang when accidentally connecting to a TLS-protected server, and interestingly, it slightly improves connection setup speed.

* Various other optimizations


# 1.6.4

changes since 1.6.3

* Correctly handle result of PREPARE statement, leave id of the prepared statement in Cursor.rowid for use in subsequent EXEC statement.

* Fix COPY ON CLIENT bug with filenames that contain spaces.

* Fix bug where not all server side result sets were closed if multiple statements were passed to one Cursor.execute() call, leading to a resource leak until the connection was closed.

* Rename Cursor.nextset(), the Python DB API reserves that name for something else.


# 1.6.3

changes since 1.6.2

* Fix crash with local control connections (#111)
* Suppress unnecessary 'close result set' commands to improve round-trip time (#110)
* Fix typo in the documentation


# 1.6.2

changes since 1.6.1

* correctly deal with DATE results where the year is less than 1000 (#108)
* fix bug for ON CLIENT binary files
* tweak documentation config for recent Sphynx versions


# 1.6.1

changes since 1.6.0

* Various packaging fixes


# 1.6.0

changes since 1.5.1

* Support for COPY INTO ON CLIENT (#57)
* Connection object leaks the password (#93)
* Make code style checking stricter (mypy and flake8) (#104)
* Make python UDF debug code work with Python3 (#45 )


# 1.5.1

changes since 1.5.0

* Fix failing unicode test by @gijzelaerr in (#98)
* Perform bin<->hex conversion directly, not in pymonetdb.Binary by @joerivanruth in (#100)


# 1.5.0

changes since 1.4.1

 - Use new MAPI handshake options, if available by @joerivanruth in (#88)
 - Allow to connect using a full mapi uri in place of the database parameter. by @joerivanruth in (#89)
 - Improve timezone handling by @joerivanruth in (#90)
 - Add Python->MonetDB UUID conversion by @kutsurak in (#92
 - Try any hash algorithm that the server gives us in order. by @sjoerdmullender in (#95)
 - Send Xclose commands to avoid server side space leaks by @joerivanruth in (#97)


# 1.4.1

changes since 1.4.0

  - Support day interval type enhancement (#81)
  - Add MBR type (#82)


# 1.4.0

changes since 1.3.0

 - SQLSTATE error code removed from error messages (#75)
 - Separate SQL errors from connection errors during query execution (#74)
 - Connection refused log message when everything is fine (#71)
 - Fix blob handling  (#67)
 - Properly hex and de-hex binary data for both py2 and py3 (#62)
 - Drop python 2 support (#55)
 - Drop python < 3.5 support (#65)


# 1.3.0

This is the last release to officially support Python < 3.5.

changes since 1.2.1

 - Change logger.warn to logger.info  (#64)
 - Add IPv6 support enhancement (#58)
 - Type analysis for OIDs (#44)
 - Fix mypy errors (#48)
 - Check PEP-249 compatibility #38)


# 1.2.1

changes since 1.2.0

 - Get MonetDB profiler events (#35)
 - low level MAPI interface: Increase the return array size? (#33)
 - Complete cleanup (#46)
 - Enable TCP_KEEPALIVE to keep consistent with stream.c (#34)


# 1.2.0

changes since 1.1.1

- Correctly monetize python datetime, date, time and timedelta types (#31)


# 1.1.1

changes since 1.1.0

- fix error made while making static code analysis happy (#30)


# 1.1.0

changes since 1.0.6

- cleanup internal naming and logic
- upcoming monetdb version will have more complicate error messages (#24)
- drop Python 2.6 support (#27)
- pymonetdb.control doesn't work over tcp (#28)
- don't use nose anymore (#29)
- license file (25)
- Make description a named tuple (#3)
- Exporting Python UDFs Using the Python Client (#26)


# 1.0.6

changes since 1.0.5:

- add timeout for server connection (#22)
- Queries ending with comments fail to execute (#20)
- Compatibility with upcoming query id field in result sets and update responses (#19)


# 1.0.5

changes since 1.0.4:

- fix cutoff long packets with utf-8 characters (#16)
- In python3 next has been renamed to __next__ (#17)


# 1.0.4

- Multiline response handling (issue #12)


# 1.0.3

- Handle MSG_INFO as a command response (issue #10)


# 1.0.2

- Added the UUID, JSON and Geometry types


# 1.0

- pymonetdb is the official MonetDB Python client now
- Added sphinx docs


# 0.1

- cleanup, pep8 compatiblity
- Use tox and travis for testing
- Fix Python3 support
- Fix an issue where query contains unicode chars
- Improved error handling (#2)
- Forked from python-monetdb
- switched to setuptools from distutils
