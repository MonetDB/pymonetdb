development
===========

Github
------

We maintain pymonetdb on `github <https://github.com/gijzelaerr/pymonetdb>`_.
If you have any problems with pymonetdb please raise an issue in the
`issue tracker <https://github.com/gijzelaerr/pymonetdb/issues>`_. Even better
is if you have a solution to problem! In that case you can make our live easier
by following these steps:

 * fork our repository on github
 * Add a tests that will fail because of the problem
 * Fix the problem
 * Run the test suite again
 * Commit to your repository
 * Issue a github pull request.

Also we try to be as much pep8 compatible as possible, where possible and
reasonable.

Test suite
----------

pymonetdb comes with a test suite This test suite verifies that the code
actually works and makes development much easier.  To run
all tests please run from the source::

    $ pip install tox
    $ tox

 * MAPIPORT - what port is MonetDB running? _50000_ by default
 * TSTHOSTNAME -  where is MonetDB running? _localhost_ by default
 * TSTPASSPHRASE - what passphrase to test control command? _testdb_ by default
 * TSTDB -  what database to use for testing? _demo_ by default
 * TSTUSERNAME - username, _monetdb_ by default
 * TSTPASSWORD - password, _monetdb_ by default

Note that you first need to create and start a monetdb database. If you
want to run the control tests you need to set a passphrase and enable remote
control::

 $ monetdb create demo
 $ monetdb release demo
 $ monetdbd set control=yes <path to dbfarm>
 $ monetdbd set passphrase=testdb <path to dbfarm>

