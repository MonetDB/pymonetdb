Development
===========


Github
------

We maintain pymonetdb on `GitHub <https://github.com/gijzelaerr/pymonetdb>`_.
If you have problems with pymonetdb, please raise an issue in the
`issue tracker <https://github.com/gijzelaerr/pymonetdb/issues>`_. Even better
is if you have a solution to the problem! In that case, you can make our lives easier
by following these steps:

 * Fork our repository on GitHub
 * Add tests that will fail because of the problem
 * Fix the problem
 * Run the test suite again
 * Commit all changes to your repository
 * Issue a GitHub pull request.

Also, we try to be pep8 compatible as much as possible, where possible and
reasonable.


Test suite
----------

pymonetdb comes with a test suite to verify that the code
works and make development easier.


Prepare test databases
^^^^^^^^^^^^^^^^^^^^^^

Most tests use an existing MonetDB database that you must prepare beforehand.
By default they try to connect to a database named "demo" but
this can be configured otherwise, see below.

Some of the tests rely on a running MonetDB daemon, to test
creating and destroying new databases. This daemon also needs to be prepared
beforehand, and configured to allow control connections.
Alternatively, you may disable the control tests by setting the environment
variable `TSTCONTROL=off`.

The commands below assume an environment without any running MonetDB processes.

Create a test database farm, e.g. "/tmp/pymonetdbtest", and the "demo"
database::

  $ monetdbd create /tmp/pymonetdbtest
  $ monetdbd start /tmp/pymonetdbtest
  $ monetdb create demo
  $ monetdb release demo

If you want to run the control tests (in `tests/test_control.py`), you need to
set a passphrase and enable remote control::

  $ monetdbd set control=yes /tmp/pymonetdbtest
  $ monetdbd set passphrase=testdb /tmp/pymonetdbtest
  $ monetdbd stop /tmp/pymonetdbtest
  $ monetdbd start /tmp/pymonetdbtest

**Note 1:** Test databases created by `test_control.py` are cleaned up after the
control tests have finished. However, the `demo` database and the MonetDB daemon
itself are neither stopped nor destroyed.

**Note 2:** The above commands are also in the file `tests/initdb.sh`.  Once the
database farm has been created, you can use that script to do the remaining
work::

  $ tests/initdb.sh demo /tmp/pymonetdbtest

**WARNING:** `initdb.sh` will destroy the given database `demo` *WITHOUT*
asking for confirmation!


Run tests
^^^^^^^^^

There are many ways to run the tests.
Below we list several often-used commands.
The commands should be run in the root directory of the pymonetdb source directory.

* With Python unittest::

  $ python -m unittest # to run all tests
  $ python -m unittest -f # to run all tests but stop after the first failure
  $ python -m unittest -v # to run all tests and get information about individual test
  $ python -m unittest -v tests.test_policy # to run all tests of the module "tests.test_policy"
  $ python -m unittest -v -k test_fetch # to run the sub-test set "test_fetch*"

* With `pytest`::

  $ pytest # to run all tests
  $ pytest -v # to run all tests and get information about individual test
  $ pytest -v tests/test_oid.py # to run one test file

* With `make`::

  $ make test

Note: `make test` creates a `venv` in which it installs and runs `pytest`.  If
you get the error "Could not install packages due to an OSError: [Errno 39]
Directory not empty: '_internal'", it is probably because your pymonetdb source
is in a Vagrant shared folder.  A simple workaround is to move your pymonetdb
source to a local folder on your VM. See also `vagrant`_.

.. _vagrant: https://github.com/hashicorp/vagrant/issues/12057

* With `tox`::

  $ pip install tox; tox

Note: If it is not listed there, you must add your Python version to the `envlist` in the
`tox.ini` file.

Environment variables
^^^^^^^^^^^^^^^^^^^^^

Several environment variables are defined in `tests/util.py`.
Many of them are self-explanatory.
Here we just highlight a few:

* TSTDB is the name of the preexisting database used by most of the tests.
  TSTHOSTNAME, TSTUSERNAME, TSTPASSWORD and MAPIPORT control the other connection
  parameters. Note that for historical reasons it is MAPIPORT, not TSTPORT.

* TSTPASSPHRASE is the Merovingian passphrase you must set to run the control
  test (see `Prepare test databases`_ above).

* Some tests are skipped unless you set TSTFULL to `true`, e.g.::

  $ TSTFULL=true python3 -m unittest -v tests/test_control.py

* TSTCONTROL is used to control the tests in `test_control.py`. The default
  `tcp,local` means run the tests over TCP/IP (e.g. on port 50000) and the Unix
  domain socket (e.g. "/tmp/s.merovingian.50000"). When you run MonetDB in,
  e.g., a Docker container, you can turn off the tests over the Unix socket
  using `TSTCONTROL=tcp`.  If you want to turn off all Merovingian tests, you
  can use `TSTCONTROL=off` (actually, any string other than "tcp" and "local"
  will do)::

  $ TSTFULL=true TSTCONTROL=tcp  python3 -m unittest -v tests/test_control.py

* TSTREPLYSIZE, TSTMAXPREFETCH and TSTBINARY control the size and format of the
  result set transfer (see :ref:`batch_size`). Check out the tests in
  `test_policy.py` for examples of implemented data transfer policies and how
  setting the variables `replysize`, `maxprefetch` and `binary` affects those
  policies.
