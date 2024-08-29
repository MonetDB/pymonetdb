# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

# This file is used by test_connect_timeout.py. It should be run as
#      .../path/to/python3 -m tests.timeout_helper
#           URL
#           [-g GLOBAL_TIMEOUT]
#           [-e EXPECTED_EXCEPTION...]

#
# It will try to connect to the monetdb url passed as a parameter.
#
# On success, it will write the duration in seconds to stdout and exit with exit
# code 0
#
# If an exception e is thrown such that str(e) contains one of the
# EXPECTED_EXCEPTION's, it will write the duration in seconds to stdout and exit
# with exit code 1
#
# If any other exception is thrown, it will write the stack trace to stderr and
# exit with exit code 2.

import argparse
import socket
import sys
import time
import traceback

import pymonetdb


argparser = argparse.ArgumentParser()
argparser.add_argument('url')
argparser.add_argument('-g', '--global-timeout', type=float)
argparser.add_argument('-e', '--expect-exceptions', nargs='+')


def main(args):
    if args.global_timeout is not None:
        socket.setdefaulttimeout(args.global_timeout)
    try:
        t0 = time.time()
        with pymonetdb.connect(args.url):
            print(time.time() - t0)
            return 0
    except Exception as e:
        if args.expect_exceptions:
            for msg in args.expect_exceptions:
                if msg in str(e) or msg in str(type(e)):
                    print(time.time() - t0)
                    return 1
        print(traceback.format_exc(), file=sys.stderr)
        return 2


if __name__ == "__main__":
    args = argparser.parse_args()
    sys.exit(main(args) or 0)
