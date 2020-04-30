# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
"""
This implements a connection to the profiler.
"""

from pymonetdb import mapi
from pymonetdb.exceptions import OperationalError


class ProfilerConnection(object):
    """
    A connection to the MonetDB profiler.
    """

    def __init__(self):
        self._mapi = mapi.Connection()
        self._heartbeat = 0
        self._buffer = ""
        self._objects = list()

    def connect(self, database, username="monetdb", password="monetdb", hostname=None, port=50000, heartbeat=0):
        self._heartbeat = heartbeat
        self._mapi.connect(database, username, password, "mal", hostname, port)
        self._mapi.cmd("profiler.setheartbeat(%d);\n" % heartbeat)
        try:
            self._mapi.cmd("profiler.openstream();\n")
        except OperationalError as e:
            # We might be talking to an older version of MonetDB. Try connecting
            # the old way.
            self._mapi.cmd("profiler.openstream(3);\n")

    def read_object(self):
        self._buffer = self._mapi._getblock()
        while not self._buffer.endswith("}\n"):
            self._buffer += self._mapi._getblock()

        return self._buffer[:-1]
