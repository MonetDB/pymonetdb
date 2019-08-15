"""
This implements a connection to the profiler.
"""

from pymonetdb import mapi


class ProfilerConnection(object):
    """
    A connection to the MonetDB profiler.
    """

    def __init__(self):
        self._mapi = None
        self._heartbeat = 0
        self._buffer = ""
        self._objects = list()

    def connect(self, database, username, password, hostname=None, port=50000, heartbeat=0):
        self._heartbeat = heartbeat
        self._mapi = mapi.Connection()
        self._mapi.connect(database, username, password, "mal", hostname, port)
        self._mapi.cmd("profiler.setheartbeat(%d);\n" % heartbeat)
        self._mapi.cmd("profiler.openstream(3);\n")

    def read_object(self):
        self._buffer = self._mapi._getblock()
        while not self._buffer.endswith("}\n"):
            self._buffer += self._mapi._getblock()

        return self._buffer[:-1]
