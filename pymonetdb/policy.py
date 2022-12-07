# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


import math
from typing import Optional


class FetchPolicy:
    """This class centralizes the decisions about arraysize, reply size, fetch size, etc."""

    DEFAULT_VALUE = 100
    DEFAULT_MAXVALUE = 100_000


    _binary_requested: bool
    _binary_supported: bool
    _fetchsize: int
    _maxfetchsize: int

    def __init__(self,
                 fetchsize_parameter: Optional[int],
                 maxfetchsize_parameter: Optional[int],
                 binary_parameter: Optional[bool],
                 ):

        self._binary_supported = False # for the time being
        self._binary_requested = binary_parameter if binary_parameter is not None else True

        if fetchsize_parameter is not None:
            self._fetchsize = int(fetchsize_parameter)
        else:
            self._fetchsize = self.DEFAULT_VALUE

        if maxfetchsize_parameter is not None:
            maxfetchsize = int(maxfetchsize_parameter)
        else:
            maxfetchsize = self.DEFAULT_MAXVALUE
        self._maxfetchsize = max(maxfetchsize, self._fetchsize)

    def set_server_supports_binary(self, server_supports_binary):
        self._binary_supported = server_supports_binary

    def use_binary(self) -> bool:
        return self._binary_requested and self._binary_supported

    def decide_connect_reply_size(self) -> int:
        """Decide the reply size to be set at connect time.

        This is sent to the server at connect time.
        """
        if self._fetchsize > 0:
            return self._fetchsize
        elif self.use_binary():
            # <= 0 means fetch all data at once but if we have binary
            # we prefer to do it in binary so we send a small positive value.
            return math.ceil(self.DEFAULT_VALUE / 10)
        else:
            # ask server to return all data at once
            return -1

    def decide_arraysize(self) -> int:
        """Decide the default arraysize for new cursors."""
        if self._fetchsize > 0:
            return self._fetchsize
        else:
            return self.DEFAULT_VALUE

    def decide_cursor_reply_size(self, arraysize: int) -> int:
        """
        Decide the reply size to use for queries on a specific cursor.

        This is the reply size set just before a new query is sent.
        """
        if self._fetchsize > 0 and arraysize > 0:
            return arraysize
        else:
            # handles the special cases around binary
            return self.decide_connect_reply_size()

    def decide_fetch_amount(self,
                            arraysize: int,
                            adjacent: int,
                            current_row: int,
                            total_rows: int
                            ) -> int:
        """Decide how large a block to fetch next

        Parameters:
        - arraysize: the cursors arraysize
        - adjacent: see below
        - current_row: first row to fetch
        - total_rows: size of result set

        Adjacent must start at 1.
        It must be incremented right after every call to this function.
        When cursor.scroll() is called it must be reset to 0.
        """

        available = total_rows - current_row

        if self._fetchsize <= 0:
            return available

        n = self.decide_cursor_reply_size(arraysize) << adjacent

        return min(available, n, self._maxfetchsize)
