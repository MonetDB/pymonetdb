# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


import copy


class BatchPolicy:
    """This class centralizes the decisions about result set batch sizes"""

    SMALL_NUMBER = 10
    DEFAULT_NUMBER = 100
    BIG_NUMBER = 2_500

    # To be set by the user
    binary_level = 1
    replysize = DEFAULT_NUMBER
    maxprefetch = BIG_NUMBER

    # Determined during handshake
    server_binexport_level = 0

    # per-cursor state
    last = 0

    def __init__(self):
        pass

    def clone(self) -> "BatchPolicy":
        return copy.copy(self)

    def use_binary(self) -> bool:
        return self.binary_level > 0 and self.server_binexport_level > 0

    def _effective_reply_size(self) -> int:
        if self.use_binary() and self.replysize < 0:
            # Only include a few rows in the initial reply so the rest can
            # be fetched using the binary protocol
            return self.SMALL_NUMBER
        else:
            # return replysize even when it's negative
            return self.replysize

    def handshake_reply_size(self) -> int:
        return self._effective_reply_size()

    def decide_arraysize(self) -> int:
        if self.replysize > 0:
            return self.replysize
        else:
            return self.DEFAULT_NUMBER

    def new_query(self) -> int:
        # reply size computation is the same
        reply_size = self._effective_reply_size()
        # Note how we set self.last to reply_size rather than to 0 as we do
        # in .scroll().
        # This is because the initial reply will already have happened
        # by the time we look at it.
        self.last = reply_size
        return reply_size

    def scroll(self):
        # Note how we set self.last to 0 rather than to reply_size as we do
        # in .new_query().
        # This is because scrolling does not cause a fetch, it just disposes
        # the currently cached rows.
        self.last = 0

    def batch_size(self,
                   already_used: int,
                   request_start: int, request_end: int,
                   result_end: int
                   ) -> int:

        assert request_start <= request_end <= result_end

        if self.use_binary() and self.replysize < 0:
            # special case. application wants to retrieve
            # everything in one go but we kept the initial
            # reply small because the binary protocol
            # is more efficient.
            # now retrieve the rest in one go.
            return result_end - request_start

        if self.last > 0:
            size = 2 * self.last
        else:
            size = self._effective_reply_size()
        prefetch_end = request_start + size

        # align to fetchmany stride,
        # exploit that the % operator always returns nonnegative.
        real_start = request_start - already_used
        adjustment = (real_start - prefetch_end) % (request_end - real_start)
        prefetch_end += adjustment

        # apply maxprefetch
        if self.maxprefetch >= 0:
            limit = request_end + self.maxprefetch
            if prefetch_end > limit:
                prefetch_end = limit

        # do not fetch beyond the end
        if prefetch_end > result_end:
            prefetch_end = result_end

        # We have computed how much we would prefetch, but maybe the user
        # explicitly asked for more than that
        end = max(prefetch_end, request_end)
        to_fetch = end - request_start
        self.last = to_fetch
        return to_fetch
