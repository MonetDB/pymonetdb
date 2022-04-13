"""
Classes related to file transfer requests as used by COPY INTO ON CLIENT.
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


from abc import ABC, abstractmethod
from io import BufferedIOBase, TextIOWrapper
from pymonetdb import mapi as mapi_protocol
from pymonetdb.exceptions import ProgrammingError


class Download:
    """
    Represents a request from the server to download data from the server. It is
    passed to the Downloader registered by the application, which for example
    might write the data to a file on the client system. See
    pymonetdb.Connection.set_downloader().

    Use the method send_error() to refuse the download, binary_reader() to get a
    binary file object to read bytes from, or text_reader() to get a text-mode
    file object to read text from.

    Implementations should be EXTREMELY CAREFUL to validate the file name before
    opening and writing to any files on the client system!
    """

    def __init__(self, mapi: "mapi_protocol.Connection"):
        self.mapi = mapi
        self.started = False
        self.buffer = bytearray(8190)
        self.pos = 0
        self.len = 0
        self.reader = None
        self.treader = None

    def send_error(self, message: str) -> None:
        """
        Tell the server the requested download is refused
        """
        if self.started:
            raise ProgrammingError("Cannot send error anymore")
        if not self.mapi:
            return
        self.started = True
        if not message.endswith("\n"):
            message += "\n"
        self.mapi._putblock(message)
        self._shutdown()

    def binary_reader(self):
        """Returns a binary file-like object to read the downloaded data from."""
        if not self.reader:
            if not self.mapi:
                raise ProgrammingError("download has already been closed")
            self.started = True
            self.mapi._putblock("\n")
            self.reader = DownloadIO(self)
        return self.reader

    def text_reader(self):
        """Returns a text mode file-like object to read the downloaded data from."""
        if not self.treader:
            self.treader = TextIOWrapper(self.binary_reader(), encoding='utf-8', newline='\n')
        return self.treader

    def close(self):
        """End the download succesfully. Any unconsumed data will be discarded."""
        while self.mapi:
            self._fetch()
        assert not self.mapi

    def _available(self) -> int:
        return self.len - self.pos

    def _consume(self, n: int) -> memoryview:
        end = min(self.pos + n, self.len)
        ret = memoryview(self.buffer)[self.pos:end]
        self.pos = end
        return ret

    def _fetch(self):
        if not self.mapi:
            return
        self.pos = 0
        self.len = 0   # safety in case of exceptions
        self.len, last = self.mapi._get_minor_block(self.buffer, 0)
        if last:
            self._shutdown()

    def _shutdown(self):
        self.started = True
        self.mapi = None


class DownloadIO(BufferedIOBase):

    def __init__(self, download: Download):
        self.download = download

    def readable(self):
        return True

    def read(self, n=0):
        if self.download._available() == 0:
            self.download._fetch()
        return bytes(self.download._consume(n))

    def read1(self, n=0):
        return self.read(n)


class Downloader(ABC):
    """
    Base class for download hooks. Instances of subclasses of this class can be
    registered using pymonetdb.Connection.set_downloader(). Every time a
    download request arrives, a Download object is created and passed to this
    objects .handle_download() method.

    SECURITY NOTE! Make sure to carefully validate the file name before opening
    files on the file system. Otherwise, if an adversary has taken control of
    the network connection or of the server, they can use download requests to
    OVERWRITE ARBITRARY FILES on your computer
    """

    @abstractmethod
    def handle_download(self, download: Download, filename: str, text_mode: bool):
        """
        Called when a download request is received. Implementations should either
        send an error using download.send_error(), or request a reader using
        download.text_reader() or download.binary_reader().

        Parameter 'filename' is the file name used in the COPY INTO statement.
        Parameter 'text_mode' indicates whether the server requested text
        or binary mode.

        SECURITY NOTE! Make sure to carefully validate the file name before
        opening files on the file system. Otherwise, if an adversary has taken
        control of the network connection or of the server, they can use file
        download requests to overwrite arbitrary files on your computer.
        (../../)
        """
        pass
