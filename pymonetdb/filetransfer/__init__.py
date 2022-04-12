# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
import typing
from abc import ABC, abstractmethod
from pymonetdb.exceptions import ProgrammingError
import pymonetdb.filetransfer.uploads
import pymonetdb.filetransfer.downloads

if typing.TYPE_CHECKING:
    from pymonetdb.mapi import Connection


def handle_file_transfer(mapi: "Connection", cmd: str):
    if cmd.startswith("r "):
        parts = cmd[2:].split(' ', 2)
        if len(parts) == 2:
            try:
                n = int(parts[0])
            except ValueError:
                pass
            return handle_upload(mapi, parts[1], True, n)
    elif cmd.startswith("rb "):
        return handle_upload(mapi, cmd[2:], False, 0)
    elif cmd.startswith("w "):
        return handle_download(mapi, cmd[2:], True)
    elif cmd.startswith("wb "):
        return handle_download(mapi, cmd[3:], False)
    else:
        pass
    # we only reach this if decoding the cmd went wrong:
    mapi._putblock(f"Invalid file transfer command: {cmd!r}")


def handle_upload(mapi: "Connection", filename: str, text_mode: bool, offset: int):
    if not mapi.uploader:
        mapi._putblock("No upload handler has been registered with pymonetdb\n")
        return
    skip_amount = offset - 1 if offset > 0 else 0
    upload = pymonetdb.filetransfer.uploads.Upload(mapi)
    try:
        mapi.uploader.handle_upload(upload, filename, text_mode, skip_amount)
    except Exception as e:
        # We must make sure the server doesn't think this is a succesful upload.
        # The protocol does not allow us to flag an error after the upload has started,
        # so the only thing we can do is kill the connection
        upload.error = True
        mapi._sabotage()
        raise e
    finally:
        upload.close()
    if not upload.has_been_used():
        raise ProgrammingError("Upload handler didn't do anything")


def handle_download(mapi: "Connection", filename: str, text_mode: bool):
    if not mapi.downloader:
        mapi._putblock("No download handler has been registered with pymonetdb\n")
        return
    download = pymonetdb.filetransfer.downloads.Download(mapi)
    try:
        mapi.downloader.handle_download(download, filename, text_mode)
    except Exception as e:
        # For consistency we also drop the connection on these exceptions.
        #
        # # Alternatively we might just discard the incoming data and allow
        # work to continue, but in 99% of the cases the application is about
        # to crash and it makes no sense to delay that by first reading all
        # the data.
        #
        # Also, if the download has not really started yet we might send
        # an error message to the server but then you get inconsistent
        # behaviour: if the download hadn't started yet, the transaction ends
        # up in an aborted state and must be ROLLed BACK, but if the download
        # has started we discard all data and allow it to continue without
        # error.
        #
        # Bottom line is that it's easier to understand if we just always
        # crash the connection.
        download._shutdown()
        mapi._sabotage()
        raise e
    finally:
        download.close()


class Uploader(ABC):
    """
    Base class for upload hooks. Instances of subclasses of this class can be
    registered using pymonetdb.Connection.set_uploader(). Every time an upload
    request is received, an Upload object is created and passed to this objects
    .handle_upload() method.

    If the server cancels the upload halfway, the .cancel() methods is called
    and all further data written is ignored.
    """

    @abstractmethod
    def handle_upload(self, upload: "pymonetdb.filetransfer.uploads.Upload", filename: str, text_mode: bool, skip_amount: int):
        """
        Called when an upload request is received. Implementations should either
        send an error using upload.send_error(), or request a writer using
        upload.text_writer() or upload.binary_writer(). All data written to the
        writer will be sent to the server.

        Parameter 'filename' is the file name used in the COPY INTO statement.
        Parameter 'text_mode' indicates whether the server requested a text file
        or a binary file. In case of a text file, 'skip_amount' indicates the
        number of lines to skip. In binary mode, 'skip_amount' is always 0.

        SECURITY NOTE! Make sure to carefully validate the file name before
        opening files on the file system. Otherwise, if an adversary has taken
        control of the network connection or of the server, they can use file
        upload requests to read arbitrary files from your computer
        (../../)

        """
        pass

    def cancel(self):
        """Optional method called when the server cancels the upload."""
        pass


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
    def handle_download(self, download: "pymonetdb.filetransfer.downloads.Download", filename: str, text_mode: bool):
        """
        Called when a download request is received from the server. Implementations
        should either refuse by sending an error using download.send_error(), or
        request a reader using download.binary_reader() or download.text_reader().

        Parameter 'filename' is the file name used in the COPY INTO statement.
        Parameter 'text_mode' indicates whether the server requested to send a binary
        file or a text file.

        SECURITY NOTE! Make sure to carefully validate the file name before opening
        files on the file system. Otherwise, if an adversary has taken control of
        the network connection or of the server, they can use download requests to
        OVERWRITE ARBITRARY FILES on your computer
        """
        pass
