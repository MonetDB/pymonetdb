# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
import typing

from pymonetdb.exceptions import ProgrammingError
from .uploads import Uploader, Upload
from .downloads import Downloader, Download
from .directoryhandler import SafeDirectoryHandler

if typing.TYPE_CHECKING:
    from pymonetdb.mapi import Connection

# these are used in the code but they are referred to in the docs
(Uploader, Downloader, SafeDirectoryHandler)


def handle_file_transfer(mapi: "Connection", cmd: str):
    if cmd.startswith("r "):
        # r 0 filename.txt
        # where 0 is the number of lines to skip
        parts = cmd[2:].split(' ', 1)
        if len(parts) == 2:
            try:
                n = int(parts[0])
            except ValueError:
                pass
            return handle_upload(mapi, parts[1], True, n)
    elif cmd.startswith("rb "):
        # rb filename.bin
        return handle_upload(mapi, cmd[3:], False, 0)
    elif cmd.startswith("w "):
        # w filename.txt
        return handle_download(mapi, cmd[2:], True)
    elif cmd.startswith("wb "):
        # wb filename.bin
        return handle_download(mapi, cmd[3:], False)
    else:
        pass
    # we only reach this if decoding the cmd went wrong:
    mapi._putblock(f"Invalid file transfer command: {cmd!r}\n")


def handle_upload(mapi: "Connection", filename: str, text_mode: bool, offset: int):
    if not mapi.uploader:
        mapi._putblock("No upload handler has been registered with pymonetdb\n")
        return
    skip_amount = offset - 1 if offset > 0 else 0
    upload = Upload(mapi)
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
    download = Download(mapi)
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
