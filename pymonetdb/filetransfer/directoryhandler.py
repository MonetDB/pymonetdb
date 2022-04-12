"""
Classes related to file transfer requests as used by COPY INTO ON CLIENT.
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


import codecs
from importlib import import_module
from pathlib import Path
from shutil import copyfileobj
from typing import Optional
from pymonetdb.filetransfer.uploads import Upload, Uploader
from pymonetdb.filetransfer.downloads import Download, Downloader


class SafeDirectoryHandler(Uploader, Downloader):
    """
    File transfer handler which uploads and downloads files from a given
    directory, taking care not to allow access to files outside that directory.
    Instances of this class can be registered using the pymonetb.Connection's
    set_uploader() and set_downloader() methods.

    When downloading text files, the downloaded text is converted according to
    the `encoding` and `newline` parameters, if present. Valid values for
    `encoding` are any encoding known to Python, or None. Valid values for
    `newline` are `"\\\\n"`, `"\\\\r\\\\n"` or None. None means to use the
    system default.

    For binary up- and downloads, no conversions are applied.

    When uploading text files, the `encoding` parameter indicates how the text
    is read and `newline` is mostly ignored: both `\\\\n` and `\\\\r\\\\n` are
    valid line endings. The exception is that because the server expects its
    input to be `\\\\n`-terminated UTF-8 text,  if you set encoding to "utf-8"
    and newline to "\\\\n", text mode transfers are performed as binary, which
    improves performance. For uploads, only do this if you are absolutely,
    positively sure that all files in the directory are actually valid UTF-8
    encoded and have Unix line endings.

    If `compression` is set to True, which is the default, the
    SafeDirectoryHandler will automatically compress and decompress files with
    extensions .gz, .bz2, .xz and .lz4. Note that the first three algorithms are
    built into Python, but LZ4 only works if the lz4.frame module is available.
    """

    def __init__(self, dir, encoding: Optional[str] = None, newline: Optional[str] = None, compression=True):
        self.dir = Path(dir).resolve()
        self.encoding = encoding
        self.is_utf8 = (self.encoding and (codecs.lookup('utf-8') == codecs.lookup(self.encoding)))
        self.newline = newline
        self.compression = compression

    def secure_resolve(self, filename: str) -> Optional[Path]:
        p = self.dir.joinpath(filename).resolve()
        if str(p).startswith(str(self.dir)):
            return p
        else:
            return None

    def handle_upload(self, upload: Upload, filename: str, text_mode: bool, skip_amount: int):  # noqa: C901
        """:meta private:"""  # keep the API docs cleaner, this has already been documented on class Uploader.

        p = self.secure_resolve(filename)
        if not p:
            return upload.send_error("Forbidden")

        if self.is_utf8 and self.newline == "\n" and skip_amount == 0:
            # optimization
            text_mode = False

        # open
        if text_mode:
            mode = "rt"
            encoding = self.encoding
            newline = self.newline
        else:
            mode = "rb"
            encoding = None
            newline = None
        try:
            opener = lookup_compression_algorithm(filename) if self.compression else open
        except ModuleNotFoundError as e:
            return upload.send_error(str(e))
        try:
            f = opener(p, mode=mode, encoding=encoding, newline=newline)
        except IOError as e:
            return upload.send_error(str(e))

        with f:
            if text_mode:
                tw = upload.text_writer()
                for _ in range(skip_amount):
                    if not f.readline():
                        break
                self._upload_data(upload, f, tw)
            else:
                bw = upload.binary_writer()
                self._upload_data(upload, f, bw)

    def _upload_data(self, upload: Upload, src, dst):
        # Due to duck typing this method works equally well in text- and binary mode
        bufsize = 1024 * 1024
        while not upload.is_cancelled():
            data = src.read(bufsize)
            if not data:
                break
            dst.write(data)

    def handle_download(self, download: Download, filename: str, text_mode: bool):
        p = self.secure_resolve(filename)
        if not p:
            return download.send_error("Forbidden")

        if self.is_utf8 and self.newline == "\n":
            # optimization
            text_mode = False

        # open
        mode = "w" if text_mode else "wb"
        if text_mode:
            mode = "wt"
            encoding = self.encoding
            newline = self.newline
        else:
            mode = "wb"
            encoding = None
            newline = None
        try:
            opener = lookup_compression_algorithm(filename) if self.compression else open
        except ModuleNotFoundError as e:
            return download.send_error(str(e))
        try:
            f = opener(p, mode=mode, encoding=encoding, newline=newline)
        except IOError as e:
            return download.send_error(str(e))

        with f:
            if text_mode:
                tr = download.text_reader()
                copyfileobj(tr, f)
            else:
                br = download.binary_reader()
                copyfileobj(br, f)


def lookup_compression_algorithm(filename: str):
    lowercase = str(filename).lower()
    if lowercase.endswith('.gz'):
        mod = 'gzip'
    elif lowercase.endswith('.bz2'):
        mod = 'bz2'
    elif lowercase.endswith('.xz'):
        mod = 'lzma'
    elif lowercase.endswith('.lz4'):
        # not always available
        mod = 'lz4.frame'
    else:
        return open
    opener = import_module(mod).open
    return opener
