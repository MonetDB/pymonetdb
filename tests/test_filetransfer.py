"""
This is the python implementation of the mapi protocol.
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


import codecs
from io import StringIO
import os
from pathlib import Path
import re
from shutil import copyfileobj
import sys
from tempfile import mkdtemp
from typing import Optional, Tuple
from unittest import TestCase, skipUnless


from pymonetdb import connect, Error as MonetError
from pymonetdb.exceptions import OperationalError, ProgrammingError
from pymonetdb import Download, Downloader, Upload, Uploader
from pymonetdb.filetransfer import DefaultHandler
from tests.util import test_args, test_full


class MyException(Exception):
    pass


class MyUploader(Uploader):
    rows: int = 5_000
    error_at: Optional[int] = None
    chunkSize: int = 10_000
    force_binary: bool = False
    forget_to_return_after_error: bool = False
    do_nothing_at_all: bool = False
    ignore_cancel: bool = False
    #
    cancelled_at: Optional[int] = None

    def handle_upload(self, upload: Upload, filename: str, text_mode: bool, skip_amount: int):
        if self.do_nothing_at_all:
            return
        if filename.startswith("_"):
            # do nothing at all
            return
        elif filename.startswith("x"):
            upload.send_error("filename must not start with 'x'")
            if not self.forget_to_return_after_error:
                return

        iter = range(skip_amount + 1, self.rows + 1)
        upload.set_chunk_size(self.chunkSize)
        if text_mode and not self.force_binary:
            tw = upload.text_writer()
            for i in iter:
                if i == self.error_at:
                    raise MyException(f"Oops {i}")
                if upload.is_cancelled() and not self.ignore_cancel:
                    self.cancelled_at = i
                    break
                s = f"{i}\n"
                tw.write(s)
        else:
            bw = upload.binary_writer()
            for i in iter:
                if i == self.error_at:
                    raise MyException(f"Oops {i}")
                if upload.is_cancelled() and not self.ignore_cancel:
                    self.cancelled_at = i
                    break
                s = f"{i}\n"
                bw.write(bytes(s, 'ascii'))


class MyDownloader(Downloader):
    lines: Optional[int] = None
    error_at_line: Optional[int] = None
    refuse: Optional[str] = None
    forget_to_return_after_refusal: bool = False
    buffer: StringIO

    def __init__(self):
        self.buffer = StringIO()

    def handle_download(self, download: Download, filename: str, text_mode: bool):
        if self.refuse:
            download.send_error(self.refuse)
            if not self.forget_to_return_after_refusal:
                return
        if self.lines is None:
            copyfileobj(download.text_reader(), self.buffer)
        else:
            i = 0
            for line in download.text_reader():
                if i >= self.lines >= 0:
                    break
                elif self.error_at_line is not None and self.error_at_line == i:
                    raise MyException("oopsie")
                i += 1
                self.buffer.write(line)

    def get(self):
        return self.buffer.getvalue()


class TestFileTransfer(TestCase):
    first = True
    tmpdir: Optional[Path] = None

    def file(self, filename):
        if not self.tmpdir:
            self.tmpdir = Path(mkdtemp(prefix="filetrans_"))
        return self.tmpdir.joinpath(filename)

    def open(self, filename, mode, **kwargs):
        fullname = self.file(filename)
        return open(fullname, mode, **kwargs)

    def setUp(self):
        super().setUp()
        self.conn = conn = connect(**test_args)
        self.uploader = MyUploader()
        conn.set_uploader(self.uploader)
        self.downloader = MyDownloader()
        conn.set_downloader(self.downloader)

        self.cursor = c = self.conn.cursor()
        if self.first:
            c.execute('DROP TABLE IF EXISTS foo')
            c.execute('CREATE TABLE foo(i INT)')
            c.execute('DROP TABLE IF EXISTS foo2')
            c.execute('CREATE TABLE foo2(i INT, t VARCHAR(10))')
            conn.commit()
            self.first = False

    def tearDown(self):
        try:
            self.cursor.close()
            self.conn.rollback()
            self.conn.close()
        except MonetError:
            pass
        super().tearDown()

    def fill_foo(self, nrows):
        self.execute("INSERT INTO foo(i) SELECT * FROM sys.generate_series(1, %s + 1)", [nrows])

    def execute(self, *args, **kwargs):
        return self.cursor.execute(*args, **kwargs)

    def expect(self, expected_resultset):
        actual_resultset = self.cursor.fetchall()
        self.assertEqual(expected_resultset, actual_resultset)

    def expect1(self, value):
        self.expect([(value,)])

    def test_do_nothing_at_all(self):
        self.uploader.do_nothing_at_all = True
        with self.assertRaises(ProgrammingError):
            # Handler must either refuse or create a writer.
            # Not writing to the writer is not a problem, that's just an empty file
            self.execute("COPY INTO foo FROM 'foo' ON CLIENT")

    def test_upload(self):
        self.execute("COPY INTO foo FROM 'foo' ON CLIENT")
        self.execute("SELECT COUNT(*) FROM foo")
        self.expect1(self.uploader.rows)

    # Also see test_NormalizeCrLf from the Java tests
    def test_upload_crlf(self):
        class CustomUploader(Uploader):
            def handle_upload(self, upload: Upload, filename: str, text_mode: bool, skip_amount: int):
                w = upload.text_writer()
                w.write("1|A\r\n2|BB\r")
                w.flush()
                w.write("\n3|CCC\r\n")

        self.conn.set_uploader(CustomUploader())
        self.execute("COPY INTO foo2 FROM 'foo2' ON CLIENT")
        self.execute("SELECT i, t FROM foo2")
        self.expect([(1, "A"), (2, "BB"), (3, "CCC")])

    def test_client_refuses_upload(self):
        # our Uploader refuses filename that start with 'x'
        with self.assertRaises(OperationalError):
            self.execute("COPY INTO foo FROM 'xfoo' ON CLIENT")

    def test_upload_offset0(self):
        # OFFSET 0 and OFFSET 1 behave the same, they do nothing
        self.uploader.chunkSize = 100
        self.uploader.rows = 100
        self.execute("COPY OFFSET 0 INTO foo FROM 'foo' ON CLIENT")
        self.execute("SELECT MIN(i) AS mi, MAX(i) AS ma FROM foo")
        self.expect([(1, 100)])

    def test_upload_offset1(self):
        # OFFSET 0 and OFFSET 1 behave the same, they do nothing
        self.uploader.chunkSize = 100
        self.uploader.rows = 100
        self.execute("COPY OFFSET 1 INTO foo FROM 'foo' ON CLIENT")
        self.execute("SELECT MIN(i) AS mi, MAX(i) AS ma FROM foo")
        self.expect([(1, 100)])

    def test_upload_offset5(self):
        self.uploader.chunkSize = 100
        self.uploader.rows = 100
        self.execute("COPY OFFSET 5 INTO foo FROM 'foo' ON CLIENT")
        self.execute("SELECT MIN(i) AS mi, MAX(i) AS ma FROM foo")
        self.expect([(5, 100)])

    def test_server_cancels_upload(self):
        # self.uploader.chunkSize = 100
        self.execute("COPY 10 RECORDS INTO foo FROM 'foo' ON CLIENT")
        self.assertGreater(self.uploader.cancelled_at, 0)
        self.execute("SELECT COUNT(*) FROM foo")
        self.expect1(10)

    def test_download_refused(self):
        self.downloader.refuse = 'no thanks'
        with self.assertRaises(OperationalError):
            self.execute("COPY (SELECT * FROM foo) INTO 'foo' ON CLIENT")
        # connection still alive
        self.conn.rollback()
        self.execute("SELECT 42")
        self.expect1(42)

    def test_download(self):
        self.fill_foo(5)
        self.execute("COPY (SELECT * FROM foo) INTO 'foo' ON CLIENT")
        self.assertEqual("1\n2\n3\n4\n5\n", self.downloader.get())

    def test_download_lines(self):
        self.downloader.lines = -1
        self.fill_foo(5)
        self.execute("COPY (SELECT * FROM foo) INTO 'foo' ON CLIENT")
        self.assertEqual("1\n2\n3\n4\n5\n", self.downloader.get())

    def test_download_stop_reading_halfway(self):
        self.fill_foo(10000)
        self.downloader.lines = 10
        self.execute("COPY (SELECT * FROM foo) INTO 'foo' ON CLIENT")
        self.assertEqual("1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", self.downloader.get())
        # connection still alive
        self.execute("SELECT 42")
        self.expect1(42)

    @skipUnless(test_full, "full test disabled")
    def test_large_upload(self):
        n = 1_000_000
        self.uploader.rows = n
        self.uploader.chunkSize = 1024 * 1024
        self.execute("COPY INTO foo FROM 'foo' ON CLIENT")
        self.assertIsNone(self.uploader.cancelled_at)
        self.execute("SELECT COUNT(*) FROM foo")
        self.expect1(n)

    @skipUnless(test_full, "full test disabled")
    def test_large_download(self):
        n = 1_000_000
        self.fill_foo(n)
        self.execute("COPY (SELECT * FROM foo) INTO 'banana' ON CLIENT")
        content = self.downloader.get()
        nlines = len(content.splitlines())
        self.assertEqual(n, nlines)

    def test_upload_native_text_file(self):
        self.upload_file('native.csv', {}, True)

    def test_upload_unix_text_file(self):
        self.upload_file('unix.csv', dict(newline="\n"), True)

    def test_upload_dos_text_file(self):
        self.upload_file('dos.csv', dict(newline="\r\n"), True)

    def test_upload_unix_binary_file(self):
        self.upload_file('unix.csv', dict(newline="\n"), False)

    def upload_file(self, filename, write_opts, read_text):
        n = 1000
        f = self.open(filename, 'w', **write_opts)
        for i in range(n):
            print(f"{i}|Únïçøðε{i}", file=f)
        f.close()
        testcase = self

        class CustomUploader(Uploader):
            def handle_upload(self, upload: Upload, filename: str, text_mode: bool, skip_amount: int):
                if read_text:
                    f = testcase.open(filename, 'r')
                    tw = upload.text_writer()
                    copyfileobj(f, tw)
                else:
                    f = testcase.open(filename, 'rb')
                    bw = upload.binary_writer()
                    copyfileobj(f, bw)
                f.close()

        self.conn.set_uploader(CustomUploader())
        self.execute(f"COPY INTO foo2 FROM '{filename}' ON CLIENT")
        self.execute("SELECT t FROM foo2 where i = 999")
        self.expect1("Únïçøðε999")

    def test_fail_upload_late(self):
        self.uploader.error_at = 99
        # If the handler raises an exception, ..
        with self.assertRaises(MyException):
            self.execute("COPY INTO foo FROM 'foo' ON CLIENT")
        # .. the connection is dropped
        with self.assertRaisesRegex(ProgrammingError, "ot connected"):
            self.execute("SELECT COUNT(*) FROM foo")

    def test_download_immediate_exception(self):

        class CustomDownloader(Downloader):
            def handle_download(self, download: Download, filename: str, text_mode: bool):
                raise MyException("fail early")

        self.fill_foo(5000)
        self.conn.set_downloader(CustomDownloader())
        # If the handler raises an exception, ..
        with self.assertRaises(MyException):
            self.execute("COPY (SELECT * FROM foo) INTO 'foo' ON CLIENT")
        # .. the connection is dropped
        with self.assertRaisesRegex(ProgrammingError, "ot connected"):
            self.execute("SELECT COUNT(*) FROM foo")

    def test_fail_download_late(self):
        self.fill_foo(5000)
        self.downloader.lines = 6000
        self.downloader.error_at_line = 4000
        # If the handler raises an exception, ..
        with self.assertRaises(MyException):
            self.execute("COPY (SELECT * FROM foo) INTO 'foo' ON CLIENT")
        # .. the connection is dropped
        with self.assertRaisesRegex(ProgrammingError, "ot connected"):
            self.execute("SELECT COUNT(*) FROM foo")

    def get_testdata_name(self, enc_name: str, newline: str) -> str:
        newline_name = {None: "none", "\n": "lf", "\r\n": "crlf"}[newline]
        return f"{enc_name}_{newline_name}.txt"

    def get_testdata(self, enc_name: str, newline: str, lines: int) -> str:
        encoding = codecs.lookup(enc_name) if enc_name else None
        fname = self.get_testdata_name(enc_name, newline)
        p = self.file(fname)
        if not p.exists():
            enc = encoding.name if encoding else None
            f = open(p, mode="w", encoding=enc, newline=newline)
            for n in range(lines):
                i, t = self.line(n)
                print(f"{i}|{t}", file=f)
            f.close()
            assert p.exists()

        return fname

    def line(self, i: int) -> Tuple[int, str]:
        k = i + 1
        if i % 7 == 0:
            s = ""
        else:
            # ÷ is interesting because it appears in all of UTF-8, Latin1 and
            # Shift-JIS, but with different encodings.
            s = f"÷{k}"
        return (k, s)

    def test_upload_encodings_and_line_endings(self):
        # We want to demonstrate the following:
        # 1. If we specify the encoding properly, files with that encoding get
        #    uploaded correctly.
        # 2. If we don't specify a line ending, or if we specify CRLF, both LF
        #    and CRLF get uploaded correctly
        # 3. If we specify LF line endings, LF-ended files upload correctly and
        #    we don't say anything about CRLF-ended files.
        encodings = [
            'utf-8',
            'latin1',
            'shift-jis',
            None  # means native
        ]
        file_endings = [
            "\n",
            "\r\n",
        ]
        offsets = [None, 0, 1, 2, 5]
        for encoding in encodings:
            for handler_ending in file_endings + [None]:
                for file_ending in file_endings:
                    if handler_ending is not None and handler_ending != file_ending:
                        continue
                    for offset in offsets:
                        with self.subTest(encoding=encoding, file_ending=file_ending, handler_ending=handler_ending, offset=offset):
                            self.perform_upload_test(encoding, file_ending, handler_ending, offset=offset)

    def perform_upload_test(self, encoding, file_ending, handler_ending, offset=None, end=10):
        if offset is None:
            offset_clause = ''
            skip = 0
        else:
            offset_clause = f" OFFSET {offset}"
            skip = offset - 1 if offset else 0
        uploader = DefaultHandler(self.file(''), encoding, handler_ending)
        self.conn.set_uploader(uploader)
        fname = self.get_testdata(encoding, file_ending, end)
        # Test the test:
        marker = {'utf-8': b'\xC3\xB7', 'latin1': b'\xF7', 'shift-jis': b'\x81\x80', None: None}[encoding]
        if marker:
            f = self.open(fname, 'rb')
            content = f.read()
            f.close()
            self.assertTrue(marker in content)
        # Run the test
        # self.conn.rollback()
        self.execute("DELETE FROM foo2")
        self.execute("COPY" + offset_clause + " INTO foo2 FROM %s ON CLIENT", [fname])
        self.execute("SELECT * FROM foo2")
        rows = self.cursor.fetchall()
        expected = [self.line(i) for i in range(skip, end)]
        self.assertEqual(expected, rows)

    def test_upload_utf8_lf_uses_binary(self):
        class CustomHandler(DefaultHandler):
            used_mode = None

            def __init__(self, dir):
                super().__init__(dir, 'utf-8', '\n')

            def handle_upload(self, upload: Upload, filename: str, text_mode: bool, skip_amount: int):
                super().handle_upload(upload, filename, text_mode, skip_amount)
                # peek into the internals of the upload
                if upload.writer:
                    self.used_mode = 'binary'
                if upload.twriter:
                    # overwrite
                    self.used_mode = 'text'

        fname = self.get_testdata('utf-8', '\n', 10)
        uploader = CustomHandler(self.file(''))
        self.conn.set_uploader(uploader)
        self.execute("COPY INTO foo2 FROM %s ON CLIENT", fname)
        self.assertEqual('binary', uploader.used_mode)

    def test_download_encodings_and_line_endings(self):
        encodings = [
            'utf-8',
            'latin1',
            'shift-jis',
            None  # means native
        ]
        file_endings = [
            "\n",
            "\r\n",
            None,
        ]
        for encoding in encodings:
            for handler_ending in file_endings + [None]:
                with self.subTest(encoding=encoding, handler_ending=handler_ending):
                    self.perform_download_test(encoding, handler_ending)

    def perform_download_test(self, encoding, handler_ending):
        # We want to check that when asked to use the given encoding and line endings,
        # this happens.
        n = 10
        downloader = DefaultHandler(self.file(''), encoding, handler_ending)
        self.conn.set_downloader(downloader)
        self.execute("DELETE FROM foo2")
        #
        enc = encoding or sys.getdefaultencoding()
        expected = b""
        for k in range(n):
            i, s = self.line(k)
            expected += bytes(str(i), enc)
            expected += b'|"'
            expected += bytes(s, enc)
            expected += b'"'
            expected += bytes(handler_ending or os.linesep, enc)
            self.execute("INSERT INTO foo2(i, t) VALUES (%s, %s)", [i, s])
        #
        fname = self.get_testdata_name(encoding, handler_ending)
        self.execute("COPY (SELECT * FROM foo2) INTO %s ON CLIENT", [fname])
        f = self.open(fname, 'rb')
        content = f.read()
        f.close()
        #
        self.assertEqual(expected, content)

    def test_download_utf8_lf_uses_binary(self):
        class CustomHandler(DefaultHandler):
            used_mode = None

            def __init__(self, dir):
                super().__init__(dir, 'utf-8', '\n')

            def handle_download(self, download: Download, filename: str, text_mode: bool):
                super().handle_download(download, filename, text_mode)
                # peek into the internals of the download
                if download.reader:
                    self.used_mode = 'binary'
                if download.treader:
                    # overwrite
                    self.used_mode = 'text'

        fname = self.get_testdata_name('utf-8', '\n')
        downloader = CustomHandler(self.file(''))
        self.conn.set_downloader(downloader)
        self.execute("COPY SELECT * FROM sys.generate_series(0,10) INTO %s ON CLIENT", fname)
        self.assertEqual('binary', downloader.used_mode)