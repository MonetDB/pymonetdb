"""
This is the python implementation of the mapi protocol.
"""
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.


import codecs
from io import BufferedIOBase, StringIO
import os
from pathlib import Path
from shutil import copyfileobj, copyfile
import signal
import struct
import sys
from tempfile import mkdtemp
from threading import Condition, Thread
import time
from typing import Optional, Tuple
from unittest import TestCase, skipUnless


from pymonetdb import connect, Error as MonetError
from pymonetdb.exceptions import OperationalError, ProgrammingError
from pymonetdb import Download, Downloader, Upload, Uploader
from pymonetdb.filetransfer.directoryhandler import SafeDirectoryHandler, lookup_compression_algorithm
from pymonetdb.filetransfer.uploads import NormalizeCrLf
from tests.util import test_have_lz4, test_args, test_full


class MyException(Exception):
    pass


class MyUploader(Uploader):
    rows: int = 5_000
    error_at: Optional[int] = None
    chunk_size: int = 10_000
    force_binary: bool = False
    forget_to_return_after_error: bool = False
    do_nothing_at_all: bool = False
    ignore_cancel: bool = False
    #
    cancelled_at: Optional[int] = None

    def handle_upload(self, upload: Upload, filename: str, text_mode: bool, skip_amount: int):  # noqa: C901
        if self.do_nothing_at_all:
            return
        elif filename.startswith("x"):
            upload.send_error("filename must not start with 'x'")
            if not self.forget_to_return_after_error:
                return

        iter = range(skip_amount + 1, self.rows + 1)
        upload.set_chunk_size(self.chunk_size)
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
    filename: Optional[str] = None

    def __init__(self):
        self.buffer = StringIO()

    def handle_download(self, download: Download, filename: str, text_mode: bool):
        self.filename = filename
        if self.refuse:
            download.send_error(self.refuse)
            if not self.forget_to_return_after_refusal:
                return
        if self.lines is None:
            copyfileobj(download.text_reader(), self.buffer)
        else:
            tr = download.text_reader()
            i = 0
            while self.lines is None or self.lines < 0 or i < self.lines:
                if i == self.error_at_line:
                    raise MyException("oopsie")
                else:
                    line = tr.readline()
                    if not line:
                        break
                    self.buffer.write(line)
                i += 1

    def get(self):
        return self.buffer.getvalue()

    def get_filename(self):
        return self.filename


class DeadManHandle:
    """
    Watchdog timer. Kills the process after a certain time. Great to detect
    deadlocks, but can be inconvenient when running tests in the debugger. In
    that case, temporarily call deadman.cancel() at the start of the test.
    """

    def __init__(self):
        self.cond = Condition()
        self.deadline = None
        self.message = None
        self.thread = Thread(target=self.work, daemon=True)
        self.thread.start()

    def set_timeout(self, t, msg):
        self.set_deadline(time.time() + t, msg)

    def set_deadline(self, d, msg):
        with self.cond:
            self.deadline = d
            if msg:
                self.message = msg
            self.cond.notify_all()

    def cancel(self):
        with self.cond:
            self.deadline = None
            self.message = None

    def work(self):
        with self.cond:
            while True:
                now = time.time()
                if self.deadline:
                    delta = self.deadline - now
                    if delta <= 0:
                        print("\n\nTIMEOUT:", self.message, "\n\n", file=sys.stderr)
                        os.kill(os.getpid(), signal.SIGKILL)
                else:
                    delta = None
                self.cond.wait(timeout=delta)


deadman = DeadManHandle()


class Common:
    first = True
    tmpdir: Optional[Path] = None

    defaultencoding = None

    def file(self, filename):
        """Resolve the given relative path within our temp directory."""
        if not self.tmpdir:
            self.tmpdir = Path(mkdtemp(prefix="filetrans_"))
        return self.tmpdir.joinpath(filename)

    def open(self, filename, mode, **kwargs):
        """Open the given filename, resolved within our temp directory"""
        fullname = self.file(filename)
        return open(fullname, mode, **kwargs)

    def commonSetUp(self):
        with self.open('checkencoding.txt', 'wt') as f:
            self.defaultencoding = f.encoding

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
            c.execute('CREATE TABLE foo2(i INT, t VARCHAR(20))')
            conn.commit()
            self.first = False

        deadman.set_timeout(10, f"timeout in {self._testMethodName}()")

    def commonTearDown(self):
        deadman.cancel()
        try:
            self.cursor.close()
            self.conn.rollback()
            self.conn.close()
        except MonetError:
            pass

    def fill_foo(self, nrows):
        self.execute("INSERT INTO foo(i) SELECT * FROM sys.generate_series(1, %s + 1)", [nrows])

    def execute(self, *args, **kwargs):
        return self.cursor.execute(*args, **kwargs)

    def expect(self, expected_resultset):
        actual_resultset = self.cursor.fetchall()
        self.assertEqual(expected_resultset, actual_resultset)

    def expect1(self, value):
        self.expect([(value,)])

    def compression_prefix(self, scheme):
        return {'gz': b'\x1F\x8B', 'bz2': b'\x42\x5A\x68', 'xz': b'\xFD\x37\x7A\x58\x5A\x00',
                'lz4': b'\x04\x22\x4D\x18', None: None}[scheme]


class TestFileTransfer(TestCase, Common):

    def setUp(self):
        super().setUp()
        self.commonSetUp()

    def tearDown(self):
        self.commonTearDown()
        super().tearDown()

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

    def test_upload_empty(self):
        self.uploader.rows = 0
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
        self.uploader.chunk_size = 100
        self.uploader.rows = 100
        self.execute("COPY OFFSET 0 INTO foo FROM 'foo' ON CLIENT")
        self.execute("SELECT MIN(i) AS mi, MAX(i) AS ma FROM foo")
        self.expect([(1, 100)])

    def test_upload_offset1(self):
        # OFFSET 0 and OFFSET 1 behave the same, they do nothing
        self.uploader.chunk_size = 100
        self.uploader.rows = 100
        self.execute("COPY OFFSET 1 INTO foo FROM 'foo' ON CLIENT")
        self.execute("SELECT MIN(i) AS mi, MAX(i) AS ma FROM foo")
        self.expect([(1, 100)])

    def test_upload_offset5(self):
        self.uploader.chunk_size = 100
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
        self.assertEqual("foo", self.downloader.get_filename())

    def test_download_empty(self):
        self.fill_foo(0)
        self.execute("COPY (SELECT * FROM foo) INTO 'foo' ON CLIENT")
        self.assertEqual("", self.downloader.get())

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

    def test_download_filename_with_spaces(self):
        self.fill_foo(5)
        self.execute("COPY (SELECT * FROM foo) INTO 'foo bar' ON CLIENT")
        self.assertEqual("1\n2\n3\n4\n5\n", self.downloader.get())
        self.assertEqual("foo bar", self.downloader.get_filename())

    @skipUnless(test_full, "full test disabled")
    def test_large_upload(self):
        deadman.set_timeout(50, None)
        n = 1_000_000
        self.uploader.rows = n
        self.uploader.chunk_size = 1024 * 1024
        self.execute("COPY INTO foo FROM 'foo' ON CLIENT")
        self.assertIsNone(self.uploader.cancelled_at)
        self.execute("SELECT COUNT(*) FROM foo")
        self.expect1(n)

    @skipUnless(test_full, "full test disabled")
    def test_large_download(self):
        deadman.set_timeout(50, None)
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

    def test_upload_native_text_filename_w_whitespaces(self):
        self.upload_file('filename with spaces.csv', {}, True)

    def upload_file(self, filename, write_opts, read_text):
        encoding = self.defaultencoding if read_text else 'utf-8'
        interesting_text = "Únïçøðε¡!÷"
        encodable_text = ""
        for c in interesting_text:
            try:
                # Not all of the following characters may be available in the system encoding
                bytes(c, self.defaultencoding)
            except UnicodeEncodeError:
                continue
            encodable_text += c
        assert len(encodable_text) > 0
        n = 1000
        f = self.open(filename, 'wt', encoding=encoding, **write_opts)
        assert f.encoding
        for i in range(n):
            print(f"{i}|{encodable_text}{i}", file=f)
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
        self.expect1(f"{encodable_text}999")

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

    def test_binary_upload(self):
        items = [1, 2, 3, 0x1234_5678]

        class BinaryUploader(Uploader):
            def handle_upload(self, upload: Upload, filename: str, text_mode: bool, skip_amount: int):
                assert filename == 'binarydata'
                bw = upload.binary_writer()
                bw.write(struct.pack('>4i', *items))
        self.conn.set_uploader(BinaryUploader())
        self.execute("COPY BIG ENDIAN BINARY INTO FOO from 'binarydata' ON CLIENT")
        self.execute("SELECT * FROM foo")
        self.expect([(i,) for i in items])


class TestSafeDirectoryHandler(TestCase, Common):

    def setUp(self):
        super().setUp()
        self.commonSetUp()

    def tearDown(self):
        self.commonTearDown()
        super().tearDown()

    def test_upload_handler_security(self):
        f = self.open("foo.csv", "w")
        f.write("1\n2\n3\n")
        f.close()
        outside = self.file('')
        inside = self.file('inside')
        inside.mkdir()
        f = self.open(inside.joinpath("foo.csv"), "w")
        f.write("10\n20\n30\n")
        f.close()
        #
        handler = SafeDirectoryHandler(inside)
        self.conn.set_uploader(handler)
        #
        testcases = [
            ('foo.csv', True),
            ('./foo.csv', True),
            (inside.joinpath('foo.csv'), True),
            ('../foo.csv', False),
            (outside.joinpath('foo.csv'), False),
        ]
        for path, valid in testcases:
            with self.subTest(dir=str(inside), path=str(path), expect_valid=valid):
                self.conn.rollback()
                path = str(path)
                if valid:
                    self.execute("COPY INTO foo FROM %s ON CLIENT", [path])
                else:
                    with self.assertRaises(OperationalError):
                        self.execute("COPY INTO foo FROM %s ON CLIENT", [path])
                    continue
                self.execute("SELECT MAX(i) FROM foo")
                self.expect1(30)

    def test_download_handler_security(self):
        self.execute("INSERT INTO foo SELECT * FROM sys.generate_series(0, 10)")
        outside = self.file('')
        inside = self.file('inside')
        inside.mkdir()
        #
        handler = SafeDirectoryHandler(inside)
        self.conn.set_downloader(handler)
        #
        testcases = [
            ('foo.csv', True),
            ('./foo.csv', True),
            (inside.joinpath('foo.csv'), True),
            ('../foo.csv', False),
            (outside.joinpath('foo.csv'), False),
        ]
        for path, valid in testcases:
            with self.subTest(dir=str(inside), path=str(path), expect_valid=valid):
                self.conn.rollback()
                path = str(path)
                if valid:
                    self.execute("COPY (SELECT * FROM foo) INTO %s ON CLIENT", [path])
                else:
                    with self.assertRaises(OperationalError):
                        self.execute("COPY (SELECT * FROM foo) INTO %s ON CLIENT", [path])
                    continue

    def get_testdata_name(self,
                          enc_name: str, newline: str,
                          lines: Optional[int] = None,
                          compression: Optional[str] = None
                          ) -> str:
        newline_name = {None: "none", "\n": "lf", "\r\n": "crlf"}[newline]
        file_name = f"{enc_name}_{newline_name}"
        if lines is not None:
            file_name += f"_{lines}lines"
        file_name += ".txt"
        if compression:
            file_name += "." + compression
        return file_name

    def get_testdata(self, enc_name: str, newline: str, lines: int, compression: Optional[str] = None) -> str:
        encoding = codecs.lookup(enc_name) if enc_name else None
        fname = self.get_testdata_name(enc_name, newline, lines, compression)
        p = self.file(fname)
        if not p.exists():
            enc = encoding.name if encoding else None
            opener = lookup_compression_algorithm(p)
            f = opener(p, mode="wt", encoding=enc, newline=newline)
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

    def test_utf8_uploads(self):
        self.perform_line_endings_and_offsets_upload_tests('utf-8')

    def test_latin1_uploads(self):
        self.perform_line_endings_and_offsets_upload_tests('latin1')

    def test_shiftjis_uploads(self):
        self.perform_line_endings_and_offsets_upload_tests('shift-jis')

    def test_native_uploads(self):
        self.perform_line_endings_and_offsets_upload_tests(None)

    def perform_line_endings_and_offsets_upload_tests(self, enc):
        endings = [
            ('\n', '\n'),
            ('\r\n', '\r\n'),
            ('\n', None),
            ('\r\n', None),
        ]
        offsets = {
            None,
            0,
            1,
            2,
            5,
            15,
        }
        for file_ending, handler_ending in endings:
            for offset in offsets:
                with self.subTest(encoding=enc, file_ending=file_ending, handler_ending=handler_ending, offset=offset):
                    self.perform_upload_test(enc, file_ending, handler_ending, offset)

    def perform_upload_test(self, encoding, file_ending, handler_ending, offset=None, end=10, compression=None):
        if offset is None:
            offset_clause = ''
            skip = 0
        else:
            offset_clause = f" OFFSET {offset}"
            skip = offset - 1 if offset else 0
        uploader = SafeDirectoryHandler(self.file(''), encoding, handler_ending)
        self.conn.set_uploader(uploader)
        fname = self.get_testdata(encoding, file_ending, end, compression=compression)
        # Double check the compression, are we testing what we want tot test?
        compression_prefix = self.compression_prefix(compression)
        if compression_prefix:
            f = self.open(fname, 'rb')
            content = f.read()
            content_prefix = content[:len(compression_prefix)]
            f.close()
            self.assertEqual(compression_prefix, content_prefix)
        # Double check the testdata encoding, are we testing what we want tot test?
        # These are the various encodings of the '÷' character as used by the
        # .line() method above.
        encmarker = {'utf-8': b'\xC3\xB7', 'latin1': b'\xF7', 'shift-jis': b'\x81\x80', None: None}[encoding]
        if encmarker:
            full_name = self.file(fname)
            opener = lookup_compression_algorithm(full_name)
            f = opener(full_name, 'rb')
            content = f.read()
            f.close()
            self.assertTrue(content == b'' or encmarker in content)
        # Run the test
        # self.conn.rollback()
        self.execute("DELETE FROM foo2")
        self.execute("COPY" + offset_clause + " INTO foo2 FROM %s ON CLIENT", [fname])
        self.execute("SELECT * FROM foo2")
        rows = self.cursor.fetchall()
        expected = [self.line(i) for i in range(skip, end)]
        self.assertEqual(expected, rows)

    def test_upload_utf8_lf_uses_binary(self):
        class CustomHandler(SafeDirectoryHandler):
            used_mode = None

            def __init__(self, dir):
                super().__init__(dir, 'utf-8', '\n')

            def handle_upload(self, upload: Upload, filename: str, text_mode: bool, skip_amount: int):
                super().handle_upload(upload, filename, text_mode, skip_amount)
                # peek into the internals of the upload to see what was used.
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
        compressions = [
            None,
            'gz',
            'bz2',
            'xz',
        ]
        if test_have_lz4:
            compressions.append('lz4')
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
        for compression in compressions:
            for encoding in encodings:
                for handler_ending in file_endings + [None]:
                    with self.subTest(compression=compression, encoding=encoding, handler_ending=handler_ending):
                        self.perform_download_test(compression, encoding, handler_ending)

    def perform_download_test(self, compression, encoding, handler_ending):
        # We want to check that when asked to use the given encoding and line endings,
        # this happens.
        n = 10
        downloader = SafeDirectoryHandler(self.file(''), encoding, handler_ending)
        self.conn.set_downloader(downloader)
        self.execute("DELETE FROM foo2")
        #
        enc = encoding or self.defaultencoding
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
        fname = self.get_testdata_name(encoding, handler_ending, compression=compression)
        self.execute("COPY (SELECT * FROM foo2) INTO %s ON CLIENT", [fname])
        # check compression
        compression_prefix = self.compression_prefix(compression)
        if compression_prefix:
            f = self.open(fname, 'rb')
            content = f.read()
            content_prefix = content[:len(compression_prefix)]
            f.close()
            self.assertEqual(compression_prefix, content_prefix)
        # check contents
        full_name = self.file(fname)
        opener = lookup_compression_algorithm(full_name)
        f = opener(full_name, 'rb')
        content = f.read()
        f.close()
        #
        self.assertEqual(expected, content)

    def test_download_utf8_lf_uses_binary(self):
        class CustomHandler(SafeDirectoryHandler):
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

    def test_upload_with_compression_disabled(self):
        fname = self.get_testdata('utf-8', '\n', 3, compression=None)
        # give it a misleading name
        misleading_name = 'banana.txt.gz'
        copyfile(self.file(fname), self.file(misleading_name))
        # now upload it
        handler = SafeDirectoryHandler(self.file(''), compression=False)
        self.conn.set_uploader(handler)
        self.execute("COPY INTO foo2 FROM %s ON CLIENT", misleading_name)
        self.conn.commit()
        self.execute("SELECT MAX(i) FROM foo2")
        self.expect1(3)

    def test_download_with_compression_disabled(self):
        fname = 'misleading.txt.gz'
        handler = SafeDirectoryHandler(self.file(''), encoding='utf-8', newline='\n', compression=False)
        self.conn.set_downloader(handler)
        self.execute("COPY SELECT value FROM sys.generate_series(1,4) INTO %s ON CLIENT", fname)
        # should not be gzipped
        f = self.open(fname, 'rb')
        content = f.read()
        f.close()
        self.assertEqual(b'1\n2\n3\n', content)


class TestNormalizeCrLf(TestCase):

    class Sink(BufferedIOBase):
        def __init__(self):
            self.written = b''

        def writable(self) -> bool:
            return True

        def write(self, buf):
            self.written += bytes(buf)
            return len(buf)

        def get_written(self):
            res = self.written
            self.written = b''
            return res

    def setUp(self):
        self.sink = self.Sink()
        self.normalizer = NormalizeCrLf(self.sink)

    def transaction(self, buf, expect_pending, expect_written):
        n = self.normalizer.write(buf)
        self.assertEqual(len(buf), n)
        written = self.sink.get_written()
        pending = self.normalizer.pending
        self.assertEqual(expect_written, written)
        self.assertEqual(expect_pending, pending)

    def test_normalizer(self):
        self.assertEqual(False, self.normalizer.pending)
        self.assertEqual(b'', self.sink.written)

        # can all be written through
        self.transaction(b"\r\naaa\n\n\r\n", False, b"\naaa\n\n\n")

        # trailing CR pending
        self.transaction(b"\n\r\naaa\r", True, b"\n\naaa")

        # LF consumes the pending CR
        self.transaction(b"\n", False, b"\n")

        # a new pending CR
        self.transaction(b"\r", True, b"")

        # a new pending CR
        self.transaction(b"a", False, b"\ra")

        # CR after CR emits one CR and stays pending
        self.transaction(b"\r", True, b"")
        self.transaction(b"\r", True, b"\r")

        # empty write stays pending
        self.transaction(b"", True, b"")

        # flushing the normalizer does not flush the pending CR
        self.normalizer.flush()
        self.assertTrue(self.normalizer.pending)

        # but closing it does
        self.normalizer.close()
        self.assertFalse(self.normalizer.pending)
        self.assertEqual(b"\r", self.sink.get_written())
