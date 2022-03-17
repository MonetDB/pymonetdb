from io import StringIO
from pathlib import Path
from shutil import copyfileobj
from tempfile import mkdtemp
from typing import Optional
from unittest import TestCase, skip, skipUnless


from pymonetdb import connect, Error as MonetError
from pymonetdb.exceptions import OperationalError, ProgrammingError
from pymonetdb.mapi import Download, Downloader, Upload, Uploader
from tests.util import test_args, test_full


class MyException(Exception):
    pass


class TestUploader(Uploader):
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
            w = upload.text_writer()
            for i in iter:
                if i == self.error_at:
                    raise MyException(f"Oops {i}")
                if upload.is_cancelled() and not self.ignore_cancel:
                    self.cancelled_at = i
                    break
                s = f"{i}\n"
                w.write(s)
        else:
            w = upload.binary_writer()
            for i in iter:
                if i == self.error_at:
                    raise MyException(f"Oops {i}")
                if upload.is_cancelled() and not self.ignore_cancel:
                    self.cancelled_at = i
                    break
                s = f"{i}\n"
                w.write(bytes(s, 'ascii'))


class TestDownloader(Downloader):
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
            self.tmpdir = Path(mkdtemp())
        return self.tmpdir.joinpath(filename)

    def open(self, filename, mode, **kwargs):
        fullname = self.file(filename)
        return open(fullname, mode, **kwargs)

    def setUp(self):
        super().setUp()
        self.conn = conn = connect(**test_args)
        self.uploader = TestUploader()
        conn.set_uploader(self.uploader)
        self.downloader = TestDownloader()
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

    @skip("forced CRLF -> LF conversion hasn't been implemented yet")
    # Also see test_NormalizeCrLf from the Java tests
    def test_upload_crlf(self):
        class CustomUploader(Uploader):
            def handle_upload(self, upload: Upload, filename: str, text_mode: bool, skip_amount: int):
                content = "1|A\r\n2|BB\r\n3|CCC\r\n"
                upload.text_writer().write(content)

        self.conn.set_uploader(CustomUploader())
        self.execute("COPY INTO foo2 FROM 'foo2' ON CLIENT")
        self.execute("SELECT i, LENGTH(t) as len FROM foo2")
        self.expect([(1, 1), (2, 2), (3, 3)])

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
                    w = upload.text_writer()
                else:
                    f = testcase.open(filename, 'rb')
                    w = upload.binary_writer()
                copyfileobj(f, w)
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

    @skip("post-exception connection handling needs to be fixed")
    def test_fail_download_late(self):
        self.fill_foo(5000)
        self.downloader.lines = 6000
        self.downloader.error_at_line = 4000
        with self.assertRaises(MyException):
            self.execute("COPY (SELECT * FROM foo) INTO 'foo' ON CLIENT")
        self.conn.rollback()
        self.execute("SELECT 42")
        self.expect1(42)
