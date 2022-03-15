from typing import Optional
from unittest import TestCase

from unittest import skip

from pymonetdb import connect, Error as MonetError
from pymonetdb.exceptions import OperationalError, ProgrammingError
from pymonetdb.mapi import Upload, Uploader
from tests.util import test_args


class TestUploader(Uploader):
    rows: int = 5_000
    error_at: Optional[int] = None
    error_msg: Optional[str] = None
    chunkSize: int = 10_000
    force_binary: bool = False
    forget_to_return_after_error: bool = False
    do_nothing_at_all: bool = False
    ignore_cancel: bool = False
    #
    cancelled_at: Optional[int] = None

    def handle(self, upload: Upload, filename: str, text_mode: bool, skip_amount: int):
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
                    raise Exception(f"Oops {i}")
                if upload.is_cancelled() and not self.ignore_cancel:
                    self.cancelled_at = i
                    break
                s = f"{i}\n"
                w.write(s)
        else:
            w = upload.binary_writer()
            for i in iter:
                if i == self.error_at:
                    raise Exception(f"Oops {i}")
                if upload.is_cancelled() and not self.ignore_cancel:
                    self.cancelled_at = i
                    break
                s = f"{i}\n"
                w.write(bytes(s, 'ascii'))


class TestUpload(TestCase):
    first = True

    def setUp(self):
        super().setUp()
        self.conn = conn = connect(**test_args)
        self.uploader = TestUploader()
        conn.set_uploader(self.uploader)

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
            def handle(self, upload: Upload, filename: str, text_mode: bool, skip_amount: int):
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

    def test_offset0(self):
        # OFFSET 0 and OFFSET 1 behave the same, they do nothing
        self.uploader.chunkSize = 100
        self.uploader.rows = 100
        self.execute("COPY OFFSET 0 INTO foo FROM 'foo' ON CLIENT")
        self.execute("SELECT MIN(i) AS mi, MAX(i) AS ma FROM foo")
        self.expect([(1, 100)])

    def test_offset1(self):
        # OFFSET 0 and OFFSET 1 behave the same, they do nothing
        self.uploader.chunkSize = 100
        self.uploader.rows = 100
        self.execute("COPY OFFSET 1 INTO foo FROM 'foo' ON CLIENT")
        self.execute("SELECT MIN(i) AS mi, MAX(i) AS ma FROM foo")
        self.expect([(1, 100)])

    def test_offset5(self):
        self.uploader.chunkSize = 100
        self.uploader.rows = 100
        self.execute("COPY OFFSET 5 INTO foo FROM 'foo' ON CLIENT")
        self.execute("SELECT MIN(i) AS mi, MAX(i) AS ma FROM foo")
        self.expect([(5, 100)])

    def test_server_cancels(self):
        # self.uploader.chunkSize = 100
        self.execute("COPY 10 RECORDS INTO foo FROM 'foo' ON CLIENT")
        self.assertGreater(self.uploader.cancelled_at, 0)
        self.execute("SELECT COUNT(*) FROM foo")
        self.expect1(10)

