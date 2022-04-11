#!/usr/bin/env python3
import pathlib
import shutil
import pymonetdb

class MyUploader(pymonetdb.Uploader):
    def __init__(self, dir):
        self.dir = pathlib.Path(dir)

    def handle_upload(self, upload, filename, text_mode, skip_amount):
        # security check
        path = self.dir.joinpath(filename).resolve()
        if not str(path).startswith(str(self.dir.resolve())):
            return upload.send_error('Forbidden')
        # open
        tw = upload.text_writer()
        with open(path) as f:
            # skip
            for i in range(skip_amount):
                f.readline()
            # bulk upload
            shutil.copyfileobj(f, tw)

conn = pymonetdb.connect('demo')
conn.set_uploader(MyUploader('datadir'))

cursor = conn.cursor()
cursor.execute("DROP TABLE foo")
cursor.execute("CREATE TABLE foo(i INT, t TEXT)")
cursor.execute("COPY 10 RECORDS OFFSET 7 INTO foo FROM 'data.csv' ON CLIENT USING DELIMITERS ','")
cursor.execute("SELECT COUNT(i), MIN(i), MAX(i) FROM foo")
row = cursor.fetchone()
print(row)

# Goodbye
conn.commit()
cursor.close()
conn.close()