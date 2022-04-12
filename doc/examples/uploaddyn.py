#!/usr/bin/env python3
import pymonetdb

class MyUploader(pymonetdb.Uploader):
    def handle_upload(self, upload, filename, text_mode, skip_amount):
        tw = upload.text_writer()
        for i in range(skip_amount, 1000):
            print(f'{i},number{i}', file=tw)

conn = pymonetdb.connect('demo')
conn.set_uploader(MyUploader())

cursor = conn.cursor()
cursor.execute("DROP TABLE foo")
cursor.execute("CREATE TABLE foo(i INT, t TEXT)")
cursor.execute("COPY 10 RECORDS OFFSET 7 INTO foo FROM 'data.csv' ON CLIENT USING DELIMITERS ','")
cursor.execute("SELECT COUNT(i), MIN(i), MAX(i) FROM foo")
row = cursor.fetchone()
print(row)
assert row[0] == 10    # ten records numbered
assert row[1] == 6     # offset 7 means skip first 6, that is, records 0, .., 5
assert row[2] == 15    # 10 records: 6, 7,8,  9,10,11,  12,13,14,  and 15

# Goodbye
conn.commit()
cursor.close()
conn.close()