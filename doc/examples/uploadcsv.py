#!/usr/bin/env python3

import os
import pymonetdb

# Create the data directory and the CSV file
try:
    os.mkdir("datadir")
except FileExistsError:
    pass
with open("datadir/data.csv", "w") as f:
    for i in range(10):
        print(f"{i},item{i + 1}", file=f)

# Connect to MonetDB and register the upload handler
conn = pymonetdb.connect('demo')
handler = pymonetdb.SafeDirectoryHandler("datadir")
conn.set_uploader(handler)
cursor = conn.cursor()

# Set up the table
cursor.execute("DROP TABLE foo")
cursor.execute("CREATE TABLE foo(i INT, t TEXT)")

# Upload the data, this will ask the handler to upload data.csv
cursor.execute("COPY INTO foo FROM 'data.csv' ON CLIENT USING DELIMITERS ','")

# Check that it has loaded
cursor.execute("SELECT t FROM foo WHERE i = 9")
row = cursor.fetchone()
assert row[0] == 'item10'

# Goodbye
conn.commit()
cursor.close()
conn.close()