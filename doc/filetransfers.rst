File Transfers
==============

MonetDB supports the non-standard :code:`COPY INTO` statement to load a CSV-like
text file into a table or to dump a table to a text file. This statement has an
optional modifier :code:`ON CLIENT` to indicate that the server should not
try to open the file server-side, but should instead ask the client to open the
file on its behalf.

For example::

	COPY INTO mytable FROM 'data.csv' ON CLIENT
	USING DELIMITERS ',', E'\n', '"';

By default, if pymonetdb receives a file request from the server, it will refuse
it for security considerations. You do not want the server or a hacker pretending
to be the server to be able to request arbitrary files on your system and even
overwrite them.

To enable file transfers, create a `pymonetdb.Uploader` and/or
`pymonetdb.Downloader` and register them with your connection::

	transfer_handler = pymonetdb.SafeDirectoryHandler(datadir)
	conn.set_uploader(transfer_handler)
	conn.set_downloader(transfer_handler)

With this in place, the COPY INTO ON CLIENT statement above will ask to open
file data.csv in the given `datadir` and upload its contents. As its name
suggests, :class:`SafeDirectoryHandler` will only allow access to the files in
that directory.

Note that in this example we register the same handler object both as an
uploader and a downloader, but it is perfectly sensible to only register an
uploader, or only a downloader, or to use two separate handlers.

See the API documentation for details.


Make up data as you go
----------------------

You can also write your own transfer handlers. And instead of opening a file,
such handlers can also make up the data on the fly, retrieve it from a remote
microservice, prompt the user interactively or do whatever else you come up
with:

.. literalinclude:: examples/uploaddyn.py
   :pyobject: MyUploader

In this example we called `upload.text_writer()` which yields a text-mode
file-like object. There is also `upload.binary_writer()` which yields a
binary-mode file-like object. This works even if the server requested a text
mode object, but in that case you have to make sure the bytes you write are valid
utf-8 and delimited with Unix line endings rather than Windows line endings.

If you want to refuse an up- or download, call `upload.send_error()` to send an
error message. This is only possible before any calls to `text_writer()` and
`binary_writer()`.

For custom downloaders the situation is similar, except that instead of
`text_writer` and `binary_writer`, the `download` parameter offers
`download.text_reader()` and `download.text_writer()`.


Skip amount
-----------

MonetDB's :code:`COPY INTO` statement allows you to skip for example the first
line in a file using the the modifier :code:`OFFSET 2`. In such a case,
the `skip_amount` parameter to `handle_upload` will be greater than zero.

Note that the offset in the SQL statement is 1-based, whereas the `skip_amount`
parameter has already been converted to be 0-based. In the example above
this allowed us to write :code:`for i in range(skip_amount, 1000):` rather
than :code:`for i in range(1000):`.


Cancellation
------------

If the server does not need all uploaded data, for example if you did::

	COPY 100 RECORDS INTO mytable FROM 'data.csv' ON CLIENT

the server may at some point cancel the upload. This does not happen instantly,
from time to time pymonetdb explicitly asks the server if they are still
interested. By default this is after every MiB of data but that can be
configured using `upload.set_chunk_size()`. If the server answers that it is no
longer interested, pymonetdb will discard any further data written to the
writer. It is recommended to occasionally call `upload.is_cancelled()` to check
for this and exit early if the upload has been cancelled.

Upload handlers also have an optional method `cancel()` that you can override.
This method is called when pymonetdb receives the cancellation request.


Copying data from or to a file-like object
------------------------------------------

If you are moving large amounts of data between pymonetdb and a file-like object
such as a file, Pythons `copyfileobj`_ function may come in handy:

.. literalinclude:: examples/uploadsafe.py
   :pyobject: MyUploader

However, note that copyfileobj does not handle cancellations as described above.

.. _copyfileobj: https://docs.python.org/3/library/shutil.html#shutil.copyfileobj


Security considerations
-----------------------

If your handler accesses the file system or the network, it is absolutely critical
to carefully validate the file name you are given. Otherwise an attacker can take
over the server or the connection to the server and cause great damage.

An example of how to validate file systems paths is given in the code sample above.
Similar considerations apply to text that is inserted into network urls and other
resource identifiers.
