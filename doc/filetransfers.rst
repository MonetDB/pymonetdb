File Transfers
==============

MonetDB supports the non-standard :code:`COPY INTO` statement to load a CSV-like
text file into a table or to dump a table into a text file. This statement has an
optional modifier :code:`ON CLIENT` to indicate that the server should not
try to open the file on the server side but instead ask the client to open it
on its behalf.

For example::

	COPY INTO mytable FROM 'data.csv' ON CLIENT
	USING DELIMITERS ',', E'\n', '"';

However, by default, if pymonetdb receives a file request from the server, it will refuse
it for security considerations. You do not want an unauthorised party pretending
to be the server to be able to request arbitrary files on your system and even
overwrite them.

To enable file transfers, create a `pymonetdb.Uploader` or
`pymonetdb.Downloader` and register them with your connection::

	transfer_handler = pymonetdb.SafeDirectoryHandler(datadir)
	conn.set_uploader(transfer_handler)
	conn.set_downloader(transfer_handler)

With this in place, the :code:`COPY INTO ... ON CLIENT` statement above will cause pymonetdb to open
the file `data.csv` in the given `datadir` and upload its contents. As its name
suggests, :class:`SafeDirectoryHandler` will only allow access to the files in
that directory.

Note that in this example, we register the same handler object as an
uploader and a downloader for demonstration purposes. In the real world, it is
good security practice only to register an uploader or a downloader
It is also possible to use two separate handlers.

See the API documentation for details.


Make up data as you go
----------------------

You can also write your own transfer handlers. And instead of opening a file,
such handlers can make up the data on the fly, for instance, retrieve it from a remote
microservice, prompt the user interactively or do whatever else you come up
with:

.. literalinclude:: examples/uploaddyn.py
   :pyobject: MyUploader

In this example, we call `upload.text_writer()` to yield a text-mode
file-like object. There is also an `upload.binary_writer()`, which creates a
binary-mode file-like object. The `binary_writer()` works even if the server requests a text
mode object, but in that case, you have to make sure the bytes you write are valid
UTF-8 and delimited with Unix line endings rather than Windows line endings.

If you want to refuse an upload or download, call `upload.send_error()` to send an
error message *before* any call to `text_writer()` or
`binary_writer()`.

For custom downloaders, the situation is similar, except that instead of
`text_writer` and `binary_writer`, the `download` parameter offers
`download.text_reader()` and `download.text_writer()`.


Skip amount
-----------

MonetDB's :code:`COPY INTO` statement allows you to skip, for example, the first
line in a file using the modifier :code:`OFFSET 2`. In such a case,
the `skip_amount` parameter to `handle_upload()` will be greater than zero.

Note that the offset in the SQL statement is 1-based, whereas the `skip_amount`
parameter has already been converted to 0-based. The example above thus
allows us to write :code:`for i in range(skip_amount, 1000):` rather
than :code:`for i in range(1000):`.


Cancellation
------------

In cases depicted by the following query, the server does not need to receive
all data of the input file:

	COPY 100 RECORDS INTO mytable FROM 'data.csv' ON CLIENT

Therefore, pymonetdb regularly asks the server if it is still interested in
receiving more data. In this way, the server can cancel the uploading after it
has received sufficient data to process the query.  By default, pymonetdb does
this after every MiB of data, but you can change this frequency using
`upload.set_chunk_size()`.

If the server answers that it is no
longer interested, pymonetdb will discard any further data written to the
writer. It is recommended to call `upload.is_cancelled()` occasionally to check
for this and exit early if the upload has been cancelled.

Upload handlers also have an optional method `cancel()` that you can override.
This method is called when pymonetdb receives the cancellation request.


Copying data from or to a file-like object
------------------------------------------

If you are moving large amounts of data between pymonetdb and a file-like object
such as a file, Python's `copyfileobj`_ function may come in handy:

.. literalinclude:: examples/uploadsafe.py
   :pyobject: MyUploader

However, note that `copyfileobj`_ does not handle cancellations as described above.

.. _copyfileobj: https://docs.python.org/3/library/shutil.html#shutil.copyfileobj


Security considerations
-----------------------

If your handler accesses the file system or the network, it is critical
to validate the file name you are given carefully. Otherwise, an attacker can take
over the server or the connection to the server and cause great damage.

The code sample above also includes an example of validating file systems paths.
Similar considerations apply to text inserted into network URLs and other
resource identifiers.
