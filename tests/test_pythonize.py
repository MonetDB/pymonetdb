# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

import unittest
import pymonetdb.sql.pythonize
from six import unichr


class TestPythonize(unittest.TestCase):
    def test_Binary(self):
        input1 = ''.join([unichr(i) for i in range(256)])
        output1 = ''.join(["%02X" % i for i in range(256)]).upper()
        result1 = pymonetdb.sql.pythonize.Binary(input1.encode('latin-1'))
        self.assertEqual(output1, result1)

        input2 = '\tdharma'
        output2 = '09646861726D61'
        result2 = pymonetdb.sql.pythonize.Binary(input2.encode('latin-1'))
        self.assertEqual(output2, result2)
