# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

"""
functions for converting binary result sets to Python objects
"""

from abc import abstractmethod
import array
import json
from typing import Any, Callable, Dict, List, Optional

from pymonetdb.sql import types
import pymonetdb.sql.cursors


WIDTH_TO_ARRAY_TYPE = {}
for code in 'bhilq':
    bit_width = 8 * array.array(code).itemsize
    WIDTH_TO_ARRAY_TYPE[bit_width] = code


class BinaryDecoder:
    @abstractmethod
    def decode(self, wrong_endian: bool, data: memoryview) -> List[Any]:
        """Interpret the given bytes as a list of Python objects"""
        pass


class IntegerDecoder(BinaryDecoder):
    type_codes = {
        types.TINYINT: 8,
        types.SMALLINT: 16,
        types.INT: 32,
        types.BIGINT: 64,
        types.HUGEINT: 128,
    }

    array_letter: str
    null_value: int

    def __init__(self, description: 'pymonetdb.sql.cursors.Description'):
        width = self.type_codes[description.type_code]
        self.array_letter = WIDTH_TO_ARRAY_TYPE[width]
        self.null_value = 1 << (description.internal_size - 1)

    def decode(self, wrong_endian: bool, data: memoryview) -> List[Any]:
        arr = array.array(self.array_letter)
        arr.frombytes(data)
        if wrong_endian:
            arr.byteswap()
        values = [v if v != self.null_value else None for v in arr]
        return values


def _decode_utf8(x: bytes) -> str:
    return str(x, 'utf-8')


class ZeroDelimitedDecoder(BinaryDecoder):
    type_codes: Dict[str, Callable[[bytes], Any]]
    type_codes = {
        types.CHAR: _decode_utf8,
        types.VARCHAR: _decode_utf8,
        types.CLOB: _decode_utf8,
        types.URL: _decode_utf8,
        types.JSON: json.loads,
    }

    converter: Callable[[bytes], Any]

    def __init__(self, description: 'pymonetdb.sql.cursors.Description'):
        self.converter = self.type_codes[description.type_code]

    def decode(self, _wrong_endian, data: memoryview) -> List[Any]:
        null_value = b'\x80'
        # tobytes causes a copy but I don't see how that can be avoided
        parts = data.tobytes().split(b'\x00')
        parts.pop()  # empty tail element caused by trailing \x00
        conv = self.converter
        values = [conv(v) if v != null_value else None for v in parts]
        return values


def get_decoder(description: 'pymonetdb.sql.cursors.Description') -> Optional[BinaryDecoder]:
    type_code = description.type_code
    if type_code in IntegerDecoder.type_codes:
        return IntegerDecoder(description)
    elif type_code in ZeroDelimitedDecoder.type_codes:
        return ZeroDelimitedDecoder(description)
    else:
        return None
