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
import sys
from typing import Any, Callable, List, Optional

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
    array_letter: str
    null_value: int
    mapper: Optional[Callable[[int], Any]]

    def __init__(self,
                 width: int,
                 description: 'pymonetdb.sql.cursors.Description',
                 mapper: Optional[Callable[[int], Any]] = None):
        self.mapper = mapper
        self.array_letter = WIDTH_TO_ARRAY_TYPE[width]
        self.null_value = -(1 << (width - 1))

    def decode(self, wrong_endian: bool, data: memoryview) -> List[Any]:
        arr = array.array(self.array_letter)
        arr.frombytes(data)
        if wrong_endian:
            arr.byteswap()
        if self.mapper:
            m = self.mapper
            values = [v if v != self.null_value else None for v in arr]
            values = [m(v) if v != self.null_value else None for v in arr]
        else:
            values = [v if v != self.null_value else None for v in arr]
        return values


class HugeIntDecoder(BinaryDecoder):
    def decode(self, wrong_endian: bool, data: memoryview) -> List[Any]:
        # we want to know if the incoming data is big or little endian but we have
        # to reconstruct that from 'wrong_endian'
        if wrong_endian:
            big_endian = sys.byteorder == 'little'
        else:
            big_endian = sys.byteorder == 'big'
        # we cannot directly decode 128 bits but we can decode 32 bits
        letter = WIDTH_TO_ARRAY_TYPE[64].upper()
        arr = array.array(letter)
        arr.frombytes(data)
        if wrong_endian:
            arr.byteswap()
        # maybe some day we can come up with something faster
        result: List[Optional[int]] = []
        high1 = 1 << 64
        null_value = 1 << 127
        wrap = 1 << 128
        (hi_idx, lo_idx) = (0, 1) if big_endian else (1, 0)
        for i in range(0, len(arr), 2):
            hi = arr[i + hi_idx]
            lo = arr[i + lo_idx]
            n = high1 * hi + lo
            if n == null_value:
                result.append(None)
            elif n >= null_value:
                result.append(n - wrap)
            else:
                result.append(n)
        return result


def _decode_utf8(x: bytes) -> str:
    return str(x, 'utf-8')


class ZeroDelimitedDecoder(BinaryDecoder):
    converter: Callable[[bytes], Any]

    def __init__(self, converter: Callable[[bytes], Any], description: 'pymonetdb.sql.cursors.Description'):
        self.converter = converter

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
    mapper = mapping.get(type_code)
    if not mapper:
        return None
    decoder = mapper(description)
    return decoder


mapping = {
    types.TINYINT: lambda descr: IntegerDecoder(8, descr),
    types.SMALLINT: lambda descr: IntegerDecoder(16, descr),
    types.INT: lambda descr: IntegerDecoder(32, descr),
    types.BIGINT: lambda descr: IntegerDecoder(64, descr),
    types.HUGEINT: lambda descr: HugeIntDecoder(),

    types.BOOLEAN: lambda descr: IntegerDecoder(8, descr, bool),

    types.CHAR: lambda descr: ZeroDelimitedDecoder(_decode_utf8, descr),
    types.VARCHAR: lambda descr: ZeroDelimitedDecoder(_decode_utf8, descr),
    types.CLOB: lambda descr: ZeroDelimitedDecoder(_decode_utf8, descr),
    types.URL: lambda descr: ZeroDelimitedDecoder(_decode_utf8, descr),
    types.JSON: lambda descr: ZeroDelimitedDecoder(json.loads, descr),

    # types.DECIMAL: Decimal,



    # types.REAL: float,
    # types.FLOAT: float,
    # types.DOUBLE: float,



    # types.DATE: py_date,
    # types.TIME: py_time,
    # types.TIMESTAMP: py_timestamp,
    # types.TIMETZ: py_timetz,
    # types.TIMESTAMPTZ: py_timestamptz,

    # types.MONTH_INTERVAL: int,
    # types.SEC_INTERVAL: py_sec_interval,
    # types.DAY_INTERVAL: py_day_interval,


    # types.INET: str,
    # types.UUID: uuid.UUID,
    # types.XML: str,

    # Not supported in COPY BINARY or the binary protocol
    # types.BLOB: py_bytes,
    # types.GEOMETRY: strip,
    # types.GEOMETRYA: strip,
    # types.MBR: strip,
    # types.OID: oid,

    # These are mentioned in pythonize.py but s far as I know the server never
    # produces them
    #
    # types.SERIAL: int,
    # types.SHORTINT: int,
    # types.MEDIUMINT: int,
    # types.LONGINT: int,
    # types.WRD: int,


}
