# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import unittest

from cassandra import DriverException
from cassandra.protocol import (ProtocolHandler, LazyProtocolHandler, ResultMessage,
                                RESULT_KIND_ROWS, write_int, write_short, write_string, write_value)
from tests.unit.cython.utils import cythontest


def rows_body(rows):
    # Rows start after the metadata, so the Cython reader begins at a non-zero offset.
    f = io.BytesIO()
    write_int(f, RESULT_KIND_ROWS)
    write_int(f, ResultMessage._FLAGS_GLOBAL_TABLES_SPEC)
    write_int(f, 2)
    write_string(f, 'ks')
    write_string(f, 'tbl')
    for name, type_code in (('k', 0x0009), ('v', 0x000D)):  # int, varchar
        write_string(f, name)
        write_short(f, type_code)
    write_int(f, len(rows))
    for k, v in rows:
        write_value(f, k)
        write_value(f, v)
    return f.getvalue()


def decode(handler, body):
    return handler.decode_message(4, None, None, 0, 0, ResultMessage.opcode, body, None, None)


class RowParserTest(unittest.TestCase):

    @cythontest
    def test_decode_rows(self):
        body = rows_body([(b'\x00\x00\x00\x01', b'one'), (b'\x00\x00\x00\x02', b'two')])
        for handler in (ProtocolHandler, LazyProtocolHandler):
            with self.subTest(handler=handler):
                self.assertEqual(list(decode(handler, body).parsed_rows), [(1, 'one'), (2, 'two')])

    @cythontest
    def test_decode_error_reports_column(self):
        # The fallback re-parses from the rows start; a wrong rewind would misread instead.
        body = rows_body([(b'\x00\x00\x00\x01', b'ok'), (b'\x00\x00\x00\x02', b'\xff\xfe')])
        with self.assertRaisesRegex(DriverException, 'Failed decoding result column "v"'):
            decode(ProtocolHandler, body)

    @cythontest
    def test_rows_parsed_from_getvalue_at_offset(self):
        # getvalue() carries different row bytes than the stream, so only a
        # parser fed from getvalue()+tell() (no f.read() copy) yields 'XYZ'.
        real = rows_body([(b'\x00\x00\x00\x01', b'one')])
        fake = rows_body([(b'\x00\x00\x00\x01', b'XYZ')])

        class Spy(io.BytesIO):
            def getvalue(self):
                return fake

        msg_cls = ProtocolHandler.message_types_by_opcode[ResultMessage.opcode]
        f = Spy(real)
        f.seek(4)  # skip the kind int, as ResultMessage.recv_body does
        msg = msg_cls(RESULT_KIND_ROWS)
        msg.recv_results_rows(f, 4, {}, None, None)
        self.assertEqual(list(msg.parsed_rows), [(1, 'XYZ')])

    @cythontest
    def test_stream_left_at_eof(self):
        # Same cursor contract as the old f.read(): stream ends at EOF, even on error.
        msg_cls = ProtocolHandler.message_types_by_opcode[ResultMessage.opcode]
        cases = [
            (rows_body([(b'\x00\x00\x00\x01', b'one')]) + b'trailing', None),
            (rows_body([(b'\x00\x00\x00\x02', b'\xff\xfe')]), DriverException),
        ]
        for body, exc in cases:
            with self.subTest(error=exc):
                f = io.BytesIO(body)
                f.seek(4)
                if exc:
                    with self.assertRaises(exc):
                        msg_cls(RESULT_KIND_ROWS).recv_results_rows(f, 4, {}, None, None)
                else:
                    msg_cls(RESULT_KIND_ROWS).recv_results_rows(f, 4, {}, None, None)
                self.assertEqual(f.tell(), len(body))
