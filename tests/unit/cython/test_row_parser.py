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

"""
Tests for cassandra.row_parser, in particular the exception-recovery path
in recv_results_rows() that re-parses row data with TupleRowParser after
the primary ColumnParser fails partway through.
"""

import io
import types
import unittest

from tests.unit.cython.utils import cythontest

try:
    from cassandra.row_parser import make_recv_results_rows
    from cassandra.obj_parser import ListParser
    from cassandra.cqltypes import Int32Type
    from cassandra.marshal import int32_pack
    from cassandra import DriverException
except ImportError:
    make_recv_results_rows = None


def _no_op_recv_results_metadata_br(self, reader, user_type_map):
    """
    Stand-in for the real (Cython) recv_results_metadata: sets
    column_metadata directly without consuming anything from `reader`, so
    row data starts at offset 0 and the test can control the exact bytes
    the exception-recovery path re-reads.
    """
    self.column_metadata = [('ks', 'table', 'col', Int32Type)]


class RowParserExceptionRecoveryTest(unittest.TestCase):
    """
    recv_results_rows() saves the reader's position before the primary
    (fast) column-parsing attempt and, if that attempt raises, rewinds the
    reader to that saved position before re-parsing row-by-row with
    TupleRowParser for a clearer error message (see row_parser.pyx).

    That rewind must leave the reader able to correctly re-read the exact
    same bytes it started with. If the rewind were incomplete -- e.g. if
    BytesIOReader tracked a second, separate cursor that the rewind forgot
    to reset -- the re-parse would read from the wrong offset and either
    misdecode silently or blow up with an unrelated/garbled error instead
    of cleanly re-reporting the original failure.
    """

    @cythontest
    def test_exception_recovery_rereads_correct_bytes(self):
        recv_results_rows = make_recv_results_rows(ListParser(), _no_op_recv_results_metadata_br)

        # Row 1 is a valid 4-byte int32 value. Row 2 declares a [bytes]
        # length of 2, which is too short for Int32Type's 4-byte decode --
        # this is what makes the *first* (fast) parse attempt raise.
        row1 = int32_pack(4) + int32_pack(123)
        row2 = int32_pack(2) + b'xy'
        body = int32_pack(2) + row1 + row2  # rowcount = 2

        msg = types.SimpleNamespace(column_metadata=None, parsed_rows=None)
        with self.assertRaises(DriverException) as ctx:
            recv_results_rows(msg, io.BytesIO(body), 4, {}, None, None)

        # The recovery path re-reads rowcount and both rows from the
        # rewound position. If it landed on the correct bytes, the
        # re-raised error still names the same column/type and the same
        # underlying cause as the original failure -- not some other
        # column, a bogus rowcount-driven EOFError, or silent garbage.
        message = str(ctx.exception)
        self.assertIn('"col"', message)
        self.assertIn('Requested more than length of buffer', message)

    @cythontest
    def test_exception_recovery_after_valid_prefix_rows(self):
        """
        Same as above, but with several valid rows before the bad one, so
        the saved/rewound position is a genuinely non-zero, mid-buffer
        offset rather than 0 -- exercising an actual seek-backwards rather
        than a no-op rewind to the start.
        """
        recv_results_rows = make_recv_results_rows(ListParser(), _no_op_recv_results_metadata_br)

        good_rows = b''.join(int32_pack(4) + int32_pack(v) for v in (1, 2, 3))
        bad_row = int32_pack(2) + b'xy'
        body = int32_pack(4) + good_rows + bad_row  # rowcount = 4

        msg = types.SimpleNamespace(column_metadata=None, parsed_rows=None)
        with self.assertRaises(DriverException) as ctx:
            recv_results_rows(msg, io.BytesIO(body), 4, {}, None, None)

        message = str(ctx.exception)
        self.assertIn('"col"', message)
        self.assertIn('Requested more than length of buffer', message)
