# Copyright DataStax, Inc.
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

from cassandra.parsing cimport ParseDesc, ColumnParser
from cassandra.policies import ColDesc
from cassandra.obj_parser import TupleRowParser
from cassandra.deserializers import make_deserializers

include "ioutils.pyx"

def make_recv_results_rows(ColumnParser colparser, recv_results_metadata_br):
    """
    Create a recv_results_rows closure that uses:
      - recv_results_metadata_br: Cython metadata parser operating on BytesIOReader
      - colparser: Cython column parser for row data

    A single BytesIOReader is created from the full remaining buffer and used
    for both metadata parsing and row parsing, eliminating the per-read bytes
    allocation overhead of Python BytesIO.
    """
    def recv_results_rows(self, f, int protocol_version, user_type_map, result_metadata, column_encryption_policy):
        """
        Parse protocol data given as a BytesIO f into a set of columns (e.g. list of tuples)
        This is used as the recv_results_rows method of (Fast)ResultMessage
        """
        # Create ONE BytesIOReader for the entire remaining buffer.
        # This is used for both metadata parsing and row parsing.
        reader = BytesIOReader(f.read())

        # Use Cython-optimized metadata parsing on BytesIOReader
        recv_results_metadata_br(self, reader, user_type_map)

        column_metadata = self.column_metadata or result_metadata

        self.column_names = [md[2] for md in column_metadata]
        self.column_types = [md[3] for md in column_metadata]

        desc = ParseDesc(self.column_names, self.column_types, column_encryption_policy,
                        [ColDesc(md[0], md[1], md[2]) for md in column_metadata],
                        make_deserializers(self.column_types), protocol_version)
        # The reader's position is now right after the metadata;
        # row data follows immediately — no need to create a second reader.
        # Save position so we can rewind to the start of row data on error.
        cdef Py_ssize_t rows_start_pos = reader.pos
        try:
            self.parsed_rows = colparser.parse_rows(reader, desc)
        except Exception as e:
            # Use explicitly the TupleRowParser to display better error messages for column decoding failures
            rowparser = TupleRowParser()
            reader.pos = rows_start_pos
            rowcount = read_int(reader)
            for i in range(rowcount):
                rowparser.unpack_row(reader, desc)

    return recv_results_rows
