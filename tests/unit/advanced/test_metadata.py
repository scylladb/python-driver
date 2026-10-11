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
import datetime
import unittest

from cassandra.metadata import _SchemaParser
from cassandra.protocol import ResultMessage, RESULT_KIND_ROWS


class SchemaParsersTests(unittest.TestCase):
    def test_metadata_query_metadata_timeout(self):
        class FakeConnection:
            def __init__(self):
                self.queries = []

            def wait_for_responses(self, *msgs, **kwargs):
                self.queries.extend(msgs)
                local_response = ResultMessage(kind=RESULT_KIND_ROWS)
                local_response.column_names = []
                local_response.parsed_rows = []

                return [[local_response, local_response] for _ in msgs]

        for schemaClass in get_all_schema_parser_classes(_SchemaParser):
            conn = FakeConnection()
            p = schemaClass(conn, 2.0, 1000, None)
            p._query_all()

            for q in conn.queries:
                assert "USING TIMEOUT" not in q.query, f"<{schemaClass.__name__}> query `{q.query}` contains `USING TIMEOUT`, while should not"

            conn = FakeConnection()
            p = schemaClass(conn, 2.0, 1000, datetime.timedelta(seconds=2))
            p._query_all()

            for q in conn.queries:
                assert "USING TIMEOUT 2000ms" in q.query, f"{schemaClass.__name__} query `{q.query}` does not contain `USING TIMEOUT 2000ms`"


def get_all_schema_parser_classes(cl):
    for child in cl.__subclasses__():
        if not child.__name__.startswith('SchemaParser') or child.__module__ != 'cassandra.metadata':
            continue
        yield child
        for c in get_all_schema_parser_classes(child):
            yield c
