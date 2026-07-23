# Copyright 2026 ScyllaDB, Inc.
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

import unittest

import pytest

from tests.integration import use_singledc, SCYLLA_VERSION, BasicSharedKeyspaceUnitTestCase

pytestmark = pytest.mark.skipif(SCYLLA_VERSION is None, reason="SCYLLA_USE_METADATA_ID is a Scylla-only protocol extension")


def setup_module():
    use_singledc()


class ScyllaMetadataIdTests(BasicSharedKeyspaceUnitTestCase):
    """
    Live-server coverage for the SCYLLA_USE_METADATA_ID protocol extension (DRIVER-153).
    """

    @classmethod
    def setUpClass(cls):
        cls.common_setup(1)
        # Skip the whole class if this Scylla build does not advertise the
        # extension (e.g. a version predating scylladb#23292). Without this the
        # tests below would error out instead of skipping on an unsupporting node.
        pool = next(iter(cls.session.get_pools()))
        connection, _ = pool.borrow_connection(timeout=2)
        try:
            if not connection.features.use_metadata_id:
                raise unittest.SkipTest(
                    "Scylla node does not advertise SCYLLA_USE_METADATA_ID")
        finally:
            pool.return_connection(connection)

    def setUp(self):
        self.table_name = "{}.{}".format(self.keyspace_name, self.function_table_name)
        self.session.execute("CREATE TABLE {} (a int PRIMARY KEY, b int, c int)".format(self.table_name))
        self.session.execute("INSERT INTO {} (a, b, c) VALUES (1, 1, 1)".format(self.table_name))

    def tearDown(self):
        self.session.execute("DROP TABLE {}".format(self.table_name))

    def test_extension_is_negotiated(self):
        """
        Sanity check that SCYLLA_USE_METADATA_ID was actually negotiated on this
        connection. Without this, the tests below could pass vacuously if
        negotiation silently failed.
        """
        pool = next(iter(self.session.get_pools()))
        connection, _ = pool.borrow_connection(timeout=2)
        try:
            assert connection.features.use_metadata_id is True
        finally:
            pool.return_connection(connection)

    def test_metadata_changed_recovers_after_schema_change(self):
        """
        Normal METADATA_CHANGED path: after ALTER TABLE, the next EXECUTE must
        come back with a fresh result_metadata_id and updated column metadata,
        picked up automatically without re-preparing.
        """
        prepared = self.session.prepare("SELECT * FROM {} WHERE a = ?".format(self.table_name))
        id_before = prepared.result_metadata_id
        assert id_before is not None
        assert len(prepared.result_metadata) == 3

        self.session.execute(prepared.bind((1,)))

        self.session.execute("ALTER TABLE {} ADD d int".format(self.table_name))
        self.session.execute(prepared.bind((1,)))

        assert prepared.result_metadata_id is not None
        assert prepared.result_metadata_id != id_before
        assert len(prepared.result_metadata) == 4

    def test_empty_sentinel_id_triggers_metadata_changed(self):
        """
        Statements prepared before the extension was negotiated (e.g. mid rolling
        upgrade) start with result_metadata_id=None and must send the empty b''
        sentinel on their first EXECUTE. This must not be treated as a protocol
        error by the server: it must be treated as a mismatch, causing Scylla to
        respond with METADATA_CHANGED (fresh id + full metadata), which the
        driver then caches.
        """
        prepared = self.session.prepare("SELECT * FROM {} WHERE a = ?".format(self.table_name))
        assert prepared.result_metadata_id is not None

        # Simulate "prepared before the extension was known" by dropping the
        # cached id while keeping the cached metadata (mirrors the java-driver's
        # should_handle_empty_metadata_id_when_executing_statement_when_supported).
        prepared.update_result_metadata(prepared.result_metadata, None)
        assert prepared.result_metadata_id is None

        result = self.session.execute(prepared.bind((1,)))

        assert list(result) == [(1, 1, 1)]
        assert prepared.result_metadata_id is not None
