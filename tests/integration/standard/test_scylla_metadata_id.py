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
from unittest.mock import patch

import pytest

from cassandra.cluster import ResponseFuture
from tests.integration import use_singledc, SCYLLA_VERSION, BasicSharedKeyspaceUnitTestCase, \
    drop_keyspace_shutdown_cluster

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
        try:
            if not cls._negotiated_use_metadata_id():
                raise unittest.SkipTest(
                    "Scylla node does not advertise SCYLLA_USE_METADATA_ID")
        except Exception:
            # setUpClass raising means unittest never calls tearDownClass, so the
            # cluster and keyspace created above are torn down here explicitly.
            drop_keyspace_shutdown_cluster(cls.ks_name, cls.session, cls.cluster)
            raise

    @classmethod
    def _negotiated_use_metadata_id(cls):
        """Whether this class's data-path connections negotiated SCYLLA_USE_METADATA_ID.

        Reads the pool's existing connection rather than borrowing one:
        borrow_connection() pops a stream id that only Connection.process_msg gives
        back, so borrowing without sending a message would leak it.
        """
        pool = next(iter(cls.session.get_pools()))
        return next(iter(pool._connections.values())).features.use_metadata_id

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
        assert self._negotiated_use_metadata_id() is True

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

        # The table was not altered, so the statement is still valid server-side and
        # nothing should re-prepare it. Spying on _reprepare keeps this test honest:
        # if the id came back via an UNPREPARED/reprepare round trip instead, the
        # METADATA_CHANGED-on-ROWS path in ResponseFuture._set_result would not
        # actually be under test here.
        with patch.object(ResponseFuture, '_reprepare', autospec=True,
                          side_effect=ResponseFuture._reprepare) as reprepare_spy:
            result = self.session.execute(prepared.bind((1,)))

        assert reprepare_spy.call_count == 0
        assert list(result) == [(1, 1, 1)]
        assert prepared.result_metadata_id is not None

    def test_conditional_statement_metadata_is_stable_across_outcomes(self):
        """
        Conditional (LWT) statements get no special handling, and this pins the
        server behaviour that makes that correct.

        Cassandra returns NO_METADATA for a conditional statement at PREPARE and
        then varies the result shape per execution — ``(True,)`` when applied, the
        conflicting row when not — which is what PYTHON-847 is about. Scylla
        instead describes the result up front as ``[applied]`` plus every column of
        the row, filling nulls when the update applied. The shape does not
        alternate, so the cached metadata id stays valid across both outcomes and
        ``skip_meta`` is exactly as safe here as for any other statement. A schema
        change still changes the id, and the driver must pick that up.
        """
        prepared = self.session.prepare(
            "INSERT INTO {} (a, b, c) VALUES (?, ?, ?) IF NOT EXISTS".format(self.table_name))
        id_before = prepared.result_metadata_id
        assert id_before is not None
        assert len(prepared.result_metadata) == 4  # [applied], a, b, c

        # a=2 is free, so the insert applies; the row columns come back null.
        assert self.session.execute(prepared.bind((2, 2, 2))).one() == (True, None, None, None)

        # a=1 exists (setUp), so this one does not apply and the conflicting row is
        # returned — same metadata, so the cached pair is still the right one.
        assert self.session.execute(prepared.bind((1, 9, 9))).one() == (False, 1, 1, 1)
        assert prepared.result_metadata_id == id_before

        self.session.execute("ALTER TABLE {} ADD d int".format(self.table_name))

        # The result gained a column, so the server must report a new id and the
        # driver must adopt it — for a conditional statement like for any other.
        assert self.session.execute(prepared.bind((1, 9, 9))).one() == (False, 1, 1, 1, None)
        assert prepared.result_metadata_id != id_before
        assert len(prepared.result_metadata) == 5

        assert self.session.execute(prepared.bind((3, 3, 3))).one() == (True, None, None, None, None)
