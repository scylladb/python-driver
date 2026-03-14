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

"""
Unit tests for cassandra.cqlengine.management module.

Focuses on verifying that _get_table_metadata gracefully handles missing
table metadata by forcing a targeted refresh and retrying, and that
_sync_table delegates to _get_table_metadata for post-DDL metadata lookup.
"""

import unittest
from unittest.mock import patch, MagicMock, PropertyMock

from cassandra.cqlengine import CQLEngineException
from cassandra.cqlengine.management import _get_table_metadata, _sync_table


class MockTableMeta:
    """Minimal stand-in for TableMetadata."""

    def __init__(self):
        self.columns = {}
        self.options = {}
        self.partition_key = []
        self.clustering_key = []


class TestGetTableMetadataRetry(unittest.TestCase):
    """Tests for _get_table_metadata retry on KeyError."""

    def _make_model(self, ks="test_ks", table="test_table"):
        model = MagicMock()
        model._get_keyspace.return_value = ks
        model._raw_column_family_name.return_value = table
        return model

    @patch("cassandra.cqlengine.management.get_cluster")
    def test_returns_table_when_present(self, mock_get_cluster):
        """Table metadata is found on first lookup -- no refresh needed."""
        table_meta = MockTableMeta()
        cluster = MagicMock()
        cluster.metadata.keyspaces = {
            "test_ks": MagicMock(tables={"test_table": table_meta})
        }
        mock_get_cluster.return_value = cluster
        model = self._make_model()

        result = _get_table_metadata(model)
        self.assertIs(result, table_meta)
        cluster.refresh_table_metadata.assert_not_called()

    @patch("cassandra.cqlengine.management.get_cluster")
    def test_retries_after_refresh_on_missing_table(self, mock_get_cluster):
        """Table missing initially, but available after refresh."""
        table_meta = MockTableMeta()
        cluster = MagicMock()

        # First lookup: table not in tables dict. After refresh: table is there.
        tables_first = {}
        tables_after = {"test_table": table_meta}
        ks_meta = MagicMock()
        type(ks_meta).tables = PropertyMock(side_effect=[tables_first, tables_after])
        cluster.metadata.keyspaces = {"test_ks": ks_meta}
        mock_get_cluster.return_value = cluster

        model = self._make_model()
        result = _get_table_metadata(model)

        self.assertIs(result, table_meta)
        cluster.refresh_table_metadata.assert_called_once_with("test_ks", "test_table")

    @patch("cassandra.cqlengine.management.get_cluster")
    def test_raises_after_failed_refresh(self, mock_get_cluster):
        """Table missing even after refresh -- raises CQLEngineException."""
        cluster = MagicMock()
        ks_meta = MagicMock()
        type(ks_meta).tables = PropertyMock(return_value={})
        cluster.metadata.keyspaces = {"test_ks": ks_meta}
        mock_get_cluster.return_value = cluster

        model = self._make_model()

        with self.assertRaises(CQLEngineException) as ctx:
            _get_table_metadata(model)

        self.assertIn("not available after refresh", str(ctx.exception))
        cluster.refresh_table_metadata.assert_called_once_with("test_ks", "test_table")


class TestSyncTableMetadataLookup(unittest.TestCase):
    """Tests that _sync_table delegates metadata lookup to _get_table_metadata."""

    def _make_model(self, ks="test_ks", table="test_table"):
        """Create a mock model that passes _sync_table's precondition checks."""
        model = MagicMock()
        model.__abstract__ = False
        model.column_family_name.return_value = '"test_ks"."test_table"'
        model._raw_column_family_name.return_value = table
        model._get_keyspace.return_value = ks
        model._get_connection.return_value = None
        model._columns = {}
        return model

    @patch("cassandra.cqlengine.management._get_table_metadata")
    @patch("cassandra.cqlengine.management._get_create_table", return_value="CREATE TABLE test")
    @patch("cassandra.cqlengine.management.execute")
    @patch("cassandra.cqlengine.management.get_cluster")
    @patch("cassandra.cqlengine.management._allow_schema_modification", return_value=True)
    @patch("cassandra.cqlengine.management.issubclass", return_value=True)
    def test_calls_get_table_metadata_after_create(
        self, mock_issubclass, mock_allow, mock_get_cluster,
        mock_execute, mock_create, mock_get_meta
    ):
        """After creating a new table, _sync_table calls _get_table_metadata."""
        table_meta = MockTableMeta()
        mock_get_meta.return_value = table_meta

        cluster = MagicMock()
        ks_meta = MagicMock()
        ks_meta.tables = {}  # table not in tables -> triggers CREATE TABLE
        cluster.metadata.keyspaces = {"test_ks": ks_meta}
        mock_get_cluster.return_value = cluster

        model = self._make_model()
        _sync_table(model)

        mock_get_meta.assert_called_once_with(model, None)

    @patch("cassandra.cqlengine.management._get_table_metadata")
    @patch("cassandra.cqlengine.management._get_create_table", return_value="CREATE TABLE test")
    @patch("cassandra.cqlengine.management.execute")
    @patch("cassandra.cqlengine.management.get_cluster")
    @patch("cassandra.cqlengine.management._allow_schema_modification", return_value=True)
    @patch("cassandra.cqlengine.management.issubclass", return_value=True)
    def test_propagates_exception_from_get_table_metadata(
        self, mock_issubclass, mock_allow, mock_get_cluster,
        mock_execute, mock_create, mock_get_meta
    ):
        """CQLEngineException from _get_table_metadata propagates out of _sync_table."""
        mock_get_meta.side_effect = CQLEngineException("Table metadata not available")

        cluster = MagicMock()
        ks_meta = MagicMock()
        ks_meta.tables = {}
        cluster.metadata.keyspaces = {"test_ks": ks_meta}
        mock_get_cluster.return_value = cluster

        model = self._make_model()

        with self.assertRaises(CQLEngineException) as ctx:
            _sync_table(model)

        self.assertIn("not available", str(ctx.exception))
