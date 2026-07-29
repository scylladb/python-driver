import unittest

import logging

from cassandra.protocol_features import ProtocolFeatures

LOGGER = logging.getLogger(__name__)


class TestProtocolFeatures(unittest.TestCase):
    def test_parsing_rate_limit_error(self):
        """
        Testing the parsing of the options command
        """
        class OptionsHolder(object):
            options = {
                'SCYLLA_RATE_LIMIT_ERROR': ["ERROR_CODE=123"]
            }

        protocol_features = ProtocolFeatures.parse_from_supported(OptionsHolder().options)

        assert protocol_features.rate_limit_error == 123
        assert protocol_features.shard_id == 0
        assert protocol_features.sharding_info is None

    # -----------------------------------------------------------------
    # Tests for is_scylla detection (independent of shard awareness)
    # Regression for: ScyllaDB misidentified as Cassandra when sharding
    # is disabled (allow_shard_aware_drivers: false).
    # -----------------------------------------------------------------

    def test_is_scylla_detected_via_lwt(self):
        """ScyllaDB is recognised from SCYLLA_LWT_ADD_METADATA_MARK alone."""
        pf = ProtocolFeatures.parse_from_supported({
            'SCYLLA_LWT_ADD_METADATA_MARK': ['LWT_OPTIMIZATION_META_BIT_MASK=8'],
        })
        assert pf.is_scylla is True
        assert pf.shard_id == 0
        assert pf.sharding_info is None  # no shard-aware connections expected

    def test_is_scylla_detected_via_rate_limit(self):
        """ScyllaDB is recognised from SCYLLA_RATE_LIMIT_ERROR alone."""
        pf = ProtocolFeatures.parse_from_supported({
            'SCYLLA_RATE_LIMIT_ERROR': ['ERROR_CODE=42'],
        })
        assert pf.is_scylla is True
        assert pf.shard_id == 0
        assert pf.sharding_info is None

    def test_is_scylla_detected_via_tablets(self):
        """ScyllaDB is recognised from TABLETS_ROUTING_V1 alone."""
        pf = ProtocolFeatures.parse_from_supported({
            'TABLETS_ROUTING_V1': [''],
        })
        assert pf.is_scylla is True
        assert pf.shard_id == 0
        assert pf.sharding_info is None

    def test_is_scylla_detected_via_sharding(self):
        """ScyllaDB with full sharding is recognised and sharding_info is populated."""
        pf = ProtocolFeatures.parse_from_supported({
            'SCYLLA_SHARD': ['3'],
            'SCYLLA_NR_SHARDS': ['12'],
            'SCYLLA_PARTITIONER': ['org.apache.cassandra.dht.Murmur3Partitioner'],
            'SCYLLA_SHARDING_ALGORITHM': ['biased-token-round-robin'],
            'SCYLLA_SHARDING_IGNORE_MSB': ['12'],
            'SCYLLA_LWT_ADD_METADATA_MARK': ['LWT_OPTIMIZATION_META_BIT_MASK=8'],
        })
        assert pf.is_scylla is True
        assert pf.shard_id == 3
        assert pf.sharding_info is not None
        assert pf.sharding_info.shards_count == 12

    def test_cassandra_is_not_scylla(self):
        """Pure Cassandra SUPPORTED response must not set is_scylla."""
        pf = ProtocolFeatures.parse_from_supported({
            'CQL_VERSION': ['3.0.0'],
            'COMPRESSION': ['lz4', 'snappy'],
        })
        assert pf.is_scylla is False
        assert pf.sharding_info is None

    def test_scylla_without_sharding_no_crash(self):
        """
        Regression test for F1: SCYLLA_PARTITIONER present but SCYLLA_SHARD /
        SCYLLA_NR_SHARDS absent must not raise TypeError. ScyllaDB gates all
        sharding fields together server-side, so this is not expected in
        practice, but if it happens the driver must not fabricate a partial
        ShardingInfo -- it should just disable shard-aware routing while
        still recognising the server as ScyllaDB via the LWT extension key.
        """
        pf = ProtocolFeatures.parse_from_supported({
            'SCYLLA_PARTITIONER': ['org.apache.cassandra.dht.Murmur3Partitioner'],
            'SCYLLA_LWT_ADD_METADATA_MARK': ['LWT_OPTIMIZATION_META_BIT_MASK=8'],
        })
        assert pf.is_scylla is True
        # Incomplete sharding fields -- no ShardingInfo is fabricated.
        assert pf.shard_id == 0
        assert pf.sharding_info is None

    def test_scylla_sharding_algorithm_only_no_crash(self):
        """
        Regression: SCYLLA_SHARDING_ALGORITHM present without SCYLLA_SHARD /
        SCYLLA_NR_SHARDS must not raise TypeError, and must not fabricate a
        partial ShardingInfo.
        """
        pf = ProtocolFeatures.parse_from_supported({
            'SCYLLA_SHARDING_ALGORITHM': ['biased-token-round-robin'],
            'SCYLLA_RATE_LIMIT_ERROR': ['ERROR_CODE=42'],
        })
        assert pf.is_scylla is True
        assert pf.shard_id == 0
        assert pf.sharding_info is None
