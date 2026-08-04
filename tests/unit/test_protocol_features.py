import unittest

import logging

from cassandra.protocol_features import ProtocolFeatures, TABLETS_ROUTING_V1, TABLETS_ROUTING_V2

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

    def test_use_metadata_id_parsing(self):
        """
        Test that SCYLLA_USE_METADATA_ID is parsed from SUPPORTED options.
        """
        options = {'SCYLLA_USE_METADATA_ID': ['']}
        protocol_features = ProtocolFeatures.parse_from_supported(options)
        assert protocol_features.use_metadata_id is True

    def test_use_metadata_id_missing(self):
        """
        Test that use_metadata_id is False when SCYLLA_USE_METADATA_ID is absent.
        """
        options = {'SCYLLA_RATE_LIMIT_ERROR': ['ERROR_CODE=1']}
        protocol_features = ProtocolFeatures.parse_from_supported(options)
        assert protocol_features.use_metadata_id is False

    def test_use_metadata_id_startup_options(self):
        """
        Test that SCYLLA_USE_METADATA_ID is included in STARTUP options when negotiated.
        """
        options = {'SCYLLA_USE_METADATA_ID': ['']}
        protocol_features = ProtocolFeatures.parse_from_supported(options)
        startup = {}
        protocol_features.add_startup_options(startup)
        assert 'SCYLLA_USE_METADATA_ID' in startup

    def test_use_metadata_id_not_in_startup_when_not_negotiated(self):
        """
        Test that SCYLLA_USE_METADATA_ID is NOT included in STARTUP when not negotiated.
        """
        protocol_features = ProtocolFeatures.parse_from_supported({})
        startup = {}
        protocol_features.add_startup_options(startup)
        assert 'SCYLLA_USE_METADATA_ID' not in startup

    def test_tablets_routing_v2_negotiation(self):
        """V2 is detected from SUPPORTED and subsumes V1 in STARTUP options."""
        options = {
            TABLETS_ROUTING_V1: [''],
            TABLETS_ROUTING_V2: [''],
        }
        features = ProtocolFeatures.parse_from_supported(options)
        assert features.tablets_routing_v1 is True
        assert features.tablets_routing_v2 is True

        # V2 subsumes V1: only TABLETS_ROUTING_V2 should appear in startup.
        startup = {}
        features.add_startup_options(startup)
        assert TABLETS_ROUTING_V2 in startup
        assert TABLETS_ROUTING_V1 not in startup

    def test_tablets_routing_v1_only(self):
        """When server only advertises V1, only V1 is negotiated."""
        options = {
            TABLETS_ROUTING_V1: [''],
        }
        features = ProtocolFeatures.parse_from_supported(options)
        assert features.tablets_routing_v1 is True
        assert features.tablets_routing_v2 is False

        startup = {}
        features.add_startup_options(startup)
        assert TABLETS_ROUTING_V1 in startup
        assert TABLETS_ROUTING_V2 not in startup

    def test_no_tablets_routing(self):
        """When server advertises neither V1 nor V2."""
        options = {}
        features = ProtocolFeatures.parse_from_supported(options)
        assert features.tablets_routing_v1 is False
        assert features.tablets_routing_v2 is False

        startup = {}
        features.add_startup_options(startup)
        assert TABLETS_ROUTING_V1 not in startup
        assert TABLETS_ROUTING_V2 not in startup
