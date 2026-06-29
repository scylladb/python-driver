import logging

from cassandra.shard_info import _ShardingInfo
from cassandra.lwt_info import _LwtInfo

log = logging.getLogger(__name__)


LWT_ADD_METADATA_MARK = "SCYLLA_LWT_ADD_METADATA_MARK"
LWT_OPTIMIZATION_META_BIT_MASK = "LWT_OPTIMIZATION_META_BIT_MASK"
RATE_LIMIT_ERROR_EXTENSION = "SCYLLA_RATE_LIMIT_ERROR"
TABLETS_ROUTING_V1 = "TABLETS_ROUTING_V1"

class ProtocolFeatures(object):
    rate_limit_error = None
    shard_id = 0
    sharding_info = None
    tablets_routing_v1 = False
    lwt_info = None
    is_scylla = False

    def __init__(self, rate_limit_error=None, shard_id=0, sharding_info=None, tablets_routing_v1=False, lwt_info=None, is_scylla=False):
        self.rate_limit_error = rate_limit_error
        self.shard_id = shard_id
        self.sharding_info = sharding_info
        self.tablets_routing_v1 = tablets_routing_v1
        self.lwt_info = lwt_info
        self.is_scylla = is_scylla

    @staticmethod
    def parse_from_supported(supported):
        rate_limit_error = ProtocolFeatures.maybe_parse_rate_limit_error(supported)
        shard_id, sharding_info = ProtocolFeatures.parse_sharding_info(supported)
        tablets_routing_v1 = ProtocolFeatures.parse_tablets_info(supported)
        lwt_info = ProtocolFeatures.parse_lwt_info(supported)
        is_scylla = ProtocolFeatures.detect_scylla(supported, sharding_info)
        return ProtocolFeatures(rate_limit_error, shard_id, sharding_info, tablets_routing_v1, lwt_info, is_scylla)

    @staticmethod
    def detect_scylla(supported, sharding_info):
        """Detect ScyllaDB from SUPPORTED extensions, independent of shard awareness.

        ScyllaDB is identified by the presence of any known Scylla-specific
        extension key in the SUPPORTED response.  Checking only shard-related
        fields (SCYLLA_NR_SHARDS, etc.) is insufficient because those are
        absent when shard-awareness is disabled on the server side
        (allow_shard_aware_drivers: false), which would cause the driver to
        misidentify a ScyllaDB cluster as Cassandra and, for example, try
        to query the peers_v2 table that ScyllaDB does not support.
        """
        return (
            LWT_ADD_METADATA_MARK in supported
            or RATE_LIMIT_ERROR_EXTENSION in supported
            or TABLETS_ROUTING_V1 in supported
            or sharding_info is not None
        )

    @staticmethod
    def maybe_parse_rate_limit_error(supported):
        vals = supported.get(RATE_LIMIT_ERROR_EXTENSION)
        if vals is not None:
            code_str = ProtocolFeatures.get_cql_extension_field(vals, "ERROR_CODE")
            return int(code_str)

    #  Looks up a field which starts with `key=` and returns the rest
    @staticmethod
    def get_cql_extension_field(vals, key):
        for v in vals:
            stripped_v = v.strip()
            if stripped_v.startswith(key) and stripped_v[len(key)] == '=':
                result = stripped_v[len(key) + 1:]
                return result
        return None

    def add_startup_options(self, options):
        if self.rate_limit_error is not None:
            options[RATE_LIMIT_ERROR_EXTENSION] = ""
        if self.tablets_routing_v1:
            options[TABLETS_ROUTING_V1] = ""
        if self.lwt_info is not None:
            options[LWT_ADD_METADATA_MARK] = str(self.lwt_info.lwt_meta_bit_mask)

    @staticmethod
    def parse_sharding_info(options):
        shard_id = options.get('SCYLLA_SHARD', [''])[0] or None
        shards_count = options.get('SCYLLA_NR_SHARDS', [''])[0] or None
        partitioner = options.get('SCYLLA_PARTITIONER', [''])[0] or None
        sharding_algorithm = options.get('SCYLLA_SHARDING_ALGORITHM', [''])[0] or None
        sharding_ignore_msb = options.get('SCYLLA_SHARDING_IGNORE_MSB', [''])[0] or None
        shard_aware_port = options.get('SCYLLA_SHARD_AWARE_PORT', [''])[0] or None
        shard_aware_port_ssl = options.get('SCYLLA_SHARD_AWARE_PORT_SSL', [''])[0] or None
        log.debug("Parsing sharding info from message options %s", options)

        if not (shards_count and sharding_ignore_msb):
            return 0, None

        return int(shard_id) if shard_id is not None else 0, _ShardingInfo(shard_id if shard_id is not None else 0,
                                            shards_count, partitioner, sharding_algorithm, sharding_ignore_msb,
                                            shard_aware_port, shard_aware_port_ssl)


    @staticmethod
    def parse_tablets_info(options):
        return TABLETS_ROUTING_V1 in options

    @staticmethod
    def parse_lwt_info(options):
        value_list = options.get(LWT_ADD_METADATA_MARK, [None])
        for value in value_list:
            if value is None or not value.startswith(LWT_OPTIMIZATION_META_BIT_MASK + "="):
                continue
            try:
                lwt_meta_bit_mask = int(value[len(LWT_OPTIMIZATION_META_BIT_MASK + "="):])
                return _LwtInfo(lwt_meta_bit_mask)
            except Exception as e:
                log.exception(f"Error while parsing {LWT_ADD_METADATA_MARK}: {e}")
                return None

        return None
