"""
End-to-end tests for TABLETS_ROUTING_V2 against a V2-capable Scylla build.

Unlike the unit tests in tests/unit/test_tablets.py and tests/unit/test_policies.py,
these tests cross the driver<->server boundary: they validate that the driver
negotiates the extension, parses the server's `tablets-routing-v2` payload with
the correct field layout, and that the tablet_version_block it sends actually
matches the server's encoding.

The whole module is opt-in: the server only advertises the extension when started
with the `strongly-consistent-tables` experimental feature, and it is exchanged on
the wire under the name `TABLETS_ROUTING_V2_EXPERIMENTAL`. When run against a
server that does not advertise it (e.g. a released Scylla), every test is skipped.
"""

from contextlib import contextmanager

import pytest

import cassandra.cqltypes as types
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import ConstantReconnectionPolicy, RoundRobinPolicy, TokenAwarePolicy
from cassandra.protocol import ExecuteMessage
from cassandra.protocol_features import (
    ProtocolFeatures, RATE_LIMIT_ERROR_EXTENSION, LWT_ADD_METADATA_MARK,
    TABLETS_ROUTING_V1, TABLETS_ROUTING_V2,
)

from tests.integration import PROTOCOL_VERSION, use_cluster


def setup_module(module):
    try:
        # Use a single DC with three racks.
        use_cluster('tablets_routing_v2', {"dc1": [1, 1, 1]}, start=True, set_keyspace=False,
                    configuration_options={
                        # `strongly-consistent-tables` is what gates the server's
                        # advertisement of TABLETS_ROUTING_V2_EXPERIMENTAL.
                        'experimental_features': ['udf', 'strongly-consistent-tables'],
                    })
    except Exception as exc:
        pytest.skip("Could not start a Scylla cluster with the "
                    f"'strongly-consistent-tables' experimental feature: {exc}",
                    allow_module_level=True)


def _startup_with_both_extensions(self, options):
    """
    Drop-in replacement for ProtocolFeatures.add_startup_options that negotiates
    BOTH tablets_routing_v1 and tablets_routing_v2 on the same connection.

    The real driver makes the two mutually exclusive (V2 wins). Forcing both lets
    us prove the server-side precedence rules: scylla checks V2 first and only
    falls back to V1 when V2 is not set.
    """
    if self.rate_limit_error is not None:
        options[RATE_LIMIT_ERROR_EXTENSION] = ""
    if self.tablets_routing_v2:
        options[TABLETS_ROUTING_V2] = ""
    if self.tablets_routing_v1:
        options[TABLETS_ROUTING_V1] = ""
    if self.lwt_info is not None:
        options[LWT_ADD_METADATA_MARK] = str(self.lwt_info.lwt_meta_bit_mask)


class TestTabletsRoutingV2Integration:
    @classmethod
    def setup_class(cls):
        cls.cluster = Cluster(contact_points=["127.0.0.1", "127.0.0.2", "127.0.0.3"],
                              protocol_version=PROTOCOL_VERSION,
                              execution_profiles={
                                  EXEC_PROFILE_DEFAULT: ExecutionProfile(
                                      load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
                              },
                              reconnection_policy=ConstantReconnectionPolicy(1))
        # pytest does not call teardown_class when setup_class raises, so any exit
        # from here must shut the Cluster down explicitly or it leaks threads and
        # sockets into later test modules.
        try:
            cls.session = cls.cluster.connect()
            # A server without the 'strongly-consistent-tables' experimental
            # feature (e.g. a released Scylla) still starts and connects, but it
            # neither advertises TABLETS_ROUTING_V2 nor accepts the
            # `consistency = 'global'` keyspace that _create_schema needs. Detect
            # that here and skip the whole class, instead of letting _create_schema
            # fail and erroring every test.
            v2_negotiated = cls._v2_negotiated()
            if v2_negotiated:
                cls._create_schema(cls.session)
        except Exception:
            cls.cluster.shutdown()
            raise
        if not v2_negotiated:
            cls.cluster.shutdown()
            pytest.skip("Server does not support TABLETS_ROUTING_V2_EXPERIMENTAL. "
                        "It must be started with the 'strongly-consistent-tables' feature "
                        "and offer support for the protocol extension.")

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    @classmethod
    def _create_schema(cls, session):
        session.execute("DROP KEYSPACE IF EXISTS test_v2")
        session.execute(
            """
            CREATE KEYSPACE test_v2
            WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}
            AND tablets = {'initial': 8}
            """)
        session.execute("CREATE TABLE test_v2.t (pk int PRIMARY KEY, v int)")
        prepared = session.prepare("INSERT INTO test_v2.t (pk, v) VALUES (?, ?)")
        for i in range(50):
            session.execute(prepared.bind((i, i)))

    # -- helpers ----------------------------------------------------------------

    @classmethod
    def _v2_negotiated(cls):
        connection = cls.session.cluster.control_connection._connection
        return bool(connection and connection.features.tablets_routing_v2)

    def _cached_tablet(self, bound):
        md = self.session.cluster.metadata
        token = md.token_map.token_class.from_key(bound.routing_key)
        tablet = md._tablets.get_tablet_for_key(bound.keyspace, bound.table, token)
        return tablet, token

    def _ensure_cached(self, bound, attempts=30):
        """
        Drive requests until the V2 routing cache is populated for `bound`.

        On a cold start the driver sends a *random* tablet_version_block, which
        only matches the server ~1/16 of the time; on a mismatch the server
        returns routing info and the cache is filled. We retry until that happens.
        """
        for _ in range(attempts):
            self.session.execute(bound)
            tablet, _token = self._cached_tablet(bound)
            if tablet is not None and tablet.tablet_version is not None:
                return tablet
        raise AssertionError("V2 routing cache was never populated; the server "
                             "never returned a 'tablets-routing-v2' payload")

    # -- tests ------------------------------------------------------------------

    def test_v2_is_negotiated(self):
        # Every per-host pool must report tablet-routing support, and every live
        # connection in it must have negotiated V2 -- V2 gates the per-connection
        # EXECUTE framing.
        for pool in self.session._pools.values():
            assert pool.supports_tablet_routing is True
            for conn in pool._connections.values():
                assert conn.features.tablets_routing_v2 is True

    def test_v2_payload_populates_cache_with_valid_fields(self):
        """Test guarding against the payload tuple being decoded out of order."""
        select = self.session.prepare("SELECT v FROM test_v2.t WHERE pk = ?")
        bound = select.bind([2])

        tablet = self._ensure_cached(bound)
        _, token = self._cached_tablet(bound)

        # If the tuple were decoded in the wrong order, first_token/last_token
        # would actually carry the version / replica list and these invariants
        # would not hold.
        assert tablet.tablet_version is not None
        # tablet_version is an unsigned 64-bit value; a signedness bug in the
        # decode path would surface here as a negative or out-of-range number.
        assert 0 <= tablet.tablet_version <= 2 ** 64 - 1
        assert tablet.first_token <= tablet.last_token
        # get_tablet_for_key matches first_token < token <= last_token.
        assert tablet.first_token < token.value and token.value <= tablet.last_token

        # Replicas must be real hosts known to the cluster with sane shard ids.
        known_host_ids = {h.host_id for h in self.session.cluster.metadata.all_hosts()}
        assert tablet.replicas, "tablet has no replicas"
        for host_id, shard in tablet.replicas:
            assert host_id in known_host_ids, \
                f"replica host_id {host_id} is not a known host (corrupt payload?)"
            assert isinstance(shard, int) and shard >= 0

    def test_matching_block_yields_no_payload(self):
        """Test guarding against a wrong tablet_version_block bit-shift."""
        select = self.session.prepare("SELECT v FROM test_v2.t WHERE pk = ?")
        bound = select.bind([7])

        # Populate the cache so the driver knows the current tablet_version.
        self._ensure_cached(bound)

        # The next request carries a block derived from the cached version. If the
        # driver's encoding agrees with the server, the versions match and NO
        # routing payload is returned. A wrong shift would mismatch and the server
        # would keep returning routing info.
        result = self.session.execute(bound)
        assert result.one() is not None
        payload = result.response_future.custom_payload
        assert not (payload and 'tablets-routing-v2' in payload), (
            "Server returned routing info despite a cached, up-to-date "
            "tablet_version; the driver's tablet_version_block encoding likely "
            "disagrees with the server (locator::compare_tablet_version_block)")

    # -- low-level helpers ------------------------------------------------------

    @staticmethod
    def _right_block(version, idx=0):
        """
        Build a tablet_version_block that the server will accept as a match for
        block `idx` of `version` (high nibble = index, low nibble = that nibble
        of the version).
        """
        idx &= 0xF
        return (idx << 4) | ((version >> (idx * 4)) & 0xF)

    def _send_raw_execute(self, conn, bound, tablet_version_block):
        """
        Send an EXECUTE directly on a specific shard connection with a chosen
        tablet_version_block (or None to omit the byte entirely, i.e. behave like
        the pre-V2 protocol), and return the decoded response message.

        This bypasses ResponseFuture/load balancing so we control exactly which
        node+shard the request hits and which byte is on the wire; it also avoids
        polluting the driver's tablet cache.
        """
        ps = bound.prepared_statement
        msg = ExecuteMessage(
            ps.query_id, bound.values, ConsistencyLevel.LOCAL_ONE,
            serial_consistency_level=None, fetch_size=None, paging_state=None,
            timestamp=None, skip_meta=False,
            result_metadata_id=ps.result_metadata_id,
            tablet_version_block=tablet_version_block)
        return conn.wait_for_response(msg, timeout=30)

    def _decode_v2_payload(self, payload):
        ctype = types.lookup_casstype(
            'TupleType(LongType, LongType, ListType(TupleType(UUIDType, Int32Type)), LongType)')
        info = ctype.from_binary(payload['tablets-routing-v2'], self.cluster.protocol_version)
        # LongType decodes as signed, but tablet_version is an unsigned 64-bit
        # value; mask it the same way Tablet.from_row does so the decoded value
        # matches what the driver cached.
        return {'first_token': info[0], 'last_token': info[1],
                'replicas': info[2],
                'tablet_version': info[3] & 0xFFFFFFFFFFFFFFFF}

    def _any_connection(self):
        for pool in self.session._pools.values():
            for conn in pool._connections.values():
                return conn
        raise AssertionError("no shard connections available")

    @staticmethod
    def _all_shard_connections(session):
        for host, pool in session._pools.items():
            for shard, conn in pool._connections.items():
                yield host, shard, conn

    @staticmethod
    def _wait_for_shard_connections(session, timeout=15):
        """Wait until each pool has filled its shard-aware connections (background)."""
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            if all(
                len(pool._connections) >= (min(host.sharding_info.shards_count, 2)
                                           if host.sharding_info else 1)
                for host, pool in session._pools.items()
            ):
                return
            time.sleep(0.05)
        raise AssertionError(f"Shard-aware connection pools did not fill within {timeout}s")

    @contextmanager
    def _cluster_with_v1_and_v2(self):
        """
        Yield a (cluster, session) whose connections negotiated BOTH V1 and V2.
        Restores the original startup behavior and shuts the cluster down on exit.
        """
        original = ProtocolFeatures.add_startup_options
        ProtocolFeatures.add_startup_options = _startup_with_both_extensions

        cluster = None
        try:
            cluster = Cluster(contact_points=["127.0.0.1", "127.0.0.2", "127.0.0.3"],
                              protocol_version=PROTOCOL_VERSION,
                              execution_profiles={
                                  EXEC_PROFILE_DEFAULT: ExecutionProfile(
                                      load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
                              },
                              reconnection_policy=ConstantReconnectionPolicy(1))
            session = cluster.connect('test_v2')
            self._wait_for_shard_connections(session)
            yield cluster, session
        finally:
            ProtocolFeatures.add_startup_options = original
            if cluster is not None:
                cluster.shutdown()

    @staticmethod
    def _find_replica_wrong_shard(session, tablet):
        """
        Find a connection to a host that *is* a replica of `tablet` but on a shard
        that the host does NOT own for it ("right node, wrong shard"). Returns
        (host, owner_shard, wrong_shard, conn) or None if no host has >=2 shards.
        """
        replica_shard = {host_id: shard for host_id, shard in tablet.replicas}
        for host, pool in session._pools.items():
            owner = replica_shard.get(host.host_id)
            if owner is None:
                continue
            for shard, conn in pool._connections.items():
                if shard != owner:
                    return host, owner, shard, conn
        return None

    # -- scenario tests ---------------------------------------------------------

    def test_index0_all_block_values_exactly_one_match(self):
        """
        Scenario 1: for block index 0, exactly one of the 16 possible values
        matches the server's tablet_version; every other value is reported as a
        mismatch carrying that same tablet_version, whose nibble 0 equals the
        value that matched.
        """
        select = self.session.prepare("SELECT v FROM test_v2.t WHERE pk = ?")
        bound = select.bind([11])
        tablet = self._ensure_cached(bound)
        version = tablet.tablet_version

        conn = self._any_connection()

        matched_values = []
        reported_versions = []
        for value in range(16):
            block = value  # index 0 -> high nibble 0, low nibble = value
            resp = self._send_raw_execute(conn, bound, block)
            payload = resp.custom_payload or {}
            if 'tablets-routing-v2' in payload:
                reported_versions.append(self._decode_v2_payload(payload)['tablet_version'])
            else:
                matched_values.append(value)

        # Exactly one value matches: the low nibble of the version.
        assert matched_values == [version & 0xF], \
            f"expected exactly one matching block value, got {matched_values}"
        # All 15 mismatches report the same tablet_version ...
        assert len(reported_versions) == 15
        assert set(reported_versions) == {version}
        # ... and that version's block-0 nibble is the value that matched.
        assert (version & 0xF) == matched_values[0]

    def test_right_block_to_all_nodes_and_shards_never_returns_payload(self):
        """
        Scenario 2: a correct tablet_version_block matches on every node and every
        shard (the server's V2 check ignores shard), so no routing payload is ever
        returned.
        """
        select = self.session.prepare("SELECT v FROM test_v2.t WHERE pk = ?")
        bound = select.bind([13])
        tablet = self._ensure_cached(bound)
        version = tablet.tablet_version

        sent = 0
        for host, shard, conn in self._all_shard_connections(self.session):
            # Vary the block index per shard to also exercise non-zero indices.
            block = self._right_block(version, idx=shard)
            resp = self._send_raw_execute(conn, bound, block)
            payload = resp.custom_payload or {}
            assert 'tablets-routing-v2' not in payload, (
                f"host {host} shard {shard} returned a routing payload for a correct "
                "tablet_version_block")
            sent += 1
        assert sent >= 1, "no shard connections were exercised"

    def test_v2_takes_precedence_over_v1_no_v1_payload_on_wrong_shard(self):
        """
        Scenario 3: with BOTH V1 and V2 negotiated, send a correct V2 block to the
        wrong shard. The server checks V2 first; since the block matches there is
        no payload at all -- crucially no `tablets-routing-v1`, which V1 would have
        emitted for a wrong-shard request.
        """
        select = self.session.prepare("SELECT v FROM test_v2.t WHERE pk = ?")
        bound = select.bind([17])
        tablet = self._ensure_cached(bound)
        version = tablet.tablet_version

        with self._cluster_with_v1_and_v2() as (_cluster, session):
            target = self._find_replica_wrong_shard(session, tablet)
            if target is None:
                pytest.skip("need a replica host with >=2 shards to target a wrong shard")
            _host, _owner_shard, _wrong_shard, conn = target
            assert conn.features.tablets_routing_v1 and conn.features.tablets_routing_v2, \
                "test setup failed: connection did not negotiate both V1 and V2"

            resp = self._send_raw_execute(conn, bound, self._right_block(version))
            payload = resp.custom_payload or {}
            assert 'tablets-routing-v1' not in payload, (
                "server emitted V1 routing info despite V2 being negotiated; "
                "V2 must take precedence (select_statement.cc)")
            # The correct V2 block also means no V2 payload.
            assert 'tablets-routing-v2' not in payload
