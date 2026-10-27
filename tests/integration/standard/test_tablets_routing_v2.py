"""
End-to-end tests for TABLETS_ROUTING_V2 against a V2-capable Scylla build.

Unlike the unit tests in tests/unit/test_tablets.py and tests/unit/test_policies.py,
these tests cross the driver<->server boundary: they validate that the driver
negotiates the extension, parses the server's `tablets-routing-v2` payload with
the correct field layout, and that the tablet_version_block it sends actually
matches the server's encoding.

The whole module is opt-in: the server only advertises the extension when started
with the `strongly-consistent-tables` experimental feature, and it is exchanged on
the wire under the name `TABLETS_ROUTING_V2_EXPERIMENTAL_V2`. When run against a
server that does not advertise it (e.g. a released Scylla), every test is skipped.
"""

from contextlib import contextmanager
import uuid

import pytest

import cassandra.cqltypes as types
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.metadata import _ConsistencyMode
from cassandra.policies import ConstantReconnectionPolicy, RoundRobinPolicy, TokenAwarePolicy
from cassandra.protocol import ExecuteMessage, NO_ROUTING_STATEMENT
from cassandra.protocol_features import (
    ProtocolFeatures, TABLETS_ROUTING_V1, TABLETS_ROUTING_V2,
)
from cassandra.query import BatchStatement, BoundStatement, SimpleStatement, Statement

from tests.integration import PROTOCOL_VERSION, use_cluster


def setup_module(module):
    try:
        # Use a single DC with three racks.
        use_cluster('tablets_routing_v2', {"dc1": [1, 1, 1]}, start=True, set_keyspace=False,
                    configuration_options={
                        # `strongly-consistent-tables` is what gates the server's
                        # advertisement of TABLETS_ROUTING_V2_EXPERIMENTAL_V2.
                        'experimental_features': ['udf', 'strongly-consistent-tables'],
                    })
    except Exception as exc:
        pytest.skip("Could not start a Scylla cluster with the "
                    f"'strongly-consistent-tables' experimental feature: {exc}",
                    allow_module_level=True)


_add_startup_options = ProtocolFeatures.add_startup_options


def _startup_with_both_extensions(self, options):
    """
    Drop-in replacement for ProtocolFeatures.add_startup_options that negotiates
    BOTH tablets_routing_v1 and tablets_routing_v2 on the same connection.

    The real driver makes the two mutually exclusive (V2 wins). Forcing both lets
    us prove the server-side precedence rules: scylla checks V2 first and only
    falls back to V1 when V2 is not set.

    Every other extension must be negotiated exactly as the driver would, so this
    delegates to the real implementation and only adds V1 on top of the V2 it
    already requested. Enumerating the options here instead would silently stop
    requesting any extension added later, while ProtocolFeatures still reports it
    as negotiated (it is parsed from SUPPORTED, not from what STARTUP asked for) --
    and an extension that changes the frame layout, such as
    SCYLLA_USE_METADATA_ID, then desynchronizes every request on the connection.
    """
    _add_startup_options(self, options)
    if self.tablets_routing_v1:
        options[TABLETS_ROUTING_V1] = ""


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
            pytest.skip("Server does not support TABLETS_ROUTING_V2_EXPERIMENTAL_V2. "
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

        session.execute("DROP KEYSPACE IF EXISTS test_v2_sc")
        session.execute(
            """
            CREATE KEYSPACE test_v2_sc
            WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}
            AND tablets = {'initial': 8}
            AND consistency = 'global'
            """)
        session.execute("CREATE TABLE test_v2_sc.t (pk int PRIMARY KEY, v int)")
        prepared_sc = session.prepare("INSERT INTO test_v2_sc.t (pk, v) VALUES (?, ?)")
        # Writes to a strongly-consistent (Raft) table are rejected unless they
        # use QUORUM/LOCAL_QUORUM. The session default is LOCAL_ONE, so request
        # LOCAL_QUORUM for these inserts; it propagates to every statement bound
        # from this prepared one.
        prepared_sc.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        for i in range(50):
            session.execute(prepared_sc.bind((i, i)))

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
        for pool in session._pools.values():
            host = pool.host
            for shard, conn in pool._connections.items():
                yield host, shard, conn

    @staticmethod
    def _wait_for_shard_connections(session, timeout=15):
        """Wait until each pool has filled its shard-aware connections (background)."""
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            if all(
                len(pool._connections) >= (min(pool.host.sharding_info.shards_count, 2)
                                           if pool.host.sharding_info else 1)
                for pool in session._pools.values()
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
        for pool in session._pools.values():
            host = pool.host
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

    # -- strongly-consistent (leader-aware) routing -----------------------------

    def test_strongly_consistent_keyspace_metadata(self):
        """
        The driver must learn from system_schema.scylla_keyspaces which keyspaces
        are strongly consistent: test_v2_sc (consistency='global') reports GLOBAL,
        test_v2 (no consistency clause) reports EVENTUAL. A GLOBAL mode is the
        precondition for leader-aware routing in
        TokenAwarePolicy.make_query_plan.
        """
        self.session.cluster.refresh_schema_metadata()
        keyspaces = self.session.cluster.metadata.keyspaces
        assert keyspaces['test_v2_sc']._consistency_mode is _ConsistencyMode.GLOBAL
        assert keyspaces['test_v2']._consistency_mode is _ConsistencyMode.EVENTUAL

    def test_consistency_mode_tracks_dynamic_keyspace_changes(self):
        """
        The driver reads system_schema.scylla_keyspaces on every schema refresh
        and sets KeyspaceMetadata._consistency_mode for each keyspace.

        The mode is not a one-off computed at connect time -- it has to track the
        live schema. Because the driver refreshes its metadata synchronously in
        response to a DDL it executes (ResponseFuture handles
        RESULT_KIND_SCHEMA_CHANGE by refreshing before returning), a keyspace
        created or dropped *after* connecting is immediately reflected in
        cluster.metadata.keyspaces, with no sleep or manual refresh required. A
        keyspace created by some *other* client is picked up through the very same
        refresh path, driven by the control connection's schema-change events
        (subject to the schema refresh window); this test drives the changes
        through the connected session so the assertions stay deterministic.
        """
        metadata = self.session.cluster.metadata
        ec_ks = "dyn_ec_ks"   # eventually consistent (no consistency clause)
        sc_ks = "dyn_sc_ks"   # strongly consistent (Raft, consistency='global')

        # Start from a known-clean slate so the test is repeatable.
        self.session.execute("DROP KEYSPACE IF EXISTS {0}".format(ec_ks))
        self.session.execute("DROP KEYSPACE IF EXISTS {0}".format(sc_ks))
        try:
            assert ec_ks not in metadata.keyspaces
            assert sc_ks not in metadata.keyspaces

            # Create an eventually-consistent keyspace after connecting: it shows
            # up in the map as EVENTUAL. This exercises the
            # "absent from scylla_keyspaces -> eventual" path.
            self.session.execute(
                "CREATE KEYSPACE {0} WITH replication = "
                "{{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} "
                "AND tablets = {{'initial': 1}}".format(ec_ks))
            assert ec_ks in metadata.keyspaces
            assert metadata.keyspaces[ec_ks]._consistency_mode is _ConsistencyMode.EVENTUAL

            # Create a strongly-consistent keyspace: same map, mode now GLOBAL.
            # (RF is irrelevant here -- the mode only tracks the consistency option.)
            self.session.execute(
                "CREATE KEYSPACE {0} WITH replication = "
                "{{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} "
                "AND tablets = {{'initial': 1}} AND consistency = 'global'".format(sc_ks))
            assert sc_ks in metadata.keyspaces
            assert metadata.keyspaces[sc_ks]._consistency_mode is _ConsistencyMode.GLOBAL
            # The previously-created keyspace keeps its EVENTUAL mode.
            assert metadata.keyspaces[ec_ks]._consistency_mode is _ConsistencyMode.EVENTUAL

            # A full refresh (the bulk get_all_keyspaces path, as opposed to the
            # single-keyspace get_keyspace path the DDL above exercised) agrees.
            self.session.cluster.refresh_schema_metadata()
            assert metadata.keyspaces[ec_ks]._consistency_mode is _ConsistencyMode.EVENTUAL
            assert metadata.keyspaces[sc_ks]._consistency_mode is _ConsistencyMode.GLOBAL

            # Dropping a keyspace removes it from the map and leaves the other
            # keyspace's mode untouched.
            self.session.execute("DROP KEYSPACE {0}".format(sc_ks))
            assert sc_ks not in metadata.keyspaces
            assert metadata.keyspaces[ec_ks]._consistency_mode is _ConsistencyMode.EVENTUAL

            self.session.execute("DROP KEYSPACE {0}".format(ec_ks))
            assert ec_ks not in metadata.keyspaces
        finally:
            self.session.execute("DROP KEYSPACE IF EXISTS {0}".format(ec_ks))
            self.session.execute("DROP KEYSPACE IF EXISTS {0}".format(sc_ks))

    def test_leader_aware_routing_targets_the_raft_leader(self):
        """
        For a strongly-consistent table, the server orders the replica list with
        the Raft leader first. Once that payload is cached, a TokenAwarePolicy
        must route every leader-requiring request (here a LOCAL_QUORUM read) for
        the tablet to replicas[0] (the leader), saving the extra
        coordinator->leader hop. This is the strongly-consistent counterpart to
        the eventually consistent test_v2 tests above, which never assert *which*
        replica is hit.

        A read at ONE/LOCAL_ONE is intentionally *not* pinned to the leader (any
        single replica satisfies it); that carve-out is covered by the policy
        unit tests.
        """
        select = self.session.prepare("SELECT v FROM test_v2_sc.t WHERE pk = ?")
        bound = select.bind([2])
        # Leader-first routing only applies to requests that actually need the
        # leader, so request LOCAL_QUORUM (a strong read). The session default is
        # LOCAL_ONE, which the policy would deliberately spread across replicas.
        bound.consistency_level = ConsistencyLevel.LOCAL_QUORUM

        tablet = self._ensure_cached(bound)
        assert tablet.replicas, "strongly-consistent tablet has no replicas"
        leader_host_id = tablet.replicas[0][0]

        # Leader-first routing only triggers when the keyspace is known to be
        # strongly consistent, i.e. when its consistency mode is GLOBAL.
        ks_meta = self.session.cluster.metadata.keyspaces['test_v2_sc']
        assert ks_meta._consistency_mode is _ConsistencyMode.GLOBAL

        # With an up-to-date cache the block always matches, so the server returns
        # no further payload and replicas[0] stays the leader; every request must
        # therefore be coordinated by that leader.
        for _ in range(10):
            result = self.session.execute(bound)
            coordinator = result.response_future.coordinator_host
            assert coordinator is not None and coordinator.host_id == leader_host_id, (
                "request coordinated by {} but the Raft leader is replicas[0]={}; "
                "leader-aware routing did not target the leader".format(
                    getattr(coordinator, 'host_id', None), leader_host_id))

    # -- batches ------------------------------------------------------------------

    @contextmanager
    def _new_table(self, keyspace, columns="pk int, ck int, v int, PRIMARY KEY (pk, ck)"):
        """
        Yield the name of a new table in `keyspace`, with `columns`. No request
        has accessed it yet, so the driver has no cached tablet of it.
        """
        table = "batch_" + uuid.uuid4().hex[:8]
        # The tablets of an empty table could be merged while the test runs.
        self.session.execute(
            f"CREATE TABLE {keyspace}.{table} ({columns}) "
            "WITH tablets = {'min_tablet_count': '8'}")
        try:
            yield table
        finally:
            self.session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table}")

    @staticmethod
    def _routing_payload(result):
        return (result.response_future.custom_payload or {}).get('tablets-routing-v2')

    def _execute_until_routing_information(self, statement, attempts=30):
        """
        Execute `statement` until its response carries routing information, and
        return the decoded payload. Without a cached tablet the driver sends a
        random tablet_version_block, which matches ~1/16 of the time.
        """
        for _ in range(attempts):
            result = self.session.execute(statement)
            if self._routing_payload(result) is not None:
                return result, self._decode_v2_payload(result.response_future.custom_payload)
        raise AssertionError("the server never returned a 'tablets-routing-v2' payload")

    def _assert_block_matches(self, statement, bound, attempts=10):
        """
        Assert that `statement` is sent with a block matching the cached tablet
        of `bound`, so the server returns no routing information for it.

        The tablet may change meanwhile, e.g. if the load balancer migrates it,
        and then the server rightly returns routing information for its new
        version. It's only wrong if it returns it for the version the block was
        chosen for.
        """
        for _ in range(attempts):
            tablet, _ = self._cached_tablet(bound)
            assert tablet is not None, "the tablet of the statement isn't cached"
            result = self.session.execute(statement)
            if self._routing_payload(result) is None:
                return
            info = self._decode_v2_payload(result.response_future.custom_payload)
            assert info['tablet_version'] != tablet.tablet_version, (
                "the server returned routing information despite a cached, up-to-date "
                "tablet_version")
        raise AssertionError(f"the tablet changed during each of {attempts} requests")

    @pytest.mark.parametrize("keyspace", ["test_v2", "test_v2_sc"])
    def test_batch_routing_information_is_cached_under_routing_statement_table(self, keyspace):
        """
        A BATCH names the statement it is routed by, and the routing information
        returned for its tablet is cached under that statement's table. Once it
        is, the block matches and no more routing information is returned.
        """
        with self._new_table(keyspace) as table:
            insert = self.session.prepare(f"INSERT INTO {keyspace}.{table} (pk, ck, v) VALUES (?, ?, ?)")
            # Strongly-consistent tables only accept QUORUM/LOCAL_QUORUM writes.
            batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            batch.add(insert, (1, 1, 1))
            batch.add(insert, (1, 2, 2))
            bound = insert.bind((1, 1, 1))
            assert self._cached_tablet(bound)[0] is None

            result, info = self._execute_until_routing_information(batch)
            assert result.response_future.message.routing_statement_index == 0

            tablet, token = self._cached_tablet(bound)
            assert tablet is not None, "the routing information wasn't cached under the table"
            assert tablet.tablet_version == info['tablet_version']
            assert tablet.first_token < token.value <= tablet.last_token

            self._assert_block_matches(batch, bound)

    def test_batch_routed_by_statement_after_query_text(self):
        """
        The routing statement doesn't have to come first: statements sent as
        query text can't be routed by, but they can precede it.
        """
        with self._new_table("test_v2") as table:
            insert = self.session.prepare(f"INSERT INTO test_v2.{table} (pk, ck, v) VALUES (?, ?, ?)")
            batch = BatchStatement()
            batch.add(f"INSERT INTO test_v2.{table} (pk, ck, v) VALUES (1, 0, 0)")
            batch.add(SimpleStatement(f"INSERT INTO test_v2.{table} (pk, ck, v) VALUES (1, 1, 1)"))
            batch.add(insert, (1, 2, 2))

            result, info = self._execute_until_routing_information(batch)

            assert result.response_future.message.routing_statement_index == 2
            _, token = self._cached_tablet(insert.bind((1, 2, 2)))
            assert info['first_token'] < token.value <= info['last_token']

    def test_batch_without_routing_statement(self):
        """
        A batch the server can't compute the token of names no routing
        statement: the server accepts it, and never returns routing information
        for it. That's a batch of statements sent as query text, even if one
        has a routing key, and an empty batch.
        """
        def assert_no_routing_statement(batch):
            for _ in range(3):
                result = self.session.execute(batch)
                assert result.response_future.message.routing_statement_index == NO_ROUTING_STATEMENT
                assert self._routing_payload(result) is None

        with self._new_table("test_v2") as table:
            query = f"INSERT INTO test_v2.{table} (pk, ck, v) VALUES (1, 1, 1)"

            batch = BatchStatement()
            batch.add(query)
            assert_no_routing_statement(batch)

            batch = BatchStatement()
            batch.add(SimpleStatement(query, keyspace='test_v2',
                                      routing_key=types.Int32Type.serialize(1, PROTOCOL_VERSION)))
            assert batch.routing_key is not None
            assert_no_routing_statement(batch)

            assert_no_routing_statement(BatchStatement())

    def test_batch_not_routed_by_statement_without_partition_key_bind_indices(self):
        """
        The marker is named pk but sets v, so the partition key is a literal and
        the server sends no partition key bind indices. The driver guesses a
        routing key from the marker's name, but the batch names no routing
        statement: the server would reject it.
        """
        with self._new_table("test_v2") as table:
            update = self.session.prepare(f"UPDATE test_v2.{table} SET v = :pk WHERE pk = 1 AND ck = 1")
            batch = BatchStatement()
            batch.add(update, (5,))
            assert batch.routing_key is not None

            for _ in range(3):
                result = self.session.execute(batch)
                assert result.response_future.message.routing_statement_index == NO_ROUTING_STATEMENT
                assert self._routing_payload(result) is None

    def test_batch_with_overridden_routing_key_gets_routing_information_of_routing_statement(self):
        """
        A batch the user routes by another routing key still names its routing
        statement, and gets routing information for the tablet of the partition
        the statement writes. Once it's cached, the block matches.
        """
        with self._new_table("test_v2") as table:
            insert = self.session.prepare(f"INSERT INTO test_v2.{table} (pk, ck, v) VALUES (?, ?, ?)")
            batch = BatchStatement()
            batch.add(insert, (1, 1, 1))
            batch.routing_key = insert.bind((2, 1, 1)).routing_key

            result, info = self._execute_until_routing_information(batch)

            assert result.response_future.message.routing_statement_index == 0
            tablet, token = self._cached_tablet(insert.bind((1, 1, 1)))
            assert info['first_token'] < token.value <= info['last_token']
            assert tablet is not None and tablet.tablet_version == info['tablet_version']

            self._assert_block_matches(batch, insert.bind((1, 1, 1)))

    def test_batch_with_changed_keyspace_or_table_names_no_routing_statement(self):
        """
        Once the user changes the keyspace or the table of a batch, it names no
        routing statement: the server accepts it, and returns no routing
        information, which the driver would cache under the wrong table.
        """
        with self._new_table("test_v2") as table, self._new_table("test_v2") as other:
            insert = self.session.prepare(f"INSERT INTO test_v2.{table} (pk, ck, v) VALUES (?, ?, ?)")
            metadata = self.session.cluster.metadata
            token = metadata.token_map.token_class.from_key(insert.bind((1, 1, 1)).routing_key)

            for keyspace, changed_table in (("test_v2_sc", table), ("test_v2", other)):
                batch = BatchStatement()
                batch.add(insert, (1, 1, 1))
                assert batch._get_routing_statement() is not None
                batch.keyspace = keyspace
                batch.table = changed_table

                for _ in range(3):
                    result = self.session.execute(batch)
                    assert result.response_future.message.routing_statement_index == NO_ROUTING_STATEMENT
                    assert self._routing_payload(result) is None
                assert metadata._tablets.get_tablet_for_key(keyspace, changed_table, token) is None

            assert metadata._tablets.get_tablet_for_key("test_v2", table, token) is None

    def test_multi_partition_batch_gets_routing_information_of_routing_statement(self):
        """
        An eventually consistent batch may write partitions of several tablets.
        It only gets routing information for the tablet of the routing
        statement.
        """
        with self._new_table("test_v2") as table:
            insert = self.session.prepare(f"INSERT INTO test_v2.{table} (pk, ck, v) VALUES (?, ?, ?)")
            routing_pk = 0
            routing_tablet = self._ensure_cached(insert.bind((routing_pk, 0, 0)))

            def in_routing_tablet(pk):
                _, token = self._cached_tablet(insert.bind((pk, 0, 0)))
                return routing_tablet.first_token < token.value <= routing_tablet.last_token

            other_pk = next((pk for pk in range(1, 1000) if not in_routing_tablet(pk)), None)
            assert other_pk is not None, "no partition outside the tablet of the routing statement"
            # Forget the tablet, so that the batch is sent with a random block.
            self.session.cluster.metadata._tablets.drop_tablets("test_v2", table)

            batch = BatchStatement()
            batch.add(f"INSERT INTO test_v2.{table} (pk, ck, v) VALUES ({other_pk}, 1, 1)")
            batch.add(insert, (routing_pk, 1, 1))
            batch.add(insert, (other_pk, 2, 2))

            _, info = self._execute_until_routing_information(batch)

            assert (info['first_token'], info['last_token']) == \
                (routing_tablet.first_token, routing_tablet.last_token)

    @staticmethod
    def _prepared_batch_query(keyspace, table):
        # Partition key bind indices: [3], the last marker targeting pk.
        return (f"BEGIN BATCH "
                f"INSERT INTO {keyspace}.{table} (pk, ck, v) VALUES (?, 1, ?); "
                f"UPDATE {keyspace}.{table} SET v = ? WHERE pk = ? AND ck = 2; "
                f"APPLY BATCH")

    def test_prepared_strongly_consistent_batch_gets_routing_information(self):
        """
        A prepared BEGIN BATCH ... APPLY BATCH is sent as an EXECUTE, and a
        strongly-consistent one is version-checked like any other prepared
        statement.
        """
        with self._new_table("test_v2_sc") as table:
            prepared = self.session.prepare(self._prepared_batch_query("test_v2_sc", table))
            assert prepared.routing_key_indexes == [3]
            bound = prepared.bind((1, 1, 2, 1))
            bound.consistency_level = ConsistencyLevel.LOCAL_QUORUM

            tablet = self._ensure_cached(bound)
            _, token = self._cached_tablet(bound)
            assert tablet.first_token < token.value <= tablet.last_token

            self._assert_block_matches(bound, bound)

    def test_prepared_eventually_consistent_batch_gets_no_routing_information(self):
        """
        The server ignores the tablet_version_block of an eventually consistent
        prepared batch. Nothing is cached, so every request carries another
        random block.
        """
        with self._new_table("test_v2") as table:
            prepared = self.session.prepare(self._prepared_batch_query("test_v2", table))
            bound = prepared.bind((1, 1, 2, 1))

            for _ in range(16):
                result = self.session.execute(bound)
                assert self._routing_payload(result) is None
            assert self._cached_tablet(bound)[0] is None

    def test_prepared_batch_with_literal_partition_key_gets_no_routing_information(self):
        """
        The partition key of a strongly-consistent prepared batch is a literal,
        so the server sends no partition key bind indices for it, and the driver
        guesses no routing key. The driver can't compute the token, so it sends
        a zero block, and the server never version-checks the batch.
        """
        with self._new_table("test_v2_sc") as table:
            prepared = self.session.prepare(
                f"BEGIN BATCH "
                f"INSERT INTO test_v2_sc.{table} (pk, ck, v) VALUES (1, 1, ?); "
                f"UPDATE test_v2_sc.{table} SET v = ? WHERE pk = 1 AND ck = 2; "
                f"APPLY BATCH")
            assert prepared._pk_indexes is None
            assert prepared.routing_key_indexes is None
            bound = prepared.bind((1, 2))
            bound.consistency_level = ConsistencyLevel.LOCAL_QUORUM
            assert bound.routing_key is None

            for _ in range(3):
                result = self.session.execute(bound)
                assert result.response_future.message.tablet_version_block == 0
                assert self._routing_payload(result) is None

    def test_batch_routed_by_statement_of_table_with_composite_partition_key(self):
        """
        The routing key of a composite partition key is packed from the values
        of its columns in the order of the partition key, not of the markers.
        The token computed from it is the one the server computes, so the batch
        gets routing information for the right tablet, and once it's cached,
        the block matches.
        """
        with self._new_table("test_v2", "pk1 int, pk2 int, v int, PRIMARY KEY ((pk1, pk2))") as table:
            insert = self.session.prepare(f"INSERT INTO test_v2.{table} (v, pk2, pk1) VALUES (?, ?, ?)")
            assert insert._pk_indexes == [2, 1]
            bound = insert.bind((0, 2, 1))
            batch = BatchStatement()
            batch.add(bound)

            result, info = self._execute_until_routing_information(batch)

            assert result.response_future.message.routing_statement_index == 0
            tablet, token = self._cached_tablet(bound)
            server_token = self.session.execute(
                f"SELECT token(pk1, pk2) FROM test_v2.{table} WHERE pk1 = 1 AND pk2 = 2").one()[0]
            assert token.value == server_token
            assert info['first_token'] < token.value <= info['last_token']
            assert tablet is not None and tablet.tablet_version == info['tablet_version']

            self._assert_block_matches(batch, bound)

    def _assert_batch_gets_routing_information_of_composite_partition(self, table, batch, prepared, values):
        """
        Assert that `batch`, routed by another routing key than the one of the
        partition its routing statement writes, gets routing information for
        that partition, and once it's cached, the block matches. `values` bind
        `prepared` as INSERT INTO <table> (pk1, pk2, v), so pk1 and pk2 are
        values[0] and values[1].
        """
        # The routing key of the partition, packed independently of the code
        # under test.
        server_routing_key = b"".join(Statement()._key_parts_packed(
            [types.Int32Type.serialize(v, PROTOCOL_VERSION) for v in values[:2]]))
        token_class = self.session.cluster.metadata.token_map.token_class
        # The batch is routed elsewhere.
        assert token_class.from_key(batch.routing_key) != token_class.from_key(server_routing_key)

        result, info = self._execute_until_routing_information(batch)

        assert result.response_future.message.routing_statement_index == 0
        # The batch wrote the row, so the server can compute its token.
        server_token = self.session.execute(
            f"SELECT token(pk1, pk2) FROM test_v2.{table} WHERE pk1 = %s AND pk2 = %s", values[:2]).one()[0]
        assert token_class.from_key(server_routing_key).value == server_token
        partition = BoundStatement(prepared, routing_key=server_routing_key).bind(values)
        tablet, token = self._cached_tablet(partition)
        assert info['first_token'] < token.value <= info['last_token']
        assert tablet is not None and tablet.tablet_version == info['tablet_version']

        self._assert_block_matches(batch, partition)

    def test_batch_routed_by_reordered_routing_key_indexes_of_composite_partition_key(self):
        """
        The user routes by the partition key columns in another order, so the
        batch is routed by another token than the server computes from pk1 and
        pk2. The batch still gets routing information for the tablet of their
        partition, and once it's cached, the block matches.
        """
        with self._new_table("test_v2", "pk1 int, pk2 int, v int, PRIMARY KEY ((pk1, pk2))") as table:
            insert = self.session.prepare(f"INSERT INTO test_v2.{table} (pk1, pk2, v) VALUES (?, ?, ?)")
            assert insert._pk_indexes == [0, 1]
            insert.routing_key_indexes = [1, 0]
            batch = BatchStatement()
            batch.add(insert, (1, 2, 0))

            self._assert_batch_gets_routing_information_of_composite_partition(table, batch, insert, (1, 2, 0))

    def test_batch_routed_by_explicit_routing_key_of_bound_statement_of_composite_partition_key(self):
        """
        The user gives the bound statement a routing key of another partition,
        so the batch is routed by it. The batch still gets routing information
        for the tablet of the partition the statement writes, and once it's
        cached, the block matches.
        """
        with self._new_table("test_v2", "pk1 int, pk2 int, v int, PRIMARY KEY ((pk1, pk2))") as table:
            insert = self.session.prepare(f"INSERT INTO test_v2.{table} (pk1, pk2, v) VALUES (?, ?, ?)")
            other_routing_key = b"".join(Statement()._key_parts_packed(
                [types.Int32Type.serialize(v, PROTOCOL_VERSION) for v in (3, 4)]))
            batch = BatchStatement()
            batch.add(BoundStatement(insert, routing_key=other_routing_key).bind((1, 2, 0)))
            assert batch.routing_key is other_routing_key

            self._assert_batch_gets_routing_information_of_composite_partition(table, batch, insert, (1, 2, 0))

    def test_batch_not_routed_by_statement_with_partly_bound_partition_key(self):
        """
        Only one column of the composite partition key is bound by a marker, the
        other one is a literal. The server sends no partition key bind indices
        for the statement, and the driver guesses no routing key for it either,
        so the batch is routed by the next statement that has one, and names it.
        """
        with self._new_table("test_v2", "pk1 int, pk2 int, v int, PRIMARY KEY ((pk1, pk2))") as table:
            partial = self.session.prepare(f"INSERT INTO test_v2.{table} (pk1, pk2, v) VALUES (?, 2, ?)")
            assert partial._pk_indexes is None
            assert partial.routing_key_indexes is None
            insert = self.session.prepare(f"INSERT INTO test_v2.{table} (pk1, pk2, v) VALUES (?, ?, ?)")
            bound = insert.bind((3, 4, 0))
            batch = BatchStatement()
            batch.add(partial, (1, 0))
            batch.add(bound)

            result, info = self._execute_until_routing_information(batch)

            assert result.response_future.message.routing_statement_index == 1
            tablet, token = self._cached_tablet(bound)
            assert info['first_token'] < token.value <= info['last_token']
            assert tablet is not None and tablet.tablet_version == info['tablet_version']

            self._assert_block_matches(batch, bound)

    def test_batch_routed_by_user_routing_key_of_partly_bound_partition_key(self):
        """
        The user may make the driver route by the bound column of a partition
        key whose other column is a literal. The server sent no partition key
        bind indices for the statement, so the batch names no routing
        statement: the server would reject it.
        """
        with self._new_table("test_v2", "pk1 int, pk2 int, v int, PRIMARY KEY ((pk1, pk2))") as table:
            partial = self.session.prepare(f"INSERT INTO test_v2.{table} (pk1, pk2, v) VALUES (?, 2, ?)")
            assert partial._pk_indexes is None
            partial.routing_key_indexes = [0]
            batch = BatchStatement()
            batch.add(partial, (1, 0))
            assert batch.routing_key is not None

            for _ in range(3):
                result = self.session.execute(batch)
                assert result.response_future.message.routing_statement_index == NO_ROUTING_STATEMENT
                assert self._routing_payload(result) is None
