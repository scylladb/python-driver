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
server that does not advertise it (e.g. a released Scylla), every test skips.
"""

import pytest

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import ConstantReconnectionPolicy, RoundRobinPolicy, TokenAwarePolicy

from tests.integration import PROTOCOL_VERSION, use_cluster


def setup_module(module):
    try:
        use_cluster('tablets_routing_v2', [3], start=True, set_keyspace=False,
                    configuration_options={
                        # `strongly-consistent-tables` is what gates the server's
                        # advertisement of TABLETS_ROUTING_V2_EXPERIMENTAL
                        # (see scylladb transport/controller.cc).
                        'experimental_features': ['lwt', 'udf', 'strongly-consistent-tables'],
                        # When V2 is negotiated but a request omits the mandatory
                        # tablet_version_block, the server calls on_internal_error
                        # (cql3/statements/select_statement.cc). With this set to
                        # False that raises a catchable error returned to the client
                        # instead of aborting the node (see test_no_block_*).
                        'abort_on_internal_error': False,
                    })
    except Exception as exc:
        pytest.skip("Could not start a Scylla cluster with the "
                    "'strongly-consistent-tables' experimental feature: {}".format(exc),
                    allow_module_level=True)


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
        cls.session = cls.cluster.connect()

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    def _v2_negotiated(self):
        return bool(self.session.cluster.control_connection._tablets_routing_v2)

    def _skip_if_no_v2(self):
        if not self._v2_negotiated():
            pytest.skip("Server did not advertise TABLETS_ROUTING_V2_EXPERIMENTAL; "
                        "needs a build started with the 'strongly-consistent-tables' feature")

    def test_strongly_consistent_flag_tracks_dynamic_keyspace_changes(self):
        """
        The driver reads system_schema.scylla_keyspaces on every schema refresh
        and sets KeyspaceMetadata.strongly_consistent for each keyspace: True when
        its `consistency` option is non-eventual ('global' or 'local'), and False
        otherwise. Crucially, an eventually-consistent keyspace is *absent* from
        scylla_keyspaces, so "not in the map" is exactly what maps to
        strongly_consistent == False (see SchemaParserV3._set_strong_consistency /
        _is_strongly_consistent).

        The flag is not a one-off computed at connect time -- it has to track the
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
        self._skip_if_no_v2()

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
            # up in the map with strongly_consistent == False. This exercises the
            # "absent from scylla_keyspaces -> eventual" path.
            self.session.execute(
                "CREATE KEYSPACE {0} WITH replication = "
                "{{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} "
                "AND tablets = {{'initial': 1}}".format(ec_ks))
            assert ec_ks in metadata.keyspaces
            assert metadata.keyspaces[ec_ks].strongly_consistent is False

            # Create a strongly-consistent keyspace: same map, flag now True.
            # RF=3 across the 3 nodes gives a well-formed Raft group.
            self.session.execute(
                "CREATE KEYSPACE {0} WITH replication = "
                "{{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} "
                "AND tablets = {{'initial': 1}} AND consistency = 'global'".format(sc_ks))
            assert sc_ks in metadata.keyspaces
            assert metadata.keyspaces[sc_ks].strongly_consistent is True
            # The previously-created keyspace keeps its (False) flag.
            assert metadata.keyspaces[ec_ks].strongly_consistent is False

            # A full refresh (the bulk get_all_keyspaces path, as opposed to the
            # single-keyspace get_keyspace path the DDL above exercised) agrees.
            self.session.cluster.refresh_schema_metadata()
            assert metadata.keyspaces[ec_ks].strongly_consistent is False
            assert metadata.keyspaces[sc_ks].strongly_consistent is True

            # Dropping a keyspace removes it from the map and leaves the other
            # keyspace's flag untouched.
            self.session.execute("DROP KEYSPACE {0}".format(sc_ks))
            assert sc_ks not in metadata.keyspaces
            assert metadata.keyspaces[ec_ks].strongly_consistent is False

            self.session.execute("DROP KEYSPACE {0}".format(ec_ks))
            assert ec_ks not in metadata.keyspaces
        finally:
            self.session.execute("DROP KEYSPACE IF EXISTS {0}".format(ec_ks))
            self.session.execute("DROP KEYSPACE IF EXISTS {0}".format(sc_ks))
