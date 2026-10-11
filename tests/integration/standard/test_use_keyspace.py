import os
import threading
import time
import logging

import unittest

from unittest.mock import patch

from cassandra.connection import Connection
from cassandra.cluster import Cluster
from cassandra.pool import HostConnection
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy, ConstantReconnectionPolicy

from tests.integration import use_cluster, PROTOCOL_VERSION, local

LOGGER = logging.getLogger(__name__)

_saved_scylla_ext_opts = None


def setup_module():
    global _saved_scylla_ext_opts
    _saved_scylla_ext_opts = os.environ.get('SCYLLA_EXT_OPTS')
    os.environ['SCYLLA_EXT_OPTS'] = "--smp 2 --memory 2048M"
    use_cluster('shared_aware', [3], start=True)


def teardown_module():
    if _saved_scylla_ext_opts is None:
        os.environ.pop('SCYLLA_EXT_OPTS', None)
    else:
        os.environ['SCYLLA_EXT_OPTS'] = _saved_scylla_ext_opts


@local
class TestUseKeyspace(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.cluster = Cluster(contact_points=["127.0.0.1"], protocol_version=PROTOCOL_VERSION,
                              load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
                              reconnection_policy=ConstantReconnectionPolicy(1))
        cls.session = cls.cluster.connect()
        LOGGER.info(cls.cluster.is_shard_aware())
        LOGGER.info(cls.cluster.shard_aware_stats())

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()
    
    def test_set_keyspace_slow_connection(self):
        # Test that "USE keyspace" gets propagated
        # to all connections.
        #
        # Reproduces an issue #187 where some pending
        # connections for shards would not 
        # receive "USE keyspace".
        #
        # Simulate that scenario by adding an artifical
        # delay before sending "USE keyspace" on
        # connections.

        original_set_keyspace_blocking = Connection.set_keyspace_blocking

        def patched_set_keyspace_blocking(*args, **kwargs):
            time.sleep(1)
            return original_set_keyspace_blocking(*args, **kwargs)

        with patch.object(Connection, "set_keyspace_blocking", patched_set_keyspace_blocking):
            self.session.execute("CREATE KEYSPACE test_set_keyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
            self.session.execute("CREATE TABLE test_set_keyspace.set_keyspace_slow_connection(pk int, PRIMARY KEY(pk))")

            session2 = self.cluster.connect()
            session2.execute("USE test_set_keyspace")
            for i in range(200):
                session2.execute(f"SELECT * FROM set_keyspace_slow_connection WHERE pk = 1")

    def test_use_during_background_shard_connection_open(self):
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS use_race_old WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS use_race_new WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
        self.session.execute("CREATE TABLE IF NOT EXISTS use_race_new.t (pk int PRIMARY KEY)")

        original_open = HostConnection._open_connection_to_missing_shard
        original_set = Connection.set_keyspace_blocking
        opening = threading.local()
        on_old_keyspace = threading.Event()
        use_done = threading.Event()

        def open_shard(pool, shard_id):
            opening.active = True
            try:
                return original_open(pool, shard_id)
            finally:
                opening.active = False

        def set_keyspace(conn, keyspace):
            if getattr(opening, 'active', False) and keyspace == 'use_race_old':
                on_old_keyspace.set()
                assert use_done.wait(10)
            return original_set(conn, keyspace)

        cluster = Cluster(contact_points=['127.0.0.1'], protocol_version=PROTOCOL_VERSION)
        try:
            with patch.object(HostConnection, '_open_connection_to_missing_shard', open_shard), \
                    patch.object(Connection, 'set_keyspace_blocking', set_keyspace):
                session = cluster.connect('use_race_old')
                assert on_old_keyspace.wait(10)
                session.execute('USE use_race_new')
                use_done.set()
                with session._lock:
                    pools = tuple(session.get_pools())
                for pool in pools:
                    for future in pool._shard_connections_futures:
                        future.result(timeout=30)

            with session._lock:
                pools = tuple(session.get_pools())
            assert all(conn.keyspace == 'use_race_new'
                       for pool in pools
                       for conn in pool.get_connections())
            session.execute('SELECT * FROM t WHERE pk = 1')
        finally:
            use_done.set()
            cluster.shutdown()
