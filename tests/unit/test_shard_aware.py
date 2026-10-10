# Copyright 2020 ScyllaDB, Inc.
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

import faulthandler
import logging
import os
import time
from unittest.mock import MagicMock
from concurrent.futures import ThreadPoolExecutor

import pytest

from cassandra.cluster import ShardAwareOptions
from cassandra.pool import HostConnection, HostDistance
from cassandra.connection import (ShardingInfo, DefaultEndPoint,
                                  UnixSocketEndPoint)
from cassandra.metadata import Murmur3Token
from cassandra.protocol_features import ProtocolFeatures
from cassandra.shard_info import _ShardingInfo

try:
    import cassandra.c_shard_info as c_shard_info
    from cassandra.c_shard_info import ShardingInfo as CShardingInfo
except ModuleNotFoundError as exc:
    # Only tolerate the extension simply not being built. A missing transitive
    # dependency or a failed module init must not silently skip the parity test.
    if exc.name != "cassandra.c_shard_info":
        raise
    c_shard_info = None
    CShardingInfo = None

LOGGER = logging.getLogger(__name__)

# Bound every wait on pool work, so a pool bug that spins fails the test
# instead of hanging it.
FUTURE_TIMEOUT = 10
# Last resort for hangs no future timeout can catch: pool work run inline by
# the test, or a worker thread that keeps spinning after its future timed out.
# Dumps every thread's stack and kills the process.
HANG_TIMEOUT = 60


@pytest.fixture(autouse=True)
def hang_guard(capfd):
    # Dup the real stderr while capture is off; os._exit discards captured
    # output, which would hide the stack dump.
    with capfd.disabled():
        stderr = os.dup(2)
    faulthandler.dump_traceback_later(HANG_TIMEOUT, exit=True, file=stderr)
    yield
    faulthandler.cancel_dump_traceback_later()
    os.close(stderr)


class MockSession(MagicMock):
    is_shutdown = False
    keyspace = "ks1"

    def __init__(self, ssl_options=None, ssl_context=None, sharding_info=None,
                 *args, **kwargs):
        super(MockSession, self).__init__(*args, **kwargs)
        self.cluster = MagicMock()
        self.cluster.ssl_options = ssl_options
        self.cluster.ssl_context = ssl_context
        self.cluster.shard_aware_options = ShardAwareOptions()
        self.cluster.executor = ThreadPoolExecutor(max_workers=2)
        self.cluster.signal_connection_failure = lambda *args, **kwargs: False
        self.cluster.connection_factory = self.mock_connection_factory
        self.connection_counter = 0
        self.futures = []
        self.sharding_info = sharding_info

    def submit(self, fn, *args, **kwargs):
        logging.info("Scheduling %s with args: %s, kwargs: %s", fn, args, kwargs)
        if not self.is_shutdown:
            f = self.cluster.executor.submit(fn, *args, **kwargs)
            self.futures += [f]
            return f

    def mock_connection_factory(self, *args, **kwargs):
        connection = MagicMock()
        # Track keyspace like a real connection does; an auto-created mock
        # attribute never equals the session keyspace, so pool code that
        # re-checks it would loop forever.
        connection.keyspace = None
        connection.set_keyspace_blocking.side_effect = (
            lambda ks: setattr(connection, "keyspace", ks))
        connection.is_shutdown = False
        connection.is_defunct = False
        connection.is_closed = False
        connection.orphaned_threshold_reached = False
        connection.endpoint = args[0]
        sharding_info = self.sharding_info or ShardingInfo(
            shard_id=1, shards_count=4, partitioner="",
            sharding_algorithm="", sharding_ignore_msb=0,
            shard_aware_port=19042, shard_aware_port_ssl=19045)
        connection.features = ProtocolFeatures(
            shard_id=kwargs.get('shard_id', self.connection_counter),
            sharding_info=sharding_info)
        self.connection_counter += 1

        return connection


class TestShardAware(unittest.TestCase):
    def _session(self, **kwargs):
        session = MockSession(**kwargs)
        # Runs even when the test fails, and does not block on a spinning worker.
        self.addCleanup(session.cluster.executor.shutdown,
                        wait=False, cancel_futures=True)
        return session

    @unittest.skipUnless(CShardingInfo, "Cython sharding extension is not available")
    def test_cython_sharding_info_matches_python(self):
        """
        Testing that the compiled extension computes the same shard id as the
        pure-Python fallback, for both of its multiply-high implementations.

        The extension uses native 128-bit arithmetic where the compiler has it
        and a portable 64-bit decomposition otherwise (MSVC). Only one of the
        two is wired up on any given platform, so both are exercised directly
        here to keep the MSVC path covered on every platform.
        """
        for shards_count in (1, 2, 4, 12, 128, 1024):
            for sharding_ignore_msb in (0, 1, 12, 63):
                args = (1, shards_count, "", "", sharding_ignore_msb, 0, 0)
                cython_info = CShardingInfo(*args)
                python_info = _ShardingInfo(*args)
                for token in (
                        -9223372036854775808, -1, 0, 1,
                        9223372036854775807):
                    with self.subTest(shards_count=shards_count,
                                      sharding_ignore_msb=sharding_ignore_msb,
                                      token=token):
                        expected = python_info.shard_id_from_token(token)
                        self.assertEqual(
                            cython_info.shard_id_from_token(token), expected)
                        for portable in (True, False):
                            self.assertEqual(
                                c_shard_info._shard_id_from_token_impl(
                                    token, shards_count, sharding_ignore_msb,
                                    portable),
                                expected)

    def test_parsing_and_calculating_shard_id(self):
        """
        Testing the parsing of the options command
        and the calculation getting a shard id from a Murmur3 token
        """
        class OptionsHolder(object):
            options = {
                'SCYLLA_SHARD': ['1'], 
                'SCYLLA_NR_SHARDS': ['12'],
                'SCYLLA_PARTITIONER': ['org.apache.cassandra.dht.Murmur3Partitioner'],
                'SCYLLA_SHARDING_ALGORITHM': ['biased-token-round-robin'],
                'SCYLLA_SHARDING_IGNORE_MSB': ['12']
            }
        shard_id, shard_info = ProtocolFeatures.parse_sharding_info(OptionsHolder().options)

        assert shard_id == 1
        assert shard_info.shard_id_from_token(Murmur3Token.from_key(b"a").value) == 4
        assert shard_info.shard_id_from_token(Murmur3Token.from_key(b"b").value) == 6
        assert shard_info.shard_id_from_token(Murmur3Token.from_key(b"c").value) == 6
        assert shard_info.shard_id_from_token(Murmur3Token.from_key(b"e").value) == 4
        assert shard_info.shard_id_from_token(Murmur3Token.from_key(b"100000").value) == 2

    def test_shard_aware_endpoint_carries_the_nodes_tls_identity(self):
        """
        The alternate listener must resume from the session cached for the node,
        not key on its own port.
        """
        host = MagicMock()
        host.endpoint = DefaultEndPoint("1.2.3.4")
        session = self._session(ssl_context=object())
        pool = HostConnection(host=host, host_distance=HostDistance.REMOTE,
                              session=session)
        self.addCleanup(pool.shutdown)
        for f in session.futures:
            f.result(timeout=FUTURE_TIMEOUT)
        shard_aware_endpoint = pool._get_shard_aware_endpoint()
        assert shard_aware_endpoint.port == 19045
        assert (shard_aware_endpoint.tls_session_cache_key ==
                host.endpoint.tls_session_cache_key)

    def test_advanced_shard_aware_port(self):
        """
        Test that on given a `shard_aware_port` on the OPTIONS message (ShardInfo class)
        the next connections would be open using this port
        """
        host = MagicMock()
        host.endpoint = DefaultEndPoint("1.2.3.4")

        for port, ssl_options, ssl_context in [
                (19042, None, None),
                (19045, {'some_ssl_options': True}, None),
                (19045, {}, None),
                (19045, None, object())]:
            session = self._session(ssl_options=ssl_options, ssl_context=ssl_context)
            pool = HostConnection(host=host, host_distance=HostDistance.REMOTE, session=session)
            for f in session.futures:
                f.result(timeout=FUTURE_TIMEOUT)
            assert len(pool._connections) == 4
            for shard_id, connection in pool._connections.items():
                assert connection.features.shard_id == shard_id
                if shard_id == 0:
                    assert connection.endpoint == DefaultEndPoint("1.2.3.4")
                else:
                    assert connection.endpoint == DefaultEndPoint("1.2.3.4", port=port)

    def test_unix_socket_bypasses_advanced_shard_aware_port(self):
        endpoint = UnixSocketEndPoint('/tmp/maintenance.sock')
        host = MagicMock()
        host.endpoint = endpoint
        session = self._session()
        pending = []

        def submit(fn, *args, **kwargs):
            pending.append((fn, args, kwargs))

        session.submit = submit
        connection_factory = MagicMock(
            side_effect=session.mock_connection_factory)
        session.cluster.connection_factory = connection_factory

        pool = HostConnection(
            host=host, host_distance=HostDistance.REMOTE,
            session=session)
        while pending:
            fn, args, kwargs = pending.pop(0)
            fn(*args, **kwargs)

        assert pool._get_shard_aware_endpoint() is None
        assert set(pool._connections) == {0, 1, 2, 3}
        assert connection_factory.call_count == 4
        for factory_call in connection_factory.call_args_list:
            args, kwargs = factory_call
            assert args[0] is endpoint
            assert 'shard_id' not in kwargs
            assert 'total_shards' not in kwargs

    def test_ssl_advanced_shard_aware_port_requires_ssl_port(self):
        """
        Test that SSL connections do not fall back to the plaintext
        shard-aware port when the SSL shard-aware port is unavailable.
        """
        host = MagicMock()
        host.endpoint = DefaultEndPoint("1.2.3.4")
        sharding_info = ShardingInfo(
            shard_id=1, shards_count=4, partitioner="", sharding_algorithm="",
            sharding_ignore_msb=0, shard_aware_port=19042,
            shard_aware_port_ssl=None)
        for label, ssl_options, ssl_context in [
                ('ssl_options', {'some_ssl_options': True}, None),
                ('empty_ssl_options', {}, None),
                ('ssl_context', None, object())]:
            with self.subTest(label=label):
                session = self._session(
                    ssl_options=ssl_options,
                    ssl_context=ssl_context,
                    sharding_info=sharding_info)
                pool = HostConnection(host=host, host_distance=HostDistance.REMOTE, session=session)
                for f in session.futures:
                    f.result(timeout=FUTURE_TIMEOUT)

                assert pool._get_shard_aware_endpoint() is None

    def test_advanced_shard_aware_cooldown(self):
        """
        `disable_advanced_shard_aware` must suppress the shard-aware endpoint for
        the duration of the cool-down window, then automatically restore it once
        the deadline has passed. The hard-disable flag must suppress the endpoint
        unconditionally.
        """
        host = MagicMock()
        host.endpoint = DefaultEndPoint("1.2.3.4")
        session = self._session()

        pool = HostConnection(host=host, host_distance=HostDistance.REMOTE, session=session)
        for f in session.futures:
            f.result(timeout=FUTURE_TIMEOUT)

        # Baseline: shard-aware port is returned.
        endpoint = pool._get_shard_aware_endpoint()
        assert endpoint is not None
        assert endpoint.port == 19042

        # During the cool-down window `_get_shard_aware_endpoint` must return None.
        pool.disable_advanced_shard_aware(600)
        assert pool._get_shard_aware_endpoint() is None

        # Once the deadline has passed, the shard-aware port must be used again.
        pool.advanced_shardaware_block_until = time.time() - 1
        endpoint = pool._get_shard_aware_endpoint()
        assert endpoint is not None
        assert endpoint.port == 19042

        # The hard-disable flag must suppress the endpoint regardless of the timer.
        session.cluster.shard_aware_options.disable_shardaware_port = True
        assert pool._get_shard_aware_endpoint() is None
