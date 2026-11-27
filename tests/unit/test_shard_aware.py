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
import subprocess
import sys
import textwrap
import threading
import time
import uuid
from unittest.mock import MagicMock, patch
from concurrent.futures import Future, ThreadPoolExecutor
from threading import RLock

import pytest

from cassandra._lifecycle import PoolOwnershipReservation
from cassandra.cluster import ShardAwareOptions
from cassandra.pool import HostConnection, HostDistance
from cassandra.client_routes import ClientRouteProxy, ClientRoutesConfig, _ClientRoutesHandler, _Route
from cassandra.connection import (ShardingInfo, DefaultEndPoint, ClientRoutesEndPoint,
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
WORKER_PREFIX = "shard-aware-test"


class _RearmOnFailure:
    """pytest's faulthandler plugin cancels the watchdog on every reported
    failure, including a failed subtest that the test method runs past.
    Re-arm it right after, so the rest of the test stays guarded."""

    def __init__(self, stderr):
        self.stderr = stderr

    @pytest.hookimpl(trylast=True)
    def pytest_exception_interact(self):
        faulthandler.dump_traceback_later(
            HANG_TIMEOUT, exit=True, file=self.stderr)


@pytest.fixture(autouse=True)
def hang_guard(capfd, pytestconfig):
    # Dup the real stderr while capture is off; os._exit discards captured
    # output, which would hide the stack dump.
    with capfd.disabled():
        stderr = os.dup(2)
    rearm = _RearmOnFailure(stderr)
    pytestconfig.pluginmanager.register(rearm)
    faulthandler.dump_traceback_later(HANG_TIMEOUT, exit=True, file=stderr)
    yield
    # A worker stuck past its future timeout keeps running after the test and
    # blocks interpreter exit, so wait for every worker under the watchdog:
    # a slow one finishes, a stuck one gets killed.
    for t in threading.enumerate():
        if t.name.startswith(WORKER_PREFIX):
            t.join()
    faulthandler.cancel_dump_traceback_later()
    pytestconfig.pluginmanager.unregister(rearm)
    os.close(stderr)


# PyPy on Windows ships a no-op faulthandler stub without dump_traceback.
# It cannot terminate the deliberately hung subprocesses in these self-tests.
@pytest.mark.skipif(not hasattr(faulthandler, "dump_traceback"),
                    reason="Native faulthandler watchdog is unavailable")
@pytest.mark.parametrize("body", [
    # A worker outlives its future timeout and never finishes.
    """
    def test_stuck_worker():
        executor = ThreadPoolExecutor(thread_name_prefix=guarded.WORKER_PREFIX)
        executor.submit(threading.Event().wait).result(timeout=1)
    """,
    # A failed subtest, then the test method itself hangs.
    """
    class T(unittest.TestCase):
        def test_hang_after_failed_subtest(self):
            with self.subTest(i=0):
                assert False
            with self.subTest(i=1):
                threading.Event().wait()
    """,
], ids=["stuck_worker", "hang_after_failed_subtest"])
def test_hang_guard_kills_hung_test(tmp_path, body):
    # Run the hanging case in a child pytest: the guard's job is to kill it.
    child = tmp_path / "test_hang.py"
    child.write_text(textwrap.dedent("""
        import importlib.util, threading, unittest
        from concurrent.futures import ThreadPoolExecutor

        spec = importlib.util.spec_from_file_location("guarded", %r)
        guarded = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(guarded)
        guarded.HANG_TIMEOUT = 2
        hang_guard = guarded.hang_guard
    """ % __file__) + textwrap.dedent(body))
    # Raises TimeoutExpired, failing this test, if the guard lets it hang.
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", str(child)],
        cwd=tmp_path, capture_output=True, text=True, timeout=30,
        # cwd=tmp_path drops the checkout from the child's path; hand it ours.
        env=dict(os.environ, PYTHONPATH=os.pathsep.join(sys.path)))
    assert result.returncode == 1
    assert "Timeout (0:00:02)!" in result.stderr


class MockSession(MagicMock):
    is_shutdown = False
    keyspace = "ks1"

    def __init__(self, ssl_options=None, ssl_context=None, sharding_info=None,
                 *args, **kwargs):
        super(MockSession, self).__init__(*args, **kwargs)
        self._lock = RLock()
        self._pools = {}
        self.cluster = MagicMock(is_shutdown=False)
        self.cluster.ssl_options = ssl_options
        self.cluster.ssl_context = ssl_context
        self.cluster.shard_aware_options = ShardAwareOptions()
        self.cluster.executor = ThreadPoolExecutor(
            max_workers=2, thread_name_prefix=WORKER_PREFIX)
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
    def make_pool(self, host, session):
        host.lock = RLock()
        host._is_removed = False
        ownership = PoolOwnershipReservation(session, host, host.endpoint)
        pool = HostConnection(host, HostDistance.REMOTE, session,
                              _ownership=ownership)
        with pool._membership_lock, session._lock, host.lock:
            session._pools[host] = pool
            ownership.published = True
        return pool

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
        pool = self.make_pool(host, session)
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
            pool = self.make_pool(host, session)
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
            return Future()

        session.submit = submit
        connection_factory = MagicMock(
            side_effect=session.mock_connection_factory)
        session.cluster.connection_factory = connection_factory

        pool = self.make_pool(host, session)
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
                pool = self.make_pool(host, session)
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

        pool = self.make_pool(host, session)
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


class TestClientRoutesShardAware(unittest.TestCase):
    """
    Advanced shard awareness through client routes (PrivateLink): shard-aware
    connections choose their shard through the source port but are dialed at
    the node's route, never at the advertised shard-aware port.
    """

    ROUTE_ADDRESS = "10.9.9.9"
    ROUTE_PORT = 41000
    ROUTE_TLS_PORT = 41001

    def _host(self, ssl_enabled=False, routed=True):
        connection_id = str(uuid.uuid4())
        host_id = uuid.uuid4()
        handler = _ClientRoutesHandler(
            ClientRoutesConfig([ClientRouteProxy(connection_id)], advanced_shard_awareness=True),
            ssl_enabled=ssl_enabled)
        port = self.ROUTE_TLS_PORT if ssl_enabled else self.ROUTE_PORT
        if routed:
            handler._routes.update([_Route(connection_id=connection_id, host_id=host_id,
                                           address=self.ROUTE_ADDRESS, port=port)])
        host = MagicMock()
        host.endpoint = ClientRoutesEndPoint(host_id=host_id, handler=handler,
                                             original_address="1.2.3.4", original_port=9042)
        return host

    def _pool(self, host, session, pool_class=HostConnection):
        """Publish a pool for ``host`` as TestShardAware.make_pool does."""
        self.addCleanup(session.cluster.executor.shutdown,
                        wait=False, cancel_futures=True)
        host.lock = RLock()
        host._is_removed = False
        ownership = PoolOwnershipReservation(session, host, host.endpoint)
        pool = pool_class(host, HostDistance.REMOTE, session, _ownership=ownership)
        with pool._membership_lock, session._lock, host.lock:
            session._pools[host] = pool
            ownership.published = True
        for f in session.futures:
            f.result(timeout=FUTURE_TIMEOUT)
        return pool

    def test_shard_aware_connections_dial_the_route(self):
        host = self._host()
        session = MockSession()
        requests = []
        factory = session.cluster.connection_factory

        def recording_factory(*args, **kwargs):
            requests.append((args[0], kwargs.get('shard_id')))
            return factory(*args, **kwargs)
        session.cluster.connection_factory = recording_factory

        pool = self._pool(host, session)

        assert len(pool._connections) == 4
        for shard_id, connection in pool._connections.items():
            assert connection.features.shard_id == shard_id
            assert connection.endpoint is host.endpoint
        # Every connection after the first one asked for its shard.
        assert sorted(shard_id for _, shard_id in requests[1:]) == [1, 2, 3]
        # The shard-aware endpoint dials the route, not the advertised 19042.
        endpoint = pool._get_shard_aware_endpoint()
        assert endpoint is host.endpoint
        assert endpoint.resolve() == (self.ROUTE_ADDRESS, self.ROUTE_PORT)

    def test_tls_requires_advertised_ssl_shard_aware_port(self):
        host = self._host(ssl_enabled=True)
        without_ssl_port = ShardingInfo(
            shard_id=1, shards_count=4, partitioner="", sharding_algorithm="",
            sharding_ignore_msb=0, shard_aware_port=19042, shard_aware_port_ssl=None)
        pool = self._pool(host, MockSession(ssl_options={}, sharding_info=without_ssl_port))
        assert pool._get_shard_aware_endpoint() is None

        host = self._host(ssl_enabled=True)
        pool = self._pool(host, MockSession(ssl_options={}))
        endpoint = pool._get_shard_aware_endpoint()
        assert endpoint is host.endpoint
        assert endpoint.resolve() == (self.ROUTE_ADDRESS, self.ROUTE_TLS_PORT)

    def test_disabled_shard_aware_port_returns_no_endpoint(self):
        session = MockSession()
        session.cluster.shard_aware_options.disable_shardaware_port = True
        pool = self._pool(self._host(), session)
        assert pool._get_shard_aware_endpoint() is None

    def test_mismatch_triggers_fallback(self):
        host = self._host()
        session = MockSession()
        pool = self._pool(host, session)

        def landing_on_shard_2(*args, **kwargs):
            connection = MockSession.mock_connection_factory(session, *args, **kwargs)
            connection.features = ProtocolFeatures(shard_id=2, sharding_info=connection.features.sharding_info)
            return connection
        session.cluster.connection_factory = landing_on_shard_2

        with patch.object(pool, '_on_unexpected_shard') as on_unexpected_shard:
            pool._open_connection_to_missing_shard(3)
        on_unexpected_shard.assert_called_once_with(3, 2)

    def test_mismatch_warns_once_then_falls_back(self):
        pool = self._pool(self._host(), MockSession())
        logger = 'cassandra.pool'

        with self.assertLogs(logger, level='DEBUG') as logs:
            pool._on_unexpected_shard(3, 2)
        assert [r.levelno for r in logs.records] == [logging.WARNING]
        assert "PROXY protocol v2" in logs.output[0]
        assert "advanced_shard_awareness=False" in logs.output[0]
        assert pool._get_shard_aware_endpoint() is None

        # The fallback ends with the cool-down window, and a repeated mismatch
        # is no longer worth a warning.
        pool.advanced_shardaware_block_until = time.time() - 1
        assert pool._get_shard_aware_endpoint() is not None
        with self.assertLogs(logger, level='DEBUG') as logs:
            pool._on_unexpected_shard(1, 0)
        assert [r.levelno for r in logs.records] == [logging.DEBUG]
        assert pool._get_shard_aware_endpoint() is None

    def test_concurrent_mismatches_warn_once(self):
        """
        Connections to missing shards open concurrently, so several can
        report a mismatch at once; only one of them warns.

        Reading the warned flag pauses, so without the pool lock every thread
        would read it before any of them sets it, and all would warn.
        """
        class SlowFlagHostConnection(HostConnection):
            @property
            def _client_routes_shard_mismatch_warned(self):
                value = self.__dict__.get('_warned', False)
                time.sleep(0.05)
                return value

            @_client_routes_shard_mismatch_warned.setter
            def _client_routes_shard_mismatch_warned(self, value):
                self.__dict__['_warned'] = value

        pool = self._pool(self._host(), MockSession(), pool_class=SlowFlagHostConnection)
        threads_count = 8
        barrier = threading.Barrier(threads_count)

        def report_mismatch(shard_id):
            barrier.wait()
            pool._on_unexpected_shard(shard_id, (shard_id + 1) % 4)

        with self.assertLogs('cassandra.pool', level='DEBUG') as logs:
            threads = [threading.Thread(target=report_mismatch, args=(i % 4,))
                       for i in range(threads_count)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(5)
        assert not any(t.is_alive() for t in threads)
        levels = [r.levelno for r in logs.records if "[client routes]" in r.getMessage()]
        assert len(levels) == threads_count
        assert levels.count(logging.WARNING) == 1

    def test_unrouted_host_dials_advertised_shard_aware_port(self):
        """
        A client routes host without a route is reached at its own address,
        so its shard-aware connections use the advertised shard-aware port.
        """
        for ssl_options, port in [(None, 19042), ({}, 19045)]:
            with self.subTest(ssl=ssl_options is not None):
                host = self._host(ssl_enabled=ssl_options is not None, routed=False)
                pool = self._pool(host, MockSession(ssl_options=ssl_options))
                endpoint = pool._get_shard_aware_endpoint()
                assert endpoint is not host.endpoint
                assert endpoint.resolve() == ("1.2.3.4", port)
                assert host.endpoint.resolve() == ("1.2.3.4", 9042)
                assert endpoint.tls_session_cache_key == host.endpoint.tls_session_cache_key

    def test_mismatch_on_unrouted_host_keeps_nat_warning(self):
        """No route means no route endpoint to blame for a shard mismatch."""
        pool = self._pool(self._host(routed=False), MockSession())

        with self.assertLogs('cassandra.pool', level='WARNING') as logs:
            pool._on_unexpected_shard(3, 2)
        assert "behind NAT" in logs.output[0]
        assert not any("[client routes]" in line for line in logs.output)
        assert pool._get_shard_aware_endpoint() is None

    def test_mismatch_without_client_routes_keeps_nat_warning(self):
        host = MagicMock()
        host.endpoint = DefaultEndPoint("1.2.3.4")
        pool = self._pool(host, MockSession())

        for _ in range(2):
            with self.assertLogs('cassandra.pool', level='WARNING') as logs:
                pool._on_unexpected_shard(3, 2)
            assert "behind NAT" in logs.output[0]
        assert pool._get_shard_aware_endpoint() is None
