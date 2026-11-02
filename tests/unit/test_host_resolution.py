# Copyright ScyllaDB, Inc.
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

from collections import deque
from concurrent.futures import Future
from threading import RLock
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from cassandra.cluster import (Cluster, ControlConnectionQueryFallback,
                               EXEC_PROFILE_DEFAULT, ExecutionProfile,
                               NoHostAvailable, Session)
from cassandra import concurrent
from cassandra.connection import Connection
from cassandra.policies import RoundRobinPolicy, SimpleConvictionPolicy
from cassandra.pool import Host
from cassandra.protocol import ResultMessage, RESULT_KIND_ROWS
from cassandra.query import BatchStatement, PreparedStatement, SimpleStatement


def new_host(address='127.0.0.1', host_id=None):
    host = Host(address, SimpleConvictionPolicy, host_id=host_id or uuid4())
    host.set_up()
    return host


def owned_pool(host, paging_states=(None,)):
    connection = Mock(spec=Connection, lock=RLock())
    connection._requests = {}
    connection.orphaned_request_ids = set()
    connection.request_ids = deque()
    connection.is_defunct = False
    pool = Mock(host=host, endpoint=host.endpoint, is_shutdown=False)
    pool.borrow_connection.return_value = (connection, 1)
    pages = iter(paging_states)

    def send_msg(message, request_id, cb, **kwargs):
        response = ResultMessage(kind=RESULT_KIND_ROWS)
        response.paging_state = next(pages)
        response.column_names = ['host_id']
        response.column_types = []
        response.parsed_rows = [(host.host_id,)]
        cb(response)
        return 1

    connection.send_msg.side_effect = send_msg
    return pool, connection


@pytest.fixture
def routed_session(request):
    host = new_host()
    kwargs = {'load_balancing_policy': RoundRobinPolicy()} if getattr(
        request, 'param', False) else {}
    with Cluster(protocol_version=4, **kwargs) as cluster:
        cluster.metadata.add_or_return_host(host)
        cluster._populate_hosts()
        ready = Future()
        ready.set_result(True)
        with patch.object(Session, 'add_or_renew_pool', return_value=ready):
            session = Session(cluster, [host])
        cluster.sessions.add(session)
        pool, connection = owned_pool(host)
        session._pools[host] = pool
        with patch.object(cluster.connection_class, 'create_timer'):
            yield session, host, pool, connection


def execute(session, query, host, asynchronous):
    if asynchronous:
        return session.execute_async(query, host=host).result()
    return session.execute(query, host=host)


@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('changed_endpoint', [False, True])
def test_explicit_host_resolves_by_id(routed_session, asynchronous,
                                     changed_endpoint):
    session, current, pool, connection = routed_session
    cached = new_host('127.0.0.2' if changed_endpoint else current.address,
                      current.host_id)
    cached.set_down()
    cached._is_removed = True

    result = execute(session, 'SELECT host_id FROM system.local', cached,
                     asynchronous)

    assert result.one().host_id == current.host_id
    assert result.response_future.coordinator_host is current
    assert result.response_future.attempted_hosts == [current]
    connection.send_msg.assert_called_once()


@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('failure', ['unknown', 'missing_pool', 'shutdown_pool',
                                    'obsolete_pool'])
def test_explicit_host_failure_does_not_select_another_node(
        routed_session, asynchronous, failure):
    session, current, pool, connection = routed_session
    # Reusing an address with a different ID must not match the old node.
    cached = new_host(current.address,
                      uuid4() if failure == 'unknown' else current.host_id)
    other = new_host('127.0.0.3')
    other_pool, other_connection = owned_pool(other)
    session.cluster.metadata.add_or_return_host(other)
    session._pools[other] = other_pool
    if failure == 'missing_pool':
        session._pools.pop(current)
    elif failure == 'shutdown_pool':
        pool.is_shutdown = True
    elif failure == 'obsolete_pool':
        pool.host = cached
    session.cluster.allow_control_connection_query_fallback = (
        ControlConnectionQueryFallback.Fallback)

    with pytest.raises(NoHostAvailable) as exc:
        execute(session, 'SELECT host_id FROM system.local', cached,
                asynchronous)

    assert list(exc.value.errors) == [cached]
    if failure == 'unknown':
        assert 'Host ID' in str(exc.value.errors[cached])
    connection.send_msg.assert_not_called()
    other_connection.send_msg.assert_not_called()


@pytest.mark.parametrize('statement_kind', ['simple', 'prepared', 'bound', 'batch'])
def test_statement_types_share_host_resolution(routed_session, statement_kind):
    session, host, _, _ = routed_session
    cached = new_host('127.0.0.2', host.host_id)
    prepared = PreparedStatement([], b'query-id', [], 'SELECT now() FROM system.local',
                                 None, 4, [], None)
    statements = {
        'simple': SimpleStatement('SELECT now() FROM system.local'),
        'prepared': prepared,
        'bound': prepared.bind(()),
        'batch': BatchStatement().add('INSERT INTO ks.t (k) VALUES (1)'),
    }

    assert session.execute(statements[statement_kind], host=cached).one().host_id == host.host_id


class CachedHostPolicy(RoundRobinPolicy):
    def __init__(self, hosts):
        super().__init__()
        self.hosts = hosts

    def make_query_plan(self, working_keyspace=None, query=None):
        return iter(self.hosts)


@pytest.mark.parametrize('source', ['default_profile', 'named_profile', 'listener'])
def test_extension_query_plans_resolve_cached_hosts(routed_session, source):
    session, host, _, _ = routed_session
    cached = new_host('127.0.0.2', host.host_id)
    policy = CachedHostPolicy([cached])
    kwargs = {}
    if source == 'listener':
        session.add_request_init_listener(
            lambda future: setattr(future, 'query_plan', iter([cached])))
    else:
        name = EXEC_PROFILE_DEFAULT if source == 'default_profile' else 'cached'
        session.cluster.profile_manager.profiles[name] = ExecutionProfile(
            load_balancing_policy=policy)
        kwargs['execution_profile'] = name

    result = session.execute_async('SELECT host_id FROM system.local', **kwargs).result()

    assert result.response_future.coordinator_host is host


def test_policy_skips_unknown_host_and_retains_failover(routed_session):
    session, current, _, _ = routed_session
    unknown = new_host(current.address)
    cached = new_host('127.0.0.2', current.host_id)
    session.cluster.profile_manager.default.load_balancing_policy = (
        CachedHostPolicy([unknown, cached]))

    result = session.execute('SELECT host_id FROM system.local')

    assert result.response_future.coordinator_host is current
    assert unknown in result.response_future._errors


@pytest.mark.parametrize('routed_session', [True], indirect=True)
def test_legacy_policy_resolves_cached_hosts(routed_session):
    session, host, _, _ = routed_session
    cached = new_host('127.0.0.2', host.host_id)
    session.cluster.load_balancing_policy = CachedHostPolicy([cached])

    result = session.execute('SELECT host_id FROM system.local')

    assert result.response_future.coordinator_host is host


def test_resolution_does_not_bypass_replacement_during_borrow(routed_session):
    session, host, pool, connection = routed_session
    cached = new_host('127.0.0.2', host.host_id)
    replacement = new_host('127.0.0.3', host.host_id)
    new_pool, new_connection = owned_pool(replacement)

    def replace_during_borrow(**kwargs):
        session.cluster.metadata.replace_host_endpoint(host, replacement)
        session._pools[replacement] = new_pool
        return connection, 1

    pool.borrow_connection.side_effect = replace_during_borrow
    with pytest.raises(NoHostAvailable):
        session.execute('SELECT host_id FROM system.local', host=cached)

    pool.return_connection.assert_called_once_with(connection)
    assert list(connection.request_ids) == [1]
    connection.send_msg.assert_not_called()
    new_connection.send_msg.assert_not_called()

    # A subsequent public request can select the new generation.
    result = session.execute('SELECT host_id FROM system.local', host=cached)
    assert result.response_future.coordinator_host is replacement


@pytest.mark.parametrize('api', ['future', 'result_set', 'iterator'])
@pytest.mark.parametrize('explicit', [False, True])
def test_paging_resolves_host_replaced_between_pages(routed_session, api,
                                                    explicit):
    session, host, _, _ = routed_session
    old_pool, old_connection = owned_pool(host, (b'page-2',))
    session._pools[host] = old_pool
    if not explicit:
        session.cluster.profile_manager.default.load_balancing_policy = (
            CachedHostPolicy([host]))
    result = session.execute('SELECT host_id FROM system.local',
                             host=host if explicit else None)
    replacement = new_host('127.0.0.2', host.host_id)
    session.cluster.metadata.replace_host_endpoint(host, replacement)
    new_pool, new_connection = owned_pool(replacement)
    session._pools[replacement] = new_pool

    if api == 'future':
        result.response_future.start_fetching_next_page()
        assert result.response_future.result().one().host_id == host.host_id
    elif api == 'result_set':
        result.fetch_next_page()
        assert result.one().host_id == host.host_id
    else:
        assert len(list(result)) == 2

    assert result.response_future.coordinator_host is replacement
    old_connection.send_msg.assert_called_once()
    new_connection.send_msg.assert_called_once()


@pytest.mark.parametrize('session_method', [False, True])
@pytest.mark.parametrize('variant', ['execute_concurrent',
                                    'execute_concurrent_with_args',
                                    'execute_concurrent_async'])
def test_concurrent_wrappers_share_policy_resolution(routed_session,
                                                    session_method, variant):
    session, host, _, _ = routed_session
    cached = new_host('127.0.0.2', host.host_id)
    session.cluster.profile_manager.default.load_balancing_policy = (
        CachedHostPolicy([cached]))
    query = SimpleStatement('SELECT host_id FROM system.local')
    args = (query, [None]) if variant.endswith('_with_args') else ([(query, None)],)
    if session_method:
        results = getattr(session, variant)(*args)
    else:
        results = getattr(concurrent, variant)(session, *args)
    if variant.endswith('_async'):
        results = results.result()

    success, result = results[0]
    assert success
    assert result.response_future.coordinator_host is host
