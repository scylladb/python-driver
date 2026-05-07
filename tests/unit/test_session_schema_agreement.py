from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock
import uuid

import pytest

import cassandra.cluster as cluster_module
from cassandra.connection import ConnectionBusy
from cassandra.cluster import ControlConnection, Session, ResultSet
from cassandra.policies import HostDistance, SimpleConvictionPolicy
from cassandra.pool import Host
from cassandra.util import maybe_add_timeout_to_query


class FakeTime:
    def __init__(self):
        self.clock = 0

    def time(self):
        return self.clock

    def sleep(self, amount):
        self.clock += amount


class MockPool:
    def __init__(self, host):
        self.host = host
        self.is_shutdown = False


class MockSchemaVersionFuture:
    def __init__(self, outcome, auto_complete=True):
        self._outcome = outcome
        self._auto_complete = auto_complete
        self._delivered = False
        self._callback_state = None
        self._col_names = ("schema_version",)
        self._col_types = None
        self.has_more_pages = False
        self._continuous_paging_session = None

    def _deliver(self):
        if self._delivered or self._callback_state is None:
            return

        self._delivered = True
        callback, errback, callback_args, callback_kwargs, errback_args, errback_kwargs = self._callback_state
        if isinstance(self._outcome, Exception):
            errback(self._outcome, *errback_args, **errback_kwargs)
        else:
            row = SimpleNamespace(schema_version=self._outcome)
            callback([row], *callback_args, **callback_kwargs)

    def add_callbacks(self, callback, errback,
                      callback_args=(), callback_kwargs=None,
                      errback_args=(), errback_kwargs=None):
        self._callback_state = (
            callback,
            errback,
            callback_args,
            callback_kwargs or {},
            errback_args,
            errback_kwargs or {},
        )
        if self._auto_complete:
            self._deliver()
        return self

    def complete(self):
        self._deliver()

    def result(self):
        if isinstance(self._outcome, Exception):
            raise self._outcome
        return ResultSet(self, [SimpleNamespace(schema_version=self._outcome)])


def _host_query_count(session, target_host):
    return sum(1 for call in session.execute_async.call_args_list if call.kwargs.get("host") is target_host)


def _new_session(schema_versions, distances=None, metadata_request_timeout=timedelta(seconds=2), timeout=2.0):
    hosts = []
    connections = {}
    distance_map = {}

    if distances is None:
        distances = [HostDistance.LOCAL] * len(schema_versions)

    for index, schema_version in enumerate(schema_versions):
        host = Host("127.0.0.%d" % (index + 1), SimpleConvictionPolicy, host_id=uuid.uuid4())
        host.set_up()
        hosts.append(host)
        distance_map[host] = distances[index]

    cluster = SimpleNamespace(
        max_schema_agreement_wait=10,
        control_connection=SimpleNamespace(
            _timeout=timeout,
            _metadata_request_timeout=metadata_request_timeout,
        ),
    )

    session = Session.__new__(Session)
    session.cluster = cluster
    session._profile_manager = SimpleNamespace(distance=lambda host: distance_map.get(host, HostDistance.LOCAL))
    session._pools = {}
    session.is_shutdown = False

    for host, schema_version in zip(hosts, schema_versions):
        connection = Mock(endpoint=host.endpoint)
        connection.future_outcomes = [schema_version]
        session._pools[host] = MockPool(host)
        connections[host] = connection

    def execute_async(query, parameters=None, trace=False,
                     custom_payload=None, execution_profile=None,
                     paging_state=None, timeout=None, host=None, execute_as=None):
        connection = connections[host]
        outcome = connection.future_outcomes.pop(0) if len(connection.future_outcomes) > 1 else connection.future_outcomes[0]
        return MockSchemaVersionFuture(outcome)

    session.execute_async = Mock(side_effect=execute_async)

    return session, hosts, connections


def test_wait_for_schema_agreement_retries_with_module_time(monkeypatch):
    session, hosts, connections = _new_session(["a", "b"])
    clock = FakeTime()
    monkeypatch.setattr(cluster_module, "time", clock)
    connections[hosts[1]].future_outcomes = ["b", "a"]

    assert session.wait_for_schema_agreement(wait_time=1)
    assert clock.clock == pytest.approx(0.2)
    for host in hosts:
        assert _host_query_count(session, host) == 2


@pytest.mark.parametrize("wait_time", [0, -1])
def test_wait_for_schema_agreement_rejects_non_positive_wait_time(wait_time):
    session, _, _ = _new_session(["a"])

    with pytest.raises(ValueError, match="wait_time must be greater than 0"):
        session.wait_for_schema_agreement(wait_time=wait_time)

    assert session.execute_async.call_count == 0


def test_wait_for_schema_agreement_returns_false_when_no_hosts_match_scope(monkeypatch):
    session, _, _ = _new_session(["a"], distances=[HostDistance.IGNORED])
    clock = FakeTime()
    monkeypatch.setattr(cluster_module, "time", clock)

    assert session.wait_for_schema_agreement(wait_time=1) is False
    assert session.execute_async.call_count == 0
    assert clock.clock == pytest.approx(1.0)


def test_wait_for_schema_agreement_uses_host_targeted_session_queries():
    session, hosts, _ = _new_session(["a", "a"])

    assert session.wait_for_schema_agreement(wait_time=0.1)

    expected_query = maybe_add_timeout_to_query(
        ControlConnection._SELECT_SCHEMA_LOCAL,
        timedelta(seconds=2),
    )
    assert session.execute_async.call_count == 2
    assert [call.args[0] for call in session.execute_async.call_args_list] == [expected_query, expected_query]
    assert [call.kwargs["host"] for call in session.execute_async.call_args_list] == hosts
    for call in session.execute_async.call_args_list:
        assert 0 < call.kwargs["timeout"] <= 0.1


def test_wait_for_schema_agreement_retries_after_host_targeted_query_error(monkeypatch):
    session, hosts, connections = _new_session(["a", "a"])
    clock = FakeTime()
    monkeypatch.setattr(cluster_module, "time", clock)
    connections[hosts[1]].future_outcomes = [ConnectionBusy("connection overloaded"), "a"]

    assert session.wait_for_schema_agreement(wait_time=1)
    assert clock.clock == pytest.approx(0.2)
    for host in hosts:
        assert _host_query_count(session, host) == 2


def test_wait_for_schema_agreement_queries_hosts_in_order_under_one_deadline(monkeypatch):
    session, hosts, _ = _new_session(["a", "a", "a"])
    clock = FakeTime()
    monkeypatch.setattr(cluster_module, "time", clock)

    def execute_async(query, parameters=None, trace=False,
                     custom_payload=None, execution_profile=None,
                     paging_state=None, timeout=None, host=None, execute_as=None):
        clock.sleep(0.01)
        return MockSchemaVersionFuture("a")

    session.execute_async = Mock(side_effect=execute_async)

    assert session.wait_for_schema_agreement(wait_time=1)
    assert [call.kwargs["host"] for call in session.execute_async.call_args_list] == hosts
