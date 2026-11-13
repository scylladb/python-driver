# Copyright ScyllaDB
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

from concurrent.futures import Future
from io import BytesIO
from unittest.mock import Mock, patch
import uuid

import pytest

from cassandra import ConsistencyLevel as CL
from cassandra.cluster import Cluster, Session, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy, SimpleConvictionPolicy
from cassandra.pool import Host
from cassandra.protocol import (
    BatchMessage, ExecuteMessage, QueryMessage, read_byte, read_int, read_longstring,
    read_short, read_string,
)
from cassandra.query import BatchStatement, PreparedStatement, SimpleStatement


@pytest.fixture(params=['default', 'profile', 'legacy'])
def session(request):
    kwargs = {'protocol_version': 4}
    if request.param == 'profile':
        kwargs['execution_profiles'] = {EXEC_PROFILE_DEFAULT: ExecutionProfile()}
    elif request.param == 'legacy':
        kwargs['load_balancing_policy'] = RoundRobinPolicy()
    with Cluster(**kwargs) as cluster:
        connected = Future()
        connected.set_result(object())
        with patch.object(Session, 'add_or_renew_pool', return_value=connected):
            session = Session(cluster, [Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.uuid4())])
        session.default_fetch_size = None
        session.use_client_timestamp = False
        yield session


def prepared():
    return PreparedStatement([], b'query-id', [],
                             'INSERT INTO t (k) VALUES (1) IF NOT EXISTS',
                             None, 4, [], None, is_lwt=True)


def statement(kind):
    if kind == 'read':
        return 'SELECT * FROM t'
    if kind == 'write':
        return 'INSERT INTO t (k) VALUES (1)'
    if kind == 'lwt':
        return 'INSERT INTO t (k) VALUES (1) IF NOT EXISTS'
    if kind == 'simple':
        return SimpleStatement('INSERT INTO t (k) VALUES (1) IF NOT EXISTS')
    if kind == 'prepared':
        return prepared()
    if kind == 'bound':
        return prepared().bind(())
    return BatchStatement().add('INSERT INTO t (k) VALUES (1) IF NOT EXISTS')


def wire_consistencies(session, query, version=4):
    session._protocol_version = version
    future = session._create_response_future(query, None, False, {}, None)
    message = future.message
    buffer = BytesIO()
    message.send_body(buffer, version)
    buffer.seek(0)
    if isinstance(message, QueryMessage):
        read_longstring(buffer)
    elif isinstance(message, ExecuteMessage):
        read_string(buffer)
        if version >= 5:
            read_string(buffer)  # result metadata id
    else:
        assert isinstance(message, BatchMessage)
        read_byte(buffer)  # batch type
        assert read_short(buffer) == 1
        assert read_byte(buffer) == 0  # unprepared query
        read_longstring(buffer)
        assert read_short(buffer) == 0  # no parameters
    regular = read_short(buffer)
    flags = read_int(buffer) if version >= 5 else read_byte(buffer)
    if isinstance(message, ExecuteMessage):
        assert flags & 0x01
        assert read_short(buffer) == 0  # no bound values
    serial = read_short(buffer) if flags & 0x10 else None
    assert buffer.read() == b''
    return regular, serial


@pytest.mark.parametrize('kind', ['read', 'write', 'lwt', 'simple', 'prepared', 'bound', 'batch'])
@pytest.mark.parametrize('version', [3, 4, 5])
def test_defaults_reach_the_wire(session, kind, version):
    assert wire_consistencies(session, statement(kind), version) == (CL.LOCAL_QUORUM, CL.LOCAL_SERIAL)


@pytest.mark.parametrize('serial', [CL.SERIAL, None])
def test_explicit_profile_and_legacy_defaults_are_preserved(session, serial):
    from cassandra.cluster import _ConfigMode

    if session.cluster._config_mode == _ConfigMode.LEGACY:
        session.default_consistency_level = CL.LOCAL_ONE
        session.default_serial_consistency_level = serial
    else:
        session.cluster.profile_manager.profiles[EXEC_PROFILE_DEFAULT] = ExecutionProfile(
            consistency_level=CL.LOCAL_ONE, serial_consistency_level=serial)
    assert wire_consistencies(session, statement('lwt')) == (CL.LOCAL_ONE, serial)


@pytest.mark.parametrize('kind', ['simple', 'prepared', 'bound', 'batch'])
def test_statement_overrides_are_preserved(session, kind):
    query = statement(kind)
    query.consistency_level = CL.ALL
    query.serial_consistency_level = CL.SERIAL
    assert wire_consistencies(session, query) == (CL.ALL, CL.SERIAL)


def test_batch_uses_its_own_defaults(session):
    child = SimpleStatement('INSERT INTO t (k) VALUES (1) IF NOT EXISTS',
                            consistency_level=CL.ONE, serial_consistency_level=CL.SERIAL)
    assert wire_consistencies(session, BatchStatement().add(child)) == (CL.LOCAL_QUORUM, CL.LOCAL_SERIAL)


def test_dbaas_adjustment_preserves_explicit_profile_overrides(session):
    cluster = session.cluster
    explicit = ExecutionProfile(consistency_level=CL.ONE, serial_consistency_level=CL.SERIAL)
    cluster.profile_manager.profiles['explicit'] = explicit
    cluster.metadata.dbaas = True
    cluster._set_default_dbaas_consistency(session)
    assert explicit.consistency_level == CL.ONE
    assert explicit.serial_consistency_level == CL.SERIAL


@pytest.mark.parametrize('product_type, expected', [
    ('DATASTAX_APOLLO', True), ('SCYLLA_CLOUD', False), (None, False),
])
def test_dbaas_detection_is_specific_to_apollo(product_type, expected):
    with Cluster() as cluster:
        control = cluster.control_connection
        connection = Mock(_product_type=product_type)
        with patch.object(control, '_reconnect_internal', return_value=connection), \
                patch.object(control, '_set_new_connection',
                             side_effect=lambda conn: setattr(control, '_connection', conn)):
            control.connect()
        assert cluster.metadata.dbaas is expected
