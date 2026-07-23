# Copyright DataStax, Inc.
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

import logging
import ssl
import sys
from unittest.mock import Mock, sentinel

from cassandra import ConsistencyLevel
from cassandra.cluster import (
    ExecutionProfile, GraphExecutionProfile, GraphAnalyticsExecutionProfile
)
from cassandra.connection import Connection
from cassandra.datastax.graph.query import GraphOptions
from cassandra.datastax.insights.reporter import MonitorReporter
from cassandra.datastax.insights.registry import insights_registry
from cassandra.datastax.insights.serializers import initialize_registry
from cassandra.policies import (
    LoadBalancingPolicy,
    HostDistance,
    DCAwareRoundRobinPolicy,
    TokenAwarePolicy,
    WhiteListRoundRobinPolicy,
    HostFilterPolicy,
    ConstantReconnectionPolicy,
    ExponentialReconnectionPolicy,
    RetryPolicy,
    SpeculativeExecutionPolicy,
    ConstantSpeculativeExecutionPolicy,
    WrapperPolicy
)


log = logging.getLogger(__name__)

initialize_registry(insights_registry)


class TestGetConfig(unittest.TestCase):

    def test_invalid_object(self):
        class NoConfAsDict(object):
            pass

        obj = NoConfAsDict()

        ns = 'tests.unit.advanced.test_insights'
        if sys.version_info > (3,):
            ns += '.TestGetConfig.test_invalid_object.<locals>'

        # no default
        # ... as a policy
        assert insights_registry.serialize(obj, policy=True) == {'type': 'NoConfAsDict',
         'namespace': ns,
         'options': {}}
        # ... not as a policy (default)
        assert insights_registry.serialize(obj) == {'type': 'NoConfAsDict',
         'namespace': ns,
         }
        # with default
        assert insights_registry.serialize(obj, default=sentinel.attr_err_default) is sentinel.attr_err_default

    def test_successful_return(self):

        class SuperclassSentinel(object):
            pass

        class SubclassSentinel(SuperclassSentinel):
            pass

        @insights_registry.register_serializer_for(SuperclassSentinel)
        def superclass_sentinel_serializer(obj):
            return sentinel.serialized_superclass

        assert insights_registry.serialize(SuperclassSentinel()) is sentinel.serialized_superclass
        assert insights_registry.serialize(SubclassSentinel()) is sentinel.serialized_superclass

        # with default -- same behavior
        assert insights_registry.serialize(SubclassSentinel(), default=object()) is sentinel.serialized_superclass


class TestMonitorReporterStartupData(unittest.TestCase):

    def _get_startup_data(self, ssl_options=None, ssl_context=None,
                          connection_ssl_options=None, connection_ssl_context=None,
                          connection_ssl_options_explicit=False,
                          connection=None):
        reporter = MonitorReporter.__new__(MonitorReporter)
        reporter._interval = 30

        if connection is None:
            connection = Mock()
            connection.host = '127.0.0.1'
            connection._compression_type = 'NONE'
            connection._socket.getsockname.return_value = ('127.0.0.1', 9042)
            if connection_ssl_options is not None:
                connection.ssl_options = connection_ssl_options
            if connection_ssl_context is not None:
                connection.ssl_context = connection_ssl_context
            connection._ssl_options_explicit = connection_ssl_options_explicit

        cluster = Mock()
        cluster.auth_provider = None
        cluster.client_id = 'client-id'
        cluster.connection_class = Connection
        cluster.control_connection._connection = connection
        cluster.application_name = None
        cluster.application_version = None
        cluster._endpoint_map_for_insights = {}
        cluster.idle_heartbeat_interval = 30
        cluster.metadata.all_hosts.return_value = []
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        cluster.protocol_version = 4
        cluster.reconnection_policy = object()
        cluster.ssl_context = ssl_context
        cluster.ssl_options = ssl_options

        session = Mock()
        session.cluster = cluster
        session.hosts = []
        session.session_id = 'session-id'

        reporter._session = session
        return reporter._get_startup_data()

    def test_empty_ssl_options_reported_as_enabled(self):
        startup_data = self._get_startup_data(ssl_options={})

        assert startup_data['data']['sslConfigured']['enabled'] is True

    def test_implicit_ssl_options_validation_reported(self):
        startup_data = self._get_startup_data(ssl_options={'ca_certs': 'ca.pem'})

        assert startup_data['data']['sslConfigured']['certValidation'] is True

    def test_check_hostname_validation_reported(self):
        startup_data = self._get_startup_data(ssl_options={'check_hostname': True})

        assert startup_data['data']['sslConfigured']['certValidation'] is True

    def test_empty_ssl_options_validation_reported_disabled(self):
        startup_data = self._get_startup_data(ssl_options={})

        assert startup_data['data']['sslConfigured']['certValidation'] is False

    def test_omitted_ssl_options_reported_as_disabled(self):
        startup_data = self._get_startup_data()

        assert startup_data['data']['sslConfigured']['enabled'] is False

    def test_implicit_empty_connection_ssl_options_reported_as_disabled(self):
        connection = Connection()

        startup_data = self._get_startup_data(connection=connection)

        assert connection.ssl_options == {}
        assert connection._ssl_options_explicit is False
        assert startup_data['data']['sslConfigured']['enabled'] is False

    def test_ssl_context_reported_as_enabled(self):
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

        startup_data = self._get_startup_data(ssl_context=ssl_context)

        assert startup_data['data']['sslConfigured']['enabled'] is True

    def test_ssl_context_cert_optional_validation_reported_enabled(self):
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_OPTIONAL

        startup_data = self._get_startup_data(ssl_context=ssl_context)

        assert startup_data['data']['sslConfigured']['certValidation'] is True

    def test_connection_ssl_options_reported_as_enabled(self):
        startup_data = self._get_startup_data(
            connection_ssl_options={}, connection_ssl_options_explicit=True)

        assert startup_data['data']['sslConfigured']['enabled'] is True
        assert startup_data['data']['sslConfigured']['certValidation'] is False


class TestConfigAsDict(unittest.TestCase):

    #  graph/query.py
    def test_graph_options(self):
        self.maxDiff = None

        go = GraphOptions(graph_name='name_for_test',
                          graph_source='source_for_test',
                          graph_language='lang_for_test',
                          graph_protocol='protocol_for_test',
                          graph_read_consistency_level=ConsistencyLevel.ANY,
                          graph_write_consistency_level=ConsistencyLevel.ONE,
                          graph_invalid_option='invalid')

        log.debug(go._graph_options)

        assert insights_registry.serialize(go) == {'source': 'source_for_test',
         'language': 'lang_for_test',
         'graphProtocol': 'protocol_for_test',
         # no graph_invalid_option
         }

    # cluster.py
    def test_execution_profile(self):
        self.maxDiff = None
        assert insights_registry.serialize(ExecutionProfile()) == {'consistency': 'LOCAL_ONE',
         'continuousPagingOptions': None,
         'loadBalancing': {'namespace': 'cassandra.policies',
                           'options': {'child_policy': {'namespace': 'cassandra.policies',
                                                        'options': {'local_dc': '',
                                                                    'used_hosts_per_remote_dc': 0},
                                                        'type': 'DCAwareRoundRobinPolicy'},
                                       'shuffle_replicas': True},
                           'type': 'TokenAwarePolicy'},
         'readTimeout': 10.0,
         'retry': {'namespace': 'cassandra.policies', 'options': {}, 'type': 'RetryPolicy'},
         'serialConsistency': None,
         'speculativeExecution': {'namespace': 'cassandra.policies',
                                  'options': {}, 'type': 'NoSpeculativeExecutionPolicy'},
         'graphOptions': None
         }

    def test_graph_execution_profile(self):
        self.maxDiff = None
        assert insights_registry.serialize(GraphExecutionProfile()) == {'consistency': 'LOCAL_ONE',
         'continuousPagingOptions': None,
         'loadBalancing': {'namespace': 'cassandra.policies',
                           'options': {'child_policy': {'namespace': 'cassandra.policies',
                                                        'options': {'local_dc': '',
                                                                    'used_hosts_per_remote_dc': 0},
                                                        'type': 'DCAwareRoundRobinPolicy'},
                                       'shuffle_replicas': True},
                           'type': 'TokenAwarePolicy'},
         'readTimeout': 30.0,
         'retry': {'namespace': 'cassandra.policies', 'options': {}, 'type': 'NeverRetryPolicy'},
         'serialConsistency': None,
         'speculativeExecution': {'namespace': 'cassandra.policies',
                                  'options': {}, 'type': 'NoSpeculativeExecutionPolicy'},
         'graphOptions': {'graphProtocol': None,
                          'language': 'gremlin-groovy',
                          'source': 'g'},
         }

    def test_graph_analytics_execution_profile(self):
        self.maxDiff = None
        assert insights_registry.serialize(GraphAnalyticsExecutionProfile()) == {'consistency': 'LOCAL_ONE',
         'continuousPagingOptions': None,
         'loadBalancing': {'namespace': 'cassandra.policies',
                           'options': {'child_policy': {'namespace': 'cassandra.policies',
                                                        'options': {'child_policy': {'namespace': 'cassandra.policies',
                                                                                     'options': {'local_dc': '',
                                                                                                 'used_hosts_per_remote_dc': 0},
                                                                                     'type': 'DCAwareRoundRobinPolicy'},
                                                                    'shuffle_replicas': True},
                                                        'type': 'TokenAwarePolicy'}},
                           'type': 'DefaultLoadBalancingPolicy'},
         'readTimeout': 604800.0,
         'retry': {'namespace': 'cassandra.policies', 'options': {}, 'type': 'NeverRetryPolicy'},
         'serialConsistency': None,
         'speculativeExecution': {'namespace': 'cassandra.policies',
                                  'options': {}, 'type': 'NoSpeculativeExecutionPolicy'},
         'graphOptions': {'graphProtocol': None,
                          'language': 'gremlin-groovy',
                          'source': 'a'},
         }

    # policies.py
    def test_DC_aware_round_robin_policy(self):
        assert insights_registry.serialize(DCAwareRoundRobinPolicy()) == {'namespace': 'cassandra.policies',
         'options': {'local_dc': '', 'used_hosts_per_remote_dc': 0},
         'type': 'DCAwareRoundRobinPolicy'}
        assert insights_registry.serialize(DCAwareRoundRobinPolicy(local_dc='fake_local_dc',
                                                            used_hosts_per_remote_dc=15)) == {'namespace': 'cassandra.policies',
         'options': {'local_dc': 'fake_local_dc', 'used_hosts_per_remote_dc': 15},
         'type': 'DCAwareRoundRobinPolicy'}

    def test_token_aware_policy(self):
        assert insights_registry.serialize(TokenAwarePolicy(child_policy=LoadBalancingPolicy())) == {'namespace': 'cassandra.policies',
         'options': {'child_policy': {'namespace': 'cassandra.policies',
                                      'options': {},
                                      'type': 'LoadBalancingPolicy'},
                     'shuffle_replicas': True},
         'type': 'TokenAwarePolicy'}

    def test_whitelist_round_robin_policy(self):
        assert insights_registry.serialize(WhiteListRoundRobinPolicy(['127.0.0.3'])) == {'namespace': 'cassandra.policies',
         'options': {'allowed_hosts': ('127.0.0.3',)},
         'type': 'WhiteListRoundRobinPolicy'}

    def test_host_filter_policy(self):
        def my_predicate(s):
            return False

        assert insights_registry.serialize(HostFilterPolicy(LoadBalancingPolicy(), my_predicate)) == {'namespace': 'cassandra.policies',
         'options': {'child_policy': {'namespace': 'cassandra.policies',
                                      'options': {},
                                      'type': 'LoadBalancingPolicy'},
                     'predicate': 'my_predicate'},
         'type': 'HostFilterPolicy'}

    def test_constant_reconnection_policy(self):
        assert insights_registry.serialize(ConstantReconnectionPolicy(3, 200)) == {'type': 'ConstantReconnectionPolicy',
        'namespace': 'cassandra.policies',
        'options': {'delay': 3, 'max_attempts': 200}
         }

    def test_exponential_reconnection_policy(self):
        assert insights_registry.serialize(ExponentialReconnectionPolicy(4, 100, 10)) == {'type': 'ExponentialReconnectionPolicy',
        'namespace': 'cassandra.policies',
        'options': {'base_delay': 4, 'max_delay': 100, 'max_attempts': 10}
         }

    def test_retry_policy(self):
        assert insights_registry.serialize(RetryPolicy()) == {'type': 'RetryPolicy',
        'namespace': 'cassandra.policies',
        'options': {}
         }

    def test_spec_exec_policy(self):
        assert insights_registry.serialize(SpeculativeExecutionPolicy()) == {'type': 'SpeculativeExecutionPolicy',
        'namespace': 'cassandra.policies',
        'options': {}
         }

    def test_constant_spec_exec_policy(self):
        assert insights_registry.serialize(ConstantSpeculativeExecutionPolicy(100, 101)) == {'type': 'ConstantSpeculativeExecutionPolicy',
         'namespace': 'cassandra.policies',
         'options': {'delay': 100,
                     'max_attempts': 101}
         }

    def test_wrapper_policy(self):
        assert insights_registry.serialize(WrapperPolicy(LoadBalancingPolicy())) == {'namespace': 'cassandra.policies',
         'options': {'child_policy': {'namespace': 'cassandra.policies',
                                      'options': {},
                                      'type': 'LoadBalancingPolicy'}
                     },
         'type': 'WrapperPolicy'}
