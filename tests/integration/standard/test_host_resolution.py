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
import pytest

from tests.integration import CASSANDRA_IP, TestCluster, remove_cluster, use_cluster


def setup_module():
    use_cluster('host_resolution', [1], start=True)


def teardown_module():
    remove_cluster()


@pytest.mark.parametrize('asynchronous', [False, True])
def test_cached_host_from_previous_cluster(asynchronous):
    with TestCluster(protocol_version=4) as previous_cluster:
        previous_cluster.connect(wait_for_all_pools=True)
        cached = previous_cluster.metadata.get_host(CASSANDRA_IP)
        assert cached is not None

    with TestCluster(protocol_version=4) as cluster:
        session = cluster.connect(wait_for_all_pools=True)
        current = cluster.metadata.get_host_by_host_id(cached.host_id)
        assert current is not None
        assert current is not cached

        query = 'SELECT host_id FROM system.local'
        if asynchronous:
            result = session.execute_async(query, host=cached).result()
        else:
            result = session.execute(query, host=cached)

        assert result.one().host_id == current.host_id
        assert result.response_future.coordinator_host is current
