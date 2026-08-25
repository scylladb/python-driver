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

from functools import wraps
from unittest.mock import Mock, patch

from concurrent.futures import Future
from cassandra.cluster import Session
from cassandra.driver_config import DriverConfigReporter


def mock_session_pools(f):
    """
    Helper decorator that allows tests to initialize :class:.`Session` objects
    without actually connecting to a Cassandra cluster.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        with patch.object(Session, "add_or_renew_pool") as mocked_add_or_renew_pool:
            future = Future()
            future.set_result(object())
            mocked_add_or_renew_pool.return_value = future
            f(*args, **kwargs)
    return wrapper


class _ClusterlessReporter(DriverConfigReporter):
    """
    Base for the reporter doubles below, which override report building and so
    never read the cluster. Supplying a Mock keeps them constructible without
    one while leaving the weak reference the real reporter holds in place.

    That reference is why the Mock is also held strongly: a temporary would be
    collected before the report is built, and the double would then drop its
    report because the cluster was gone rather than for the reason it exists to
    demonstrate -- which is a test that passes while proving nothing.
    """
    def __init__(self, cluster=None):
        self._strong_cluster = cluster if cluster is not None else Mock()
        super().__init__(self._strong_cluster)


class ThrowingReporter(_ClusterlessReporter):
    """
    A driver configuration reporter whose report cannot be built.

    Shared because two suites need it: the reporter's own tests, for the guard
    that drops a report it cannot build, and the connection tests, for the
    guarantee that such a failure leaves the STARTUP frame otherwise intact
    instead of failing the connection.
    """
    def _build_report(self, cluster, is_scylla):
        raise ValueError("simulated failure while building the report")


class StubReporter(_ClusterlessReporter):
    """
    A driver configuration reporter with a fixed, recognisable report.

    The connection tests are about where the report goes -- which connections
    carry it, and that an application cannot supply its own -- not about what is
    in it. Asserting the real report's text there would tie those guarantees to
    every configuration group that lands afterwards, and break them all at once
    for a reason that has nothing to do with connections.
    """
    REPORT = '{"stub-report":true}'

    def _build_report(self, cluster, is_scylla):
        return self.REPORT
