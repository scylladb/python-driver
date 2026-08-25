# Copyright 2026 ScyllaDB, Inc.
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

import gc
import json
import unittest
from unittest import mock
from unittest.mock import Mock

from cassandra.driver_config import (DriverConfigReporter, DRIVER_CONFIG_OPTION,
                                     DRIVER_CONFIG_SCHEMA_VERSION, MAX_DRIVER_CONFIG_LENGTH)
from tests.unit.utils import _ClusterlessReporter, ThrowingReporter


class OversizedReporter(_ClusterlessReporter):
    """
    Produces a report one byte past the limit. The schema-only report built by
    :class:`.DriverConfigReporter` cannot reach the limit on its own, so the
    guard is only reachable through a subclass.
    """
    def _build_report(self, cluster, is_scylla):
        return 'a' * (MAX_DRIVER_CONFIG_LENGTH + 1)


class MistypedReporter(_ClusterlessReporter):
    """
    Returns something that is not a string, the mistake the ``_populate_report``
    extension point invites once it describes more than the schema version.
    """
    def _build_report(self, cluster, is_scylla):
        return None


def reporter(cluster=None):
    """
    A reporter over `cluster`, defaulting to one whose configuration is never
    read because nothing populates the report yet.

    The cluster is kept alive for as long as the reporter is: it is held weakly,
    so a temporary would be collected before the report is built and every test
    here would silently exercise the cluster-is-gone path instead.
    """
    cluster = cluster if cluster is not None else Mock()
    r = DriverConfigReporter(cluster)
    r._strong_cluster = cluster
    return r


class DriverConfigReporterTest(unittest.TestCase):
    def test_reports_the_schema_version(self):
        options = {}

        reporter().add_startup_options(options, is_scylla=True)

        assert json.loads(options[DRIVER_CONFIG_OPTION]) == {'version': DRIVER_CONFIG_SCHEMA_VERSION}

    def test_report_is_compact_json(self):
        """
        The report is a wire value bounded by MAX_DRIVER_CONFIG_LENGTH, not
        something meant to be read as it is, so it carries no padding.
        """
        options = {}

        reporter().add_startup_options(options, is_scylla=True)

        assert options[DRIVER_CONFIG_OPTION] == '{"version":%d}' % DRIVER_CONFIG_SCHEMA_VERSION

    def test_report_fits_within_the_length_limit(self):
        """
        Tripwire for when the actual configuration groups land: a report over the
        limit is dropped by add_startup_options, so this would fail with a clear
        message instead of the size assertion raising an unrelated KeyError.
        """
        options = {}

        reporter().add_startup_options(options, is_scylla=True)

        assert DRIVER_CONFIG_OPTION in options, \
            "the report was dropped, it must have exceeded the length limit"
        # The limit is enforced on the encoded length, so measure bytes as well.
        assert len(options[DRIVER_CONFIG_OPTION].encode('utf8')) <= MAX_DRIVER_CONFIG_LENGTH

    def test_oversized_report_is_not_reported(self):
        options = {}

        OversizedReporter().add_startup_options(options, is_scylla=True)

        assert DRIVER_CONFIG_OPTION not in options

    def test_failure_to_build_the_report_is_not_reported(self):
        """
        Building the report must never take a connection down with it: the
        exception is swallowed and the option left out.
        """
        options = {}

        ThrowingReporter().add_startup_options(options, is_scylla=True)

        assert DRIVER_CONFIG_OPTION not in options

    def test_a_report_that_is_not_a_string_is_not_reported(self):
        """
        The guard covers the whole method, not just building the report: a
        subclass returning a non-string must be as harmless as one raising, since
        an exception here would reach the connection and defunct it.
        """
        options = {}

        MistypedReporter().add_startup_options(options, is_scylla=True)

        assert DRIVER_CONFIG_OPTION not in options

    def test_the_cluster_is_held_weakly(self):
        """
        The cluster owns the reporter and hands it to every connection it opens,
        so a strong reference here would run back through each of them and keep
        the cluster alive for as long as any connection holds a reporter.
        """
        cluster = Mock()
        r = DriverConfigReporter(cluster)
        assert r._cluster() is cluster

        del cluster
        gc.collect()
        assert r._cluster() is None

    def test_nothing_is_reported_once_the_cluster_is_gone(self):
        """
        An application dropping its Cluster while a connection is being
        established is a shutdown race, not a misconfiguration: the option is
        left out and nothing is warned about.
        """
        r = DriverConfigReporter(Mock())
        options = {}

        with mock.patch.object(r, '_cluster', return_value=None):
            r.add_startup_options(options, is_scylla=True)

        assert DRIVER_CONFIG_OPTION not in options

    def test_other_options_are_left_alone(self):
        options = {'APPLICATION_NAME': 'app'}

        OversizedReporter().add_startup_options(options, is_scylla=True)
        MistypedReporter().add_startup_options(options, is_scylla=True)
        reporter().add_startup_options(options, is_scylla=True)

        assert options['APPLICATION_NAME'] == 'app'
