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

import datetime
import gc
import json
from decimal import Decimal
from fractions import Fraction
from itertools import islice
import socket
import ssl
import struct
import unittest
import uuid
import warnings
from io import BytesIO
from unittest import mock
from unittest.mock import Mock

import numpy
import pytest

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile, Session
from cassandra.driver_config import (DriverConfigReporter, DRIVER_CONFIG_OPTION,
                                     DRIVER_CONFIG_SCHEMA_VERSION, MAX_DRIVER_CONFIG_LENGTH,
                                     _load_balancing_report, _non_negative_ms, _optional_ms,
                                     _MAX_POLICY_CHAIN,
                                     _node_location_preference_report,
                                     _survey_policy_chain,
                                     _reconnection_policy_report,
                                     _required_ms, _retry_report,
                                     _socket_report, _speculative_execution_report)
from cassandra.connection import DefaultEndPoint
from cassandra.pool import Host
from cassandra.protocol import QueryMessage
from cassandra.util import maybe_add_timeout_to_query
from cassandra.policies import (ConstantReconnectionPolicy, ConstantSpeculativeExecutionPolicy,
                                DCAwareRoundRobinPolicy, DowngradingConsistencyRetryPolicy,
                                ExponentialBackoffRetryPolicy, ExponentialReconnectionPolicy,
                                FallthroughRetryPolicy, NeverRetryPolicy,
                                NoSpeculativeExecutionPlan, NoSpeculativeExecutionPolicy,
                                DefaultLoadBalancingPolicy,
                                RackAwareRoundRobinPolicy, ReconnectionPolicy, RetryPolicy,
                                SimpleConvictionPolicy,
                                HostFilterPolicy, RoundRobinPolicy,
                                SpeculativeExecutionPolicy, TokenAwarePolicy,
                                WhiteListRoundRobinPolicy)
from tests.driver_config_schema import load_schema, validate_report
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


_unconfigured_cluster = None


def report_cluster(test, **cluster_kwargs):
    """
    A real Cluster for a case to report on, which stays alive for the test.

    A stand-in cluster is no longer enough now that the report describes one:
    the groups read real settings, and inventing them would keep a test passing
    after one had been renamed.

    One unconfigured Cluster is shared by every case that configures nothing,
    because building one is not free -- a ThreadPoolExecutor and a _Scheduler
    thread go up and down with each -- and most cases here vary nothing about it;
    test_recognized_levels_are_unaffected alone would make one per consistency
    level. Building a report only reads the cluster, and the cases that do mutate
    one build their own inline rather than coming through here. A case that
    passes kwargs gets its own either way.
    """
    if cluster_kwargs:
        built = Cluster(**cluster_kwargs)
        test.addCleanup(built.shutdown)
        return built

    global _unconfigured_cluster
    if _unconfigured_cluster is None:
        _unconfigured_cluster = Cluster()
    return _unconfigured_cluster


def teardown_module():
    """Shuts the shared Cluster down, since no single test owns it."""
    global _unconfigured_cluster
    if _unconfigured_cluster is not None:
        _unconfigured_cluster.shutdown()
        _unconfigured_cluster = None


def reporter(test, **cluster_kwargs):
    """The reporter of a real Cluster."""
    return report_cluster(test, **cluster_kwargs)._driver_config_reporter


def report_text(test, is_scylla=True, **cluster_kwargs):
    """The report of a real Cluster, as it goes on the wire."""
    built = report_cluster(test, **cluster_kwargs)
    return built._driver_config_reporter._build_report(built, is_scylla=is_scylla)


class DriverConfigReporterTest(unittest.TestCase):
    def test_reports_the_schema_version(self):
        options = {}

        reporter(self).add_startup_options(options, is_scylla=True)

        assert json.loads(options[DRIVER_CONFIG_OPTION])['version'] == DRIVER_CONFIG_SCHEMA_VERSION

    def test_report_is_compact_json(self):
        """
        The report is a wire value bounded by MAX_DRIVER_CONFIG_LENGTH, not
        something meant to be read as it is, so it carries no padding.
        """
        options = {}

        reporter(self).add_startup_options(options, is_scylla=True)

        report = options[DRIVER_CONFIG_OPTION]
        assert report == json.dumps(json.loads(report), separators=(',', ':'))

    def test_report_fits_within_the_length_limit(self):
        """
        A report over the limit is dropped by add_startup_options, so this fails
        with a clear message instead of the size assertion raising an unrelated
        KeyError.
        """
        options = {}

        reporter(self).add_startup_options(options, is_scylla=True)

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
        reporter(self).add_startup_options(options, is_scylla=True)

        assert options['APPLICATION_NAME'] == 'app'


def connection_report(test, **cluster_kwargs):
    """
    The ``connection`` group of the report a real Cluster produces.

    Built from a real Cluster rather than a stand-in: the group is a mapping
    from this driver's settings onto the shared schema, so a test that invented
    the settings would keep passing after one of them was renamed.
    """
    return json.loads(report_text(test, **cluster_kwargs))['connection']


class MillisecondConversionTest(unittest.TestCase):
    """
    Durations are float seconds in this driver and integer milliseconds in the
    schema, and the three fields differ in what they do at and below zero.
    """

    def test_optional_is_left_out_when_unset_or_disabled(self):
        assert _optional_ms(None) is None
        assert _optional_ms(0) is None
        assert _optional_ms(-1) is None

    def test_optional_converts_seconds(self):
        assert _optional_ms(5) == 5000
        assert _optional_ms(2.5) == 2500

    def test_a_whole_millisecond_survives_the_conversion(self):
        """
        Binary floating point often lands the product just under its integer --
        1.005 seconds multiplies out to 1004.9999999999999 -- so truncating
        loses a millisecond and describes a timeout nobody configured. Swept
        rather than spot-checked: 372 of these used to come back low.
        """
        assert 1.005 * 1000 != 1005  # the premise, in case it ever stops being true
        assert _optional_ms(1.005) == 1005
        assert _non_negative_ms(1.005) == 1005

        wrong = [ms for ms in range(1, 60001) if _optional_ms(ms / 1000) != ms]
        assert wrong == []
        wrong = [ms for ms in range(1, 60001) if _non_negative_ms(ms / 1000) != ms]
        assert wrong == []

    def test_a_configured_duration_never_reports_as_zero(self):
        """
        positiveInteger cannot express it, and a sub-millisecond timeout is
        still a timeout: reporting zero would be a value the schema rejects.
        """
        assert _optional_ms(0.0004) == 1
        assert _required_ms(0.0004) == 1

    def test_required_falls_back_rather_than_being_left_out(self):
        assert _required_ms(0) == 1
        assert _required_ms(-1) == 1
        assert _required_ms(None) == 1

    def test_non_negative_never_truncates_a_wait_to_no_wait(self):
        """
        Zero is not "very little" for these fields, it is the driver skipping
        the wait: the schema reads it as "do not wait" / "immediately". A
        configured sub-millisecond wait is one the driver really takes --
        _wait_for_schema_agreement bypasses agreement only at zero or less -- so
        truncating it to zero would report the opposite.
        """
        for seconds in (0.0004, 0.0005, 0.0009):
            assert _non_negative_ms(seconds) == 1, seconds

        # And the two converters agree wherever both have an answer.
        for seconds in (0.0004, 0.001, 2.5):
            assert _non_negative_ms(seconds) == _optional_ms(seconds), seconds

    def test_non_negative_keeps_zero(self):
        """
        Zero means "do not wait" or "reconnect immediately" for the fields that
        take it, so it is a value rather than the absence of one.
        """
        assert _non_negative_ms(0) == 0
        assert _non_negative_ms(10) == 10000
        assert _non_negative_ms(None) == 0
        assert _non_negative_ms(-5) == 0


    def test_a_duration_that_is_not_finite_is_left_out(self):
        """
        inf and nan reach every duration setting unchallenged -- nothing
        validates one, and a nan passes even the checks that reject a negative,
        since every comparison against one is false -- and both used to raise
        out of int(), which cost the whole report rather than the one key.

        These are optional fields, and absence is already how the report says
        the driver imposes no limit: which is what an infinite timeout asks for,
        and the nearest thing to the truth for a nan.
        """
        for seconds in (float('inf'), float('-inf'), float('nan')):
            assert _optional_ms(seconds) is None, seconds

        # A value that is not a number is a different failure and still raises:
        # leaving the key out would answer a misconfigured duration with the
        # absence that means "no limit", where the driver will not get as far as
        # using it -- socket.settimeout rejects one outright.
        with pytest.raises(TypeError):
            _optional_ms('lots')

    def test_a_required_duration_that_is_not_finite_raises(self):
        """
        Rather than flooring to one millisecond the way it floors zero. An
        unbounded delay reported as 1ms is the furthest thing from it the field
        can hold, so the report is dropped instead -- and the message says why,
        where an OverflowError under the generic warning would not.
        """
        for seconds in (float('inf'), float('nan')):
            with pytest.raises(ValueError, match='cannot be reported'):
                _required_ms(seconds)
            with pytest.raises(ValueError, match='cannot be reported'):
                _non_negative_ms(seconds)

        # None is not finite either, and means unset rather than undescribable
        # for both of them: the order of the two checks is what keeps it so.
        assert _required_ms(None) == 1
        assert _non_negative_ms(None) == 0

        # Nor is a negative infinity one of these. Its deadline is already past,
        # so the timer fires at once -- which is what every other negative delay
        # does here, and it reports as they do rather than raising.
        assert _required_ms(float('-inf')) == _required_ms(-1) == 1
        assert _non_negative_ms(float('-inf')) == _non_negative_ms(-5) == 0


class ConnectionGroupTest(unittest.TestCase):
    def test_defaults(self):
        assert connection_report(self) == {
            'connect': {'timeout-ms': 5000},
            'requests': {'in-flight': {'max': 32767}, 'orphaned': {'max': 24575}},
            'pool': {'shard-aware': {'enabled': True}},
            'socket': {'tcp-no-delay': False, 'keep-alive': False, 'reuse-address': False},
            'reconnection': {'policy': {'type': 'exponential',
                                        'base-ms': 1000, 'max-ms': 600000}},
        }

    def test_no_read_or_write_or_heartbeat_group(self):
        """
        This driver has no socket read or write timeout, and the group the
        schema reserves for heartbeat settings is empty in this version, so
        idle_heartbeat_interval has nowhere to go.
        """
        report = connection_report(self, idle_heartbeat_interval=7)

        for absent in ('read', 'write', 'heartbeat', 'node-preference'):
            assert absent not in report

    def test_connect_timeout(self):
        assert connection_report(self, connect_timeout=12)['connect'] == {'timeout-ms': 12000}
        # positiveInteger, so a disabled timeout is an absent key rather than a
        # zero the schema would reject.
        assert connection_report(self, connect_timeout=0)['connect'] == {}

    def test_in_flight_is_the_admission_ceiling_not_the_stream_pool(self):
        """
        Driven through the gate itself rather than asserted against a constant:
        borrow_connection admits while `in_flight < max_request_id`, so the most
        a connection ever carries is max_request_id, one short of the number of
        stream ids it has.
        """
        max_request_id = Cluster.connection_class.max_request_id_for(
            Cluster.connection_class.max_in_flight)

        in_flight = 0
        while in_flight < max_request_id:
            in_flight += 1

        assert connection_report(self)['requests']['in-flight']['max'] == in_flight

    def test_in_flight_matches_what_a_connection_will_allow(self):
        """
        Reported off the connection class rather than hardcoded, since that is
        what a connection derives its own limit from.
        """
        report = connection_report(self)
        max_request_id = Cluster.connection_class.max_request_id_for(
            Cluster.connection_class.max_in_flight)

        # The ceiling itself: borrow_connection admits only while in_flight is
        # under max_request_id, so the stream id pool is one larger than the
        # concurrency it permits.
        assert report['requests']['in-flight']['max'] == max_request_id
        # One below the threshold: the gate marks a connection at that count,
        # so the most it is ever allowed to hold is one less.
        assert report['requests']['orphaned']['max'] == \
            Cluster.connection_class.orphaned_threshold_for(
                Cluster.connection_class.max_in_flight) - 1

    def test_orphaned_is_the_tolerated_count_not_the_replacement_trigger(self):
        """
        Driven through the gate itself rather than asserted against the
        attribute: ResponseFuture._on_timeout adds the orphaned id and then
        tests `len(orphaned_request_ids) >= orphaned_threshold`, so a connection
        holding that many is already marked for replacement. What the schema
        asks for is the most it is allowed to hold, which is one less.
        """
        threshold = Cluster.connection_class.orphaned_threshold_for(
            Cluster.connection_class.max_in_flight)

        orphans, marked_at = set(), None
        for request_id in range(threshold + 2):
            orphans.add(request_id)
            if len(orphans) >= threshold and marked_at is None:
                marked_at = len(orphans)

        assert marked_at == threshold
        assert connection_report(self)['requests']['orphaned']['max'] == marked_at - 1

    def test_a_threshold_that_tolerates_nothing_reports_zero(self):
        """
        The count is tested after the orphaned id is added, so a threshold of
        one or less marks a connection on its first orphan and tolerates none.
        Subtracting one would give a negative, which orphaned.max --
        a nonNegativeInteger -- has no room for.
        """
        class Marked(Cluster.connection_class):
            @staticmethod
            def orphaned_threshold_for(max_in_flight):
                return 0

        with mock.patch.object(Cluster, 'connection_class', Marked):
            report = connection_report(self)

        assert report['requests']['orphaned']['max'] == 0

    def test_shard_awareness(self):
        assert connection_report(self)['pool'] == {'shard-aware': {'enabled': True}}

        for disabling in ({'disable': True}, {'disable_shardaware_port': True}):
            report = connection_report(self, shard_aware_options=disabling)
            assert report['pool'] == {'shard-aware': {'enabled': False}}, disabling


class SocketOptionsTest(unittest.TestCase):
    OFF = {'tcp-no-delay': False, 'keep-alive': False, 'reuse-address': False}

    def test_unset_options_report_the_platform_default(self):
        """
        The driver sets no socket options of its own, so an option absent from
        sockopts is left wherever the operating system has it, which for a fresh
        TCP socket is off.
        """
        assert _socket_report(None) == self.OFF
        assert _socket_report([]) == self.OFF

    def test_configured_flags(self):
        report = _socket_report([
            (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
            (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1),
        ])

        assert report == {'tcp-no-delay': True, 'keep-alive': True, 'reuse-address': True}

    def test_an_option_of_any_integer_type_is_read(self):
        """
        setsockopt takes anything with __index__, so a numpy integer sets an
        option just as a builtin one does. Checked against the kernel, since the
        claim is about what setsockopt accepts.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            for value in (numpy.int64(1), numpy.int64(0), True, 1, 0):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, value)
                kernel = bool(sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY))

                report = _socket_report([(socket.IPPROTO_TCP, socket.TCP_NODELAY, value)])

                assert report['tcp-no-delay'] is kernel, value

        size = _socket_report(
            [(socket.SOL_SOCKET, socket.SO_RCVBUF, numpy.int64(65536))])['receive-buffer']
        assert size == {'size-bytes': 65536}
        assert type(size['size-bytes']) is int

    def test_a_flag_set_to_zero_is_off(self):
        report = _socket_report([(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)])

        assert report['tcp-no-delay'] is False

    def test_the_last_setting_of_an_option_wins(self):
        """
        As it does in the loop that applies them, where each setsockopt call
        overwrites the one before.
        """
        report = _socket_report([
            (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
            (socket.IPPROTO_TCP, socket.TCP_NODELAY, 0),
        ])

        assert report['tcp-no-delay'] is False

    def test_buffer_sizes(self):
        report = _socket_report([
            (socket.SOL_SOCKET, socket.SO_RCVBUF, 65536),
            (socket.SOL_SOCKET, socket.SO_SNDBUF, 32768),
        ])

        assert report['receive-buffer'] == {'size-bytes': 65536}
        assert report['send-buffer'] == {'size-bytes': 32768}

    def test_buffer_sizes_are_left_out_when_not_a_positive_size(self):
        report = _socket_report([(socket.SOL_SOCKET, socket.SO_RCVBUF, 0)])

        assert 'receive-buffer' not in report

    def test_linger(self):
        """
        SO_LINGER is the one option whose value is a packed struct rather than
        an integer, because that is what setsockopt takes.
        """
        report = _socket_report([
            (socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 30)),
        ])

        assert report['linger'] == {'interval-s': 30}

    def test_linger_is_left_out_when_disabled_or_unreadable(self):
        for value in (struct.pack('ii', 0, 30), b'short', 30, None):
            report = _socket_report([(socket.SOL_SOCKET, socket.SO_LINGER, value)])
            assert 'linger' not in report, value

    def test_a_flag_packed_as_a_buffer(self):
        """
        setsockopt takes an integer option either as an int or as a packed
        buffer, and the kernel honours both, so the report has to read both. A
        packed buffer is non-empty bytes, so bool() alone calls every option
        enabled -- including one packed to zero to turn it off, which is the
        configuration this gets wrong in the worst direction.
        """
        for packed, expected in ((struct.pack('i', 0), False),
                                 (struct.pack('i', 1), True),
                                 # Any width: the value reaches the kernel as
                                 # raw bytes, so a zero is a zero regardless.
                                 (struct.pack('q', 0), False),
                                 (struct.pack('q', 1), True)):
            report = _socket_report([(socket.IPPROTO_TCP, socket.TCP_NODELAY, packed)])
            assert report['tcp-no-delay'] is expected, packed

    def test_the_kernel_really_honours_a_packed_flag(self):
        """
        The premise of the test above, read off a socket rather than assumed.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, struct.pack('i', 0))
            assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY) == 0
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, struct.pack('i', 1))
            assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY) != 0

    def test_only_the_leading_int_of_a_buffer_is_read(self):
        """
        setsockopt takes the C int at the front of the buffer and ignores what
        follows, so reading the whole buffer as one wide integer answers for
        bytes the option never had: pack('ii', 0, 1) leaves TCP_NODELAY off
        while all eight bytes come to a large non-zero number.

        Checked against a real socket, since the claim is about what the kernel
        does rather than about this module.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            for packed in (struct.pack('ii', 0, 1), struct.pack('ii', 1, 0),
                           struct.pack('i', 0), struct.pack('i', 1),
                           struct.pack('q', 1)):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, packed)
                kernel = bool(sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY))

                report = _socket_report([(socket.IPPROTO_TCP, socket.TCP_NODELAY, packed)])

                assert report['tcp-no-delay'] is kernel, packed

    def test_a_buffer_too_short_for_an_int_is_skipped(self):
        """
        setsockopt rejects it, so there is nothing to report for it.
        """
        report = _socket_report([(socket.IPPROTO_TCP, socket.TCP_NODELAY, b'ab')])

        assert report['tcp-no-delay'] is False

    def test_every_buffer_type_setsockopt_takes_is_read(self):
        """
        memoryview among them, which the linger group used to drop.
        """
        packed = struct.pack('ii', 1, 30)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, memoryview(packed))

        for value in (packed, bytearray(packed), memoryview(packed)):
            report = _socket_report([(socket.SOL_SOCKET, socket.SO_LINGER, value)])
            assert report['linger'] == {'interval-s': 30}, type(value)

        for value in (memoryview(struct.pack('i', 1)), b'abc'):
            report = _socket_report([(socket.SOL_SOCKET, socket.SO_LINGER, value)])
            assert 'linger' not in report, type(value)

    def test_a_buffer_size_packed_as_a_buffer(self):
        """
        Same root cause, milder symptom: a packed size used to be dropped rather
        than misread, so the option went unreported instead of wrong.
        """
        report = _socket_report([
            (socket.SOL_SOCKET, socket.SO_RCVBUF, struct.pack('i', 65536)),
            (socket.SOL_SOCKET, socket.SO_SNDBUF, struct.pack('i', 0)),
        ])

        assert report['receive-buffer'] == {'size-bytes': 65536}
        # Zero is not a size, whichever form it arrives in.
        assert 'send-buffer' not in report

    def test_packed_and_plain_values_mix(self):
        """
        Last one wins across both forms, as it does in the loop that applies
        them.
        """
        report = _socket_report([
            (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
            (socket.IPPROTO_TCP, socket.TCP_NODELAY, struct.pack('i', 0)),
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, struct.pack('i', 0)),
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        ])

        assert report['tcp-no-delay'] is False
        assert report['keep-alive'] is True

    def test_a_value_that_is_neither_reports_the_default(self):
        """
        setsockopt would reject it at connect time; there is nothing to report
        for it, and guessing enabled would be the same mistake as before.
        """
        report = _socket_report([(socket.IPPROTO_TCP, socket.TCP_NODELAY, 'yes')])

        assert report['tcp-no-delay'] is False

    def test_an_entry_that_cannot_be_a_key_is_skipped(self):
        """
        The guard has to cover recording the option, not only unpacking it: a
        level or name that cannot be a dict key raises when it is recorded, and
        an entry the user got wrong is not this module's to fail the whole
        report over.
        """
        for entry in (([1], 2, 3), (1, {}, 3)):
            report = _socket_report([entry])

            assert report == {'tcp-no-delay': False, 'keep-alive': False,
                              'reuse-address': False}, entry

        # And it costs only itself, not the entries beside it.
        report = _socket_report([([1], 2, 3),
                                 (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)])
        assert report['tcp-no-delay'] is True

    def test_sockopts_that_are_not_a_sequence_at_all(self):
        """
        _connect_socket will fail on it and there is nothing to describe, but
        the report still says what it truthfully can. Cluster() rejects a
        non-iterable, so this is reached by assigning one afterwards.
        """
        for sockopts in (5, 'nonsense'):
            report = _socket_report(sockopts)

            assert report == {'tcp-no-delay': False, 'keep-alive': False,
                              'reuse-address': False}, sockopts

    def test_a_malformed_entry_does_not_cost_the_whole_report(self):
        """
        Reporting is best effort, and this module's part of that is to leave out
        what it cannot describe rather than to take the other groups with it.
        """
        cluster = Cluster(sockopts=[([1], 2, 3)])
        self.addCleanup(cluster.shutdown)
        options = {}

        cluster._driver_config_reporter.add_startup_options(options, is_scylla=True)

        assert DRIVER_CONFIG_OPTION in options
        # The group the bad entry sits in still arrives, describing everything
        # about the connection bar the option that could not be read.
        assert 'connection' in json.loads(options[DRIVER_CONFIG_OPTION])

    def test_malformed_entries_are_skipped(self):
        """
        setsockopt also takes a (level, name, None, optlen) form, and an entry
        that is neither is the user's to get wrong when the connection applies
        it, not this module's to fail on.
        """
        report = _socket_report([
            (socket.SOL_SOCKET, socket.SO_RCVBUF, None, 4),
            'nonsense',
            None,
            (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
        ])

        assert report['tcp-no-delay'] is True


class ReconnectionPolicyReportTest(unittest.TestCase):
    def test_exponential(self):
        report = _reconnection_policy_report(ExponentialReconnectionPolicy(2.0, 60.0))

        assert report == {'type': 'exponential', 'base-ms': 2000, 'max-ms': 60000}

    def test_constant(self):
        report = _reconnection_policy_report(ConstantReconnectionPolicy(1.5))

        assert report == {'type': 'constant', 'delay-ms': 1500}

    def test_a_sub_millisecond_delay_is_not_an_immediate_one(self):
        """
        A configured wait below a millisecond is still a wait, and the schema
        reads a zero delay as "reconnect immediately", so the two must not
        report alike.
        """
        assert _reconnection_policy_report(
            ConstantReconnectionPolicy(0.0004))['delay-ms'] == 1
        assert _reconnection_policy_report(
            ConstantReconnectionPolicy(0))['delay-ms'] == 0

    def test_a_delay_that_never_comes_due_is_the_null_arm(self):
        """
        _Scheduler tests `run_at <= time.time()` and Timer `time_now >=
        self.end`, and neither is ever true of an infinite delay or of a nan --
        so the reconnection is queued and never run, which is the same thing the
        null arm says about a schedule that yields nothing.

        Reporting a delay instead would have to invent a number, and the field
        is a positiveInteger with no room for the one that was configured.
        """
        for delay in (float('inf'), float('nan')):
            assert _reconnection_policy_report(
                ConstantReconnectionPolicy(delay)) is None, delay
            assert _reconnection_policy_report(
                ExponentialReconnectionPolicy(delay, delay)) is None, delay

    def test_a_delay_already_past_reconnects_at_once(self):
        """
        `time.time() + float('-inf')` is behind every reading of the clock, so
        the timer fires at the first opportunity rather than never -- the
        opposite of the case above, and the schema says it with a delay of zero.

        The constructors reject a negative, so this is reached by assignment.
        """
        policy = ConstantReconnectionPolicy(1.0)
        policy.delay = float('-inf')

        assert _reconnection_policy_report(policy) == {'type': 'constant',
                                                       'delay-ms': 0}

    def test_a_finite_delay_is_still_reported(self):
        """The guard above is not so eager that it takes ordinary policies."""
        assert _reconnection_policy_report(
            ConstantReconnectionPolicy(1.5)) == {'type': 'constant', 'delay-ms': 1500}

    def test_delays_given_the_wrong_way_round(self):
        """
        _add_jitter clamps every delay with
        `min(max(base_delay, delay), max_delay)`, so max_delay wins when the two
        are inverted and the schedule is flat at it. Reporting base_delay would
        claim a first delay never waited, and would emit base-ms above max-ms --
        which the schema forbids the producer to do and cannot itself catch,
        being a comparison between siblings.

        The constructor rejects the pair, so this is reached by assignment.
        """
        policy = ExponentialReconnectionPolicy(1.0, 60.0)
        policy.base_delay, policy.max_delay = 100.0, 1.0

        assert list(islice(policy.new_schedule(), 3)) == [1.0, 1.0, 1.0]

        report = _reconnection_policy_report(policy)

        assert report['base-ms'] == 1000
        assert report['max-ms'] == 1000

    def test_the_delay_window_is_never_inverted(self):
        for base, maximum in ((1.0, 60.0), (100.0, 1.0), (5.0, 5.0), (0.0004, 0.0009)):
            policy = ExponentialReconnectionPolicy(1.0, 60.0)
            policy.base_delay, policy.max_delay = base, maximum

            report = _reconnection_policy_report(policy)

            assert report['max-ms'] >= report['base-ms'], (base, maximum)

    def test_a_constant_delay_of_zero_is_reported(self):
        """
        nonNegativeInteger here: zero means reconnect immediately, which is a
        setting rather than the absence of one.
        """
        report = _reconnection_policy_report(ConstantReconnectionPolicy(0))

        assert report['delay-ms'] == 0

    def test_max_attempts(self):
        report = _reconnection_policy_report(ConstantReconnectionPolicy(1, max_attempts=5))

        assert report['max-attempts'] == 5

    def test_unlimited_attempts_are_left_out(self):
        """
        None means unlimited to both policies, and so does zero to the constant
        one: its `if self.max_attempts` is falsy for zero and falls through to
        an unbounded repeat.
        """
        for policy in (ConstantReconnectionPolicy(1, max_attempts=None),
                       ConstantReconnectionPolicy(1, max_attempts=0),
                       ExponentialReconnectionPolicy(1.0, 2.0, max_attempts=None)):
            assert 'max-attempts' not in _reconnection_policy_report(policy)

    def test_an_exponential_policy_that_never_attempts_is_reported_as_no_policy(self):
        """
        The two policies read a max_attempts of zero in opposite ways. The
        exponential one drives
        `while max_attempts is None or i < max_attempts`, so zero yields nothing
        and the driver never reconnects -- the schema's null arm. Reporting it
        as an exponential policy with max-attempts left out would say the
        opposite, since absent reads as unlimited.
        """
        assert list(ExponentialReconnectionPolicy(1.0, 2.0, max_attempts=0).new_schedule()) == []
        assert _reconnection_policy_report(
            ExponentialReconnectionPolicy(1.0, 2.0, max_attempts=0)) is None

        # The constant policy really is unlimited at zero, so it keeps its arm.
        assert next(ConstantReconnectionPolicy(1, max_attempts=0).new_schedule()) == 1
        assert _reconnection_policy_report(
            ConstantReconnectionPolicy(1, max_attempts=0))['type'] == 'constant'

    def test_a_policy_that_never_reconnects_reports_null(self):
        """
        The null arm reached from a real configuration rather than from no
        policy at all. That it survives schema validation is asserted where the
        report is a whole conformant document -- see ReportConformsToTheSchemaTest
        -- which it is not yet at this point in the series.
        """
        report = json.loads(report_text(
            self, reconnection_policy=ExponentialReconnectionPolicy(1.0, 2.0, max_attempts=0)))

        assert report['connection']['reconnection']['policy'] is None

    def test_an_exponential_policy_with_no_base_delay_is_constant(self):
        """
        The schedule is base_delay * 2 ** i, so a base of zero stays zero
        however high max_delay is: the driver reconnects immediately, every
        time. Reporting the exponential arm would claim a delay that grows, and
        its base is a positiveInteger that cannot hold the zero anyway.
        """
        schedule = list(islice(ExponentialReconnectionPolicy(0, 60.0).new_schedule(), 6))
        assert schedule == [0, 0, 0, 0, 0, 0]

        assert _reconnection_policy_report(
            ExponentialReconnectionPolicy(0, 60.0)) == {'type': 'constant', 'delay-ms': 0}

    def test_a_limit_that_stops_the_schedule_is_the_null_arm(self):
        """
        Zero, a negative and a nan all leave the schedule yielding nothing, so
        the driver never reconnects -- the null arm. Leaving max-attempts out
        would say the opposite, since the schema reads its absence as unlimited.

        A nan reaches the constructor unchallenged, since `nan < 0` is false
        just as every other comparison against one is; a negative is reached by
        assignment.
        """
        for limit in (0, float('nan')):
            policy = ExponentialReconnectionPolicy(1.0, 2.0, max_attempts=limit)
            assert list(policy.new_schedule()) == [], limit

            assert _reconnection_policy_report(policy) is None, limit

        assigned = ExponentialReconnectionPolicy(1.0, 2.0, max_attempts=1)
        assigned.max_attempts = -1
        assert list(assigned.new_schedule()) == []
        assert _reconnection_policy_report(assigned) is None

    def test_a_limit_the_schedule_cannot_compare_against_is_the_null_arm(self):
        """
        The probe runs the policy's own `i < max_attempts`, so a limit that is
        not a number raises rather than answering. It is still a schedule that
        yields nothing, and for the same reason it must not cost the report: the
        TypeError comes out of _ReconnectionHandler.start too, which pulls the
        first delay with a bare next(), so no attempt is ever scheduled.

        Absent max-attempts would say the opposite -- this schema reads it as
        unlimited -- and it is what _attempt_ceiling makes of such a limit.
        """
        policy = ExponentialReconnectionPolicy(1.0, 2.0, max_attempts=1)
        policy.max_attempts = 'lots'

        with pytest.raises(TypeError):
            list(policy.new_schedule())

        assert _reconnection_policy_report(policy) is None

    def test_such_a_limit_does_not_cost_the_rest_of_the_report(self):
        """
        The whole point of answering rather than raising: one unnameable limit
        must not take the groups it has nothing to do with.
        """
        policy = ExponentialReconnectionPolicy(1.0, 2.0, max_attempts=1)
        policy.max_attempts = 'lots'

        report = json.loads(report_text(self, reconnection_policy=policy))

        assert report['connection']['reconnection']['policy'] is None
        # The rest of the group came out: the limit costs its own key and no
        # other.
        assert report['connection']['requests']['in-flight']['max'] > 0

    def test_an_unlimited_schedule_is_not_the_null_arm(self):
        """
        The distinction the probe exists for: float('inf') fails the same
        arithmetic that nan does -- no integer names either -- but it means the
        opposite, and the schedule is what tells them apart.
        """
        for limit in (None, float('inf')):
            policy = ExponentialReconnectionPolicy(1.0, 2.0, max_attempts=limit)
            assert len(list(islice(policy.new_schedule(), 6))) == 6, limit

            report = _reconnection_policy_report(policy)

            assert report is not None, limit
            assert 'max-attempts' not in report, limit

    def test_never_reconnecting_still_wins_over_a_zero_base(self):
        """
        Zero attempts means the schedule is empty, which the null arm says and
        a constant delay of zero would contradict.
        """
        assert _reconnection_policy_report(
            ExponentialReconnectionPolicy(0, 60.0, max_attempts=0)) is None

    def test_a_fractional_exponential_limit_is_finite(self):
        """
        new_schedule loops `while max_attempts is None or i < max_attempts`,
        which compares against a fraction as readily as an integer: 1.5 admits
        an i of 0 and of 1, so two attempts are made. Leaving the key out would
        report that as unlimited.
        """
        for limit, attempts in ((0.5, 1), (1.5, 2), (2.5, 3)):
            policy = ExponentialReconnectionPolicy(1.0, 60.0, max_attempts=limit)
            assert len(list(policy.new_schedule())) == attempts, limit

            assert _reconnection_policy_report(policy)['max-attempts'] == attempts, limit

    def test_a_fractional_constant_limit_has_no_count_to_report(self):
        """
        The two policies read the same attribute with different code and
        disagree about the same value, which is why the limit is read per
        policy. This one hands max_attempts to itertools.repeat, which takes
        only an integer, so a fraction is a policy that raises when it
        reconnects rather than one that counts.
        """
        policy = ConstantReconnectionPolicy(1.0, max_attempts=1.5)
        with pytest.raises(TypeError):
            policy.new_schedule()

        assert 'max-attempts' not in _reconnection_policy_report(policy)

    def test_a_limit_of_any_countable_type_is_reported(self):
        """
        The exponential schedule compares `i < max_attempts`, which works
        against anything an integer can be compared with, so a limit need not be
        a builtin number to bound it. Reporting only int and float left these
        finite schedules described as unlimited.
        """
        for limit, attempts in ((Decimal('2'), 2), (Fraction(3, 2), 2), (True, 1)):
            policy = ExponentialReconnectionPolicy(1.0, 60.0, max_attempts=limit)
            assert len(list(policy.new_schedule())) == attempts, limit

            assert _reconnection_policy_report(policy)['max-attempts'] == attempts, limit

    def test_a_constant_limit_is_whatever_repeat_accepts(self):
        """
        new_schedule hands max_attempts to itertools.repeat, and what that
        accepts is not the same on every interpreter: CPython wants __index__
        and rejects a Decimal, PyPy takes one and counts it. So the report is
        checked against the schedule the policy actually produces rather than
        against either interpreter's rule -- a driver on PyPy really does
        reconnect twice where the same configuration raises on CPython.

        A bool is one repeat either way, reported as the number 1, since the
        schema wants an integer and JSON true is not one.
        """
        report = _reconnection_policy_report(
            ConstantReconnectionPolicy(1.0, max_attempts=True))
        assert report['max-attempts'] == 1
        assert 'true' not in json.dumps(report)

        for limit in (Decimal('2'), Fraction(3, 2), 3):
            policy = ConstantReconnectionPolicy(1.0, max_attempts=limit)
            try:
                attempts = len(list(policy.new_schedule()))
            except TypeError:
                # The policy raises when it reconnects: no count to report.
                attempts = None

            report = _reconnection_policy_report(
                ConstantReconnectionPolicy(1.0, max_attempts=limit))

            if attempts is None:
                assert 'max-attempts' not in report, limit
            else:
                assert report['max-attempts'] == attempts, limit

    def test_a_limit_that_makes_an_empty_schedule_never_reconnects(self):
        """
        A negative limit is truthy, so new_schedule passes it to repeat and gets
        an empty schedule back -- on every interpreter. The driver never
        reconnects, which is the null arm; leaving max-attempts out would say
        unlimited. Only reachable by assignment, since the constructor rejects a
        negative.
        """
        policy = ConstantReconnectionPolicy(1.0, max_attempts=1)
        policy.max_attempts = -5

        assert list(policy.new_schedule()) == []
        assert _reconnection_policy_report(policy) is None

    def test_reported_counts_are_builtin_ints(self):
        """
        Whatever type the limit arrived as, what goes on the wire is a JSON
        number.
        """
        for policy in (ExponentialReconnectionPolicy(1.0, 60.0, max_attempts=Decimal('2')),
                       ConstantReconnectionPolicy(1.0, max_attempts=True)):
            attempts = _reconnection_policy_report(policy)['max-attempts']
            assert type(attempts) is int, policy

    def test_no_policy(self):
        assert _reconnection_policy_report(None) is None

    def test_a_custom_policy_is_named_and_nothing_more(self):
        class SecretiveReconnectionPolicy(ReconnectionPolicy):
            def __init__(self):
                self.password = 'hunter2'

            def new_schedule(self):
                return iter(())

        report = _reconnection_policy_report(SecretiveReconnectionPolicy())

        assert report == {'type': 'custom', 'name': 'SecretiveReconnectionPolicy'}

    def test_a_subclass_of_a_built_in_is_custom(self):
        """
        Dispatch is on the exact type: a subclass is a policy the driver knows
        nothing about, and describing it as its parent would put the parent's
        parameters against behaviour it does not have.
        """
        class Tweaked(ExponentialReconnectionPolicy):
            pass

        report = _reconnection_policy_report(Tweaked(1.0, 2.0))

        assert report == {'type': 'custom', 'name': 'Tweaked'}


class TlsReportTest(unittest.TestCase):
    def report(self, **cluster_kwargs):
        return connection_report(self, **cluster_kwargs).get('tls')

    def test_absent_when_tls_is_not_configured(self):
        assert self.report() is None

    def test_hostname_verification_from_an_ssl_context(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        assert self.report(ssl_context=context) == {'hostname-verification': False}

        verifying = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        assert verifying.check_hostname
        assert self.report(ssl_context=verifying) == {'hostname-verification': True}

    def test_hostname_verification_from_ssl_options(self):
        """
        Options on their own are turned into a context by the connection, which
        reads the same key this does.
        """
        assert self.report(ssl_options={'check_hostname': True}) == {'hostname-verification': True}
        assert self.report(ssl_options={'ca_certs': '/dev/null'}) == {'hostname-verification': False}

    def test_no_credentials_are_reported(self):
        """
        The schema is explicit that this group carries booleans only, never
        credentials, keys or host lists.
        """
        report = self.report(ssl_options={'check_hostname': True,
                                          'keyfile': '/secret/key.pem',
                                          'certfile': '/secret/cert.pem',
                                          'ca_certs': '/secret/ca.pem'})

        assert report == {'hostname-verification': True}


def control_plane_report(test, is_scylla=True, **cluster_kwargs):
    built = report_cluster(test, **cluster_kwargs)
    report = built._driver_config_reporter._build_report(built, is_scylla=is_scylla)
    return json.loads(report)['control-plane']


class ControlPlaneGroupTest(unittest.TestCase):
    def test_defaults(self):
        assert control_plane_report(self) == {
            'queries': {'system': {'timeout': {'client-side-ms': 2000,
                                               'server-side-ms': 2000}}},
            'schema': {'agreement': {'timeout-ms': 10000}},
        }

    def test_client_side_timeout(self):
        report = control_plane_report(self, control_connection_timeout=4.5)

        assert report['queries']['system']['timeout']['client-side-ms'] == 4500

    def test_client_side_timeout_is_left_out_when_disabled(self):
        report = control_plane_report(self, control_connection_timeout=0)

        assert 'client-side-ms' not in report['queries']['system']['timeout']

    def test_server_side_timeout_defaults_to_the_client_side_one(self):
        """
        Which is what the Cluster does with it when it is not given one.
        """
        report = control_plane_report(self, control_connection_timeout=3)

        assert report['queries']['system']['timeout'] == {'client-side-ms': 3000,
                                                          'server-side-ms': 3000}

    def test_server_side_timeout(self):
        report = control_plane_report(self, metadata_request_timeout=8)

        assert report['queries']['system']['timeout']['server-side-ms'] == 8000

    def test_the_server_side_timeout_is_the_clause_the_driver_sends(self):
        """
        This one value does not go through the usual conversion. What reaches
        the server is whatever maybe_add_timeout_to_query builds, and that
        divides a timedelta into whole milliseconds, truncating, and appends no
        clause at all when it comes to zero. Rounding up or promoting a
        sub-millisecond value -- as every other duration here is -- would report
        a limit the server is never given.

        Checked against the builder rather than against numbers written out
        here, so the two cannot drift apart.
        """
        for seconds in (0.0004, 0.0006, 0.001, 0.0016, 0.002, 0.0025, 1.005, 2, 0):
            statement = maybe_add_timeout_to_query(
                'SELECT 1', datetime.timedelta(seconds=seconds))
            sent = (int(statement.split('USING TIMEOUT ')[1][:-2])
                    if 'USING TIMEOUT' in statement else None)

            timeout = control_plane_report(
                self, metadata_request_timeout=seconds)['queries']['system']['timeout']

            assert timeout.get('server-side-ms') == sent, seconds

    def test_a_negative_server_side_timeout_is_left_out(self):
        """
        The builder does append it, but the clause is malformed and the server
        rejects it, and server-side-ms is a positiveInteger with nowhere to put
        a negative.
        """
        timeout = control_plane_report(
            self, metadata_request_timeout=-0.005)['queries']['system']['timeout']

        assert 'server-side-ms' not in timeout

    def test_no_server_side_timeout_against_a_non_scylla_node(self):
        """
        USING TIMEOUT is a ScyllaDB extension, so elsewhere the driver does not
        append it and there is no server-side limit to report. The report
        describes what the driver will do, not only what it was configured to.
        """
        report = control_plane_report(self, is_scylla=False, metadata_request_timeout=8)

        assert 'server-side-ms' not in report['queries']['system']['timeout']
        # The client-side timeout is the driver's own and applies regardless.
        assert 'client-side-ms' in report['queries']['system']['timeout']

    def test_no_server_side_timeout_when_disabled(self):
        """
        Zero means the driver appends no USING TIMEOUT and the server's own
        default applies, so there is no limit of the driver's to report.
        """
        report = control_plane_report(self, metadata_request_timeout=0)

        assert 'server-side-ms' not in report['queries']['system']['timeout']

    def test_both_timeouts_can_be_absent(self):
        """
        The group stays, since the schema requires it; it is the timeouts inside
        that are optional.
        """
        report = control_plane_report(self, control_connection_timeout=0,
                                      metadata_request_timeout=0)

        assert report['queries'] == {'system': {'timeout': {}}}

    def test_schema_agreement_timeout(self):
        report = control_plane_report(self, max_schema_agreement_wait=25)

        assert report['schema']['agreement']['timeout-ms'] == 25000

    def test_a_sub_millisecond_wait_is_still_a_wait(self):
        """
        _wait_for_schema_agreement bypasses agreement only for a timeout of zero
        or less, so a sub-millisecond wait is one the driver really takes.
        Truncating it to zero would report the bypass instead.
        """
        report = control_plane_report(self, max_schema_agreement_wait=0.0004)

        assert report['schema']['agreement']['timeout-ms'] == 1

    def test_not_waiting_for_schema_agreement_is_a_value(self):
        """
        nonNegativeInteger: zero says the driver does not wait, which is a
        setting rather than the absence of one, so the key stays.
        """
        report = control_plane_report(self, max_schema_agreement_wait=0)

        assert report['schema']['agreement']['timeout-ms'] == 0

    def test_a_wait_the_schema_cannot_express_drops_the_report(self):
        """
        The one that stays a failure. schema.agreement.timeout-ms is required,
        so a wait of no describable length has no conformant document to appear
        in and the report is dropped -- deliberately, and with a message that
        says which kind of value did it.
        """
        cluster = Cluster(max_schema_agreement_wait=float('inf'))
        self.addCleanup(cluster.shutdown)

        with pytest.raises(ValueError, match='cannot be reported'):
            cluster._driver_config_reporter._build_report(cluster, is_scylla=True)

        options = {}
        cluster._driver_config_reporter.add_startup_options(options, is_scylla=True)
        assert DRIVER_CONFIG_OPTION not in options


def full_report(test, is_scylla=True, **cluster_kwargs):
    """
    The parsed report, validated on the way through.

    Every configuration any test here builds is one this driver may really send,
    so each of them is a conformance case too -- cheaper and harder to forget
    than adding one to ReportConformsToTheSchemaTest by hand.
    """
    return validate_report(report_text(test, is_scylla=is_scylla, **cluster_kwargs))


def query_report(test, profile=None, **cluster_kwargs):
    if profile is not None:
        cluster_kwargs['execution_profiles'] = {EXEC_PROFILE_DEFAULT: profile}
    return full_report(test, **cluster_kwargs)['query']


class QueryDefaultsTest(unittest.TestCase):
    def test_defaults(self):
        assert query_report(self)['defaults'] == {
            'consistency': 'LOCAL_ONE',
            'idempotence': False,
            'request': {'timeout-ms': 10000},
            'page': {'size': 5000},
            'client-timestamps': True,
        }

    def test_consistency_is_reported_by_name(self):
        """
        The wire form is an integer and the schema wants the name.
        """
        report = query_report(self, ExecutionProfile(consistency_level=ConsistencyLevel.QUORUM))

        assert report['defaults']['consistency'] == 'QUORUM'

    def test_every_consistency_level_has_a_name_the_schema_accepts(self):
        """
        The driver's levels and the schema's enum have to stay in step: a level
        this driver has and the schema does not would be reported and rejected.
        """
        schema = load_schema()
        accepted = schema['$defs']['query-defaults']['properties']['consistency']['enum']

        assert set(ConsistencyLevel.value_to_name.values()) == set(accepted)

    def test_serial_consistency(self):
        report = query_report(self, ExecutionProfile(
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL))

        assert report['defaults']['serial-consistency'] == 'LOCAL_SERIAL'

    def test_serial_consistency_is_left_out_when_unset(self):
        """
        Unset means the server's own default applies, which is not this driver's
        to describe.
        """
        assert 'serial-consistency' not in query_report(self)['defaults']

    def test_a_level_that_is_not_serial_is_left_out_and_warned_about(self):
        """
        ExecutionProfile validates the argument its constructor is given and
        leaves the attribute writable, so a non-serial level is reachable. The
        schema's enum here is the two serial levels, and naming one anyway is
        the only way a live Cluster could produce a document the shared contract
        rejects.

        Warned rather than passed over: absence in this field means the server's
        default applies, which is not what is happening, and the key being
        optional is the only reason this does not take the whole report down the
        way an unnameable consistency does.
        """
        profile = ExecutionProfile()
        cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
        self.addCleanup(cluster.shutdown)
        profile.serial_consistency_level = ConsistencyLevel.QUORUM

        with self.assertLogs('cassandra.driver_config', level='WARNING') as captured:
            report = validate_report(
                cluster._driver_config_reporter._build_report(cluster, is_scylla=True))

        assert 'serial-consistency' not in report['query']['defaults']
        assert 'serial_consistency_level is 4' in '\n'.join(
            r.getMessage() for r in captured.records)

    def test_request_timeout(self):
        report = query_report(self, ExecutionProfile(request_timeout=2.5))

        assert report['defaults']['request'] == {'timeout-ms': 2500}

    def test_request_timeout_is_left_out_when_disabled(self):
        report = query_report(self, ExecutionProfile(request_timeout=None))

        assert 'request' not in report['defaults']

    def test_idempotence_is_always_false(self):
        """
        This driver has no configurable default: a statement is not idempotent
        unless it says so, and nothing at cluster or profile level changes that.
        """
        assert query_report(self)['defaults']['idempotence'] is False

    def test_a_page_size_of_any_integer_type_is_reported(self):
        """
        A page size is packed into the request as an integer, which takes
        anything with __index__, so a numpy integer paginates exactly as a
        builtin one does -- reporting nothing for it says paging is unlimited.
        A bool is one row per page, and is reported as the number 1, since the
        schema wants an integer and JSON true is not one.
        """
        for value, expected in ((numpy.int64(123), 123), (True, 1), (5000, 5000)):
            with mock.patch.object(Session, 'default_fetch_size', value):
                report = query_report(self)['defaults']

            assert report['page'] == {'size': expected}, value
            assert type(report['page']['size']) is int, value

    def test_a_page_size_that_limits_nothing_is_left_out(self):
        for value in (None, 0, 2.5):
            with mock.patch.object(Session, 'default_fetch_size', value):
                report = query_report(self)['defaults']

            assert 'page' not in report, value

    def test_client_timestamps(self):
        """
        The default generator assigns the timestamp client-side.
        """
        assert query_report(self)['defaults']['client-timestamps'] is True

    def test_client_timestamps_are_off_when_the_session_does_not_use_them(self):
        """
        use_client_timestamp gates whether the generator is consulted at all, so
        with it off the coordinator assigns every timestamp whatever generator
        the cluster holds. Read off the class for the same reason as the page
        size: it is a session setting, and no session exists yet.
        """
        with mock.patch.object(Session, 'use_client_timestamp', False):
            report = query_report(self)

        assert report['defaults']['client-timestamps'] is False

    def test_client_timestamps_are_unknown_with_a_custom_generator(self):
        """
        A custom generator is called per request and may return None for some of
        them, leaving the coordinator to assign the timestamp after all. The
        schema's way of saying that is to leave the key out.
        """
        report = query_report(self, timestamp_generator=lambda: 1234)

        assert 'client-timestamps' not in report['defaults']

    def test_client_timestamps_are_unknown_with_no_generator_at_all(self):
        """
        Reachable by assignment, since the constructor substitutes the default
        for a None. Reported as unknown rather than False, which would say the
        coordinator assigns them: Session._create_response_future calls the
        generator unconditionally under this setting, so what really happens is
        that every request raises a TypeError. The rest of the report still goes
        out, because this key is optional.
        """
        built = Cluster()
        self.addCleanup(built.shutdown)
        built.timestamp_generator = None

        report = validate_report(
            built._driver_config_reporter._build_report(built, is_scylla=True))

        assert 'client-timestamps' not in report['query']['defaults']
        assert report['query']['defaults']['consistency'] == 'LOCAL_ONE'


class RetryReportTest(unittest.TestCase):
    def test_built_in_policies(self):
        for policy, expected in (
                (RetryPolicy(), 'standard-error-aware'),
                (FallthroughRetryPolicy(), 'fallthrough'),
                (NeverRetryPolicy(), 'never'),
                (DowngradingConsistencyRetryPolicy(), 'downgrading-consistency')):
            assert _retry_report(policy, 'retry_policy') == {'policy': {'type': expected}}, policy

    def test_no_policy_is_not_a_fallthrough(self):
        """
        The fallthrough arm means the driver rethrows the original error to the
        caller untouched. With no policy at all, ResponseFuture calls
        on_request_error on None and raises AttributeError instead, losing the
        original error -- so naming it fallthrough would describe a working
        configuration where there is a broken one.

        ExecutionProfile replaces a None its constructor is given, but both it
        and Cluster.default_retry_policy stay writable, so this is reachable.
        """
        profile = ExecutionProfile()
        assert profile.retry_policy is not None      # the constructor replaced it
        profile.retry_policy = None                  # but nothing stops this

        with pytest.raises(AttributeError):
            profile.retry_policy.on_request_error(None, 1, error=None, retry_num=0)

        with pytest.raises(ValueError, match='retry_policy is None'):
            _retry_report(None, 'retry_policy')

    def test_a_report_is_dropped_rather_than_naming_a_policy_that_is_not_used(self):
        """
        policy is a required key, so there is no conformant document for such a
        configuration -- the whole report goes, as it does for a consistency
        level the driver cannot name.
        """
        cluster = Cluster()
        self.addCleanup(cluster.shutdown)
        cluster.profile_manager.default.retry_policy = None
        options = {}

        cluster._driver_config_reporter.add_startup_options(options, is_scylla=True)

        assert DRIVER_CONFIG_OPTION not in options

    def test_dispatch_is_on_the_exact_type(self):
        """
        Every built-in above is a subclass of RetryPolicy, so isinstance would
        report all of them as the standard policy. This is the mistake the
        mapping is most likely to make.
        """
        assert _retry_report(FallthroughRetryPolicy(), 'retry_policy')['policy']['type'] == 'fallthrough'

        class Tweaked(FallthroughRetryPolicy):
            pass

        assert _retry_report(Tweaked(), 'retry_policy')['policy'] == {'type': 'custom', 'name': 'Tweaked'}

    def test_exponential_backoff_is_the_standard_policy_with_a_backoff(self):
        """
        It retries what the standard policy retries and adds a growing delay,
        which is what the schema's backoff describes.
        """
        report = _retry_report(ExponentialBackoffRetryPolicy(
            max_num_retries=4, min_interval=0.2, max_interval=5.0), 'retry_policy')

        assert report == {
            'policy': {'type': 'standard-error-aware', 'max-retries': 4},
            'backoff': {'type': 'exponential', 'base-ms': 200, 'max-ms': 5000},
        }

    def test_intervals_given_the_wrong_way_round(self):
        """
        _calculate_backoff caps the whole curve at max_interval, so the initial
        delay is min(max_interval, min_interval) and not min_interval. The policy
        does not check the order, so reporting min_interval would claim a first
        delay it never waits. That also keeps the schema's requirement that
        max-ms be at least base-ms true by construction.
        """
        policy = ExponentialBackoffRetryPolicy(
            max_num_retries=1, min_interval=10.0, max_interval=1.0)
        # The un-jittered curve is flat at max_interval, never at min_interval.
        assert [min(1.0, 10.0 * 2 ** a) for a in range(4)] == [1.0, 1.0, 1.0, 1.0]

        report = _retry_report(policy, 'retry_policy')

        assert report['backoff']['base-ms'] == 1000
        assert report['backoff']['max-ms'] == 1000

    def test_a_backoff_maximum_is_never_below_its_base(self):
        for mi, mx in ((10.0, 1.0), (0.1, 10.0), (5.0, 5.0), (0.0004, 0.0009)):
            backoff = _retry_report(
                ExponentialBackoffRetryPolicy(1, mi, mx), 'retry_policy')['backoff']
            assert backoff['max-ms'] >= backoff['base-ms'], (mi, mx)

    def test_a_backoff_that_never_delays_is_left_out(self):
        """
        _calculate_backoff is min(max_interval, min_interval * 2 ** attempt)
        plus jitter scaled by min_interval, so a min_interval of zero is zero at
        every attempt whatever max_interval says. The schema leaves backoff out
        for exactly that, and rejects a delay of zero inside it.
        """
        policy = ExponentialBackoffRetryPolicy(3, min_interval=0, max_interval=10.0)
        assert [policy._calculate_backoff(a) for a in range(4)] == [0, 0, 0, 0]

        report = _retry_report(policy, 'retry_policy')

        assert 'backoff' not in report
        # The policy itself is still described, retries and all.
        assert report['policy'] == {'type': 'standard-error-aware', 'max-retries': 3}

    def test_no_retries_is_a_value(self):
        """
        max-retries is a nonNegativeInteger whose zero the schema spells "no
        retries", so unlike the counts elsewhere in the report this one says
        what it means and needs no special case.
        """
        assert ExponentialBackoffRetryPolicy(0, 0.1, 1.0).on_read_timeout(
            None, 1, 1, 1, False, 0)[0] == RetryPolicy.RETHROW
        assert _retry_report(
            ExponentialBackoffRetryPolicy(0, 0.1, 1.0), 'retry_policy')['policy']['max-retries'] == 0
        # Negative counts mean the same thing and cannot be reported as such.
        assert _retry_report(
            ExponentialBackoffRetryPolicy(-2, 0.1, 1.0), 'retry_policy')['policy']['max-retries'] == 0

    def test_a_fractional_retry_limit_rounds_up(self):
        """
        Every on_* method gives up once retry_num reaches max_num_retries, and
        the comparison is `<`, so 0.5 still permits one retry. Truncating
        reports zero, which the schema reads as no retries at all -- the
        opposite of what the policy does. The attribute is typed float, so
        fractions are an expected input rather than an abuse.
        """
        for limit, retries in ((0.5, 1), (1.5, 2), (2.5, 3)):
            policy = ExponentialBackoffRetryPolicy(limit, 0.1, 1.0)
            permitted = sum(
                policy.on_read_timeout(None, 1, 1, 1, False, n)[0] == RetryPolicy.RETRY
                for n in range(10))
            assert permitted == retries, limit

            assert _retry_report(policy, 'retry_policy')['policy']['max-retries'] == retries, limit

    def test_a_retry_limit_no_integer_can_express_leaves_the_key_out(self):
        """
        max_num_retries is typed float, so float('inf') is how an application
        says "retry until the request runs out of time" -- and the policy really
        does honour it, since every on_* method only ever compares against it.
        No integer names that limit, and an absent max-retries is the schema's
        way of saying none was configured, which is the closest true thing.
        """
        policy = ExponentialBackoffRetryPolicy(float('inf'), 0.1, 1.0)
        assert all(policy.on_read_timeout(None, 1, 1, 1, False, n)[0] == RetryPolicy.RETRY
                   for n in range(100))

        assert 'max-retries' not in _retry_report(policy, 'retry_policy')['policy']
        assert _retry_report(policy, 'retry_policy')['policy']['type'] == 'standard-error-aware'

    def test_an_unnameable_retry_limit_does_not_cost_the_rest_of_the_report(self):
        """
        The regression this guards: math.ceil raises OverflowError on inf and
        TypeError on anything that is not a number, and one unnameable limit
        used to take every other group down with it -- the connection settings,
        the control-plane timeouts, all of it.
        """
        for limit in (float('inf'), float('nan'), None, 'lots'):
            report = full_report(self, execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(
                retry_policy=ExponentialBackoffRetryPolicy(limit, 0.1, 1.0))})

            assert 'max-retries' not in report['query']['retry']['policy'], limit
            assert report['connection']['connect']['timeout-ms'] == 5000, limit

    def test_a_custom_policy_is_named_and_nothing_more(self):
        class SecretiveRetryPolicy(RetryPolicy):
            def __init__(self):
                self.password = 'hunter2'

        assert _retry_report(SecretiveRetryPolicy(), 'retry_policy') == {
            'policy': {'type': 'custom', 'name': 'SecretiveRetryPolicy'}}

    def test_a_backoff_interval_that_is_not_finite_is_left_out(self):
        """
        backoff is optional, and is already left out when there is no delay to
        describe. An interval that is not finite is the same case one step on:
        _calculate_backoff returns it or something built from it, and a retry
        scheduled that far out is one the driver never reaches. The policy
        itself is still reported -- only the delay is undescribable.
        """
        for interval in (float('inf'), float('nan')):
            report = _retry_report(
                ExponentialBackoffRetryPolicy(3, interval, interval),
                'default_retry_policy')
            assert 'backoff' not in report, interval
            assert report['policy']['type'] == 'standard-error-aware', interval


class LoadBalancingReportTest(unittest.TestCase):
    def test_token_aware_over_a_datacenter_aware_child(self):
        report = _load_balancing_report(TokenAwarePolicy(DCAwareRoundRobinPolicy('dc1')))

        assert report == {
            'policy': {'type': 'token-aware', 'load-distribution': 'shuffle',
                       'fallback-to-non-preferred-nodes': False},
            'node-preference': {'type': 'dc', 'local-dc': 'dc1'},
        }

    def test_load_distribution_follows_replica_shuffling(self):
        policy = TokenAwarePolicy(DCAwareRoundRobinPolicy('dc1'), shuffle_replicas=False)

        assert _load_balancing_report(policy)['policy']['load-distribution'] == 'replica-set'

    def test_fallback_to_non_preferred_nodes(self):
        """
        The datacenter-aware policies ignore remote hosts entirely until they
        are told how many to use.
        """
        policy = TokenAwarePolicy(DCAwareRoundRobinPolicy('dc1', used_hosts_per_remote_dc=2))

        assert _load_balancing_report(policy)['policy']['fallback-to-non-preferred-nodes'] is True

    def test_only_the_preferences_this_driver_reports_are_handled(self):
        """
        The schema has a rack-auto arm and this driver never produces it:
        RackAwareRoundRobinPolicy takes both the datacenter and the rack as
        mandatory constructor arguments and never infers either. Pinned so that
        the flag's handling stays matched to what is actually reported.
        """
        class Host:
            datacenter, rack, endpoint = 'inferred', 'r', 'e'

        inferred = DCAwareRoundRobinPolicy()
        inferred.on_up(Host())

        emitted = set()
        for policy in (DCAwareRoundRobinPolicy(), DCAwareRoundRobinPolicy('dc1'),
                       DCAwareRoundRobinPolicy(''), inferred,
                       RackAwareRoundRobinPolicy('dc1', 'rack1'),
                       RackAwareRoundRobinPolicy('dc1', ''),
                       RackAwareRoundRobinPolicy('', 'rack1'),
                       RackAwareRoundRobinPolicy('', ''),
                       RoundRobinPolicy(), None):
            reported = _node_location_preference_report(policy)
            emitted.add(reported['type'] if reported else None)

        assert emitted == {'dc', 'dc-auto', 'rack', None}

    def test_a_rack_preference_always_falls_back(self):
        """
        RackAwareRoundRobinPolicy's query plan yields the local datacenter's
        other racks straight after the local-rack tier, unconditionally --
        used_hosts_per_remote_dc gates only the remote datacenters below that.
        So a request routinely reaches a node the reported rack preference
        excludes, whatever that setting says.
        """
        for remote in (0, 2):
            policy = TokenAwarePolicy(
                RackAwareRoundRobinPolicy('dc1', 'rack1', used_hosts_per_remote_dc=remote))
            report = _load_balancing_report(policy)

            assert report['node-preference']['type'] == 'rack'
            assert report['policy']['fallback-to-non-preferred-nodes'] is True, remote

    def test_the_rack_tier_really_is_unconditional(self):
        """
        The premise of the test above, read off the policy rather than assumed:
        with no remote hosts allowed, a host in the local datacenter but another
        rack is still in the query plan.
        """
        policy = RackAwareRoundRobinPolicy('dc1', 'rack1', used_hosts_per_remote_dc=0)
        local = Host(DefaultEndPoint(1), SimpleConvictionPolicy, 'dc1', 'rack1',
                     host_id=uuid.uuid4())
        other_rack = Host(DefaultEndPoint(2), SimpleConvictionPolicy, 'dc1', 'rack2',
                          host_id=uuid.uuid4())
        policy.populate(Mock(), [local, other_rack])

        assert other_rack in list(policy.make_query_plan())

    def test_a_rack_aware_policy_without_a_rack_is_judged_as_a_datacenter_one(self):
        """
        It reports a datacenter preference, so the flag has to be answered
        against that: other racks are inside the preference, not outside it.
        """
        for remote, expected in ((0, False), (2, True)):
            policy = TokenAwarePolicy(
                RackAwareRoundRobinPolicy('dc1', '', used_hosts_per_remote_dc=remote))
            report = _load_balancing_report(policy)

            assert report['node-preference']['type'] == 'dc'
            assert report['policy']['fallback-to-non-preferred-nodes'] is expected, remote

    def test_no_preference_means_nothing_to_fall_outside_of(self):
        """
        Not because such a chain keeps requests anywhere -- round robin treats
        every host as local and will happily reach a remote datacenter. It
        reports false because it declares no preference for a request to fall
        outside of, and no node-preference is reported for it either, which is
        what the flag is defined against. The other ScyllaDB drivers do not all
        answer this the same way, so it is a deliberate choice.
        """
        report = _load_balancing_report(TokenAwarePolicy(RoundRobinPolicy()))

        assert 'node-preference' not in report
        assert report['policy']['fallback-to-non-preferred-nodes'] is False

    def test_an_inferred_datacenter(self):
        """
        Not yet known at report time is a state the schema allows for, and the
        one the first control connection is usually in.
        """
        policy = TokenAwarePolicy(DCAwareRoundRobinPolicy())

        assert _load_balancing_report(policy)['node-preference'] == {'type': 'dc-auto'}

    def test_an_inferred_datacenter_once_it_is_known(self):
        child = DCAwareRoundRobinPolicy()
        # Driven through on_up, which is what infers: assigning local_dc is the
        # application choosing one, and is reported as such.
        child.on_up(Host(DefaultEndPoint(1), SimpleConvictionPolicy, 'inferred-dc',
                         host_id=uuid.uuid4()))

        report = _load_balancing_report(TokenAwarePolicy(child))

        assert report['node-preference'] == {'type': 'dc-auto', 'local-dc': 'inferred-dc'}

    def test_the_datacenter_cannot_be_reassigned(self):
        """
        local_dc is read-only, so a configured datacenter and an inferred one
        cannot be confused: an assignment afterwards would be indistinguishable
        from on_up's inference, and telling them apart is the whole point of the
        dc / dc-auto distinction.
        """
        policy = DCAwareRoundRobinPolicy('dc1')

        with pytest.raises(AttributeError):
            policy.local_dc = 'dc2'

        assert policy.local_dc == 'dc1'
        assert _node_location_preference_report(policy) == {'type': 'dc', 'local-dc': 'dc1'}

    def test_inference_still_fills_in_an_unset_datacenter(self):
        """
        The other half: read-only to the application, still filled in by on_up
        when the constructor was given nothing -- and reported as inferred.
        """
        policy = DCAwareRoundRobinPolicy()
        assert _node_location_preference_report(policy) == {'type': 'dc-auto'}

        policy.on_up(Host(DefaultEndPoint(1), SimpleConvictionPolicy, 'inferred',
                          host_id=uuid.uuid4()))

        assert policy.local_dc == 'inferred'
        assert _node_location_preference_report(policy) == {
            'type': 'dc-auto', 'local-dc': 'inferred'}

    def test_a_rack_aware_child(self):
        policy = TokenAwarePolicy(RackAwareRoundRobinPolicy('dc1', 'rack1'))

        assert _load_balancing_report(policy)['node-preference'] == {
            'type': 'rack', 'local-dc': 'dc1', 'local-rack': 'rack1'}

    def test_no_policy_is_resolved_the_way_a_request_resolves_it(self):
        """
        ResponseFuture takes `load_balancer or _default_load_balancing_policy`,
        so a legacy cluster with none set routes with the default profile's
        policy. Reporting the None as a custom policy would tell an operator a
        user-supplied one is routing when the driver's own is.
        """
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            cluster = Cluster()
            self.addCleanup(cluster.shutdown)
            cluster.load_balancing_policy = None

            report = json.loads(
                cluster._driver_config_reporter._build_report(cluster, is_scylla=True))

        assert report['query']['load-balancing']['policy']['type'] == 'token-aware'
        assert type(cluster._default_load_balancing_policy) is TokenAwarePolicy

    def test_a_policy_nothing_resolves_is_not_a_custom_one(self):
        """
        When the fallback is None too, a request takes make_query_plan off None
        and raises. policy is a required key, so the whole report goes -- as it
        does for a retry policy of None, which crashes the same way.
        """
        cluster = Cluster(execution_profiles={
            EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=None)})
        self.addCleanup(cluster.shutdown)

        assert cluster._default_load_balancing_policy is None
        with pytest.raises(ValueError, match='load_balancing_policy is None'):
            _load_balancing_report(None)

        options = {}
        cluster._driver_config_reporter.add_startup_options(options, is_scylla=True)
        assert DRIVER_CONFIG_OPTION not in options

    def test_policies_that_are_not_token_aware_are_custom(self):
        """
        Only the token-aware policy maps onto the schema's built-in arm. The
        round-robin policies are built in to this driver but are not token
        aware, and the shared vocabulary has no term for them.
        """
        for policy in (RoundRobinPolicy(), DCAwareRoundRobinPolicy('dc1'),
                       WhiteListRoundRobinPolicy([])):
            report = _load_balancing_report(policy)
            assert report['policy'] == {'type': 'custom',
                                        'name': type(policy).__name__}, policy

    def test_a_custom_policy_still_reports_its_datacenter(self):
        """
        The preference is a sibling of the policy in the schema, not a property
        of the built-in arm. A bare DCAwareRoundRobinPolicy -- which is what
        default_lbp_factory() returns without the murmur3 extension -- pins the
        driver to a datacenter just as firmly as a token-aware one wrapping it,
        and an operator cannot tell that from the type name alone.
        """
        report = _load_balancing_report(DCAwareRoundRobinPolicy('dc1'))

        assert report == {
            'policy': {'type': 'custom', 'name': 'DCAwareRoundRobinPolicy'},
            'node-preference': {'type': 'dc', 'local-dc': 'dc1'},
        }

    def test_a_bare_rack_aware_policy_reports_its_rack(self):
        """
        RackAwareRoundRobinPolicy takes both as mandatory arguments, so the most
        deliberate pinning an application can express is also the one most
        likely to be used without a token-aware wrapper.
        """
        report = _load_balancing_report(RackAwareRoundRobinPolicy('dc1', 'rack1'))

        assert report == {
            'policy': {'type': 'custom', 'name': 'RackAwareRoundRobinPolicy'},
            'node-preference': {'type': 'rack', 'local-dc': 'dc1',
                                'local-rack': 'rack1'},
        }

    def test_the_preference_is_reported_even_for_an_undescribable_chain(self):
        """
        node-preference is a sibling of the policy rather than part of it, so a
        chain the built-in arm cannot describe still says where the driver is
        pinned. The policy itself goes to the custom arm: HostFilterPolicy
        admits only what an application-supplied predicate allows, which the
        token-aware flags have nowhere to record.
        """
        policy = TokenAwarePolicy(
            HostFilterPolicy(DCAwareRoundRobinPolicy('dc1', used_hosts_per_remote_dc=2),
                             lambda host: True))

        report = _load_balancing_report(policy)

        assert report['node-preference'] == {'type': 'dc', 'local-dc': 'dc1'}
        assert report['policy'] == {'type': 'custom', 'name': 'TokenAwarePolicy'}

    def test_a_chain_reaching_an_unknown_policy_is_custom(self):
        """
        The built-in arm's flags describe the routing of the whole chain, so
        they can only be filled in when every policy in it is one this module
        knows. Reporting them over an unknown child would assert plain
        token-aware routing and say nothing of what the child does --
        WhiteListRoundRobinPolicy confines routing to a fixed host list, and a
        RoundRobinPolicy subclass at that, which is why the check is on exact
        types.
        """
        class MyCustomPolicy(RoundRobinPolicy):
            pass

        for child in (WhiteListRoundRobinPolicy([]),
                      HostFilterPolicy(RoundRobinPolicy(), lambda host: True),
                      MyCustomPolicy()):
            report = _load_balancing_report(TokenAwarePolicy(child))

            assert report['policy'] == {'type': 'custom', 'name': 'TokenAwarePolicy'}, child

    def test_token_awareness_is_found_under_a_transparent_wrapper(self):
        """
        A wrapper above the token-aware policy does not stop the routing being
        token aware, so the arm is claimed from anywhere in the chain.
        """
        policy = DefaultLoadBalancingPolicy(
            TokenAwarePolicy(DCAwareRoundRobinPolicy('dc1')))

        report = _load_balancing_report(policy)

        assert report['policy']['type'] == 'token-aware'
        assert report['node-preference'] == {'type': 'dc', 'local-dc': 'dc1'}

    def test_no_preference_when_nothing_in_the_chain_is_location_aware(self):
        for policy in (RoundRobinPolicy(), TokenAwarePolicy(RoundRobinPolicy())):
            assert 'node-preference' not in _load_balancing_report(policy), policy

    def test_the_preference_is_found_however_deep_it_sits(self):
        """
        Stopping the walk early is not free: it reports no location preference
        at all, which reads as a client pinned to nothing rather than one whose
        preference sits deeper than the walk went. Nothing about a wrapper
        changes where requests go, so depth must not decide what is reported.
        """
        for depth in (1, 7, 8, 12, 200):
            policy = DCAwareRoundRobinPolicy('dc1')
            for _ in range(depth):
                policy = HostFilterPolicy(policy, lambda host: True)

            report = _load_balancing_report(TokenAwarePolicy(policy))

            assert report['node-preference'] == {'type': 'dc', 'local-dc': 'dc1'}, depth

    def test_a_self_referential_chain_terminates(self):
        """
        The walk stops once it reaches a policy it has already seen, which is
        what a chain looping back on itself does. This runs while a connection
        is being established, and a walk that never ends would hang the
        handshake.
        """
        policy = HostFilterPolicy(RoundRobinPolicy(), lambda host: True)
        policy._child_policy = policy

        assert 'node-preference' not in _load_balancing_report(policy)

    def test_the_chain_is_walked_once(self):
        """
        The group needs three answers about a chain, and taking them from
        separate walks costs the walk over again -- _MAX_POLICY_CHAIN policy
        objects each, for the chain that bound exists for, while a connection is
        being established.

        It also lets the answers describe different chains: a _child_policy
        returning something different on each access hands each walk its own,
        so one can find a token-aware policy where the next finds none.
        """
        built = []

        class Endless(RoundRobinPolicy):
            @property
            def _child_policy(self):
                built.append(None)
                return Endless()

        _load_balancing_report(Endless())

        assert len(built) == _MAX_POLICY_CHAIN

    def test_a_chain_that_manufactures_children_terminates(self):
        """
        The case identity cannot catch, and what the backstop is for: every
        access returns a new object, so no step is ever somewhere the walk has
        been before.
        """
        class EndlessPolicy(RoundRobinPolicy):
            @property
            def _child_policy(self):
                return EndlessPolicy()

        assert _survey_policy_chain(EndlessPolicy()).located is None

    def test_a_chain_the_walk_could_not_finish_is_custom(self):
        """
        The flags on the built-in arm describe the routing of the whole chain, so
        a chain the cap cut short cannot claim them: what is below the cut is as
        unaccounted for as an application-supplied policy is. Reporting
        token-aware here would assert shuffling and remote fallback for links the
        walk never reached.

        Every link the walk does see is a policy this module accounts for, which
        is what makes the cap the only thing that can decide it. A chain of
        subclasses would come out custom whether the cut were noticed or not.
        """
        policy = TokenAwarePolicy(RoundRobinPolicy())

        with mock.patch.object(RoundRobinPolicy, '_child_policy', create=True,
                               new_callable=mock.PropertyMock) as child:
            child.side_effect = RoundRobinPolicy

            report = _load_balancing_report(policy)

        assert report['policy'] == {'type': 'custom', 'name': 'TokenAwarePolicy'}

    def test_a_chain_that_ends_within_the_cap_is_not_treated_as_cut_short(self):
        """
        The other side of it: the sentinel must be the exhausted walk only, or
        every ordinary chain would report as custom.
        """
        report = _load_balancing_report(TokenAwarePolicy(RoundRobinPolicy()))

        assert report['policy']['type'] == 'token-aware'

    def test_identity_rather_than_equality_decides_a_loop(self):
        """
        A custom policy is free to compare equal to a different policy, which
        must not read as a chain that loops back on itself.
        """
        class EqualToAnything(RoundRobinPolicy):
            def __eq__(self, other):
                return True

            __hash__ = None  # as Python does for anything defining __eq__

        policy = EqualToAnything()
        policy._child_policy = EqualToAnything()
        policy._child_policy._child_policy = DCAwareRoundRobinPolicy('dc1')

        assert _survey_policy_chain(policy).located.local_dc == 'dc1'

    def test_a_custom_policy_is_named_and_nothing_more(self):
        class SecretiveLoadBalancingPolicy(RoundRobinPolicy):
            def __init__(self):
                self.password = 'hunter2'

        assert _load_balancing_report(SecretiveLoadBalancingPolicy()) == {
            'policy': {'type': 'custom', 'name': 'SecretiveLoadBalancingPolicy'}}


class SpeculativeExecutionReportTest(unittest.TestCase):
    def test_absent_by_default(self):
        """
        The schema leaves the group out rather than carrying a policy that does
        nothing, and doing nothing is this driver's default.
        """
        assert _speculative_execution_report(NoSpeculativeExecutionPolicy()) is None
        assert _speculative_execution_report(None) is None
        assert 'speculative-execution' not in query_report(self)

    def test_constant(self):
        report = _speculative_execution_report(
            ConstantSpeculativeExecutionPolicy(delay=0.5, max_attempts=3))

        assert report == {'policy': {'type': 'constant', 'max-executions': 3,
                                     'delay-ms': 500}}

    def test_launching_immediately_is_a_value(self):
        report = _speculative_execution_report(
            ConstantSpeculativeExecutionPolicy(delay=0, max_attempts=1))

        assert report['policy']['delay-ms'] == 0

    def test_a_policy_that_never_speculates_is_absent_too(self):
        """
        max-executions is a required positiveInteger, so the group cannot say
        "none" from the inside. A policy configured with no attempts never
        speculates -- next_execution() returns -1 from the first call, and
        ResponseFuture only schedules a delay of zero or more -- so reporting
        one execution would claim a race the driver never runs.
        """
        for attempts in (0, -1):
            plan = ConstantSpeculativeExecutionPolicy(0.5, attempts).new_plan('ks', None)
            assert plan.next_execution('host') == -1, attempts
            assert _speculative_execution_report(
                ConstantSpeculativeExecutionPolicy(0.5, attempts)) is None, attempts

    def test_a_policy_that_never_speculates_leaves_a_conformant_report(self):
        report = validate_report(report_text(self, execution_profiles={
            EXEC_PROFILE_DEFAULT: ExecutionProfile(
                speculative_execution_policy=ConstantSpeculativeExecutionPolicy(0.5, 0))}))

        assert 'speculative-execution' not in report['query']

    def test_a_negative_delay_never_races_anything(self):
        """
        next_execution hands the configured delay straight through, and
        ResponseFuture._start_timer creates the speculative timer only for a
        delay of zero or more. A negative delay is also the very value the plan
        returns once it has run out, so the driver cannot tell the two apart --
        neither starts an execution. Reporting the group would claim a race that
        never happens, and delay-ms cannot carry the negative anyway.
        """
        for delay in (-1, -0.001, -60):
            plan = ConstantSpeculativeExecutionPolicy(delay, 5).new_plan('ks', None)
            # What _start_timer tests before making a timer.
            assert plan.next_execution('host') < 0, delay

            assert _speculative_execution_report(
                ConstantSpeculativeExecutionPolicy(delay, 5)) is None, delay

    def test_an_unusable_delay_wins_over_an_unlimited_count(self):
        """
        A count no integer can express reaches the custom arm, but only if the
        policy races at all. next_execution hands the delay straight through and
        _start_timer makes a timer only for zero or more, so a negative delay
        starts nothing however many executions were asked for -- reporting the
        group would claim a race that never runs.
        """
        for delay in (-1, -0.001):
            policy = ConstantSpeculativeExecutionPolicy(delay, float('inf'))
            plan = policy.new_plan('ks', None)
            # What _start_timer tests before making a timer.
            assert plan.next_execution('host') < 0, delay

            assert _speculative_execution_report(policy) is None, delay

        # A usable delay with the same count still reaches the custom arm.
        assert _speculative_execution_report(
            ConstantSpeculativeExecutionPolicy(0.5, float('inf'))) == {
                'policy': {'type': 'custom',
                           'name': 'ConstantSpeculativeExecutionPolicy'}}

    def test_a_zero_delay_still_races(self):
        """
        The boundary the driver itself draws: zero is scheduled, below it is not.
        """
        report = _speculative_execution_report(ConstantSpeculativeExecutionPolicy(0, 2))

        assert report['policy']['delay-ms'] == 0

    def test_a_sub_millisecond_delay_is_not_an_immediate_one(self):
        """
        Sub-millisecond speculative execution is a real setting for a
        low-latency workload, and must not read as "launch immediately".
        """
        assert _speculative_execution_report(
            ConstantSpeculativeExecutionPolicy(0.0004, 2))['policy']['delay-ms'] == 1

    def test_a_fractional_execution_limit_rounds_up(self):
        """
        The plan counts `remaining` down while it is above zero, so a fractional
        limit admits the ceiling. Half an execution is still one, and omitting
        the group for it would say speculative execution is disabled when it
        runs.
        """
        for limit, executions in ((0.5, 1), (1.5, 2), (2.5, 3)):
            plan = ConstantSpeculativeExecutionPolicy(0.5, limit).new_plan('ks', None)
            launched = 0
            while plan.next_execution('host') >= 0:
                launched += 1
            assert launched == executions, limit

            report = _speculative_execution_report(
                ConstantSpeculativeExecutionPolicy(0.5, limit))
            assert report['policy']['max-executions'] == executions, limit

    def test_an_execution_limit_no_integer_can_express_is_a_policy_with_no_name(self):
        """
        float('inf') is how an application says "keep racing for as long as the
        request lives", and the plan honours it: it counts down from inf and
        never runs out. max-executions is a required positiveInteger with no way
        to say that, and leaving the group out would claim the driver never
        speculates when it always does -- so the only truthful arm left is the
        one for a policy the shared vocabulary cannot describe.
        """
        policy = ConstantSpeculativeExecutionPolicy(0.5, float('inf'))
        plan = policy.new_plan('ks', None)
        assert all(plan.next_execution('host') >= 0 for _ in range(100))

        assert _speculative_execution_report(policy) == {
            'policy': {'type': 'custom', 'name': 'ConstantSpeculativeExecutionPolicy'}}

    def test_a_limit_that_is_not_a_number_is_reported_the_same_way(self):
        """
        The policy validates nothing, so a limit it will raise on when it builds
        its plan is reachable. There is still a policy configured, which is more
        than an absent group would say, and it is no more describable than an
        unlimited one.
        """
        for limit in (None, 'two'):
            assert _speculative_execution_report(
                ConstantSpeculativeExecutionPolicy(0.5, limit)) == {
                    'policy': {'type': 'custom',
                               'name': 'ConstantSpeculativeExecutionPolicy'}}, limit

    def test_a_delay_that_cannot_be_compared_never_races_anything(self):
        """
        _start_timer is what compares the delay with zero, so a delay that
        cannot be compared raises there and no execution is ever started -- the
        same outcome as a negative one, and the same absent group.
        """
        assert _speculative_execution_report(
            ConstantSpeculativeExecutionPolicy(None, 3)) is None

    def test_an_unnameable_policy_does_not_cost_the_rest_of_the_report(self):
        """
        As for retries: math.ceil used to raise here and take every other group
        of the report with it.
        """
        for delay, limit in ((0.5, float('inf')), (0.5, None), (None, 3)):
            report = full_report(self, execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(
                speculative_execution_policy=ConstantSpeculativeExecutionPolicy(delay, limit))})

            assert report['connection']['connect']['timeout-ms'] == 5000, (delay, limit)

    def test_a_custom_policy_is_named_and_nothing_more(self):
        class SecretiveSpeculativeExecutionPolicy(SpeculativeExecutionPolicy):
            def __init__(self):
                self.password = 'hunter2'

            def new_plan(self, keyspace, statement):
                return NoSpeculativeExecutionPlan()

        assert _speculative_execution_report(SecretiveSpeculativeExecutionPolicy()) == {
            'policy': {'type': 'custom', 'name': 'SecretiveSpeculativeExecutionPolicy'}}

    def test_a_delay_that_never_comes_due_starts_nothing(self):
        """
        _start_timer schedules the additional execution at the configured
        delay, and Timer.finish tests `time_now >= self.end` -- never true of an
        infinite delay or of a nan. Nothing is ever launched, which the schema
        says by leaving the group out, exactly as it does for a delay the timer
        refuses outright.
        """
        for delay in (float('inf'), float('nan')):
            assert _speculative_execution_report(
                ConstantSpeculativeExecutionPolicy(delay, 5)) is None, delay


class ProfileSourceTest(unittest.TestCase):
    def test_the_default_profile_is_what_is_reported(self):
        report = query_report(self, ExecutionProfile(
            consistency_level=ConsistencyLevel.THREE,
            retry_policy=FallthroughRetryPolicy()))

        assert report['defaults']['consistency'] == 'THREE'
        assert report['retry']['policy']['type'] == 'fallthrough'

    def test_other_profiles_are_not_reported(self):
        """
        The schema has one query group and this driver has as many profiles as
        the application defines, so the one a statement gets when it names none
        is the one that describes the session.
        """
        cluster = Cluster(execution_profiles={
            EXEC_PROFILE_DEFAULT: ExecutionProfile(consistency_level=ConsistencyLevel.ONE),
            'other': ExecutionProfile(consistency_level=ConsistencyLevel.ALL),
        })
        self.addCleanup(cluster.shutdown)

        report = json.loads(cluster._driver_config_reporter._build_report(cluster, True))

        assert report['query']['defaults']['consistency'] == 'ONE'

    def test_policies_assigned_after_construction_are_reported(self):
        """
        Assigning either legacy policy switches the cluster to legacy mode and
        updates only the cluster attribute; the default profile keeps whatever
        it was built with. A request takes the cluster's, so reading the profile
        would describe policies nothing will ever use -- here, retries and
        token-aware routing that are not going to happen.
        """
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            cluster = Cluster()
            self.addCleanup(cluster.shutdown)
            cluster.default_retry_policy = FallthroughRetryPolicy()
            cluster.load_balancing_policy = RoundRobinPolicy()

            # The profile still holds the construction-time policies, which is
            # what makes this worth asserting.
            profile = cluster.profile_manager.default
            assert type(profile.retry_policy) is RetryPolicy
            assert type(profile.load_balancing_policy) is TokenAwarePolicy

            report = json.loads(
                cluster._driver_config_reporter._build_report(cluster, is_scylla=True))['query']

        assert report['retry']['policy'] == {'type': 'fallthrough'}
        assert report['load-balancing']['policy'] == {'type': 'custom',
                                                     'name': 'RoundRobinPolicy'}

    def test_legacy_configuration_races_nothing(self):
        """
        The legacy branch of _create_response_future leaves the speculative
        execution plan unset whatever the profile holds, so there is no group to
        report even when a policy was put on the profile by hand.
        """
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            cluster = Cluster()
            self.addCleanup(cluster.shutdown)
            cluster.default_retry_policy = FallthroughRetryPolicy()
            cluster.profile_manager.default.speculative_execution_policy = \
                ConstantSpeculativeExecutionPolicy(0.5, 2)

            report = json.loads(
                cluster._driver_config_reporter._build_report(cluster, is_scylla=True))['query']

        assert 'speculative-execution' not in report

    def test_legacy_configuration_reads_the_same(self):
        """
        A load balancing or retry policy given to the Cluster constructor is
        folded into the default profile, so both ways of configuring the driver
        report identically.
        """
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            report = query_report(self, load_balancing_policy=RoundRobinPolicy(),
                                  default_retry_policy=FallthroughRetryPolicy())

        assert report['retry']['policy']['type'] == 'fallthrough'
        assert report['load-balancing']['policy'] == {'type': 'custom',
                                                      'name': 'RoundRobinPolicy'}

    def test_legacy_defaults_come_from_the_session_not_the_profile(self):
        """
        The legacy branch of _create_response_future reads the consistency, the
        serial consistency and the timeout off the Session and never looks at
        the profile, so the profile's values are ones no request will ever use.

        The two agree by default, which is why this sets the profile away from
        them: the default profile is built with Session._default_timeout but
        with ExecutionProfile's own consistency default, so a report reading the
        profile is wrong about the consistency and right about the timeout by
        coincidence.
        """
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            cluster = Cluster(default_retry_policy=FallthroughRetryPolicy())
            self.addCleanup(cluster.shutdown)
            profile = cluster.profile_manager.default
            profile.consistency_level = ConsistencyLevel.ALL
            profile.serial_consistency_level = ConsistencyLevel.SERIAL
            profile.request_timeout = 99

            report = json.loads(
                cluster._driver_config_reporter._build_report(cluster, is_scylla=True))

        defaults = report['query']['defaults']
        assert defaults['consistency'] == 'LOCAL_ONE'
        assert 'serial-consistency' not in defaults
        assert defaults['request']['timeout-ms'] == 10000

    def test_legacy_defaults_follow_the_session_class(self):
        """
        Read off the class rather than an instance because no Session exists
        when the control connection reports: what this describes is the default
        every session created from the cluster will start with.
        """
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)
            with mock.patch.multiple(
                    Session,
                    _default_consistency_level=ConsistencyLevel.QUORUM,
                    _default_serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
                    _default_timeout=42.0):
                report = full_report(self, default_retry_policy=FallthroughRetryPolicy())

        assert report['query']['defaults']['consistency'] == 'QUORUM'
        assert report['query']['defaults']['serial-consistency'] == 'LOCAL_SERIAL'
        assert report['query']['defaults']['request']['timeout-ms'] == 42000


class ReportConformsToTheSchemaTest(unittest.TestCase):
    """
    The point of the whole series: what this driver sends is what the shared
    contract says it may send.
    """

    def test_the_default_configuration(self):
        for is_scylla in (True, False):
            validate_report(report_text(self, is_scylla=is_scylla))

    def test_a_policy_that_never_reconnects(self):
        """
        The schema's null arm, reached from a real configuration rather than
        from no policy at all. Asserted here rather than beside the policy
        mapping, since validating it needs a whole conformant document and the
        report only becomes one with this group.
        """
        report = validate_report(report_text(
            self, reconnection_policy=ExponentialReconnectionPolicy(1.0, 2.0, max_attempts=0)))

        assert report['connection']['reconnection']['policy'] is None

    def test_a_configuration_that_avoids_every_default(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        report = report_text(
            self,
            connect_timeout=1.5,
            control_connection_timeout=3,
            metadata_request_timeout=4,
            max_schema_agreement_wait=0,
            reconnection_policy=ConstantReconnectionPolicy(0, max_attempts=9),
            shard_aware_options={'disable_shardaware_port': True},
            ssl_context=context,
            sockopts=[(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
                      (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
                      (socket.SOL_SOCKET, socket.SO_RCVBUF, 65536),
                      (socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 5))],
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(
                load_balancing_policy=TokenAwarePolicy(
                    RackAwareRoundRobinPolicy('dc1', 'rack1', used_hosts_per_remote_dc=1),
                    shuffle_replicas=False),
                retry_policy=ExponentialBackoffRetryPolicy(3, 0.1, 2.0),
                consistency_level=ConsistencyLevel.EACH_QUORUM,
                serial_consistency_level=ConsistencyLevel.SERIAL,
                request_timeout=0.0004,
                speculative_execution_policy=ConstantSpeculativeExecutionPolicy(0.25, 2),
            )})

        validate_report(report)

    def test_a_configuration_of_nothing_but_custom_policies(self):
        class CustomLoadBalancingPolicy(RoundRobinPolicy):
            pass

        class CustomRetryPolicy(RetryPolicy):
            pass

        class CustomReconnectionPolicy(ReconnectionPolicy):
            def new_schedule(self):
                return iter(())

        report = validate_report(report_text(
            self,
            reconnection_policy=CustomReconnectionPolicy(),
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(
                load_balancing_policy=CustomLoadBalancingPolicy(),
                retry_policy=CustomRetryPolicy())}))

        assert report['query']['load-balancing']['policy']['name'] == 'CustomLoadBalancingPolicy'
        assert report['connection']['reconnection']['policy']['name'] == 'CustomReconnectionPolicy'

    def test_a_custom_policy_does_not_leak_its_attributes(self):
        """
        The schema permits a custom policy's public attributes to be serialized
        and this driver deliberately sends none of them: a policy is an
        arbitrary object whose __dict__ is trivially reachable, and whatever it
        holds would land in system.clients for anyone who can select from it.
        """
        class CredentialCarryingPolicy(RoundRobinPolicy):
            def __init__(self):
                super().__init__()
                self.password = 'hunter2'
                self.hosts = ['10.0.0.1', '10.0.0.2']

        report = report_text(self, execution_profiles={
            EXEC_PROFILE_DEFAULT: ExecutionProfile(
                load_balancing_policy=CredentialCarryingPolicy())})

        assert 'hunter2' not in report
        assert '10.0.0.1' not in report
        assert json.loads(report)['query']['load-balancing']['policy'] == {
            'type': 'custom', 'name': 'CredentialCarryingPolicy'}

    def test_a_duration_that_is_not_finite_still_leaves_a_report(self):
        """
        The cost of one undescribable duration used to be the whole document:
        int() raises on inf and on nan, nothing caught it before
        add_startup_options, and the operator lost every other group along with
        the key that could not be converted.

        These all describe what the driver does with such a setting instead --
        an unbounded timeout as the absence that already means "no limit here",
        a delay that never comes due as the reconnection it never performs.
        """
        cases = {
            'request_timeout': dict(execution_profiles={
                EXEC_PROFILE_DEFAULT: ExecutionProfile(request_timeout=float('inf'))}),
            'control_connection_timeout': dict(control_connection_timeout=float('inf')),
            'metadata_request_timeout': dict(metadata_request_timeout=float('inf')),
            'connect_timeout': dict(connect_timeout=float('inf')),
            'reconnection_policy': dict(
                reconnection_policy=ConstantReconnectionPolicy(float('inf'))),
            'speculative_execution_policy': dict(execution_profiles={
                EXEC_PROFILE_DEFAULT: ExecutionProfile(
                    speculative_execution_policy=ConstantSpeculativeExecutionPolicy(
                        float('inf'), 5))}),
        }
        for setting, kwargs in cases.items():
            report = json.loads(report_text(self, **kwargs))
            validate_report(report)
            assert set(report) == {'version', 'connection', 'control-plane', 'query'}, setting


class UnnameableConsistencyTest(unittest.TestCase):
    """
    ExecutionProfile validates serial_consistency_level but not
    consistency_level, so a level the driver does not define can be configured.
    """

    def test_no_working_configuration_is_affected(self):
        """
        The premise of dropping the report rather than naming something else: a
        level the schema cannot name is one the driver cannot use either.
        """
        with pytest.raises(Exception):
            QueryMessage(query='SELECT 1', consistency_level=None).send_body(BytesIO(), 4)

    def test_the_report_is_dropped_rather_than_naming_a_level_that_is_not_used(self):
        """
        consistency is a required key, so no conformant report describes such a
        configuration. Naming the driver's default instead would tell an
        operator that a client which cannot execute a query is querying at
        LOCAL_ONE.
        """
        for level in (None, 99):
            options = {}
            reporter(self, execution_profiles={
                EXEC_PROFILE_DEFAULT: ExecutionProfile(consistency_level=level)
            }).add_startup_options(options, is_scylla=True)

            assert DRIVER_CONFIG_OPTION not in options, level

    def test_the_warning_names_the_setting(self):
        """
        Otherwise this surfaces as a bare KeyError under a generic "unable to
        build the report", which does not say which setting caused it.
        """
        with self.assertLogs('cassandra.driver_config', level='WARNING') as captured:
            reporter(self, execution_profiles={
                EXEC_PROFILE_DEFAULT: ExecutionProfile(consistency_level=99)
            }).add_startup_options({}, is_scylla=True)

        logged = '\n'.join(r.getMessage() + (r.exc_text or '') for r in captured.records)
        assert 'consistency_level is 99' in logged

    def test_a_level_that_cannot_be_a_key_gets_the_same_message(self):
        """
        An unhashable level fails the lookup with a TypeError rather than a
        KeyError. Same kind of wrong, so it takes the same route: the message
        naming the setting, not the generic "unable to build the report" this
        one exists to avoid.
        """
        with self.assertLogs('cassandra.driver_config', level='WARNING') as captured:
            options = {}
            reporter(self, execution_profiles={
                EXEC_PROFILE_DEFAULT: ExecutionProfile(consistency_level=[])
            }).add_startup_options(options, is_scylla=True)

        assert DRIVER_CONFIG_OPTION not in options
        logged = '\n'.join(r.getMessage() + (r.exc_text or '') for r in captured.records)
        assert 'consistency_level is []' in logged

    def test_recognized_levels_are_unaffected(self):
        for level in ConsistencyLevel.value_to_name:
            report = query_report(self, ExecutionProfile(consistency_level=level))
            assert report['defaults']['consistency'] == ConsistencyLevel.value_to_name[level]

    def test_a_boolean_is_the_level_it_packs_as(self):
        """
        True hashes equal to 1 and names ONE, which is not a mismatch: the wire
        encoding packs it to the same two bytes, so the client really does query
        at ONE.
        """
        report = query_report(self, ExecutionProfile(consistency_level=True))

        assert report['defaults']['consistency'] == 'ONE'
