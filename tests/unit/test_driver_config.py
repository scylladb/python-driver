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
from unittest import mock
from unittest.mock import Mock

import numpy
import pytest

from cassandra.cluster import Cluster
from cassandra.driver_config import (DriverConfigReporter, DRIVER_CONFIG_OPTION,
                                     DRIVER_CONFIG_SCHEMA_VERSION, MAX_DRIVER_CONFIG_LENGTH,
                                     _non_negative_ms, _optional_ms, _reconnection_policy_report,
                                     _required_ms, _socket_report)
from cassandra.util import maybe_add_timeout_to_query
from cassandra.policies import (ConstantReconnectionPolicy, ExponentialReconnectionPolicy,
                                ReconnectionPolicy)
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


def report_text(test, **cluster_kwargs):
    """The report of a real Cluster, as it goes on the wire."""
    built = report_cluster(test, **cluster_kwargs)
    return built._driver_config_reporter._build_report(built, is_scylla=True)


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
