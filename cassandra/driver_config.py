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
"""
Reporting of the driver's own identity and configuration to the cluster through
the CQL ``STARTUP`` options. ScyllaDB echoes those options into the
``client_options`` column of its clients table, so an operator investigating an
incident can inspect the settings of a client without access to its host.
"""

import datetime
import json
import logging
import math
import operator
import socket
import struct
import weakref
from itertools import repeat

from cassandra.policies import (ConstantReconnectionPolicy,
                                ExponentialReconnectionPolicy)

log = logging.getLogger(__name__)


SESSION_ID_OPTION = 'SESSION_ID'
"""
``STARTUP`` option correlating the connections that belong to the same
:class:`~.Cluster` in the clients table. Every connection reports it, since
correlating them is the whole point of it.

The name follows the convention shared with the other ScyllaDB drivers, where a
"session" is what this driver calls a :class:`~.Cluster`; it is unrelated to
:attr:`.Session.session_id`.
"""

DRIVER_CONFIG_OPTION = 'DRIVER_CONFIG'
"""
``STARTUP`` option holding the JSON description of the effective driver
configuration. The configuration is the same for every connection of a cluster,
so only the control connection reports it, keeping the other ``STARTUP`` frames
small.
"""

DRIVER_CONFIG_SCHEMA_VERSION = 1
"""
Major version of the reported configuration schema. Adding keys to the report is
backwards compatible and does not bump it, only changing or removing the meaning
of an existing key does.
"""

MAX_DRIVER_CONFIG_LENGTH = 32 * 1024
"""
Upper bound for the length, in bytes, of the :const:`DRIVER_CONFIG_OPTION` value.

``STARTUP`` options are serialized by :func:`cassandra.protocol.write_string`,
which prefixes every value with a 16 bit length, so a longer value would fail to
pack and take the handshake down with it.

Nothing in the report is user-supplied: a custom policy contributes its type name
and nothing else, see :func:`_custom_policy_report`. Its size is therefore a
function of the driver's own settings rather than of the configuration it
describes, and stays well under a couple of kilobytes. The limit is kept anyway,
so that "reporting must never prevent a connection from being established" stays
a property of this module -- a later group describing something unbounded would
otherwise make it a property of the user's configuration without anyone noticing.

32 KiB rather than the protocol's own 65535 byte ceiling: that leaves ample
headroom while remaining far short of the point where the value would stop
protecting anything.
"""


def _finite(seconds):
    """
    Whether `seconds` is a duration that can be converted to a whole number of
    milliseconds at all.

    ``float('inf')`` and ``float('nan')`` reach every duration setting the
    driver has, unchallenged: none of them validates its argument, and a nan
    passes even the constructors that reject a negative, since every comparison
    against one is false. Both then raise out of :func:`int` -- an OverflowError
    and a ValueError respectively -- which is a failure the callers here have to
    take a view on rather than let escape, since it would cost the whole report
    and not just the key that could not be converted.

    A value that is not a number at all is a different failure and is left to
    raise: reporting it as unbounded would answer a misconfigured duration with
    a key quietly left out, where the driver itself will not get as far as using
    it -- socket.settimeout and timedelta both reject one.
    """
    return math.isfinite(seconds)


def _never_comes_due(delay):
    """
    Whether a timer scheduled `delay` seconds out will never fire.

    Both schedulers compare a deadline of ``time.time() + delay`` against the
    clock -- _Scheduler with `run_at <= time.time()`, Timer with `time_now >=
    self.end`. An infinite delay puts that deadline beyond every reading the
    clock will ever take, and a nan compares false against all of them, so
    neither is ever due and the work is queued and never run.

    A negative infinity is the opposite and not one of these: its deadline is
    already past, so the timer fires at the first opportunity, exactly as any
    other negative delay does.
    """
    try:
        if _finite(delay):
            return False
        return not delay < 0
    except TypeError:
        # Not a number at all, so it is not this that is wrong with it. The
        # converters raise on it in their turn, as they did before.
        return False


def _milliseconds(seconds):
    """
    `seconds` in milliseconds, rounded to the nearest rather than truncated.

    Truncating loses a millisecond wherever the product lands just under its
    integer, which binary floating point does often: 1.005 seconds multiplies
    out to 1004.9999999999999, and reporting 1004 describes a timeout the
    application did not set. 372 of the first 60000 whole milliseconds land that
    way.

    Rounding does not disturb the sub-millisecond handling in the callers, since
    everything below half a millisecond still arrives there as zero.
    """
    return int(round(seconds * 1000))


def _server_side_timeout_ms(seconds):
    """
    The server-side limit the driver will actually impose, in milliseconds, or
    ``None`` when it will impose none.

    Converted the way :func:`cassandra.util.maybe_add_timeout_to_query` converts
    it rather than the way every other duration here is converted, because that
    builder is what the server ends up being told: it divides a timedelta into
    whole milliseconds, truncating, and appends no ``USING TIMEOUT`` at all when
    that comes to zero. Rounding up, or promoting a sub-millisecond value to one
    as the other converters do, would report a limit the server is never given
    -- 0.0016 seconds is sent as 1ms, and 0.0006 seconds is not sent at all.

    A negative value is left out too. The builder does append it, but the clause
    is malformed and the server rejects it, and the schema has no way to carry a
    negative anyway.

    A duration that is not finite is left out as well. The builder cannot carry
    one either -- it is a timedelta, and timedelta rejects both -- so no clause
    is appended, which is what an absent server-side-ms says.
    """
    if seconds is None or not _finite(seconds):
        return None
    ms = int(datetime.timedelta(seconds=seconds) / datetime.timedelta(milliseconds=1))
    return ms if ms > 0 else None


def _optional_ms(seconds):
    """
    Milliseconds for a schema field of type ``positiveInteger``, or ``None``
    when the setting is unset or disabled and the key is to be left out.

    A configured duration below a millisecond reports as one rather than as
    zero: it is a real setting, and zero is not a value the field can take.

    A duration that is not finite is left out too. These are all optional
    fields, and absence is already how this report says the driver imposes no
    limit here -- which is what an infinite one asks for, and the nearest thing
    to the truth for a nan, whose timeout fires at no describable moment.
    """
    if seconds is None or not _finite(seconds):
        return None
    ms = _milliseconds(seconds)
    if ms < 1:
        return 1 if seconds > 0 else None
    return ms


def _required_ms(seconds):
    """
    Milliseconds for a ``positiveInteger`` field the schema requires, so there
    is no option of leaving it out: zero and below floor at one millisecond.

    A duration that never comes due raises rather than flooring with them. The
    callers all recognise one before they get here and report the driver's
    actual behaviour for it, so this is a backstop -- but it is the floor that
    makes it worth having, since flooring would answer an infinite delay with
    one millisecond, the furthest thing from it the field can hold. A negative
    infinity is not one of those and floors as every other negative does: it
    comes due at once, and one millisecond is the least the field can say.
    """
    if _never_comes_due(seconds):
        raise ValueError(
            "a duration of %r cannot be reported: the field requires a whole "
            "number of milliseconds and the schema has no way to say that a "
            "duration is unbounded" % (seconds,))
    ms = _optional_ms(seconds)
    return 1 if ms is None else ms


def _non_negative_ms(seconds):
    """
    Milliseconds for a ``nonNegativeInteger`` field, where zero is a value in
    its own right -- "do not wait", "reconnect immediately", "launch
    immediately" -- and is reported as it is rather than treated as unset.

    Which is why a configured duration below a millisecond reports as one rather
    than truncating to zero: zero here does not mean "very little", it means the
    driver skips the wait altogether, and the two are not the same claim. The
    driver draws that line in the same place -- ControlConnection.
    _wait_for_schema_agreement bypasses agreement only when its timeout is zero
    or less -- so a sub-millisecond wait is one the driver really does take.

    A duration that is not finite raises. Every caller whose field the schema
    lets it leave out recognises one first, so what reaches here is
    max_schema_agreement_wait, whose timeout-ms the schema requires: a wait of
    no describable length has no conformant document to appear in, so dropping
    the report is the outcome, and the message says why rather than leaving an
    OverflowError under the generic warning.
    """
    if seconds is None:
        return 0
    if not _finite(seconds):
        if _never_comes_due(seconds):
            raise ValueError(
                "a wait of %r cannot be reported: the field requires a whole "
                "number of milliseconds and the schema has no way to say that a "
                "wait is unbounded" % (seconds,))
        # A negative infinity, which is a negative like any other here: the
        # driver skips the wait, and zero is what the schema calls that.
        return 0
    ms = _milliseconds(seconds)
    if ms < 1:
        return 1 if seconds > 0 else 0
    return ms


_SOCKET_FLAGS = (
    ('tcp-no-delay', socket.IPPROTO_TCP, socket.TCP_NODELAY),
    ('keep-alive', socket.SOL_SOCKET, socket.SO_KEEPALIVE),
    ('reuse-address', socket.SOL_SOCKET, socket.SO_REUSEADDR),
)

_SOCKET_BUFFERS = (
    ('receive-buffer', socket.SOL_SOCKET, socket.SO_RCVBUF),
    ('send-buffer', socket.SOL_SOCKET, socket.SO_SNDBUF),
)


def _linger_report(value):
    """
    The ``linger`` group from the value of an ``SO_LINGER`` socket option.

    Unlike the other options this one is a packed ``struct linger`` -- two C
    ``int``s, on and interval -- since that is what
    :meth:`socket.socket.setsockopt` takes, so it has to be unpacked to be
    described. Every buffer type that method accepts is accepted here, and only
    the leading two ``int``s are read, for the same reason as in
    :func:`_socket_option_int`. Anything that does not unpack is left out rather
    than guessed at: it is the user's to get wrong when the connection applies
    it.
    """
    if not isinstance(value, (bytes, bytearray, memoryview)):
        return None
    raw = bytes(value)
    if len(raw) < struct.calcsize('ii'):
        return None
    try:
        onoff, interval = struct.unpack_from('ii', raw)
    except struct.error:
        return None
    if not onoff or interval < 0:
        return None
    return {'interval-s': interval}


def _socket_option_int(value):
    """
    The integer a socket option carries, or ``None`` when it carries something
    this module cannot read.

    :meth:`socket.socket.setsockopt` takes an integer option either as an
    ``int`` or as a packed buffer, and the kernel reads the two the same way, so
    this has to as well. A packed buffer is a non-empty ``bytes``, so handing one
    straight to :func:`bool` makes every option look enabled -- including one
    packed to zero precisely to turn it off.

    What is decoded is the C ``int`` at the front of the buffer, in native size
    and byte order, because that is what the kernel reads for these options: it
    takes the leading ``int`` and ignores whatever follows. Reading the buffer
    as one wide integer instead would answer for bytes the option never had --
    ``struct.pack('ii', 0, 1)`` is accepted for ``TCP_NODELAY`` and leaves it
    off, while the whole eight bytes come to a non-zero number.

    A buffer too short to hold an ``int`` is one ``setsockopt`` itself rejects,
    so there is nothing to report for it.
    """
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        if len(raw) < struct.calcsize('i'):
            return None
        return struct.unpack_from('i', raw)[0]
    # Anything else has to be an integer setsockopt would take. __index__ is
    # what CPython's takes -- a numpy integer sets an option just as a builtin
    # one does -- and operator.index returns a builtin int, so a bool does not
    # travel on into the report as one where a number is expected.
    #
    # Some interpreters are more permissive: PyPy's setsockopt accepts a Decimal
    # and the kernel sets the option from it, and such a value is reported here
    # as unset. Unlike the reconnection limit, which asks itertools.repeat
    # directly, there is no way to ask setsockopt without a socket to ask it on,
    # and a value it rejects fails the connection before there is any report to
    # be wrong -- so the gap only shows on an interpreter that takes it.
    try:
        return operator.index(value)
    except TypeError:
        return None


def _socket_report(sockopts):
    """
    The ``connection.socket`` group.

    The driver sets no socket options of its own: ``sockopts`` is what
    :meth:`cassandra.connection.Connection._connect_socket` applies, and a
    reactor that connects its own socket applies the same list, so an option that
    is not in there is left at the operating system's default. The three flags
    the schema requires are reported as off in that case, which is what every
    platform this driver runs on defaults them to for a fresh TCP socket.

    With one exception, which is reported as off all the same.
    :class:`~cassandra.io.asyncioreactor.AsyncioConnection` upgrades to TLS by
    handing its socket to ``loop.create_connection(sock=..., ssl=...)``, and
    asyncio's own transport sets TCP_NODELAY on it, so over TLS on that reactor
    the flag is on however the application configured it. Left as configured
    here, because this describes the driver's configuration: a flag that moves
    with the reactor and the transport is not one the schema has a place to say.
    """
    configured = {}
    try:
        entries = list(sockopts or ())
    except TypeError:
        # Not a sequence of options at all. _connect_socket will fail on it, and
        # there is nothing here to describe; the platform defaults below are
        # what this can truthfully say.
        entries = []

    for opt in entries:
        try:
            level, name, value = opt
            # Last one wins, as it does in the loop that applies them. Inside
            # the guard with the unpacking, not after it: a level or name that
            # cannot be a dict key -- a list, say -- raises here rather than
            # there, and an entry the user got wrong is not this module's to
            # fail the whole report over.
            configured[(level, name)] = value
        except (TypeError, ValueError):
            # setsockopt also takes a (level, name, None, optlen) form, and an
            # entry that is neither is the user's to get wrong at connect time,
            # not this module's to report on.
            continue

    report = {}
    for key, level, name in _SOCKET_FLAGS:
        report[key] = bool(_socket_option_int(configured.get((level, name))))
    for key, level, name in _SOCKET_BUFFERS:
        size = _socket_option_int(configured.get((level, name)))
        if size is not None and size > 0:
            report[key] = {'size-bytes': size}

    linger = _linger_report(configured.get((socket.SOL_SOCKET, socket.SO_LINGER)))
    if linger is not None:
        report['linger'] = linger
    return report


def _attempt_ceiling(limit):
    """
    How many attempts a ``while i < limit`` loop makes, or ``None`` when `limit`
    does not bound one.

    ``math.ceil`` rather than a check against particular numeric types: the loop
    compares against anything an integer can be compared with, and this counts
    anything that can say what its ceiling is. The result is coerced to a builtin
    ``int`` so that no other numeric type reaches the report, where the schema
    wants an integer.
    """
    if limit is None:
        return None
    try:
        attempts = int(math.ceil(limit))
    except (TypeError, ValueError, OverflowError):
        return None
    return attempts if attempts > 0 else None


def _constant_reconnection_attempts(max_attempts):
    """
    How many reconnection attempts :class:`~.ConstantReconnectionPolicy` will
    make, or ``None`` when it keeps trying or there is no count to report.

    ``new_schedule`` is ``repeat(delay, max_attempts)`` when `max_attempts` is
    truthy and an unbounded ``repeat(delay)`` when it is not, so the falsy check
    comes first: a zero there means unlimited, not none.

    Beyond that this asks :func:`itertools.repeat` itself, through the length it
    reports, rather than testing the limit against a protocol. What ``repeat``
    accepts is not the same on every interpreter -- CPython wants ``__index__``
    and rejects a ``Decimal``, PyPy takes one and counts it -- so a driver
    running on PyPy really does reconnect twice where the same configuration
    raises on CPython. Asking the callee is the only way the report describes the
    interpreter it is running on.

    A limit ``repeat`` will not take is a policy that raises when it reconnects,
    and one too large for it to count is the same; neither has a count to report.
    """
    if not max_attempts:
        return None
    try:
        return operator.length_hint(repeat(None, max_attempts))
    except (TypeError, OverflowError):
        return None


def _makes_no_reconnection_attempt(policy):
    """
    Whether `policy`'s schedule yields nothing at all, so the driver never
    reconnects.

    Asked of the schedule rather than worked out from max_attempts, because the
    values that stop it are not one kind of thing. Zero and a negative stop the
    loop on its first test; a nan stops it because every comparison against one
    is false, and a nan reaches the constructor unchallenged since `nan < 0` is
    false too. Meanwhile ``float('inf')`` passes all three of those tests and
    means the opposite -- the loop never ends.

    Pulling one delay off a fresh schedule tells the three apart without
    enumerating them. The generator holds no state on the policy, so asking
    costs nothing and changes nothing.

    Asking runs the policy's own comparison, so it can raise where the
    converters here deliberately do not: max_attempts stays writable after a
    constructor that checked it, and `i < 'lots'` is a TypeError. That is a
    schedule which yields nothing too, and for a reason the report has to carry
    rather than let escape -- _ReconnectionHandler.start pulls the first delay
    with a bare next(), so the same TypeError comes out there and no attempt is
    ever scheduled. The alternative is worse than a lost group: _attempt_ceiling
    cannot name such a limit either, so the report would come out exponential
    with max-attempts absent, which this schema reads as unlimited.
    """
    nothing = object()
    try:
        return next(iter(policy.new_schedule()), nothing) is nothing
    except TypeError:
        return True


def _reconnection_policy_report(policy):
    """
    The ``connection.reconnection.policy`` value.

    Dispatched on the exact type: a subclass of a built-in policy is a policy
    the driver knows nothing about, and describing it as its parent would put
    that parent's parameters against behaviour it does not have.
    """
    if policy is None:
        # The schema's way of saying that no reconnection will be attempted.
        return None

    if type(policy) is ExponentialReconnectionPolicy:
        if _makes_no_reconnection_attempt(policy):
            # The schedule yields nothing, so the driver never reconnects. That
            # is the schema's null arm. Reporting an exponential policy with
            # max-attempts left out would say the opposite, since the schema
            # reads an absent max-attempts as unlimited -- and absent is what a
            # limit no integer can name comes back as, which is right for
            # float('inf') and wrong for every other one.
            return None
        if _never_comes_due(policy.base_delay) or _never_comes_due(policy.max_delay):
            # A delay that never comes due is the null arm too, for the same
            # reason a schedule that yields nothing is: the driver does not
            # reconnect. _Scheduler tests `run_at <= time.time()` and Timer
            # `time_now >= self.end`, and neither is ever true of an infinite
            # delay or of a nan, so the attempt is queued and never run.
            return None
        if not policy.base_delay:
            # The curve collapses. The schedule is base_delay * 2 ** i, so a
            # base of zero stays zero however many attempts are made and however
            # high max_delay is: the driver reconnects immediately, every time.
            # That is the constant arm with a delay of zero. The exponential arm
            # cannot say it -- its base is a positiveInteger -- and reporting it
            # there would claim a delay that grows when none ever does.
            report = {'type': 'constant', 'delay-ms': 0}
        else:
            # The initial delay is min(max_delay, base_delay), not base_delay:
            # _add_jitter clamps every delay with
            # `min(max(base_delay, delay), max_delay)`, so max_delay wins when
            # the two are the wrong way round. The constructor rejects that pair,
            # but both stay writable afterwards. Taking the minimum also keeps
            # the schema's requirement that max-ms be at least base-ms true by
            # construction -- it is a cross-property invariant JSON Schema cannot
            # express, so the producer is the only thing that can hold it.
            report = {'type': 'exponential',
                      'base-ms': _required_ms(min(policy.base_delay, policy.max_delay)),
                      'max-ms': _required_ms(policy.max_delay)}
        # Absent means unlimited, which is what a max_attempts of None is here.
        # Anything else that bounds the loop is a real limit: new_schedule runs
        # `while max_attempts is None or i < max_attempts`, which compares
        # against whatever an integer can be compared with -- a fraction, a
        # Decimal, a numpy integer -- and 1.5 admits an i of 0 and of 1, so two
        # attempts are made. The count is therefore the ceiling of the limit,
        # taken through math.ceil so that every such type is counted rather than
        # a hand-written list of the ones thought of here.
        attempts = _attempt_ceiling(policy.max_attempts)
        if attempts is not None:
            report['max-attempts'] = attempts
    elif type(policy) is ConstantReconnectionPolicy:
        # Read per policy rather than shared with the arm above, because the two
        # read the same attribute with different code and disagree about the same
        # value: see _constant_reconnection_attempts.
        attempts = _constant_reconnection_attempts(policy.max_attempts)
        if attempts == 0:
            # repeat took the limit and made an empty schedule of it, which a
            # negative limit does on every interpreter. The driver never
            # reconnects, which is the null arm -- reporting a constant policy
            # with max-attempts left out would say the opposite, since the schema
            # reads an absent max-attempts as unlimited.
            return None

        if _never_comes_due(policy.delay):
            # Never reconnects, so the null arm, as in the exponential branch
            # above. A negative infinity is not this and falls through: it comes
            # due at once, which is the delay of zero the converter makes of it.
            return None
        report = {'type': 'constant', 'delay-ms': _non_negative_ms(policy.delay)}
        if attempts is not None:
            # A builtin int, since that is what length_hint returns -- which is
            # what keeps a limit of True out of the report as JSON true where a
            # number belongs.
            report['max-attempts'] = attempts
    else:
        # Only the name: see _custom_policy_report.
        return _custom_policy_report(policy)

    return report


def _custom_policy_report(policy):
    """
    A user-supplied policy, described by its type name and nothing else.

    The schema permits an implementation to serialize a custom policy's public
    attributes as well, and this driver deliberately does not. A policy object
    here is an arbitrary Python object whose ``__dict__`` is trivially
    reachable, and whatever it happens to hold -- an auth provider, a
    credential, a host list -- would go to the server, land in
    ``system.clients``, and be readable by anyone who can select from it. There
    is no way to tell which attributes are safe, so none of them are reported.

    Keeping user-supplied data out also bounds the report: what the driver sends
    is a function of its own settings, so :const:`MAX_DRIVER_CONFIG_LENGTH` is
    not something a configuration can drive it into.
    """
    return {'type': 'custom', 'name': type(policy).__name__}


class DriverConfigReporter:
    """
    Builds the :const:`DRIVER_CONFIG_OPTION` ``STARTUP`` option describing the
    effective configuration of a :class:`~.Cluster`.

    One instance is created per :class:`~.Cluster` and shared by all of its
    connections, but only the control connection ever asks it for options. Which
    connections report is decided by
    :meth:`cassandra.connection.Connection._handle_options_response`, not here.
    """

    def __init__(self, cluster):
        # Weak, because the cluster owns the reporter and hands it to every
        # connection it opens: a strong reference here would run back through
        # each of them and keep the cluster alive for as long as any connection
        # holds a reporter.
        self._cluster = weakref.ref(cluster)

    def add_startup_options(self, options, is_scylla):
        """
        Adds the configuration report to the ``STARTUP`` options being built.

        `is_scylla` says whether the node this connection is being established
        to is a ScyllaDB one, which decides the keys that describe behaviour the
        driver only has against ScyllaDB.

        Reporting is best effort: this runs while a connection is being
        established, so a report that cannot be built or does not fit is logged
        and left out rather than allowed to fail the connection.

        Everything up to and including the assignment is guarded, not just the
        building of the report: :meth:`_populate_report` is an extension point,
        so a subclass returning something that is not a string has to be as
        harmless as one raising. The assignment comes last, so nothing partial is
        left in ``options`` either.
        """
        try:
            cluster = self._cluster()
            if cluster is None:
                # The application dropped its Cluster while this connection was
                # being established. Nothing is wrong and nothing is worth
                # warning about: the connection is on its way out too.
                log.debug("The cluster is gone, its configuration will not be "
                          "reported on this connection")
                return

            report = self._build_report(cluster, is_scylla)
            length = len(report.encode('utf8'))
            if length > MAX_DRIVER_CONFIG_LENGTH:
                log.warning("The driver configuration report is %d bytes long, which exceeds the "
                            "%d bytes limit, it will not be reported to the cluster",
                            length, MAX_DRIVER_CONFIG_LENGTH)
                return

            options[DRIVER_CONFIG_OPTION] = report
        except Exception:
            log.warning("Unable to build the driver configuration report, "
                        "it will not be reported to the cluster", exc_info=True)

    def _build_report(self, cluster, is_scylla):
        """
        Returns the JSON configuration report of `cluster`.

        It is built for every control connection rather than cached, so that it
        always describes the configuration as it is at that point in time. Some
        of what it describes is only known once a connection has got this far:
        `is_scylla` comes out of the ``SUPPORTED`` response, and a datacenter
        the driver inferred rather than was given is not known until the first
        host comes up.
        """
        report = {'version': DRIVER_CONFIG_SCHEMA_VERSION}
        self._populate_report(report, cluster, is_scylla)
        # Separators without whitespace: the report is a wire value bounded by
        # MAX_DRIVER_CONFIG_LENGTH, not something meant to be read as it is.
        return json.dumps(report, separators=(',', ':'))

    def _populate_report(self, report, cluster, is_scylla):
        """
        Adds the configuration groups themselves to the report.
        """
        report['connection'] = self._connection_report(cluster)
        report['control-plane'] = self._control_plane_report(cluster, is_scylla)

    def _connection_report(self, cluster):
        """
        The ``connection`` group: what the driver does with a single connection,
        as opposed to what it does with a request.

        ``read`` and ``write`` are left out because this driver has no socket
        read or write timeout to describe, and ``heartbeat`` because the group
        the schema reserves for it is empty in this version, with nowhere to put
        :attr:`~.Cluster.idle_heartbeat_interval`.
        """
        connection_class = cluster.connection_class
        # Read off the class rather than restated here, so that the report
        # cannot drift from the limit it describes: it is derived on Connection
        # for exactly this, since the report is built before any connection
        # exists.
        #
        # This is the ceiling itself, not one below it: the admission gate in
        # HostConnection.borrow_connection is `in_flight < max_request_id`, so a
        # request is let through only while in_flight is under this. The pool of
        # stream ids is one larger -- ids run from zero to max_request_id
        # inclusive -- and reading the pool as the ceiling is the off-by-one this
        # field invites. Connection.wait_for_responses, which serves internal
        # multi-message waits rather than application queries, does admit one
        # more.
        max_request_id = connection_class.max_request_id_for(
            connection_class.max_in_flight)
        if max_request_id < 1:
            # max_in_flight is documented as tunable by lower-level
            # integrations. Tuned to one it leaves a connection whose gate never
            # admits anything, and in-flight.max is a required positiveInteger
            # with no way to say "none".
            raise ValueError(
                "connection_class.max_in_flight is %r, which leaves a connection "
                "no capacity for a request; the configuration report cannot "
                "describe a connection that admits none"
                % (connection_class.max_in_flight,))

        shard_aware_options = cluster.shard_aware_options
        report = {
            'connect': {},
            'requests': {
                'in-flight': {'max': max_request_id},
                # One below the threshold, for the mirror of the reason above.
                # ResponseFuture._on_timeout adds the orphaned id and then tests
                # `len(orphaned_request_ids) >= orphaned_threshold`, so a
                # connection holding that many is already marked for
                # replacement: the most it is ever allowed to hold, which is
                # what the schema asks for, is one less.
                #
                # Floored at zero on its own account rather than on the strength
                # of the guard above. A threshold of one or less marks a
                # connection on its first orphan -- the count is tested after the
                # id is added, so it is never zero -- which tolerates none, and
                # orphaned.max is a nonNegativeInteger with no room for the
                # negative that subtracting would otherwise give.
                'orphaned': {'max': max(0, connection_class.orphaned_threshold_for(
                    connection_class.max_in_flight) - 1)},
            },
            'pool': {
                'shard-aware': {
                    # Configuration intent, as the schema asks for: reaching a
                    # shard in one connect also needs the server to advertise
                    # the port and the client to be able to reach it, and the
                    # driver falls back transparently when it cannot.
                    'enabled': not (shard_aware_options.disable
                                    or shard_aware_options.disable_shardaware_port),
                },
            },
            'socket': _socket_report(cluster.sockopts),
            'reconnection': {
                'policy': _reconnection_policy_report(cluster.reconnection_policy),
            },
        }

        connect_timeout_ms = _optional_ms(cluster.connect_timeout)
        if connect_timeout_ms is not None:
            report['connect']['timeout-ms'] = connect_timeout_ms

        tls = self._tls_report(cluster)
        if tls is not None:
            report['tls'] = tls
        return report

    def _control_plane_report(self, cluster, is_scylla):
        """
        The ``control-plane`` group: the timeouts on the driver's own queries,
        the ones it runs to discover the cluster rather than on behalf of the
        application.

        The two system-query timeouts are different things, which is why the
        schema has both. The client-side one is how long the driver waits for a
        reply; the server-side one is a limit the server enforces, which this
        driver applies by appending ``USING TIMEOUT`` to the query.
        """
        timeout = {}

        client_side_ms = _optional_ms(cluster.control_connection_timeout)
        if client_side_ms is not None:
            timeout['client-side-ms'] = client_side_ms

        # USING TIMEOUT is a ScyllaDB extension, so against anything else the
        # driver does not append it and there is no server-side limit to report:
        # ControlConnection._try_connect drops metadata_request_timeout on a
        # connection with no sharding info, and this reports what the driver
        # will do rather than only what it was configured to do. A configured
        # zero means the same thing, letting the server's own default apply.
        if is_scylla:
            server_side_ms = _server_side_timeout_ms(cluster.metadata_request_timeout)
            if server_side_ms is not None:
                timeout['server-side-ms'] = server_side_ms

        return {
            'queries': {'system': {'timeout': timeout}},
            'schema': {
                # nonNegativeInteger and required: zero means the driver does
                # not wait for schema agreement at all, which is a setting
                # rather than the absence of one.
                'agreement': {'timeout-ms': _non_negative_ms(cluster.max_schema_agreement_wait)},
            },
        }

    def _tls_report(self, cluster):
        """
        The ``connection.tls`` group, or ``None`` when TLS is not configured.

        Booleans only: the schema is explicit that this group never carries
        credentials, keys or host lists, and nothing here reads any.

        Hostname verification is always knowable in this driver. An explicit
        ``ssl_context`` carries it as an attribute, and options on their own are
        turned into a context by
        :meth:`cassandra.connection.Connection._build_ssl_context_from_options`,
        which reads the same key this does.

        The context wins when both are given, which is the pair
        ``Cluster(cloud=...)`` builds: a context that does not verify, alongside
        an ``ssl_options`` of ``{'check_hostname': True}``. The context is what
        verifies -- :class:`~cassandra.connection.Connection` builds one from the
        options only when it has none, and ``_wrap_socket_from_context`` forwards
        ``server_hostname`` but never ``check_hostname`` -- so reading the options
        there would report verification that does not happen.
        """
        if cluster.ssl_context is not None:
            return {'hostname-verification': bool(getattr(cluster.ssl_context,
                                                          'check_hostname', False))}
        if cluster.ssl_options:
            return {'hostname-verification': bool(cluster.ssl_options.get('check_hostname', False))}
        return None
