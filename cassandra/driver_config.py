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
from collections import namedtuple
from itertools import repeat

from cassandra import ConsistencyLevel
from cassandra.policies import (ConstantReconnectionPolicy,
                                ConstantSpeculativeExecutionPolicy,
                                DCAwareRoundRobinPolicy,
                                DowngradingConsistencyRetryPolicy,
                                ExponentialBackoffRetryPolicy,
                                ExponentialReconnectionPolicy,
                                FallthroughRetryPolicy, NeverRetryPolicy,
                                NoSpeculativeExecutionPolicy,
                                DefaultLoadBalancingPolicy,
                                RackAwareRoundRobinPolicy, RetryPolicy,
                                RoundRobinPolicy, TokenAwarePolicy)
from cassandra.timestamps import MonotonicTimestampGenerator

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


def _consistency_name(level, setting):
    """
    The schema's name for a consistency level.

    The schema takes the name and this driver holds the wire integer, and its
    enum covers every level the driver defines, so any level that came from
    :class:`~.ConsistencyLevel` maps. One that did not is not a level the driver
    can use either -- ``None`` fails to pack into a request at all, and an
    unknown integer is rejected by the server -- so there is nothing truthful to
    report for it, and a report that named one anyway would tell an operator
    that a client which cannot execute a query is querying at that level.

    Raising drops the whole report, which is the right outcome: `consistency` is
    a required key, so no conformant document describes such a configuration.
    The message names the setting, since the alternative is a bare KeyError
    under a generic "unable to build the report" warning.

    A level that cannot be a dict key at all -- a list, say -- fails the lookup
    with a TypeError rather than a KeyError, and is the same kind of wrong: it is
    caught here too, so that it gets the message naming the setting instead of
    the generic warning this one exists to avoid.
    """
    try:
        return ConsistencyLevel.value_to_name[level]
    except (KeyError, TypeError):
        raise ValueError(
            "%s is %r, which is not a consistency level this driver defines; "
            "the configuration report describes the consistency a client uses "
            "and cannot describe one it cannot use" % (setting, level)) from None


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


def _integer_ceiling(limit):
    """
    `limit` rounded up to a builtin ``int``, or ``None`` when no integer can
    express it.

    ``math.ceil`` rather than a check against particular numeric types: a limit
    is compared against a counter, and anything that can say what its ceiling is
    can be counted against one. The result is coerced to a builtin ``int`` so
    that no other numeric type reaches the report, where the schema wants an
    integer, and so that a limit of ``True`` does not travel on as JSON true
    where a number belongs.

    ``None`` is for the limits arithmetic cannot name: ``float('inf')``, which is
    how an application spells "without limit" and which the policies really do
    accept, ``nan``, and anything that is not a number at all. Every caller has
    its own way of saying that a limit is not one the report can carry, and none
    of them may raise -- this runs while a connection is being established, and
    one unnameable limit must not cost the report every other group it would
    have carried.
    """
    try:
        return int(math.ceil(limit))
    except (TypeError, ValueError, OverflowError):
        return None


def _attempt_ceiling(limit):
    """
    How many attempts a ``while i < limit`` loop makes, or ``None`` when `limit`
    does not bound one -- because it admits no attempt at all, or because it is
    not a limit any integer can express.
    """
    if limit is None:
        return None
    attempts = _integer_ceiling(limit)
    if attempts is None:
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


_RETRY_POLICY_TYPES = {
    # Exact types, not a base class: every one of the others below is a subclass
    # of RetryPolicy, so isinstance would report all of them as the first entry.
    RetryPolicy: 'standard-error-aware',
    FallthroughRetryPolicy: 'fallthrough',
    NeverRetryPolicy: 'never',
    DowngradingConsistencyRetryPolicy: 'downgrading-consistency',
}


def _retry_report(policy, setting):
    """
    The ``query.retry`` group: the policy, and the delay between attempts where
    the policy has one.

    A policy of ``None`` is not the fallthrough it looks like. That arm means
    the driver rethrows the original error to the caller untouched, and what
    actually happens is that ResponseFuture calls ``on_request_error`` on it
    and raises AttributeError -- losing the original error rather than passing
    it on. Naming it fallthrough would describe a working configuration where
    there is a broken one, and `policy` is a required key, so there is no
    conformant document to be had either. ExecutionProfile replaces a None its
    constructor is given, but both it and Cluster.default_retry_policy stay
    writable and unvalidated, so this is reachable by assignment.
    """
    if policy is None:
        raise ValueError(
            "%s is None, which is not a retry policy the driver can use: a "
            "request error raises AttributeError on it rather than being "
            "retried or passed on, and the configuration report describes what "
            "a client does" % (setting,))

    policy_type = _RETRY_POLICY_TYPES.get(type(policy))
    if policy_type is not None:
        return {'policy': {'type': policy_type}}

    if type(policy) is ExponentialBackoffRetryPolicy:
        # It retries the same errors as the standard policy and adds a growing
        # delay between attempts, which is what the schema's backoff describes,
        # so it is that policy with a backoff rather than a type of its own.
        report = {'policy': {'type': 'standard-error-aware'}}
        # Every on_* method gives up once retry_num reaches max_num_retries, and
        # the comparison is `<`, so a fractional limit permits the ceiling: 0.5
        # allows one retry. Truncating would report that as no retries at all,
        # which is what the schema reads a zero as. The attribute is typed float,
        # so fractions are expected -- and so is float('inf'), which is how an
        # application says "retry until the request runs out of time". No integer
        # names that one, and the key is absent when no explicit limit is
        # configured, which is the closest true thing the schema can say about
        # it. A negative limit retries nothing, which is what a zero says.
        max_retries = _integer_ceiling(policy.max_num_retries)
        if max_retries is not None:
            report['policy']['max-retries'] = max(0, max_retries)
        # Only when there is a delay to describe. _calculate_backoff is
        # min(max_interval, min_interval * 2 ** attempt) plus jitter scaled by
        # min_interval, so a min_interval of zero is zero at every attempt
        # whatever max_interval says. The schema leaves backoff out for exactly
        # that -- "absent when there is no delay between attempts" -- and every
        # delay it does carry must be greater than zero.
        # A non-finite interval is left out with them. _calculate_backoff
        # returns that interval or something built from it, and a retry
        # scheduled at an infinite delay is one the driver never gets to --
        # there is no delay here the schema can carry, and backoff is optional
        # for precisely the case where there is none to describe.
        if policy.min_interval > 0 and _finite(policy.min_interval) and _finite(policy.max_interval):
            # The initial delay is min(max_interval, min_interval), not
            # min_interval: _calculate_backoff caps the whole curve at
            # max_interval, and the policy does not check that the two were
            # given the right way round. Reporting min_interval would claim a
            # first delay the policy never waits whenever max_interval is the
            # smaller. Taking the minimum also keeps the schema's requirement
            # that max-ms be at least base-ms true by construction.
            base_ms = _required_ms(min(policy.min_interval, policy.max_interval))
            report['backoff'] = {'type': 'exponential',
                                 'base-ms': base_ms,
                                 'max-ms': _required_ms(policy.max_interval)}
        return report

    return {'policy': _custom_policy_report(policy)}


_MAX_POLICY_CHAIN = 1024
"""
Backstop on how far to follow ``_child_policy`` looking for the policy that
holds the location preference.

The walk stops on its own once it reaches a policy it has already seen, which is
what a chain looping back on itself does, so this is not what ends an ordinary
walk. It is here for the one case identity cannot catch: a ``_child_policy``
implemented as a property that manufactures a new object on each access, where
every step looks like somewhere new. This runs while a connection is being
established, and a walk that never ends would hang the handshake -- the one
thing this module must never do.

Set far above any chain an application would build, since stopping early is not
free: the walk reports no location preference at all, which reads as a client
pinned to nothing rather than one whose preference sits deeper than the walk
went, and the chain falls back to the custom arm because what is below the cut
cannot be accounted for. The deepest chain in :mod:`cassandra.policies` is three.
"""


_TRUNCATED = object()
"""
Stands in :func:`_policy_chain`'s walk for the part of a chain the cap stopped it
reaching.

Yielded rather than returned, so that a caller reading the walk one link at a
time cannot miss it: what is below the cut is by definition unaccounted for, and
a survey that answered "every policy in this chain is one I know" about a chain
it stopped walking would put the built-in arm's routing flags against links it
never saw.
"""


_DESCRIBABLE_LOAD_BALANCING_POLICIES = (
    TokenAwarePolicy,
    DCAwareRoundRobinPolicy,
    RackAwareRoundRobinPolicy,
    RoundRobinPolicy,
    # Delegates every decision to its child bar one: it puts a query's
    # target_host first when the statement sets one, which is a per-request
    # choice rather than a property of the configuration this describes.
    DefaultLoadBalancingPolicy,
)
"""Policies whose routing the token-aware flags can describe.

Everything else makes the chain undescribable, however ordinary the policy
wrapping it. WhiteListRoundRobinPolicy confines routing to a fixed host list and
HostFilterPolicy to whatever an application-supplied predicate admits; neither
has anywhere to go in the built-in arm, so reporting that arm would assert
plain token-aware routing and say nothing of the restriction.

Exact types, as everywhere else here: a subclass is a policy this module knows
nothing about, and WhiteListRoundRobinPolicy -- a RoundRobinPolicy subclass that
is emphatically not one -- is why that matters.
"""


def _policy_chain(policy):
    """
    Each policy from `policy` down through ``_child_policy``.

    Stops on reaching a policy it has already seen, which is what a chain
    looping back on itself does. Identity rather than equality, since a custom
    policy is free to define __eq__ and compare equal to a different policy, or
    to define it without __hash__ and not go into a set at all. Each policy is
    held on to as well, so that its id cannot be reused by one created later in
    the walk and read as a loop that is not there.

    Running out of :const:`_MAX_POLICY_CHAIN` with a chain still to go yields
    :const:`_TRUNCATED` last, which a chain that ended on its own never does. A
    caller cannot otherwise tell the two apart, and the difference is whether the
    links it saw are the whole chain.
    """
    seen, pinned = set(), []
    for _ in range(_MAX_POLICY_CHAIN):
        if policy is None or id(policy) in seen:
            return
        yield policy
        seen.add(id(policy))
        pinned.append(policy)
        policy = getattr(policy, '_child_policy', None)
    if policy is not None and id(policy) not in seen:
        yield _TRUNCATED


_PolicyChainSurvey = namedtuple('_PolicyChainSurvey', 'located token_aware describable')


def _survey_policy_chain(policy):
    """
    Everything the load balancing group needs to know about a chain, from a
    single walk of it: the policy carrying the location preference, the
    token-aware policy, and whether every policy in it is one this module can
    account for.

    One walk rather than one per question, for two reasons.

    It bounds the work. The walk is capped at :const:`_MAX_POLICY_CHAIN`, and the
    case that cap exists for -- a ``_child_policy`` that manufactures a new
    object on each access -- costs that many policy objects every time the chain
    is walked, while a connection is being established.

    And it makes the answers describe the same chain. A ``_child_policy`` that
    returns something different on each access hands a different chain to each
    walk, so separate walks disagree: one finds a token-aware policy where the
    next finds none. The report would then combine a preference found in one
    chain with an arm decided from another.

    The token-aware policy is looked for anywhere in the chain rather than only
    at the top, since a transparent wrapper above it does not stop the routing
    being token aware. So is the location preference: under this schema it
    belongs to the session rather than to the policy that happens to hold it, and
    a bare :class:`~.DCAwareRoundRobinPolicy` -- what
    :func:`cassandra.cluster.default_lbp_factory` returns without the murmur3
    extension -- pins the driver to a datacenter just as firmly as a token-aware
    policy wrapping one.
    """
    located = token_aware = None
    describable = True
    for link in _policy_chain(policy):
        if link is _TRUNCATED:
            # The cap cut the walk short, so the rest of the chain is as unknown
            # as an application-supplied policy is, and for the same reason: the
            # flags below describe the routing of the whole chain, and there is
            # more of it than this walk saw. Whatever preference was found above
            # the cut still holds, since a policy deeper down cannot take it
            # back -- only add one this walk never reaches.
            describable = False
            break
        kind = type(link)
        if token_aware is None and kind is TokenAwarePolicy:
            token_aware = link
        if located is None and kind in (DCAwareRoundRobinPolicy, RackAwareRoundRobinPolicy):
            located = link
        if kind not in _DESCRIBABLE_LOAD_BALANCING_POLICIES:
            # The built-in arm's flags describe the routing of the chain, not of
            # the policy at the top of it, so they can only be filled in when
            # every policy in it is one this module knows. A chain reaching an
            # application-supplied policy is reported as custom even with a
            # driver policy wrapping it: the flags would otherwise assert
            # something about query plans this code cannot see.
            describable = False
    return _PolicyChainSurvey(located, token_aware, describable)


def _node_location_preference_report(policy):
    """
    The ``node-preference`` value describing which datacenter, and possibly
    which rack, the driver prefers.

    Sourced from the load balancing policy, which is where this driver keeps it;
    the schema asks for it here in that case rather than in a group of its own.
    Takes the location-aware policy :func:`_survey_policy_chain` found, which is
    ``None`` when the chain holds none at all.
    """
    if type(policy) is DCAwareRoundRobinPolicy:
        if policy._local_dc_explicit:
            return {'type': 'dc', 'local-dc': policy.local_dc}
        # Inferred from the first host to come up, and not necessarily known
        # yet: the schema allows local-dc to be absent until it is.
        report = {'type': 'dc-auto'}
        if policy.local_dc:
            report['local-dc'] = policy.local_dc
        return report

    if type(policy) is RackAwareRoundRobinPolicy:
        # Both are mandatory constructor arguments and are never reassigned, so
        # they are configured rather than inferred whenever they are set at all.
        if policy.local_dc and policy.local_rack:
            return {'type': 'rack', 'local-dc': policy.local_dc,
                    'local-rack': policy.local_rack}
        if policy.local_dc:
            return {'type': 'dc', 'local-dc': policy.local_dc}

    return None


def _falls_back_to_non_preferred_nodes(located, node_preference):
    """
    Whether a request may reach a node outside the reported ``node-preference``.

    Defined against what was reported rather than against the policy type,
    because that is how the schema defines it, and because a rack-aware policy
    whose rack is unset reports a datacenter preference and has to be judged as
    one.

    A rack preference always allows it. ``RackAwareRoundRobinPolicy``'s query
    plan yields the local datacenter's other racks straight after the local-rack
    tier, unconditionally -- ``used_hosts_per_remote_dc`` gates only the remote
    datacenters below that -- so a request routinely reaches a node the reported
    preference excludes. ``used_hosts_per_remote_dc`` of zero would otherwise
    report the opposite. The schema's single boolean cannot say "leaves the rack
    but not the datacenter", and of the two answers this is the true one.

    Only ``rack``, and not the schema's ``rack-auto``:
    :func:`_node_location_preference_report` never reports the latter, because
    ``RackAwareRoundRobinPolicy`` takes both the datacenter and the rack as
    mandatory constructor arguments and never infers either. Testing for it here
    would read as though this driver produces it somewhere.

    A datacenter preference allows it only once the policy is told how many
    remote hosts to use, since both datacenter-aware policies ignore them
    entirely until then.

    No preference at all does not, and not because such a chain keeps requests
    anywhere -- a round-robin policy treats every host as local and will happily
    reach a remote datacenter. It reports false because it declares no
    preference for a request to fall outside of, and no node-preference is
    reported for it either, which is what this flag is defined against. The
    other ScyllaDB drivers do not all answer this the same way, so it is a
    deliberate choice rather than the only reading.
    """
    if node_preference is None:
        return False
    if node_preference['type'] == 'rack':
        return True
    return bool(getattr(located, 'used_hosts_per_remote_dc', 0))


def _load_balancing_report(policy):
    """
    The ``query.load-balancing`` group.

    The built-in ``token-aware`` arm carries flags describing where a request may
    go, so it is claimed only when the chain holds a token-aware policy *and*
    every policy in it is one this module can account for: see
    :func:`_survey_policy_chain`. A transparent
    wrapper above the token-aware policy does not disqualify the chain, since it
    does not change where requests go; a policy whose routing this module cannot
    see does, however ordinary the policy wrapping it. Everything else is a
    policy the shared vocabulary has no terms for, and is reported by name --
    the plain round-robin policies among them, built in to this driver but not
    token-aware.

    The datacenter preference is reported either way. It is a sibling of the
    policy in the schema rather than a property of the built-in arm, and a
    policy this module has no name for still pins the driver somewhere the
    operator has to be able to see, so it is taken from wherever in the chain
    :func:`_survey_policy_chain` found it.

    No policy at all is not a custom one, whatever the chain walk makes of it.
    The caller resolves a load balancer of ``None`` the way ResponseFuture does,
    so reaching here with one means nothing resolved it: a request takes
    ``make_query_plan`` off ``None`` and raises. Reporting a custom policy would
    tell an operator a user-supplied one is routing, and `policy` is a required
    key, so there is no conformant document to be had either.
    """
    if policy is None:
        raise ValueError(
            "load_balancing_policy is None, which is not a policy the driver can "
            "route with: a request raises AttributeError on it, and the "
            "configuration report describes what a client does")

    survey = _survey_policy_chain(policy)
    located = survey.located
    # Built first: the fallback flag below is defined against what this reports.
    node_preference = _node_location_preference_report(located)

    token_aware = survey.token_aware
    if token_aware is not None and survey.describable:
        report = {
            'policy': {
                'type': 'token-aware',
                # Replicas are yielded in a random order unless that is turned
                # off, in which case they keep the order the replica set has.
                'load-distribution': ('shuffle' if token_aware.shuffle_replicas
                                      else 'replica-set'),
                'fallback-to-non-preferred-nodes': _falls_back_to_non_preferred_nodes(
                    located, node_preference),
            },
        }
    else:
        # Named after the policy the application configured, which is the one at
        # the top of the chain rather than whichever link made it undescribable.
        report = {'policy': _custom_policy_report(policy)}

    if node_preference is not None:
        report['node-preference'] = node_preference
    return report


def _default_fetch_size():
    """
    The default page size, or ``None`` when paging is not limited by default.

    Read off the :class:`~.Session` class rather than an instance: paging is a
    session setting in this driver, and no session exists yet when the control
    connection reports. What this describes is the default every session created
    from that cluster will start with, which is the closest thing to a
    cluster-wide answer there is; a session that then sets its own
    ``default_fetch_size`` is not reflected here.

    Imported where it is used, since :mod:`cassandra.cluster` imports this
    module.
    """
    from cassandra.cluster import Session

    # operator.index rather than a check against int: a page size is packed into
    # the request as an integer, which takes anything with __index__, so a numpy
    # integer paginates exactly as a builtin one does and describing it as
    # unlimited would be wrong. It also returns a builtin int, which keeps a
    # page size of True out of the report as JSON true where a number belongs.
    try:
        fetch_size = operator.index(Session.default_fetch_size)
    except TypeError:
        return None
    return fetch_size if fetch_size > 0 else None


def _client_timestamps(timestamp_generator):
    """
    Whether the client assigns the write timestamp, or ``None`` when that cannot
    be answered.

    Two things decide it. :attr:`.Session.use_client_timestamp` gates whether
    the generator is consulted at all -- with it off the coordinator assigns
    every timestamp, whatever generator the cluster holds. It is read off the
    class for the same reason as :func:`_default_fetch_size`: it is a session
    setting, and no session exists yet when the control connection reports.

    Then a custom generator is the schema's "unknown": it is called per request
    and may return None for some of them, in which case the coordinator assigns
    the timestamp after all, and there is no way to tell from here which it will
    do.

    So is no generator at all, with the setting left on. That is not the
    coordinator assigning timestamps, which is what a False here says to the
    operator reading it: Session._create_response_future calls
    self.cluster.timestamp_generator() unconditionally under this setting, so
    every request raises a TypeError instead. Neither answer is true of such a
    cluster, and unlike the consistency level or the retry policy this key is
    optional -- so it is left out, rather than the whole report dropped over a
    configuration the driver will not get a query out of anyway.
    """
    from cassandra.cluster import Session

    if not Session.use_client_timestamp:
        return False
    if timestamp_generator is None:
        return None
    if type(timestamp_generator) is MonotonicTimestampGenerator:
        return True
    return None


def _speculative_delay_ms(delay):
    """
    The delay before each additional execution in milliseconds, or ``None`` when
    no execution will ever be started with it.

    A negative delay starts nothing: ``next_execution()`` hands the configured
    delay straight through, and
    :meth:`cassandra.cluster.ResponseFuture._start_timer` creates the
    speculative timer only for a delay of zero or more. It is also the very
    value the plan returns once it has run out, so the driver cannot tell a
    negative delay from an exhausted plan.

    A delay that cannot be compared with zero at all starts nothing either, and
    takes the request with it: that comparison is ``_start_timer``'s, and it
    raises there. :class:`~.ConstantSpeculativeExecutionPolicy` validates its
    arguments no more than it validates the rest, so both are reachable.

    Neither has a value the group can carry -- ``delay-ms`` is a
    ``nonNegativeInteger`` -- which is why both come back as the absence the
    caller turns into an absent group.

    A delay that is not finite starts nothing either. _start_timer schedules at
    that delay and an infinite one comes due at no moment the timer ever
    reaches, while a nan sorts against no deadline at all -- so no additional
    execution is launched, and the absent group says exactly that.
    """
    try:
        if delay < 0:
            return None
    except TypeError:
        return None
    if not _finite(delay):
        return None
    return _non_negative_ms(delay)


def _speculative_execution_report(policy):
    """
    The ``query.speculative-execution`` group, or ``None`` when the driver will
    not start a duplicate execution -- which the schema expresses by leaving the
    group out rather than by a policy that does nothing.
    """
    if policy is None or type(policy) is NoSpeculativeExecutionPolicy:
        return None

    if type(policy) is ConstantSpeculativeExecutionPolicy:
        # The delay decides first, because an unusable one starts nothing
        # whatever the count says -- including a count of float('inf'), which
        # otherwise reaches the custom arm below and reports a policy that races
        # as often as it likes while _start_timer never makes it a timer at all.
        # No usable delay: see _speculative_delay_ms.
        delay_ms = _speculative_delay_ms(policy.delay)
        if delay_ms is None:
            return None

        # The plan counts `remaining` down while it is above zero, so a
        # fractional limit yields the ceiling here too: 0.5 admits one execution
        # and 1.5 admits two.
        max_executions = _integer_ceiling(policy.max_attempts)
        if max_executions is None:
            # A limit no integer can express, float('inf') being the one an
            # application would reach for to keep racing for as long as the
            # request lives. The plan counts down from it and never runs out, so
            # the driver does speculate, and leaving the group out would say it
            # never does. max-executions is a required positiveInteger with no
            # way to say "without limit", so the arm for a policy the shared
            # vocabulary cannot describe is the only truthful one left -- as it
            # is for a limit that is not a number at all, which is a policy that
            # raises when it builds its plan.
            return {'policy': _custom_policy_report(policy)}

        if max_executions < 1:
            # The other way to configure a policy that never races anything, and
            # the group cannot describe it from the inside either:
            # max-executions is a required positiveInteger with no way to say
            # "none", so absence is how the schema says it -- exactly as for the
            # no-op policy above. The plan's next_execution() returns -1 from the
            # very first call.
            return None

        return {'policy': {'type': 'constant',
                           'max-executions': max_executions,
                           'delay-ms': delay_ms}}

    return {'policy': _custom_policy_report(policy)}


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
        report['query'] = self._query_report(cluster)

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

    def _query_report(self, cluster):
        """
        The ``query`` group: what the driver does with a statement that does not
        override any of it.

        Reported from the default execution profile. The schema has one query
        group and this driver has as many profiles as the application cares to
        define, so the one that describes the session is the one a statement
        gets when it names none. Profiles other than the default cannot be
        described under this schema version.

        Which of the two configuration modes is live decides where the policies
        and the defaults come from, and the profile is not always the answer.
        Assigning
        :attr:`~.Cluster.default_retry_policy` or
        :attr:`~.Cluster.load_balancing_policy` after construction switches the
        cluster to legacy mode and updates only the cluster attribute, leaving
        the default profile holding whatever it was built with. A request then
        takes the cluster's, so reading the profile would describe policies
        nothing will ever use. Given to the constructor instead, the two agree,
        because the profile is built from those same attributes.

        This is the choice
        :meth:`cassandra.cluster.Session._create_response_future` makes for
        every request, and
        :meth:`cassandra.cluster.ControlConnection._try_connect_to_hosts` for
        its own connections. Imported where it is used, since
        :mod:`cassandra.cluster` imports this module.
        """
        from cassandra.cluster import _ConfigMode

        profile = cluster.profile_manager.default
        legacy = cluster._config_mode == _ConfigMode.LEGACY

        report = {
            'defaults': self._query_defaults_report(cluster, profile, legacy),
            'retry': _retry_report(
                cluster.default_retry_policy if legacy else profile.retry_policy,
                'default_retry_policy' if legacy else 'retry_policy'),
            # ResponseFuture resolves a load balancer of None to the default
            # profile's policy -- `load_balancer or _default_load_balancing_policy`
            # -- so in legacy mode with none set that is what a request routes
            # with, and reporting the None would describe a policy nothing uses.
            'load-balancing': _load_balancing_report(
                (cluster.load_balancing_policy if legacy else profile.load_balancing_policy)
                or cluster._default_load_balancing_policy),
        }

        # Legacy configuration races nothing, whatever the profile holds: the
        # legacy branch of _create_response_future leaves its speculative
        # execution plan unset, so there is no group to report.
        speculative_execution = None if legacy else _speculative_execution_report(
            profile.speculative_execution_policy)
        if speculative_execution is not None:
            report['speculative-execution'] = speculative_execution
        return report

    def _query_defaults_report(self, cluster, profile, legacy):
        """
        The ``query.defaults`` group.

        A snapshot taken before any :class:`~.Session` exists, so the settings
        this driver keeps on the session rather than on the profile are read off
        the :class:`~.Session` class -- ``default_fetch_size`` and
        ``use_client_timestamp`` always, and the three below in legacy
        configuration mode. What that describes is the default every session
        created from this cluster will start with, which is the closest thing to
        a cluster-wide answer there is; a session that then sets its own is not
        reflected here.

        Which of the two configuration modes is live decides where the
        consistency, the serial consistency and the request timeout come from,
        the same way it does for the policies in :meth:`_query_report`. The
        legacy branch of
        :meth:`cassandra.cluster.Session._create_response_future` reads
        ``default_consistency_level``, ``default_serial_consistency_level`` and
        ``default_timeout`` off the session and never looks at the profile, so
        reading the profile there would describe a consistency nothing will ever
        query at: the profile is built holding
        :attr:`.ExecutionProfile.consistency_level`'s own default rather than the
        session's. Under profiles the profile is the answer.
        """
        from cassandra.cluster import Session

        if legacy:
            consistency = Session._default_consistency_level
            serial_consistency = Session._default_serial_consistency_level
            request_timeout = Session._default_timeout
            consistency_setting, serial_setting = ('default_consistency_level',
                                                   'default_serial_consistency_level')
        else:
            consistency = profile.consistency_level
            serial_consistency = profile.serial_consistency_level
            request_timeout = profile.request_timeout
            consistency_setting, serial_setting = ('consistency_level',
                                                   'serial_consistency_level')

        report = {
            'consistency': _consistency_name(consistency, consistency_setting),
            # This driver has no configurable default: Statement.is_idempotent
            # is False unless a statement says otherwise, and nothing at cluster
            # or profile level changes that.
            'idempotence': False,
        }

        # Unset means the server's own default applies, which is not this
        # driver's to describe. A level that is not a serial one is not this
        # driver's to describe either: ExecutionProfile validates the argument
        # its constructor is given and leaves the attribute writable, and
        # Session.default_serial_consistency_level's setter validates every
        # assignment, so a non-serial level is reachable through the profile
        # alone. The schema's enum here is the two serial levels, so naming one
        # would put a value in the field no consumer has to accept -- and a
        # non-serial level is not one a conditional statement can use anyway.
        if serial_consistency is not None:
            if ConsistencyLevel.is_serial(serial_consistency):
                report['serial-consistency'] = _consistency_name(
                    serial_consistency, serial_setting)
            else:
                # Warned rather than passed over: absence in this field means
                # the server's default applies, which is not what is happening,
                # and the key being optional is the only reason this does not
                # take the whole report down the way an unnameable consistency
                # does.
                log.warning("%s is %r, which is not a serial consistency level; "
                            "it will be left out of the driver configuration report",
                            serial_setting, serial_consistency)

        request_timeout_ms = _optional_ms(request_timeout)
        if request_timeout_ms is not None:
            report['request'] = {'timeout-ms': request_timeout_ms}

        # Paging is a Session setting rather than a profile one, and no Session
        # exists yet when the control connection reports: this is the default
        # every Session created from now on will start with.
        page_size = _default_fetch_size()
        if page_size is not None:
            report['page'] = {'size': page_size}

        client_timestamps = _client_timestamps(cluster.timestamp_generator)
        if client_timestamps is not None:
            report['client-timestamps'] = client_timestamps
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
