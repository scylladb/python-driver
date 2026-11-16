# Copyright ScyllaDB, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0

"""Bounded management execution contexts.

No query-path work belongs here.

Owners handle short, nonblocking messages. Physical work belongs to
an existing executor and lifecycle notifications to the serial callback
lane. Charged future callback batches run on the selected reactor. After
a batch retires, external registrations may drain through the same
serialized adapter on their caller. C/P attach to a current charged
batch. State not yet migrated still requires its original publication
locks.
"""

from collections import deque
from concurrent.futures import Future, TimeoutError
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
from itertools import chain
import logging
import os
from threading import (
    Condition, Event, Lock, Thread, current_thread, enumerate as active_threads,
    local)
import time
from typing import NamedTuple

from cassandra import DriverException

log = logging.getLogger(__name__)
_context = local()
_PENDING = object()


class ManagementClosed(DriverException):
    """The original management lifetime has ended."""


class ManagementSuperseded(DriverException):
    """The operation was superseded without losing its waiters."""


class ManagementOverloaded(DriverException):
    """No reservation is available for a nonblocking submission."""


class ManagementDeliveryFailed(DriverException):
    """Delivery failed while native outcomes remain terminal."""


class ManagementContextError(DriverException):
    """A blocking management call would wait on its own context."""


def execution_context():
    return getattr(_context, 'kind', None)


def assert_can_wait():
    if execution_context() in ('cluster', 'pool', 'bridge'):
        raise ManagementContextError(
            'Blocking management call on an owner or reactor')


def _future_access(method):
    @wraps(method)
    def access(self, *args, **kwargs):
        self._record.runtime._check_pid()
        previous = getattr(_context, 'future', None)
        _context.future = self
        try:
            return method(self, *args, **kwargs)
        finally:
            _context.future = previous
    return access


class _FutureCondition(Condition):
    """Keep bulk-wait rejection inside its cleanup context."""

    def __init__(self, runtime):
        super().__init__()
        self._runtime = runtime
        self._acquire = self.acquire
        self._release = self.release
        self.acquire = self._checked_acquire
        self.release = self._checked_release

    def _checked_acquire(self, *args, **kwargs):
        # Stdlib bulk acquisition does not unwind an acquisition failure.
        # Skip inherited locks; the Future state guard rejects inside the
        # bulk context, whose exit then releases earlier fresh locks.
        if self._runtime._pid != os.getpid():
            return False
        return self._acquire(*args, **kwargs)

    def _checked_release(self):
        if self._runtime._pid == os.getpid():
            self._release()
        return None

    def __enter__(self):
        self._runtime._check_pid()
        return super().__enter__()


@contextmanager
def _entered(kind, record=None):
    previous = (getattr(_context, 'kind', None),
                getattr(_context, 'record', None))
    _context.kind, _context.record = kind, record
    try:
        yield
    finally:
        _context.kind, _context.record = previous


@dataclass(frozen=True)
class Envelope:
    operation: int
    kind: str
    origin: object
    lifetime: object
    identity: tuple


@dataclass
class _HealthSlot:
    incarnation: object
    apply: object = None
    notification: object = None
    removed: bool = False


class _MailboxEntry(NamedTuple):
    record: object
    callback: object
    retain_on_close: bool


class OperationFuture(Future):
    """Native futures complete without running callbacks on owners."""

    def __init__(self, record):
        super().__init__()
        self._record = record
        self._condition = _FutureCondition(record.runtime)
        self._budget = 'ordinary'
        self._delivery = _CallbackDelivery(self, record)
        self._publishing = False

    @property
    def _state(self):
        self._record.runtime._check_pid()
        state = self.__state
        if (state not in ('CANCELLED_AND_NOTIFIED', 'FINISHED') and
                getattr(_context, 'future', None) is not self):
            # Bulk waits read state after acquiring ALL future conditions.
            # Reject before waiter installation, where their context unwinds
            # every lock. Own nonblocking methods use scoped access above.
            self._assert_can_wait()
        return state

    @_state.setter
    def _state(self, value):
        self.__state = value

    @_future_access
    def __repr__(self):
        return super().__repr__()

    @_future_access
    def done(self):
        return super().done()

    @_future_access
    def running(self):
        return super().running()

    @_future_access
    def cancelled(self):
        return super().cancelled()

    @_future_access
    def result(self, timeout=None):
        if not self.done():
            self._assert_can_wait()
        return super().result(timeout)

    @_future_access
    def exception(self, timeout=None):
        if not self.done():
            self._assert_can_wait()
        return super().exception(timeout)

    def _assert_can_wait(self):
        assert_can_wait()
        current = getattr(_context, 'record', None)
        if (execution_context() == 'callback' and current is not None and
                current.runtime is self._record.runtime and
                self._record.envelope.kind == 'notification'):
            raise ManagementContextError(
                'Notification wait would block its own callback lane')

    @_future_access
    def cancel(self):
        cancelled = super().cancel()
        if cancelled:
            # Future.cancel() alone leaves concurrent.futures.wait() asleep.
            with self._condition:
                if self._state == 'CANCELLED':
                    self.set_running_or_notify_cancel()
        return cancelled

    @_future_access
    def set_result(self, result):
        return super().set_result(result)

    @_future_access
    def set_exception(self, exception):
        return super().set_exception(exception)

    @_future_access
    def set_running_or_notify_cancel(self):
        return super().set_running_or_notify_cancel()

    def _invoke_callbacks(self):
        if not self._publishing:
            self._record.runtime._schedule_callbacks(self)

    def add_done_callback(self, fn):
        self._record.runtime._check_pid()
        self._delivery.add(fn)


class _CallbackDelivery:
    """Serialize callbacks while handing delivery between charged records.

    The runtime condition precedes the Future condition whenever both are
    needed. The standard Future callback list remains on the Future itself.
    """

    def __init__(self, future, record):
        self.future = future
        self.record = record
        self.error = None
        self.cursor = 0
        self.running = False
        self.waiters = set()

    @property
    def pending(self):
        return self.cursor < len(self.future._done_callbacks)

    def fail(self, error):
        with self.future._condition:
            self.error = error
            self.future._done_callbacks.clear()
            self.cursor = 0

    def add(self, fn):
        if self.error is not None:
            raise self.error
        if self.future._budget in ('reentry', 'aggregate'):
            raise ManagementContextError(
                'Private synchronous waiters have no callback batch')
        while True:
            record = self.record
            owner = execution_context() in ('cluster', 'pool')
            target = (getattr(_context, 'record', None)
                      if record.released and owner else record)
            if target is None:
                raise ManagementContextError(
                    'Owner callback has no reservation')
            with target.runtime._condition:
                with self.future._condition:
                    if record is not self.record:
                        continue
                    if owner and record.released and target is record:
                        # Retirement can happen after target selection but
                        # before acquiring the runtime condition.
                        continue
                    if self.error is not None:
                        raise self.error
                    if record.released and owner:
                        if target.released or not target.budgets['ordinary']:
                            raise ManagementContextError(
                                'Owner callback has no reservation')
                        self.record = target
                        target.foreign_futures.add(self.future)
                    self.future._done_callbacks.append(fn)
                    terminal = self.future.done()
                if not terminal:
                    return
                if not target.released:
                    target.runtime._queue_delivery_locked(target)
                    return
            # Released callbacks share a caller-side drainer; a concurrent
            # registrant may leave its callback to the active drainer.
            self.drain()
            return

    def drain(self, record=None):
        """Drain the adapter's serialized callback stream outside C/P."""
        with self.future._condition:
            if self.running:
                if record is not None:
                    self.waiters.add(record)
                return False
            self.running = True
            original = self.record
        while True:
            with self.future._condition:
                if (original is not self.record or
                        self.cursor == len(self.future._done_callbacks)):
                    self.running = False
                    resume, self.waiters = (
                        self.waiters, set())
                    if original is not self.record:
                        # An owner took responsibility for callbacks appended
                        # during a caller-side drain. Hand them back to its
                        # reactor batch after this callback has returned.
                        resume.add(self.record)
                    break
                callback = self.future._done_callbacks[self.cursor]
                self.cursor += 1
            try:
                callback(self.future)
            except BaseException:
                log.exception('Error in management completion callback')
        for waiting in resume:
            waiting.runtime._resume_delivery(waiting)
        return True


class _DeliveryTicket:
    """One cancellable completion ticket per charged record."""

    def __init__(self, callback):
        self._lock = Lock()
        self._callback = callback
        self._active = False

    def __call__(self):
        with self._lock:
            callback = self._callback
            if callback is None:
                return
            self._active = True
        try:
            callback()
        finally:
            with self._lock:
                self._callback = None
                self._active = False

    def cancel(self):
        with self._lock:
            if self._active:
                return False
            self._callback = None
            return True


class _Record:
    def __init__(self, runtime, envelope, handler, pool, key):
        self.runtime, self.envelope, self.handler = runtime, envelope, handler
        self.pool, self.key = pool, key
        self.budgets = dict.fromkeys(('ordinary', 'reentry', 'aggregate'), 0)
        self.private_released = False
        self.futures = []
        self.foreign_futures = set()
        self.physical = 0
        self.mailbox_pending = 0
        self.started = Event()
        self.handler_done = False
        self.delivery = False
        self.delivery_ticket = None
        self.delivery_again = False
        self.notification = False
        self.released = False


class _PhysicalAttempt:
    """Own one worker-to-owner handoff through adoption or disposal.

    Shared state is protected by the runtime condition. Physical credit stays
    charged until the worker has finished disposing any rejected result.
    """

    def __init__(self, record, function, accept, dispose):
        self.runtime = record.runtime
        self.record = record
        self.function, self.accept, self.dispose = function, accept, dispose
        self.returned_to_owner = Event()
        self.adopted = False
        self.received = False
        self.work_started = False
        self.submission_failed = False

    def submit(self, executor):
        try:
            physical = executor.submit(self.physical_work)
        except BaseException as exc:
            with self.runtime._condition:
                self.submission_failed = True
                self.runtime._finish(self.record, error=exc)
                if not self.work_started:
                    self.receive(None, exc)
            return _PENDING
        try:
            physical.add_done_callback(self.completed)
        except BaseException as exc:
            self.runtime._finish(self.record, error=exc)
            if physical.cancel():
                # No worker will discharge a cancellation before execution.
                # A callback installed before an interrupted registration may
                # also deliver it; receive claims the obligation exactly once.
                self.receive(None, exc)
        return _PENDING

    def completed(self, future):
        with _entered('worker', self.record):
            try:
                value, error = future.result(), None
            except BaseException as exc:
                value, error = None, exc
            self.receive(value, error)

    def receive(self, value, error):
        with self.runtime._condition:
            if self.received:
                return
            self.received = True
            if not self.runtime._closing:
                self.runtime._enqueue_locked(
                    self.record, lambda: self.returned(value, error),
                    retain_on_close=True)
                queued = True
            else:
                queued = False
        if queued and error is None:
            # Only the short native result transition is awaited. Neither
            # application callback delivery nor notification progress is
            # part of this handoff.
            self.returned_to_owner.wait()
        # Keep physical ownership until adoption or disposal completes,
        # including results rejected by a closed owner mailbox.
        try:
            if error is None and not self.adopted:
                self.dispose(value)
        except BaseException:
            log.exception('Error disposing late management result')
        finally:
            with self.runtime._condition:
                self.record.physical -= 1
                self.runtime._release_locked(self.record)

    def returned(self, value, error):
        try:
            with self.runtime._condition:
                if self.runtime._closing or self.record.handler_done:
                    return
                if error is not None:
                    self.runtime._finish(self.record, error=error)
                else:
                    try:
                        accepted = self.accept(value)
                    except BaseException as exc:
                        self.runtime._finish(self.record, error=exc)
                    else:
                        self.adopted = True
                        self.runtime._finish(self.record, accepted)
        finally:
            self.returned_to_owner.set()

    def physical_work(self):
        with self.runtime._condition:
            if self.submission_failed:
                # submit() can queue a callable before raising. Its
                # reservation was retired only if execution never began.
                return None
            self.work_started = True
        with _entered('worker', self.record):
            try:
                value = self.function()
            except BaseException as exc:
                self.receive(None, exc)
                raise
            else:
                # Hand off before completing the executor Future. A late
                # callback registration can run inline on C/P, but by then
                # this worker already owns adoption/disposal responsibility.
                self.receive(value, None)
                return value


class _LifetimeHandle:
    def __init__(self, runtime):
        self.runtime = runtime
        self.closed = Event()


class ManagementRuntime:
    """One C owner, two P runners and one callback lane per Cluster.

    ``dispatch`` schedules a callable on the shared reactor, never
    inline on C/P. It must remain usable until ``finalized``. A
    controlled dispatcher may be supplied by unit tests; production
    obtains it from its reactor adapter.

    Catastrophic loss of that validated shared service closes
    admission and records ``completion_error``. Native terminal
    outcomes remain immutable; undeliverable application callbacks
    are explicitly marked failed while physical cleanup continues.
    Normal startup, idle and close use the adapter's lifetime
    guarantee rather than this failure policy.
    """

    def __init__(self, dispatch, limit=1024, pool_limit=64, reentry_window=64,
                 on_finalized=None):
        self._dispatch = dispatch
        self.limit, self.pool_limit = limit, pool_limit
        self.reentry_window = reentry_window
        self.lifetime = object()
        self._pid = os.getpid()
        self._condition = Condition()
        self._cluster = deque()
        self._pools = {}
        self._ready = deque()
        self._active_pools = set()
        self._notifications = deque()
        self._records = set()
        self._keys = {}
        self._pool_credits = {}
        self._credits = 0
        self._mailbox_pending = 0
        self._delivery_batches = 0
        self._reentry_credits = 0
        self._reentry_active = False
        self._sequence = 0
        self._started = False
        self._closing = False
        self.completion_error = None
        self.cleanup_error = None
        self._threads = []
        self._exited = 0
        self._cleanup_obligations = 0
        self._finalizing = False
        self._on_finalized = on_finalized
        self._health = {}
        self._unknown_refresh = None
        self._lifetimes = set()
        self.closed = Event()
        self.finalized = Event()
        self.high_water = 0
        self.high_waters = dict.fromkeys(
            ('ordinary', 'reentry', 'aggregate', 'health', 'close', 'delivery',
             'mailbox'), 0)

    def _observe_counts_locked(self):
        counts = {
            'ordinary': self._credits, 'reentry': self._reentry_credits,
            'aggregate': int(self._reentry_active),
            'health': len(self._health),
            'close': len(self._lifetimes) + self._cleanup_obligations,
            'delivery': self._delivery_batches,
            'mailbox': self._mailbox_pending}
        for kind, count in counts.items():
            self.high_waters[kind] = max(self.high_waters[kind], count)
        return counts

    def counts(self):
        self._check_pid()
        with self._condition:
            return self._observe_counts_locked()

    def start(self):
        self._check_pid()
        with self._condition:
            self._check_pid()
            if self._closing:
                raise ManagementClosed('Management lifetime is closed')
            if self._started:
                return
            self._started = True
            targets = [('cluster', self._run_cluster),
                       ('pool', self._run_pool), ('pool', self._run_pool),
                       ('callback', self._run_notifications)]
            try:
                for kind, target in targets:
                    thread = Thread(target=self._run, args=(kind, target),
                                    name='cassandra-management-' + kind,
                                    daemon=True)
                    try:
                        thread.start()
                    except BaseException:
                        # A start interrupted after native launch still owns a
                        # runner, even before bootstrap publishes its ident.
                        if thread in active_threads():
                            self._threads.append(thread)
                        raise
                    self._threads.append(thread)
            except BaseException:
                self._begin_close_locked()
                # Started threads exit once this startup lock is released.
                raise

    def _check_pid(self):
        if self._pid != os.getpid():
            raise ManagementClosed(
                'Inherited management runtime cannot run after fork')

    def assert_owner(self, pool=None):
        self._check_pid()
        record = getattr(_context, 'record', None)
        if (execution_context() != ('cluster' if pool is None else 'pool') or
                record is None or record.runtime is not self or
                record.pool is not pool):
            raise ManagementContextError(
                'Management state accessed outside its owner')

    def _capacity(self, pool, budget):
        if budget == 'aggregate':
            return not self._reentry_active
        if budget == 'reentry':
            return self._reentry_credits < self.reentry_window
        return (self._credits < self.limit and
                (pool is None or
                 self._pool_credits.get(pool, 0) < self.pool_limit))

    def submit(self, handler, *, pool=None, key=None, identity=(),
               kind='operation', origin=None, wait=False, timeout=None,
               budget='ordinary', reserve=0):
        if budget not in ('ordinary', 'reentry', 'aggregate'):
            raise ManagementContextError('Unknown management admission budget')
        if reserve not in (0, 1) or (reserve and key is not None):
            raise ManagementContextError(
                'Physical fanout requires separate admitted records')
        if wait:
            assert_can_wait()
        self.start()
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            self._check_pid()
            while not self._closing and not self._capacity(pool, budget):
                if not wait:
                    raise ManagementOverloaded(
                        'Management admission capacity exhausted')
                current = getattr(_context, 'record', None)
                if (execution_context() == 'callback' and
                        current is not None and current.runtime is self and
                        budget == 'ordinary'):
                    raise ManagementContextError(
                        'Callback admission cannot wait behind its own credit')
                remaining = (None if deadline is None
                             else deadline - time.monotonic())
                if remaining is not None and remaining <= 0:
                    raise TimeoutError()
                self._condition.wait(remaining)
            if self._closing:
                raise ManagementClosed('Management lifetime is closed')
            if budget == 'aggregate':
                self._reentry_active = True
            elif budget == 'reentry':
                self._reentry_credits += 1
            else:
                self._credits += 1
                if pool is not None:
                    self._pool_credits[pool] = (
                        self._pool_credits.get(pool, 0) + 1)
                self.high_water = max(self.high_water, self._credits)
            coalesce = (pool, key) if key is not None else None
            record = self._keys.get(coalesce) if coalesce is not None else None
            if record is None:
                self._sequence += 1
                envelope = Envelope(self._sequence, kind, origin,
                                    self.lifetime, tuple(identity))
                record = _Record(self, envelope, handler, pool, coalesce)
                record.physical = reserve
                self._records.add(record)
                if coalesce is not None:
                    self._keys[coalesce] = record
                self._enqueue_locked(record, lambda: self._handle(record))
            future = OperationFuture(record)
            future._budget = budget
            record.futures.append(future)
            record.budgets[budget] += 1
            self._observe_counts_locked()
            self._condition.notify_all()
            return future

    def _account_mailbox_locked(self, record, delta):
        record.mailbox_pending += delta
        self._mailbox_pending += delta

    def _dequeue_locked(self, queue):
        entry = queue.popleft()
        self._account_mailbox_locked(entry.record, -1)
        return entry

    def _set_delivery_locked(self, record, active):
        if record.delivery != active:
            self._delivery_batches += 1 if active else -1
            record.delivery = active

    def _enqueue_locked(self, record, callback, retain_on_close=False):
        self._account_mailbox_locked(record, 1)
        entry = _MailboxEntry(record, callback, retain_on_close)
        if record.pool is None:
            self._cluster.append(entry)
        else:
            queue = self._pools.setdefault(record.pool, deque())
            queue.append(entry)
            if (record.pool not in self._active_pools and
                    record.pool not in self._ready):
                self._ready.append(record.pool)
        self._condition.notify_all()

    def _handle(self, record):
        if record.handler_done:
            return
        record.started.set()
        try:
            outcome = record.handler(record)
            if outcome is _PENDING:
                return
        except BaseException as exc:
            self._finish(record, error=exc)
        else:
            self._finish(record, outcome)

    def _finish(self, record, result=None, error=None):
        with self._condition:
            if record.handler_done:
                return
            record.handler_done = True
            record.started.set()
            if record.key is not None:
                self._keys.pop(record.key, None)
            for future in record.futures:
                with future._condition:
                    if not future.done():
                        # Keep cancellation and terminal publication atomic,
                        # but defer the cross-future delivery scan until this
                        # condition is released. Stdlib bulk waits acquire
                        # these conditions in a different order.
                        future._publishing = True
                        try:
                            if error is None:
                                future.set_result(result)
                            else:
                                future.set_exception(error)
                        finally:
                            future._publishing = False
            self._queue_delivery_locked(record)
            self._release_locked(record)

    def supersede(self, future):
        self._check_pid()
        self._finish(future._record,
                     error=ManagementSuperseded('Operation superseded'))

    def work(self, record, executor, function, *, accept=lambda value: value,
             dispose=lambda value: None):
        """Reserve result delivery before physical work, on C/P only.

        The executor runs work outside C/P. Its completion retains the worker
        until the owner accepts or rejects the value, then disposes a rejected
        value there. ``accept`` is a short native state transition, serialized
        with shutdown and supersession; it must not block or run user code.
        """
        self.assert_owner(record.pool)
        with self._condition:
            if (getattr(_context, 'record', None) is not record or
                    record.released or record.handler_done or record.physical):
                raise ManagementContextError(
                    'Physical work requires its current unused '
                    'result reservation')
            record.physical = 1
            self._condition.notify_all()
        return _PhysicalAttempt(record, function, accept, dispose).submit(
            executor)

    def notify(self, callback, *, wait=False, timeout=None, budget='ordinary'):
        self._check_pid()
        def admit(record):
            with self._condition:
                if self._closing or record.handler_done or record.released:
                    return _PENDING
                record.notification = True
                self._notifications.append((record, callback))
                self._condition.notify_all()
            return _PENDING
        return self.submit(admit, wait=wait, timeout=timeout, budget=budget,
                           kind='notification')

    @contextmanager
    def synchronous_reentry(self):
        """Reserve one L aggregate and a bounded prerequisite window."""
        self._check_pid()
        assert_can_wait()
        current = getattr(_context, 'record', None)
        if (execution_context() != 'callback' or current is None or
                current.runtime is not self):
            yield 'ordinary'
            return
        with self._condition:
            if self._reentry_active:
                raise ManagementContextError(
                    'Recursive native reentry aggregate')
            if self._closing:
                raise ManagementClosed('Management lifetime is closed')
            self._reentry_active = True
        try:
            yield 'reentry'
        finally:
            self._check_pid()
            with self._condition:
                self._reentry_active = False
                self._condition.notify_all()

    def register_health(self, incarnation):
        self.start()
        with self._condition:
            if self._closing:
                raise ManagementClosed('Management lifetime is closed')
            self._health.setdefault(
                id(incarnation), _HealthSlot(incarnation))
            self._observe_counts_locked()

    def observe_negative(self, incarnation, apply, notification=None,
                         removed=False):
        """Apply incarnation health despite ordinary admission load."""
        self._check_pid()
        with self._condition:
            if self._closing:
                return False
            slot = self._health.get(id(incarnation))
            if slot is None or slot.incarnation is not incarnation:
                self._unknown_refresh = True
                return False
            # An already pending removal is terminal for this incarnation.
            if slot.removed and not removed:
                return True
            slot.apply, slot.notification, slot.removed = (
                apply, notification, removed)
            self._condition.notify_all()
            return True

    def take_refresh_demand(self):
        self.assert_owner()
        with self._condition:
            demand, self._unknown_refresh = self._unknown_refresh, None
            return bool(demand)

    def unregister_health(self, incarnation):
        self._check_pid()
        with self._condition:
            self._health.pop(id(incarnation), None)

    def register_lifetime(self):
        """Return a caller-owned handle for an active lifetime."""
        self._check_pid()
        with self._condition:
            if self._closing:
                raise ManagementClosed('Management lifetime is closed')
            handle = _LifetimeHandle(self)
            self._lifetimes.add(handle)
            self._observe_counts_locked()
            return handle

    def close_lifetime(self, handle, retire):
        self._check_pid()
        if (not isinstance(handle, _LifetimeHandle) or
                handle.runtime is not self):
            raise ManagementContextError(
                'Close handle belongs to another runtime')
        with self._condition:
            closed = handle.closed
            if closed.is_set():
                return closed
            # Retirement is native, short and must not run application code.
            retire()
            closed.set()
            self._lifetimes.discard(handle)
            return closed

    def _schedule_callbacks(self, future):
        with self._condition:
            record = future._record
            self._queue_delivery_locked(record)

    def _delivery_failed_locked(self, record, error):
        for future in chain(record.futures, record.foreign_futures):
            future._delivery.fail(error)

    def _queue_delivery_locked(self, record):
        if record.delivery:
            return
        if not any(
                future.done() and
                future._delivery.pending
                for future in chain(record.futures, record.foreign_futures)):
            return
        if self.completion_error is not None:
            self._delivery_failed_locked(record, self.completion_error)
            return
        self._set_delivery_locked(record, True)
        self._observe_counts_locked()
        self._dispatch_delivery_locked(record)

    def _resume_delivery(self, record):
        """Resume a deferred batch when its drainer returns."""
        self._check_pid()
        with self._condition:
            if record.released or not record.delivery:
                return
            if record.delivery_ticket is not None:
                record.delivery_again = True
            else:
                self._dispatch_delivery_locked(record)

    def _dispatch_delivery_locked(self, record):
        if self.completion_error is not None:
            self._delivery_failed_locked(record, self.completion_error)
            self._set_delivery_locked(record, False)
            self._release_locked(record)
            return
        record.delivery_ticket = _DeliveryTicket(lambda: self._deliver(record))
        try:
            self._dispatch(record.delivery_ticket)
        except BaseException as exc:
            self._set_delivery_locked(record, False)
            error = ManagementDeliveryFailed(
                'Completion reactor rejected management delivery')
            error.__cause__ = exc
            self.completion_error = error
            for pending in tuple(self._records):
                self._delivery_failed_locked(pending, error)
                ticket = pending.delivery_ticket
                if ticket is None or ticket.cancel():
                    self._set_delivery_locked(pending, False)
                    pending.delivery_ticket = None
                    self._release_locked(pending)
            # Infrastructure failure closes admission. Native outcomes already
            # published are immutable; remaining waiters receive shutdown.
            # Inactive queued tickets are abandoned; an active delivery keeps
            # its credit until it returns. Undeliverable callbacks are never
            # retried or executed as application code on an owner.
            log.exception('Management reactor completion delivery failed')
            self.close()

    def _deliver(self, record):
        with _entered('bridge', record):
            with self._condition:
                record.delivery_again = False
            while True:
                with self._condition:
                    futures = tuple(chain(record.futures,
                                          record.foreign_futures))
                stalled = False
                for future in futures:
                    if future.done():
                        if not future._delivery.drain(record):
                            stalled = True
                with self._condition:
                    if any(
                            future.done() and future._delivery.pending
                            for future in chain(record.futures,
                                                record.foreign_futures)):
                        if stalled:
                            # Another caller is executing a callback. Keep the
                            # charged batch and yield I/O until its handoff.
                            record.delivery_ticket = None
                            if record.delivery_again:
                                record.delivery_again = False
                                self._dispatch_delivery_locked(record)
                            return
                        continue
                    self._set_delivery_locked(record, False)
                    record.delivery_ticket = None
                    record.delivery_again = False
                    self._release_locked(record)
                    return

    def _release_locked(self, record):
        if (record.released or not record.handler_done or record.physical or
                record.mailbox_pending):
            return
        # Coalesced waiters share physical work, but private native completion
        # must release its own window independently of ordinary callback debt.
        if not record.private_released:
            if record.budgets['aggregate']:
                self._reentry_active = False
            self._reentry_credits -= record.budgets['reentry']
            record.private_released = True
            self._condition.notify_all()
        if record.delivery or record.notification:
            return
        record.released = True
        count = record.budgets['ordinary']
        if count:
            self._credits -= count
            if record.pool is not None:
                remaining = self._pool_credits[record.pool] - count
                if remaining:
                    self._pool_credits[record.pool] = remaining
                else:
                    del self._pool_credits[record.pool]
        self._records.discard(record)
        record.handler = None
        self._condition.notify_all()
        self._maybe_finalized_locked()

    def _run(self, kind, target):
        with _entered(kind):
            try:
                target()
            finally:
                with self._condition:
                    self._exited += 1
                    self._maybe_finalized_locked()

    def _health_ready(self):
        return any(slot.apply is not None or
                   (slot.notification is not None and
                    self._capacity(None, 'ordinary'))
                   for slot in self._health.values())

    def _run_cluster(self):
        while True:
            with self._condition:
                while (not self._cluster and not self._closing and
                       not self._health_ready()):
                    self._condition.wait()
                if self._closing and not self._cluster:
                    return
                health = next((slot for slot in self._health.values()
                               if slot.apply is not None), None)
                if health is not None:
                    callback, health.apply = health.apply, None
                    self._sequence += 1
                    record = _Record(
                        self, Envelope(self._sequence, 'health', None,
                                       self.lifetime, ()), None, None, None)
                    self._records.add(record)
                    native_health = True
                else:
                    pending = next((slot for slot in self._health.values()
                                    if slot.notification is not None and
                                    self._capacity(None, 'ordinary')), None)
                    if pending is not None:
                        notification, pending.notification = (
                            pending.notification, None)
                        self.notify(notification)
                        notification = pending = None
                        continue
                    record, callback, _ = self._dequeue_locked(self._cluster)
                    native_health = False
            self._execute(record, callback, 'cluster')
            if native_health:
                self._finish(record)
            with self._condition:
                self._release_locked(record)
            record = callback = health = None

    def _run_pool(self):
        while True:
            with self._condition:
                while not self._ready and not self._closing:
                    self._condition.wait()
                if self._closing and not self._ready:
                    return
                pool = self._ready.popleft()
                self._active_pools.add(pool)
                record, callback, _ = self._dequeue_locked(self._pools[pool])
            self._execute(record, callback, 'pool')
            with self._condition:
                self._active_pools.remove(pool)
                self._release_locked(record)
                if self._pools[pool]:
                    self._ready.append(pool)
                else:
                    del self._pools[pool]
                self._condition.notify_all()
            record = callback = pool = None

    def _execute(self, record, callback, kind):
        with _entered(kind, record):
            try:
                callback()
            except BaseException as exc:
                self._finish(record, error=exc)

    def _run_notifications(self):
        while True:
            with self._condition:
                while not self._notifications and not self._closing:
                    self._condition.wait()
                if self._closing:
                    return
                record, callback = self._notifications.popleft()
            with _entered('callback', record):
                try:
                    value = callback()
                except BaseException as exc:
                    self._finish(record, error=exc)
                else:
                    self._finish(record, value)
                finally:
                    # Drop a bound adapter before publishing released credits;
                    # an idle lane must not root its last coordinator.
                    callback = None
                    with self._condition:
                        record.notification = False
                        self._release_locked(record)
            record = callback = value = None

    def retain_cleanup(self):
        """Retain one cleanup obligation independently of load."""
        self._check_pid()
        with self._condition:
            if self._finalizing:
                raise ManagementClosed('Management cleanup lifetime has ended')
            self._cleanup_obligations += 1
            self._observe_counts_locked()
        completed = Event()
        def release():
            self._check_pid()
            with self._condition:
                if not completed.is_set():
                    completed.set()
                    self._cleanup_obligations -= 1
                    self._maybe_finalized_locked()
        return release

    def close(self, wait=False):
        self._check_pid()
        if wait:
            assert_can_wait()
        with self._condition:
            self._check_pid()
            self._begin_close_locked()
        if wait and execution_context() not in ('callback', 'worker'):
            self.closed.wait()
        return self.closed

    def _begin_close_locked(self):
        if self._closing:
            return
        self._closing = True
        for record in tuple(self._records):
            self._finish(record, error=ManagementClosed(
                'Management lifetime ended'))
        for record, _ in self._notifications:
            record.notification = False
            self._release_locked(record)
        self._notifications.clear()
        def retained_results(queue):
            retained = deque()
            for item in queue:
                if item.retain_on_close:
                    retained.append(item)
                else:
                    self._account_mailbox_locked(item.record, -1)
                    self._release_locked(item.record)
            return retained
        self._cluster = retained_results(self._cluster)
        for pool, queue in tuple(self._pools.items()):
            retained = retained_results(queue)
            if retained or pool in self._active_pools:
                self._pools[pool] = retained
            else:
                del self._pools[pool]
        self._ready = deque(pool for pool in self._ready
                            if pool in self._pools)
        self._keys.clear()
        self._health.clear()
        for handle in self._lifetimes:
            handle.closed.set()
        self._lifetimes.clear()
        self._condition.notify_all()
        self.closed.set()
        self._maybe_finalized_locked()

    def _maybe_finalized_locked(self):
        if (self.closed.is_set() and not self._records and
                not self._cleanup_obligations
                and self._exited == len(self._threads)
                and not self._finalizing):
            self._finalizing = True
            if self._on_finalized is None:
                self.finalized.set()
            else:
                try:
                    self._on_finalized(self.finalized.set)
                except BaseException:
                    log.exception(
                        'Error detaching management completion channel')
                    self.finalized.set()

    def join(self, timeout=None):
        self._check_pid()
        assert_can_wait()
        if execution_context() in ('callback', 'worker'):
            raise ManagementContextError('Cannot join own management resource')
        if not self.finalized.wait(timeout):
            raise TimeoutError()
        for thread in self._threads:
            if thread is not current_thread():
                thread.join(timeout)


class ReactorChannel:
    """Borrow dispatch without owning the process-global I/O service.

    Libev holds its I/O runner live until the final admitted delivery
    drains; thread startup failures consequently precede runtime
    admission. Closing one channel releases only its borrow, never
    another Cluster's reactor.
    """

    def __init__(self, connection_class):
        dispatch = getattr(connection_class, '_management_dispatch', None)
        in_context = getattr(connection_class, '_management_in_context', None)
        if dispatch is None or in_context is None:
            raise ManagementContextError(
                'Connection class has no management completion adapter')
        self._closed = False
        self._dispatch = dispatch
        self._release = None
        connection_class.initialize_reactor()
        borrow = getattr(connection_class, '_management_borrow', None)
        if borrow is not None:
            self._release = borrow()
        elif not in_context():
            ready = Event()
            dispatch(ready.set)
            if not ready.wait(5):
                raise ManagementContextError(
                    'Management reactor channel startup did not complete')

    def dispatch(self, callback):
        if self._closed:
            raise ManagementClosed('Management completion channel detached')
        self._dispatch(callback)

    def close(self, completed=lambda: None):
        if self._closed:
            completed()
            return
        self._closed = True
        release, self._release = self._release, None
        self._dispatch = None
        if release is None:
            completed()
        else:
            release(completed)


def assert_synchronous_api(connection_class):
    assert_can_wait()
    in_context = getattr(connection_class, '_management_in_context', None)
    if in_context is not None and in_context():
        raise ManagementContextError(
            'Blocking management call on a reactor thread')


@contextmanager
def physical_work_context(cleanup=False):
    """Mark physical work; cleanup may finish child closes inline."""
    previous = getattr(_context, 'cleanup', False)
    _context.cleanup = cleanup
    try:
        with _entered('worker'):
            yield
    finally:
        _context.cleanup = previous


def performing_cleanup():
    return getattr(_context, 'cleanup', False)


class _CompatibilityCall:
    """Admit legacy phases while their state writers still use locks.

    The calling stack performs existing blocking work. A reserved
    physical obligation holds its credit; C never waits for that
    stack. Policy wrappers release a completed phase before arbitrary
    policy reentry, then acquire the next phase after the policy
    returns. Later migrations replace these bodies with native
    asynchronous owner continuations.
    """

    def __init__(self, runtime):
        self.runtime = runtime
        current = getattr(_context, 'record', None)
        self.budget = ('aggregate' if execution_context() == 'callback' and
                       current is not None and current.runtime is runtime
                       else 'ordinary')
        self.record = None

    def acquire(self):
        if self.record is not None:
            return
        if self.budget == 'aggregate' and self.runtime._reentry_active:
            raise ManagementContextError('Recursive native reentry aggregate')
        future = self.runtime.submit(lambda record: _PENDING, wait=True,
                                     budget=self.budget, reserve=1)
        self.record = future._record
        try:
            self.record.started.wait()
            if future.done():
                future.result()
        except BaseException:
            self.release()
            raise

    def release(self):
        record, self.record = self.record, None
        if record is not None:
            with self.runtime._condition:
                self.runtime._finish(record)
                record.physical -= 1
                self.runtime._release_locked(record)


@contextmanager
def compatibility_call(runtime):
    previous = getattr(_context, 'call', None)
    call = _CompatibilityCall(runtime)
    call.acquire()
    _context.call = call
    try:
        yield
    finally:
        call.release()
        _context.call = previous


def policy_call(method):
    """Run user policy between released compatibility phases."""
    @wraps(method)
    def call_policy(*args, **kwargs):
        call = getattr(_context, 'call', None)
        if call is None:
            return method(*args, **kwargs)
        call.release()
        _context.call = None
        try:
            result = method(*args, **kwargs)
        finally:
            _context.call = call
        call.acquire()
        return result
    return call_policy


@contextmanager
def compatibility_work(runtime):
    """Charge physical work; L fanout uses its independent window."""
    if runtime is None:
        # Internal tests construct bare Sessions with __new__, without a
        # registered production lifetime or management admission contract.
        yield lambda work: None
        return
    assert_can_wait()
    call = getattr(_context, 'call', None)
    same_call = call is not None and call.runtime is runtime
    budget = ('reentry' if same_call and call.budget == 'aggregate'
              else 'ordinary')
    if same_call and budget == 'ordinary':
        # Admission cannot retain a phase's credit while waiting for a child
        # record in a full window, or retain a Session lock needed by workers.
        call.release()
    future = runtime.submit(lambda record: _PENDING, wait=same_call,
                            budget=budget, reserve=1)
    record = future._record
    attached = False
    completed = False

    def complete(work):
        nonlocal completed
        with runtime._condition:
            if completed:
                return
            completed = True
            runtime._finish(record)
            record.physical -= 1
            runtime._release_locked(record)

    def attach(work):
        nonlocal attached
        if (work is not None and
                callable(getattr(work, 'add_done_callback', None))):
            try:
                attached = True
                work.add_done_callback(complete)
            except BaseException:
                # Registration may have been interrupted before or after
                # installation. Keep this synchronous caller responsible for
                # the physical work until it actually settles; the callback
                # and this fallback share the same once-only retirement.
                while not work.done():
                    try:
                        work.result()
                    except BaseException:
                        pass
                complete(work)
                raise
    try:
        record.started.wait()
        if future.done():
            future.result()
        yield attach
    finally:
        if not attached:
            complete(None)
        if same_call and budget == 'ordinary':
            call.acquire()
