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

from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass
from functools import wraps
import logging
import os
from threading import Condition, Event, Lock, local
from typing import NamedTuple

from cassandra import DriverException

log = logging.getLogger(__name__)
_context = local()


class ManagementClosed(DriverException):
    """The original management lifetime has ended."""


class ManagementSuperseded(DriverException):
    """The operation was superseded without losing its waiters."""


class ManagementOverloaded(DriverException):
    """No reservation is available for a nonblocking submission."""


class ManagementDeliveryFailed(DriverException):
    """Delivery failed while native outcomes remain terminal."""


class ManagementContextError(DriverException):
    """The management context does not support this operation."""


def execution_context():
    return getattr(_context, 'kind', None)


def _callback_runtime():
    """Identify L even when capture retirement has no live record."""
    if execution_context() == 'callback':
        return getattr(_context, 'runtime', None)
    return None


def assert_can_wait():
    if execution_context() in ('cluster', 'pool', 'bridge', 'cleanup'):
        raise ManagementContextError(
            'Blocking management call on an owner, reactor or cleanup lane')


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
def _entered(kind, record=None, runtime=None):
    previous = (getattr(_context, 'kind', None),
                getattr(_context, 'record', None),
                getattr(_context, 'runtime', None))
    _context.kind, _context.record = kind, record
    _context.runtime = (record.runtime if record is not None else
                        runtime if runtime is not None else previous[2])
    try:
        yield
    finally:
        _context.kind, _context.record, _context.runtime = previous


@dataclass(frozen=True)
class Envelope:
    operation: int
    kind: str
    origin: object
    lifetime: object
    identity: tuple


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
        if (_callback_runtime() is self._record.runtime and
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
    """Serialize callbacks during handoff between charged records.

    The runtime condition precedes the Future condition whenever both
    are needed. The standard Future callback list remains on the Future
    itself.
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
            retired = self.future._done_callbacks[:]
            self.future._done_callbacks.clear()
            self.cursor = 0
            return retired

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
                    target.runtime._queue_delivery_locked(target, self.future)
                    return
            # Released callbacks share a caller-side drainer; a concurrent
            # registrant may leave its callback to the active drainer.
            self.drain()
            return

    def drain(self, record=None):
        """Drain serialized callbacks outside C/P."""
        with self.future._condition:
            if record is None and not self.record.released:
                # An owner may have adopted this stream after registration
                # released the runtime condition. Its reactor batch owns it.
                return False
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
                    # Keep the standard list, but retire its delivered prefix.
                    # Hold references until outside the condition: releasing a
                    # callback's captured objects can run application code.
                    retired = self.future._done_callbacks[:self.cursor]
                    del self.future._done_callbacks[:self.cursor]
                    self.cursor = 0
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
        retired.clear()
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
            callback, self._callback = self._callback, None
        # Target disposal may re-enter this helper.
        del callback
        return True


class _ThreadStart:
    """Revoke an unentered target independently of native launch."""

    def __init__(self, callback):
        self._lock = Lock()
        self._callback = callback
        self._entered = False

    def __call__(self):
        with self._lock:
            callback, self._callback = self._callback, None
            self._entered = callback is not None
        if callback is not None:
            callback()

    def revoke(self):
        """Return whether target entry already won the handoff."""
        with self._lock:
            callback, self._callback = self._callback, None
            entered = self._entered
        # Target disposal may re-enter this helper.
        del callback
        return entered


class _Record:
    def __init__(self, runtime, envelope, handler, pool, key):
        self.runtime, self.envelope, self.handler = runtime, envelope, handler
        self.pool, self.key = pool, key
        self.budgets = dict.fromkeys(('ordinary', 'reentry', 'aggregate'), 0)
        self.private_released = False
        self.futures = []
        self.foreign_futures = set()
        self.physical = 0
        self.attempt = None
        self.mailbox_pending = 0
        self.started = Event()
        self.handler_done = False
        self.executing = False
        self.delivery = False
        self.delivery_ticket = None
        self.delivery_again = False
        self.notification = False
        self.released = False
