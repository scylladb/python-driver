from collections import deque
from concurrent.futures import (
    Future, ThreadPoolExecutor, as_completed, wait, TimeoutError)
from threading import Barrier, Event, Lock, Thread, current_thread, get_ident
import gc
import platform
import weakref

import pytest

from cassandra import _management as management
from cassandra._management import (
    ManagementClosed, ManagementContextError, ManagementOverloaded,
    ManagementRuntime, execution_context)


class Dispatcher:
    def __init__(self):
        self.queue = deque()
        self.lock = Lock()

    def __call__(self, callback):
        with self.lock:
            self.queue.append(callback)

    def drain(self):
        while True:
            with self.lock:
                if not self.queue:
                    return
                callback = self.queue.popleft()
            callback()


def complete_physical(manager, future, value):
    """Bound a controlled Future's owner-handoff callback."""
    errors = []
    def complete():
        try:
            future.set_result(value)
        except BaseException as exc:
            errors.append(exc)
    thread = Thread(target=complete, daemon=True)
    thread.start()
    thread.join(5)
    if thread.is_alive():
        manager.close()
        thread.join(5)
    assert not thread.is_alive(), 'physical result handoff did not finish'
    assert not errors


def drain_controlled_callbacks(manager):
    """Bound callback cleanup on runtime lanes driven by the test."""
    assert not manager._threads
    errors = []
    def drain():
        try:
            manager._run_notifications()
        except BaseException as exc:
            errors.append(exc)
    drainer = Thread(target=drain, daemon=True)
    drainer.start()
    try:
        drainer.join(5)
        assert not drainer.is_alive(), 'callback retirement did not finish'
    finally:
        if drainer.is_alive():
            # These controlled runtimes have no physical work or real runners.
            # Release a stranded loop after recording the regression failure.
            with manager._condition:
                manager._records.clear()
                manager._condition.notify_all()
            drainer.join(5)
    assert not errors


def assert_runtime_counts(manager):
    with manager._condition:
        counts = manager.counts()
        assert counts['mailbox'] == sum(
            record.mailbox_pending for record in manager._records)
        assert counts['delivery'] == sum(
            record.delivery for record in manager._records)
        assert all(value >= 0 for value in counts.values())
        return counts


def wait_for_retirement(manager, *futures):
    with manager._condition:
        assert manager._condition.wait_for(
            lambda: all(future._record.released for future in futures), 5)


@pytest.fixture
def runtime(monkeypatch):
    dispatch = Dispatcher()
    runtime = ManagementRuntime(dispatch, limit=4, pool_limit=2)
    observe = runtime._observe_counts_locked
    invariant_errors = []
    def checked_counts():
        counts = observe()
        for name, attribute in (('mailbox', 'mailbox_pending'),
                                ('delivery', 'delivery')):
            expected = sum(getattr(record, attribute)
                           for record in runtime._records)
            if counts[name] != expected:
                invariant_errors.append(
                    '%s count: observed %s, expected %s' %
                    (name, counts[name], expected))
        return counts
    monkeypatch.setattr(runtime, '_observe_counts_locked', checked_counts)
    try:
        yield runtime, dispatch
        assert not invariant_errors, '\n'.join(invariant_errors)
        assert_runtime_counts(runtime)
    finally:
        # A failed invariant must not interrupt shutdown's own observations.
        monkeypatch.setattr(runtime, '_observe_counts_locked', observe)
        try:
            runtime.close()
        finally:
            try:
                dispatch.drain()
            finally:
                runtime.join(5)
    assert all(value == 0 for value in assert_runtime_counts(runtime).values())


def test_coalesced_cancellation_bookkeeping_is_linear(monkeypatch):
    manager = ManagementRuntime(Dispatcher(), limit=64)
    done_checks = 0
    done = management.OperationFuture.done
    def counted_done(future):
        nonlocal done_checks
        done_checks += 1
        return done(future)
    try:
        futures = [manager.submit(lambda record: management._PENDING,
                                  key='shared') for _ in range(64)]
        assert futures[0]._record.started.wait(5)
        monkeypatch.setattr(management.OperationFuture, 'done', counted_done)
        for future in futures:
            assert future.cancel()
        assert done_checks <= 2 * len(futures)
    finally:
        manager.close()
        manager.join(5)


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Requires synchronous reference destruction')
@pytest.mark.parametrize('pool', [None, 'pool'])
def test_handler_captures_retire_on_callback_lane_outside_locks(pool):
    manager = ManagementRuntime(Dispatcher())
    observed, probes = [], []
    entered, released, cleanup_release = Event(), Event(), Event()
    class Payload:
        def __del__(self):
            observed.append((execution_context(),
                             manager._condition._is_owned()))
            probe = Thread(target=lambda: (manager.counts(), released.set()),
                           daemon=True)
            probes.append(probe)
            probe.start()
            probe.join(2)
            observed.append(released.is_set())
            entered.set()
            if released.is_set():
                observed.append(cleanup_release.wait(5))

    try:
        with manager._condition:
            payload = Payload()
            future = manager.submit(lambda record, payload=payload: 7,
                                    pool=pool)
            del payload
        assert future.result(5) == 7
        assert entered.wait(5)
        assert observed == [('callback', False), True]
        manager.close()
        assert not manager.finalized.is_set()
        assert manager.counts()['close'] == 0
        cleanup_release.set()
        manager.join(5)
        assert observed == [('callback', False), True, True]
    finally:
        cleanup_release.set()
        manager.close()
        manager.join(5)
        for probe in probes:
            probe.join(5)
            assert not probe.is_alive()


@pytest.mark.parametrize('kind,pool', [
    ('operation', None), ('operation', 'pool'), ('notification', None)])
def test_handler_capture_retirement_retains_admission(kind, pool):
    import gc
    import weakref

    manager = ManagementRuntime(Dispatcher(),
                                limit=2 if pool is None else 3, pool_limit=1)
    entered, release = Event(), Event()
    class Payload:
        pass
    try:
        blocked = manager.notify(
            lambda: (entered.set(), release.wait(5))[1])
        assert entered.wait(5)
        payload = Payload()
        reference = weakref.ref(payload)
        if kind == 'notification':
            with manager._condition:
                future = manager.notify(lambda payload=payload: None)
                manager.supersede(future)
        else:
            future = manager.submit(lambda record, payload=payload: 7,
                                    pool=pool)
        del payload
        if kind == 'notification':
            with pytest.raises(management.ManagementSuperseded):
                future.result(5)
        else:
            assert future.result(5) == 7
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: future._record.handler is None, 5)
            assert not future._record.released
            assert manager.counts()['ordinary'] == 2
            if pool is not None:
                assert manager._pool_credits[pool] == 1
        assert reference() is not None
        with pytest.raises(ManagementOverloaded):
            manager.submit(lambda record: None, pool=pool)
        release.set()
        assert blocked.result(5) is True
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: future._record.released, 5)
        gc.collect()
        assert reference() is None
        assert manager.submit(lambda record: 9, pool=pool).result(5) == 9
    finally:
        release.set()
        manager.close()
        manager.join(5)


def test_fifo_one_owner_and_exception_progress(runtime):
    manager, dispatch = runtime
    observed = []
    owner = []
    def handler(record):
        manager.assert_owner()
        observed.append(record.envelope.operation)
        owner.append(get_ident())
        return record.envelope.operation
    futures = [manager.submit(handler) for _ in range(4)]
    assert [f.result(5) for f in futures] == observed == [1, 2, 3, 4]
    wait_for_retirement(manager, *futures)
    assert len(set(owner)) == 1
    def fail(record):
        raise ValueError('bad operation')
    with pytest.raises(ValueError):
        manager.submit(fail).result(5)
    assert manager.submit(handler).result(5) == 6


def test_native_completion_preserves_delivery_capacity(runtime):
    manager, dispatch = runtime
    entered, release = Event(), Event()
    def handler(record):
        entered.set()
        assert release.wait(5)
        return 7
    futures = [manager.submit(handler) for _ in range(4)]
    order = []
    for i, future in enumerate(futures):
        future.add_done_callback(
            lambda f, i=i: order.append((i, execution_context())))
    assert entered.wait(5)
    release.set()
    assert all(f.result(5) == 7 for f in futures)
    assert wait(futures, timeout=5).not_done == set()
    with manager._condition:
        assert manager._credits == 4
        assert len(dispatch.queue) == 4
    for _ in range(20):
        with pytest.raises(ManagementOverloaded):
            manager.submit(lambda record: None)
    futures[0].add_done_callback(
        lambda f: order.append(('extra', execution_context())))
    assert len(dispatch.queue) == 4
    dispatch.drain()
    assert order == [(0, 'bridge'), ('extra', 'bridge'), (1, 'bridge'),
                     (2, 'bridge'), (3, 'bridge')]
    wait_for_retirement(manager, *futures)
    assert manager._credits == 0


def test_coalesced_cancel_waiters_and_physical_work(runtime):
    manager, dispatch = runtime
    entered, release = Event(), Event()
    attempts = []
    with ThreadPoolExecutor(1) as executor:
        def physical():
            attempts.append(True)
            entered.set()
            assert release.wait(5)
            return 42
        first = manager.submit(
            lambda r: manager.work(r, executor, physical), key='same')
        assert entered.wait(5)
        second = manager.submit(
            lambda r: pytest.fail('duplicate handler'), key='same')
        assert first is not second
        assert first.cancel()
        assert wait([first], timeout=1).not_done == set()
        with pytest.raises(TimeoutError):
            second.result(0)
        release.set()
        assert second.result(5) == 42
        assert attempts == [True]


def test_late_result_disposal_does_not_need_admission(runtime):
    manager, dispatch = runtime
    entered, release, disposed = Event(), Event(), Event()
    resource = object()
    with ThreadPoolExecutor(1) as executor:
        def physical():
            entered.set()
            assert release.wait(5)
            return resource
        future = manager.submit(lambda r: manager.work(
            r, executor, physical,
            dispose=lambda value: disposed.set()
            if value is resource else None))
        assert entered.wait(5)
        manager.close()
        with pytest.raises(ManagementClosed):
            future.result(5)
        assert not manager.finalized.is_set()
        release.set()
        assert disposed.wait(5)
    manager.join(5)


def test_callback_lane_wait_does_not_block_native_or_reactor_completion(
        runtime):
    manager, dispatch = runtime
    entered, release = Event(), Event()
    complete = Event()
    future = manager.notify(lambda: (entered.set(), release.wait(5)))
    try:
        assert entered.wait(5)
        result = manager.submit(lambda record: (complete.wait(5), 9)[1])
        seen = []
        result.add_done_callback(
            lambda f: seen.append((f.result(), execution_context())))
        complete.set()
        assert result.result(5) == 9
        # Native publication precedes enqueue; fence that owner phase before
        # observing the independently locked dispatcher.
        with manager._condition:
            assert len(dispatch.queue) == 1
        dispatch.drain()
        assert seen == [(9, 'bridge')]
        assert not future.done()
        manager.close()
        assert future.done()
    finally:
        complete.set()
        release.set()


@pytest.mark.parametrize('waiting', [
    lambda future: future.result(),
    lambda future: future.exception(),
    lambda future: wait([future]),
    lambda future: next(as_completed([future]))])
def test_callback_lane_rejects_its_pending_notification_wait(runtime, waiting):
    manager, dispatch = runtime
    nested = []
    def callback():
        future = manager.notify(lambda: 17)
        nested.append(future)
        with pytest.raises(ManagementContextError, match='callback lane'):
            waiting(future)
        # Ordinary owner operations remain available to synchronous reentry.
        with manager.synchronous_reentry() as budget:
            future = manager.submit(lambda record: 18, budget=budget)
            assert future.result(5) == 18
    manager.notify(callback).result(5)
    assert nested[0].result(5) == 17


def test_owner_wait_fails_and_pool_handlers_are_serial(runtime):
    manager, dispatch = runtime
    entered, release = Event(), Event()
    pool = object()
    def hold(record):
        manager.assert_owner(pool)
        entered.set()
        assert release.wait(5)
    first = manager.submit(hold, pool=pool)
    assert entered.wait(5)
    second = manager.submit(lambda record: None, pool=pool)
    assert not second.done()
    with pytest.raises(ManagementOverloaded):
        manager.submit(lambda record: None, pool=pool)
    release.set()
    first.result(5)
    second.result(5)
    def self_wait(record):
        other = manager.submit(lambda r: 1)
        with pytest.raises(ManagementContextError):
            other.result()
    manager.submit(self_wait).result(5)


def test_executor_rejection_releases_reservation(runtime):
    manager, dispatch = runtime
    class Rejected:
        def submit(self, function):
            raise RuntimeError('executor closed')
    with pytest.raises(RuntimeError, match='executor closed'):
        manager.submit(
            lambda r: manager.work(r, Rejected(), lambda: None)).result(5)
    with manager._condition:
        assert manager._condition.wait_for(lambda: manager._credits == 0, 5)


@pytest.mark.parametrize('already_running', [False, True])
def test_executor_failure_after_queueing_preserves_physical_ownership(
        runtime, already_running):
    manager, dispatch = runtime
    entered, release = Event(), Event()
    calls, disposed, queued, errors = [], [], [], []
    resource = object()
    def physical():
        calls.append(get_ident())
        entered.set()
        assert release.wait(5)
        return resource
    def run(function):
        try:
            function()
        except BaseException as exc:
            errors.append(exc)
    class QueuedThenFailed:
        def submit(self, function):
            queued.append(Thread(target=run, args=(function,), daemon=True))
            if already_running:
                queued[0].start()
                assert entered.wait(5)
            raise RuntimeError('failed after queueing')
    future = manager.submit(lambda record: manager.work(
        record, QueuedThenFailed(), physical,
        dispose=lambda value: disposed.append(value)))
    try:
        with pytest.raises(RuntimeError, match='after queueing'):
            future.result(5)
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: future._record.physical == int(already_running), 5)
        if already_running:
            manager.close()
            assert not manager.finalized.is_set()
        else:
            queued[0].start()
    finally:
        release.set()
        for thread in queued:
            if thread.ident is not None:
                thread.join(5)
                assert not thread.is_alive()
    assert not errors
    assert bool(calls) == already_running
    assert disposed == ([resource] if already_running else [])
    with manager._condition:
        assert manager._condition.wait_for(lambda: not manager._records, 5)


@pytest.mark.parametrize('notification', [False, True])
def test_callback_rejects_saturated_ordinary_admission(notification):
    manager = ManagementRuntime(Dispatcher(), limit=1)
    finished = Event()
    errors = []
    def callback():
        try:
            if notification:
                manager.notify(lambda: None, wait=True)
            else:
                manager.submit(lambda record: None, wait=True)
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()
    try:
        manager.notify(callback)
        assert finished.wait(5), 'callback waited behind its own credit'
        assert len(errors) == 1
        assert isinstance(errors[0], ManagementContextError)
    finally:
        manager.close()
        manager.join(5)


def test_health_and_close_bypass_full_delivery_and_io_capacity(runtime):
    manager, dispatch = runtime
    # Fill ordinary credits with terminal records awaiting callback delivery.
    gate = Event()
    futures = [manager.submit(lambda record: gate.wait(5)) for _ in range(4)]
    for future in futures:
        future.add_done_callback(lambda f: None)
    gate.set()
    for future in futures:
        future.result(5)
    assert manager._credits == 4
    incarnation = object()
    manager.register_health(incarnation)
    native, notified = Event(), Event()
    assert manager.observe_negative(
        incarnation, native.set, notified.set, removed=True)
    assert native.wait(5)
    assert not notified.is_set()
    # A pending removal cannot be overwritten by a positive/less terminal
    # event.
    assert manager.observe_negative(
        incarnation, lambda: pytest.fail('old health'))
    child_one = manager.register_lifetime()
    child_two = manager.register_lifetime()
    effects = []
    one = manager.close_lifetime(child_one, lambda: effects.append(1))
    repeated = manager.close_lifetime(
        child_one, lambda: pytest.fail('duplicate'))
    assert repeated is one
    two = manager.close_lifetime(child_two, lambda: effects.append(2))
    assert two.is_set()
    assert effects == [1, 2]
    dispatch.drain()
    assert notified.wait(5)


def test_callback_reserved_window_progresses_at_full_delivery_capacity(
        runtime):
    manager, dispatch = runtime
    gate = Event()
    entered, release, completed = Event(), Event(), Event()
    def listener():
        entered.set()
        assert release.wait(5)
        with manager.synchronous_reentry() as budget:
            assert budget == 'reentry'
            with ThreadPoolExecutor(1) as executor:
                for _ in range(100):
                    future = manager.submit(
                        lambda r: manager.work(r, executor, lambda: 11),
                        budget=budget, wait=True)
                    assert future.result(5) == 11
        completed.set()
    notification = manager.notify(listener)
    assert entered.wait(5)
    futures = [manager.submit(lambda record: gate.wait(5)) for _ in range(3)]
    for future in futures:
        future.add_done_callback(lambda f: None)
    gate.set()
    for future in futures:
        future.result(5)
    assert manager._credits == 4
    release.set()
    assert completed.wait(5)
    notification.result(5)
    with manager._condition:
        assert manager._condition.wait_for(
            lambda: manager._reentry_credits == 0, 5)


def test_superseded_physical_result_is_disposed(runtime):
    from cassandra._management import ManagementSuperseded
    manager, dispatch = runtime
    entered, release, disposed = Event(), Event(), Event()
    with ThreadPoolExecutor(1) as executor:
        def physical():
            entered.set()
            assert release.wait(5)
            return object()
        future = manager.submit(
            lambda r: manager.work(
                r, executor, physical, dispose=lambda value: disposed.set()))
        assert entered.wait(5)
        manager.supersede(future)
        with pytest.raises(ManagementSuperseded):
            future.result(5)
        release.set()
        assert disposed.wait(5)


@pytest.mark.parametrize('ending', ['close', 'supersede'])
def test_result_adoption_is_atomic_with_terminal_settlement(runtime, ending):
    manager, dispatch = runtime
    accepting, finish_accept, ending_started = Event(), Event(), Event()
    resource = object()
    disposed, errors = [], []
    def accept(value):
        assert value is resource
        # Acceptance publishes native state. Other terminal paths must take
        # this same condition before they can claim the operation's outcome.
        assert manager._condition._is_owned()
        accepting.set()
        assert finish_accept.wait(5)
        return value
    with ThreadPoolExecutor(1) as executor:
        future = manager.submit(lambda record: manager.work(
            record, executor, lambda: resource, accept=accept,
            dispose=disposed.append))
        def end():
            ending_started.set()
            try:
                if ending == 'close':
                    manager.close()
                else:
                    manager.supersede(future)
            except BaseException as exc:
                errors.append(exc)
        ending_thread = Thread(target=end)
        try:
            assert accepting.wait(5)
            ending_thread.start()
            assert ending_started.wait(5)
        finally:
            finish_accept.set()
            if ending_thread.ident is not None:
                ending_thread.join(5)
        assert not ending_thread.is_alive()
        assert not errors
        assert future.result(5) is resource
    assert disposed == []


def test_rejected_acceptance_retains_worker_until_disposal_finishes(runtime):
    manager, dispatch = runtime
    disposing, finish_dispose = Event(), Event()
    resource = object()
    physical_threads, disposal_threads = [], []
    def physical():
        physical_threads.append(get_ident())
        return resource
    def accept(value):
        raise ValueError('cannot adopt resource')
    def dispose(value):
        assert value is resource
        disposal_threads.append(get_ident())
        disposing.set()
        assert finish_dispose.wait(5)
    with ThreadPoolExecutor(1) as executor:
        future = manager.submit(lambda record: manager.work(
            record, executor, physical, accept=accept, dispose=dispose))
        try:
            with pytest.raises(ValueError, match='cannot adopt resource'):
                future.result(5)
            assert disposing.wait(5)
            following = manager.submit(lambda record: 23)
            assert following.result(5) == 23
            wait_for_retirement(manager, following)
            manager.close()
            assert not manager.finalized.is_set()
            assert manager.counts()['ordinary'] == 1
        finally:
            finish_dispose.set()
    assert disposal_threads == physical_threads
    manager.join(5)
    assert manager.counts()['ordinary'] == 0


@pytest.mark.parametrize('cancelled', [False, True])
def test_executor_returns_terminal_failure_without_owner_self_wait(
        runtime, cancelled):
    from concurrent.futures import CancelledError
    manager, dispatch = runtime
    physical = Future()
    if cancelled:
        physical.cancel()
    else:
        physical.set_exception(ValueError('executor failed before execution'))
    class FailedExecutor:
        def submit(self, function):
            return physical
    future = manager.submit(lambda record: manager.work(
        record, FailedExecutor(), lambda: pytest.fail('physical work ran')))
    with pytest.raises(CancelledError if cancelled else ValueError):
        future.result(5)
    assert manager.submit(lambda record: 24).result(5) == 24


def test_completed_executor_future_does_not_dispose_on_registering_owner(
        runtime):
    manager, dispatch = runtime
    resource = object()
    disposal_threads = []
    disposal_contexts = []
    completed = Event()

    def dispose(value):
        disposal_threads.append(current_thread())
        disposal_contexts.append(execution_context())
    with ThreadPoolExecutor(1) as executor:
        class ClosingExecutor:
            def submit(self, function):
                manager.close()
                physical = executor.submit(function)
                # Completion precedes callback registration, so Future will
                # invoke that callback on this owner. Cleanup must already
                # have happened on the physical worker, exactly once.
                assert physical.result(5) is resource
                completed.set()
                return physical
        future = manager.submit(lambda record: manager.work(
            record, ClosingExecutor(), lambda: resource,
            dispose=dispose))
        with pytest.raises(ManagementClosed):
            future.result(5)
        # Shutdown publishes the native Future before executor submission has
        # returned on the owner. Keep the executor open through that handoff.
        assert completed.wait(5)
    manager.join(5)
    assert len(disposal_threads) == 1
    # Native thread IDs may be reused; compare actual Thread objects.
    assert all(disposal_threads[0] is not thread for thread in manager._threads)
    assert disposal_contexts == ['worker']
    assert manager.counts()['ordinary'] == 0


@pytest.mark.parametrize('installed', [False, True])
def test_interrupted_physical_callback_registration_releases_once(
        runtime, installed):
    manager, dispatch = runtime
    physical = Future()
    register = physical.add_done_callback
    def interrupted(callback):
        if installed:
            register(callback)
        raise KeyboardInterrupt('physical registration interrupted')
    physical.add_done_callback = interrupted
    class PendingExecutor:
        def submit(self, function):
            return physical
    future = manager.submit(lambda record: manager.work(
        record, PendingExecutor(), lambda: pytest.fail('cancelled work ran')))
    with pytest.raises(KeyboardInterrupt, match='registration interrupted'):
        future.result(5)
    second = manager.submit(lambda record: 25)
    assert second.result(5) == 25
    assert physical.cancelled()
    with manager._condition:
        assert manager._condition.wait_for(
            lambda: future._record.released and second._record.released, 5)
        assert future._record.physical == 0
        assert manager.counts()['ordinary'] == 0


@pytest.mark.parametrize('failed_phase', [1, 2, 3, 4, 5])
@pytest.mark.parametrize('startup_error', [RuntimeError, KeyboardInterrupt])
def test_start_failure_and_repeated_close_leave_no_threads(
        monkeypatch, failed_phase, startup_error):
    real_start = Thread.start
    starts = []
    def start(thread):
        starts.append(thread)
        if len(starts) == failed_phase:
            raise startup_error('thread startup failed')
        return real_start(thread)
    dispatch = Dispatcher()
    manager = ManagementRuntime(dispatch)
    try:
        with monkeypatch.context() as patcher:
            patcher.setattr(Thread, 'start', start)
            with pytest.raises(startup_error, match='thread startup failed'):
                manager.start()
    finally:
        manager.close()
        manager.join(5)
    assert all(not thread.is_alive() for thread in manager._threads)
    for _ in range(100):
        manager = ManagementRuntime(dispatch)
        try:
            assert manager.submit(lambda record: 1).result(5) == 1
            first = manager.close()
            second = manager.close()
            assert second is first
        finally:
            manager.close()
            manager.join(5)
        assert not manager._records
        assert all(not thread.is_alive() for thread in manager._threads)


@pytest.mark.parametrize('failed_phase', [1, 2, 3, 4, 5])
@pytest.mark.parametrize('failure_point', ['construct', 'before', 'after'])
@pytest.mark.parametrize('startup_error', [RuntimeError, KeyboardInterrupt])
def test_start_failure_retires_registered_lifetimes(
        monkeypatch, failed_phase, failure_point, startup_error):
    finalized = []
    manager = ManagementRuntime(Dispatcher(), on_finalized=lambda done: (
        finalized.append((manager.closed.is_set(), handle.closed.is_set(),
                          manager.counts()['close'])), done()))
    handle = manager.register_lifetime()
    real_start = Thread.start
    constructed = []
    failure = startup_error('startup interrupted')

    def construct(*args, **kwargs):
        phase = len(constructed) + 1
        if phase == failed_phase and failure_point == 'construct':
            raise failure
        thread = Thread(*args, **kwargs)
        constructed.append(thread)
        return thread

    def start(thread):
        if len(constructed) == failed_phase:
            if failure_point == 'before':
                raise failure
            real_start(thread)
            raise failure
        real_start(thread)

    try:
        with monkeypatch.context() as patcher:
            patcher.setattr(management, 'Thread', construct)
            patcher.setattr(Thread, 'start', start)
            with pytest.raises(startup_error) as caught:
                manager.start()
        assert caught.value is failure
        assert handle.closed.is_set()
        assert manager.closed.is_set()
        # Startup failure must retire the lifetime without a subsequent close.
        manager.join(5)
        for _ in range(3):
            closed = manager.close()
            assert closed is manager.closed
        assert finalized == [(True, True, 0)]
        assert not manager._lifetimes
        assert all(not thread.is_alive() for thread in manager._threads)
    finally:
        manager.close()
        manager.join(5)


def test_delivery_failure_reentering_close_waits_for_lifetime_retirement():
    from cassandra._management import ManagementDeliveryFailed

    finalized, preconditions = [], []
    def reject(callback):
        # The outer close has begun, but its retirement is still in progress.
        preconditions.append(manager._closing and not manager.closed.is_set())
        raise RuntimeError('reactor failed')

    manager = ManagementRuntime(reject, on_finalized=lambda done: (
        finalized.append((manager.closed.is_set(), handle.closed.is_set(),
                          manager.counts())), done()))
    handle = manager.register_lifetime()
    # Hold admission in the mailbox so close publishes the terminal state.
    manager._started = True
    future = manager.submit(lambda record: pytest.fail('shutdown work ran'))
    future.add_done_callback(lambda future: pytest.fail('delivery ran'))
    manager.close()
    assert not manager.finalized.is_set()
    # This controlled runtime has no launched lanes; drain retained callback
    # references using the same lane loop before waiting for finalization.
    drain_controlled_callbacks(manager)
    assert preconditions == [True]
    manager.join(5)
    manager.close()
    assert isinstance(manager.completion_error, ManagementDeliveryFailed)
    assert len(finalized) == 1
    closed, retired, counts = finalized[0]
    assert closed and retired and all(value == 0 for value in counts.values())


@pytest.mark.parametrize('bootstrap_pending', [False, True])
def test_start_interruption_retires_entered_or_revokes_unentered_runner(
        monkeypatch, bootstrap_pending):
    manager = ManagementRuntime(Dispatcher())
    launched, entered, release = Event(), Event(), Event()
    threads = []
    real_start = Thread.start
    real_bootstrap = Thread._bootstrap_inner
    real_run = manager._run
    def run(kind, target):
        entered.set()
        real_run(kind, target)
    manager._run = run
    def bootstrap(thread):
        if bootstrap_pending:
            launched.set()
            assert release.wait(5)
        real_bootstrap(thread)
    def start(thread):
        threads.append(thread)
        if not bootstrap_pending:
            real_start(thread)
            assert entered.wait(5)
            raise KeyboardInterrupt('interrupted after launch')
        original_wait = thread._started.wait
        def interrupted_wait(*args, **kwargs):
            assert launched.wait(5)
            assert thread.ident is None
            raise KeyboardInterrupt('interrupted before bootstrap')
        thread._started.wait = interrupted_wait
        try:
            real_start(thread)
        finally:
            thread._started.wait = original_wait
    try:
        with monkeypatch.context() as patcher:
            patcher.setattr(Thread, '_bootstrap_inner', bootstrap)
            patcher.setattr(Thread, 'start', start)
            with pytest.raises(KeyboardInterrupt):
                manager.start()
            manager.close()
            if bootstrap_pending:
                # The native runner cannot touch the runtime after revocation,
                # even if its bootstrap has not published an ident yet.
                manager.join(5)
                assert not manager._threads
                assert manager.finalized.is_set()
                assert not entered.is_set()
            else:
                assert manager._threads == threads
    finally:
        release.set()
        for thread in threads:
            assert thread._started.wait(5)
            thread.join(5)
            assert not thread.is_alive()
        manager.close()
        manager.join(5)
    assert manager._exited == (0 if bootstrap_pending else 1)
    assert entered.is_set() is (not bootstrap_pending)


@pytest.mark.parametrize('failed_phase', [1, 2, 3, 4, 5])
def test_start_interruption_before_native_launch_retires_limbo_entry(
        monkeypatch, failed_phase):
    import threading

    primitive = ('_start_joinable_thread'
                 if hasattr(threading, '_start_joinable_thread')
                 else '_start_new_thread')
    if not hasattr(threading, primitive):
        pytest.skip('threading native launch primitive is unavailable')
    real_launch = getattr(threading, primitive)
    attempts, abandoned = [], []
    manager = ManagementRuntime(Dispatcher())
    handle = manager.register_lifetime()
    def launch(target, *args, **kwargs):
        attempts.append(target)
        if len(attempts) == failed_phase:
            abandoned.append(target.__self__)
            raise KeyboardInterrupt('interrupted before native launch')
        return real_launch(target, *args, **kwargs)
    try:
        with monkeypatch.context() as patcher:
            patcher.setattr(threading, primitive, launch)
            with pytest.raises(KeyboardInterrupt):
                manager.start()
        assert len(abandoned) == 1
        thread = abandoned[0]
        assert thread.ident is None
        manager.join(5)
        assert manager.closed.is_set() and handle.closed.is_set()
        assert manager.finalized.is_set()
        assert thread not in manager._threads
        assert thread._target._callback is None
        # Running an abandoned target later cannot enter the retired runtime.
        thread._target()
        assert manager._exited == len(manager._threads)
    finally:
        # CPython can retain its own unlaunched Thread in _limbo after a
        # BaseException. The driver must not wait for or retain that entry.
        with threading._active_limbo_lock:
            for thread in abandoned:
                threading._limbo.pop(thread, None)
        manager.close()
        manager.join(5)


def test_post_terminal_owner_callbacks_use_current_charged_batch(runtime):
    manager, dispatch = runtime
    old = manager.submit(lambda record: 3)
    assert old.result(5) == 3
    wait_for_retirement(manager, old)
    calls = []
    def handler(record):
        for i in range(20):
            old.add_done_callback(
                lambda f, i=i: calls.append((i, execution_context())))
        assert old._delivery.record is record
        assert calls == []
    current = manager.submit(handler)
    current.result(5)
    assert manager._credits == 1
    assert len(dispatch.queue) == 1
    dispatch.drain()
    assert calls == [(i, 'bridge') for i in range(20)]
    wait_for_retirement(manager, current)
    assert manager._credits == 0


def test_owner_callback_reselects_batch_retired_during_registration(runtime):
    manager, dispatch = runtime
    entered, finish, selected, register = [Event() for _ in range(4)]
    armed = Event()
    calls = []
    old = manager.submit(lambda record: (entered.set(), finish.wait(5), 1)[2])
    assert entered.wait(5)
    old.add_done_callback(lambda future: calls.append('old'))
    finish.set()
    assert old.result(5) == 1
    condition = manager._condition

    class RegistrationGate:
        def __getattr__(self, name):
            return getattr(condition, name)

        def __enter__(self):
            if execution_context() == 'cluster' and armed.is_set():
                armed.clear()
                selected.set()
                assert register.wait(5)
            return condition.__enter__()

        def __exit__(self, *args):
            return condition.__exit__(*args)

    manager._condition = RegistrationGate()
    current = None
    try:
        def handler(record):
            armed.set()
            old.add_done_callback(lambda future: calls.append('new'))
            assert old._delivery.record is record
        current = manager.submit(handler)
        assert selected.wait(5)
        dispatch.drain()
        wait_for_retirement(manager, old)
        assert old._record.released
        register.set()
        current.result(5)
        dispatch.drain()
        assert calls == ['old', 'new']
    finally:
        register.set()
        if current is not None:
            current.result(5)
        manager._condition = condition


def test_post_terminal_registration_is_serial_across_threads(runtime):
    manager, dispatch = runtime
    future = manager.submit(lambda record: 1)
    future.result(5)
    wait_for_retirement(manager, future)
    entered, release = Event(), Event()
    order = []
    def first(future):
        order.append('first-enter')
        entered.set()
        assert release.wait(5)
        order.append('first-exit')
    thread = Thread(target=lambda: future.add_done_callback(first))
    thread.start()
    assert entered.wait(5)
    future.add_done_callback(lambda f: order.append('second'))
    assert order == ['first-enter']
    release.set()
    thread.join(5)
    assert order == ['first-enter', 'first-exit', 'second']


def test_concurrent_producers_have_one_fifo_owner(monkeypatch):
    dispatch = Dispatcher()
    manager = ManagementRuntime(dispatch, limit=64)
    admitted, handled, owners, errors = [], [], set(), []
    producer_lock = Lock()
    start = Event()
    admission = Barrier(8)
    original_start = manager.start
    def synchronized_start():
        # All eight callers have entered the actual submit() frame before any
        # reaches its internal admission lock. No producer lock serializes it.
        admission.wait(5)
        original_start()
    monkeypatch.setattr(manager, 'start', synchronized_start)
    def producer():
        try:
            assert start.wait(5)
            for _ in range(8):
                future = manager.submit(lambda r: (
                    handled.append(r.envelope.operation),
                    owners.add(get_ident())))
                with producer_lock:
                    admitted.append(
                        (future._record.envelope.operation, future))
        except BaseException as exc:
            with producer_lock:
                errors.append(exc)
    threads = [Thread(target=producer) for _ in range(8)]
    try:
        for thread in threads:
            thread.start()
        start.set()
        for thread in threads:
            thread.join(5)
            assert not thread.is_alive()
        assert not errors
        assert len(admitted) == 64
        for _, future in admitted:
            future.result(5)
        assert handled == sorted(number for number, _ in admitted)
        assert len(handled) == 64
        assert len(owners) == 1
    finally:
        start.set()
        admission.abort()
        manager.close()
        manager.join(5)


@pytest.mark.parametrize('pool', [None, 'logical-pool'])
def test_result_reserved_before_close_is_still_disposed(runtime, pool):
    manager, dispatch = runtime
    worker = Future()
    class ControlledExecutor:
        def submit(self, function):
            return worker
    started, blocked, release, disposed = Event(), Event(), Event(), Event()
    disposal_context = []
    def dispose(value):
        disposal_context.append((execution_context(), get_ident()))
        disposed.set()
    def initial(record):
        started.set()
        return manager.work(record, ControlledExecutor(), object,
                            dispose=dispose)
    manager.pool_limit = 3
    future = manager.submit(initial, pool=pool)
    completion = Thread(target=lambda: worker.set_result(object()),
                        daemon=True)
    try:
        assert started.wait(5)
        blocker = manager.submit(
            lambda record: (blocked.set(), release.wait(5)), pool=pool)
        assert blocked.wait(5)
        queued = manager.submit(
            lambda record: pytest.fail('closed ordinary work ran'),
            pool=pool)
        # The reserved result sits behind ordinary work.
        completion.start()
        with manager._condition:
            def result_queued():
                queue = (manager._cluster if pool is None
                         else manager._pools[pool])
                return any(item.retain_on_close for item in queue)
            assert manager._condition.wait_for(result_queued, 5)
        manager.close()
        for pending in (future, blocker, queued):
            with pytest.raises(ManagementClosed):
                pending.result(5)
        with manager._condition:
            queue = manager._cluster if pool is None else manager._pools[pool]
            assert len(queue) == 1 and queue[0].retain_on_close
            if pool is not None:
                assert pool in manager._active_pools
        assert not disposed.is_set()
    finally:
        release.set()
        if completion.ident is None:
            manager.close()
            completion.start()
        completion.join(5)
        if completion.is_alive():
            manager.close()
            completion.join(5)
        assert not completion.is_alive()
    assert disposed.wait(5)
    assert disposal_context == [('worker', completion.ident)]
    assert completion.ident not in {
        thread.ident for thread in manager._threads}


@pytest.mark.parametrize('health_notification', [False, True])
def test_detached_coordinator_does_not_keep_runtime_threads_alive(
        health_notification):
    import gc
    import weakref
    from functools import partial
    from cassandra._lifecycle import LifecycleCoordinator
    coordinator = LifecycleCoordinator()
    coordinator.submit(lambda: None)
    manager = coordinator._runtime
    reference = weakref.ref(coordinator)
    release = Event()
    try:
        if health_notification:
            entered, delivered = Event(), Event()
            def hold():
                entered.set()
                assert release.wait(5)
            def completed(callback):
                callback()
                delivered.set()
            manager.limit = 1
            manager.notify(hold, wait=True, timeout=5)
            assert entered.wait(5)
            incarnation = object()
            manager.register_health(incarnation)
            manager.observe_negative(
                incarnation, None, partial(completed, coordinator.drain))
            release.set()
            assert delivered.wait(5)
        # Lifecycle idle is signalled inside its callback, before the native
        # notification reservation finishes. A FIFO fence passes the earlier
        # C admission and L callback frames before testing idle retention.
        manager.notify(lambda: None, wait=True, timeout=5).result(5)
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: not manager._records, 5)
        del coordinator
        gc.collect()
        assert reference() is None
        manager.join(5)
        assert all(not thread.is_alive() for thread in manager._threads)
    finally:
        release.set()
        manager.close()
        manager.join(5)


def test_synchronous_admission_waits_without_allocating_pending_records(
        runtime, monkeypatch):
    manager, dispatch = runtime
    gate = Event()
    futures = [manager.submit(lambda record: gate.wait(5)) for _ in range(4)]
    result = []
    waiting, done = Event(), Event()
    original_wait = manager._condition.wait

    def observe_wait(timeout=None):
        if get_ident() == thread.ident:
            assert manager._credits == manager.limit
            waiting.set()
        return original_wait(timeout)

    def caller():
        result.append(manager.submit(lambda record: 8, wait=True).result(5))
        done.set()
    thread = Thread(target=caller)
    monkeypatch.setattr(manager._condition, 'wait', observe_wait)
    thread.start()
    try:
        assert waiting.wait(5)
        with manager._condition:
            assert len(manager._records) == 4
            assert manager._credits == 4
        assert not done.is_set()
    finally:
        gate.set()
        thread.join(5)
    for future in futures:
        future.result(5)
    assert done.wait(5)
    thread.join(5)
    assert result == [8]


def test_pool_mailboxes_share_two_runners_without_concurrent_handlers():
    dispatch = Dispatcher()
    manager = ManagementRuntime(dispatch, limit=20)
    entered = [Event(), Event()]
    release = Event()
    pools = [object(), object(), object()]
    running = set()
    lock = Lock()
    seen = []
    def handler(index):
        def run(record):
            manager.assert_owner(pools[index])
            with lock:
                assert pools[index] not in running
                running.add(pools[index])
                seen.append((index, get_ident()))
            if index < 2:
                entered[index].set()
                assert release.wait(5)
            with lock:
                running.remove(pools[index])
        return run
    try:
        first = manager.submit(handler(0), pool=pools[0])
        second = manager.submit(handler(1), pool=pools[1])
        assert all(event.wait(5) for event in entered)
        queued_same = manager.submit(handler(0), pool=pools[0])
        third = manager.submit(handler(2), pool=pools[2])
        assert not third.done()
        assert not queued_same.done()
        release.set()
        for future in [first, second, queued_same, third]:
            future.result(5)
        assert len({thread for _, thread in seen}) == 2
        assert len(manager._threads) == 5
    finally:
        release.set()
        manager.close()
        manager.join(5)


def test_worker_close_and_join_guard(runtime):
    manager, dispatch = runtime
    contexts, errors = [], []
    with ThreadPoolExecutor(1) as executor:
        def physical():
            contexts.append(execution_context())
            manager.close()
            try:
                manager.join(1)
            except BaseException as exc:
                errors.append(exc)
            return object()
        future = manager.submit(lambda r: manager.work(r, executor, physical))
        with pytest.raises(ManagementClosed):
            future.result(5)
    manager.join(5)
    assert contexts == ['worker']
    assert len(errors) == 1 and isinstance(errors[0], ManagementContextError)


@pytest.mark.parametrize('entrypoint', [
    'submit', 'observe_negative', 'register_lifetime', 'close_lifetime',
    'unregister_health', 'retain_cleanup', 'counts', 'release_cleanup'])
def test_inherited_runtime_rejects_before_using_parent_locks(
        runtime, entrypoint):
    import os
    import select
    import signal
    if not hasattr(os, 'fork'):
        pytest.skip('fork unavailable')
    manager, dispatch = runtime
    manager.start()
    handle = manager.register_lifetime()
    finish_cleanup = manager.retain_cleanup()
    locked, release = Event(), Event()
    def holder():
        with manager._condition:
            locked.set()
            assert release.wait(10)
    thread = Thread(target=holder)
    thread.start()
    assert locked.wait(5)
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(read_fd)
            try:
                {
                    'submit': lambda: manager.submit(lambda record: None),
                    'observe_negative': lambda: manager.observe_negative(
                        object(), None),
                    'register_lifetime': manager.register_lifetime,
                    'close_lifetime': lambda: manager.close_lifetime(
                        handle, lambda: None),
                    'unregister_health': lambda: manager.unregister_health(
                        object()),
                    'retain_cleanup': manager.retain_cleanup,
                    'counts': manager.counts,
                    'release_cleanup': finish_cleanup,
                }[entrypoint]()
            except ManagementClosed:
                os.write(write_fd, b'rejected')
        finally:
            os._exit(0)
    os.close(write_fd)
    try:
        ready, _, _ = select.select([read_fd], [], [], 5)
        if not ready:
            os.kill(pid, signal.SIGKILL)
        assert ready, 'inherited runtime entered a parent-held condition'
        assert os.read(read_fd, 8) == b'rejected'
    finally:
        release.set()
        thread.join(5)
        finish_cleanup()
        # The child is killed on timeout; after writing it exits in finally.
        os.waitpid(pid, 0)
        os.close(read_fd)


def test_one_physical_result_per_current_admitted_record(runtime):
    manager, dispatch = runtime
    physical = Future()
    submissions = []
    disposed = Event()
    class Executor:
        def submit(self, function):
            submissions.append(function)
            return physical
    def handler(record):
        manager.work(record, Executor(), lambda: None,
                     dispose=lambda value: disposed.set())
        # A second work item would create an uncharged result queue node.
        return manager.work(record, Executor(), lambda: None)
    future = manager.submit(handler)
    try:
        with pytest.raises(ManagementContextError):
            future.result(5)
        assert len(submissions) == 1
    finally:
        complete_physical(manager, physical, object())
    assert disposed.wait(5)
    old = future._record
    def reuse(record):
        with pytest.raises(ManagementContextError):
            manager.work(old, Executor(), lambda: None)
    manager.submit(reuse).result(5)
    assert len(submissions) == 1


def test_inherited_future_rejects_before_parent_held_condition(runtime):
    import os
    import select
    import signal
    if not hasattr(os, 'fork'):
        pytest.skip('fork unavailable')
    manager, dispatch = runtime
    future = manager.submit(lambda record: 1)
    assert future.result(5) == 1
    locked, release = Event(), Event()
    def holder():
        with future._condition:
            locked.set()
            assert release.wait(10)
    thread = Thread(target=holder)
    thread.start()
    assert locked.wait(5)
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(read_fd)
            calls = [future.done, future.running, future.cancelled,
                     future.result, future.exception, future.cancel,
                     lambda: future.add_done_callback(lambda f: None),
                     lambda: wait([future], timeout=0)]
            rejected = 0
            for call in calls:
                try:
                    call()
                except ManagementClosed:
                    rejected += 1
            os.write(write_fd, str(rejected).encode())
        finally:
            os._exit(0)
    os.close(write_fd)
    try:
        ready, _, _ = select.select([read_fd], [], [], 5)
        if not ready:
            os.kill(pid, signal.SIGKILL)
        assert ready, 'inherited Future entered a parent-held condition'
        assert os.read(read_fd, 8) == b'8'
    finally:
        release.set()
        thread.join(5)
        os.close(read_fd)
        os.waitpid(pid, 0)


def test_dispatch_rejection_closes_without_stranding_waiters_or_resources():
    from cassandra._management import ManagementDeliveryFailed
    attempts, calls = [], []
    def reject(callback):
        attempts.append(callback)
        raise RuntimeError('reactor rejected callback')
    (handler_entered, handler_release, worker_started, worker_release,
     disposed) = [Event() for _ in range(5)]
    with ThreadPoolExecutor(1) as executor:
        manager = ManagementRuntime(reject, limit=2)
        def physical():
            worker_started.set()
            assert worker_release.wait(5)
            return object()
        try:
            late = manager.submit(lambda r: manager.work(
                r, executor, physical, dispose=lambda value: disposed.set()))
            assert worker_started.wait(5)
            def handler(record):
                handler_entered.set()
                assert handler_release.wait(5)
                return 4
            future = manager.submit(handler)
            assert handler_entered.wait(5)
            future.add_done_callback(
                lambda f: calls.append(execution_context()))
            handler_release.set()
            assert future.result(5) == 4
            assert manager.closed.wait(5)
            assert isinstance(
                manager.completion_error, ManagementDeliveryFailed)
            assert isinstance(future._delivery.error, ManagementDeliveryFailed)
            with pytest.raises(ManagementDeliveryFailed):
                future.add_done_callback(lambda f: calls.append('late'))
            with pytest.raises(ManagementClosed):
                late.result(5)
            worker_release.set()
            assert disposed.wait(5)
        finally:
            handler_release.set()
            worker_release.set()
            manager.close()
            manager.join(5)
    assert len(attempts) == 1
    assert calls == []
    assert manager._credits == 0
    assert not manager._records


def test_fatal_service_loss_abandons_queued_tickets_without_retaining_results(
):
    from cassandra._management import ManagementDeliveryFailed
    queue = []
    calls = []
    def dispatch(ticket):
        if queue:
            raise RuntimeError('validated reactor lost')
        queue.append(ticket)
    manager = ManagementRuntime(dispatch, limit=2)
    (first_entered, first_release, second_entered,
     second_release) = [Event() for _ in range(4)]
    def first_handler(record):
        first_entered.set()
        assert first_release.wait(5)
        return object()
    def second_handler(record):
        second_entered.set()
        assert second_release.wait(5)
    try:
        first = manager.submit(first_handler)
        assert first_entered.wait(5)
        first.add_done_callback(lambda f: calls.append('first'))
        first_release.set()
        first.result(5)
        second = manager.submit(second_handler)
        assert second_entered.wait(5)
        second.add_done_callback(lambda f: calls.append('second'))
        second_release.set()
        second.result(5)
        assert manager.closed.wait(5)
        manager.join(5)
    finally:
        first_release.set()
        second_release.set()
        manager.close()
        for ticket in queue:
            ticket()
        manager.join(5)
    assert isinstance(first._delivery.error, ManagementDeliveryFailed)
    assert queue[0]._callback is None
    queue[0]()  # A late reactor wakeup cannot resurrect an abandoned batch.
    assert calls == []
    assert manager._credits == 0


def test_cancellation_racing_settlement_does_not_strand_coalesced_waiters(
        runtime, monkeypatch):
    manager, dispatch = runtime
    entered, release, at_setter, cancel_attempted = [Event() for _ in range(4)]
    cancelled_state = Event()
    def handler(record):
        entered.set()
        assert release.wait(5)
        return 17
    first = manager.submit(handler, key='shared')
    second = manager.submit(handler, key='shared')
    assert entered.wait(5)
    setter = first.set_result
    invoke = first._invoke_callbacks
    def observed_cancel():
        cancelled_state.set()
        invoke()
    def paused_setter(value):
        at_setter.set()
        assert cancel_attempted.wait(5)
        # If settlement owns the future lock, cancellation must wait for its
        # terminal update. Otherwise force the competing cancellation to win.
        if not first._condition._is_owned():
            assert cancelled_state.wait(5)
        setter(value)
    monkeypatch.setattr(first, '_invoke_callbacks', observed_cancel)
    monkeypatch.setattr(first, 'set_result', paused_setter)
    def cancel():
        assert at_setter.wait(5)
        cancel_attempted.set()
        first.cancel()
    thread = Thread(target=cancel)
    thread.start()
    try:
        release.set()
        assert second.result(5) == 17
        assert first.done()
    finally:
        release.set()
        thread.join(5)
        assert not thread.is_alive()


def test_notification_admission_after_close_does_not_retain_callback(
        runtime, monkeypatch):
    import gc
    import weakref
    manager, dispatch = runtime
    entered, release = Event(), Event()
    original = manager.submit
    def submit(handler, **kwargs):
        def paused(record):
            entered.set()
            assert release.wait(5)
            return handler(record)
        return original(paused, **kwargs)
    monkeypatch.setattr(manager, 'submit', submit)
    class Callback:
        def __call__(self):
            pytest.fail('closed notification ran')
    callback = Callback()
    reference = weakref.ref(callback)
    future = manager.notify(callback)
    assert entered.wait(5)
    manager.close()
    release.set()
    manager.join(5)
    del callback
    gc.collect()
    assert reference() is None
    assert not manager._notifications
    with pytest.raises(ManagementClosed):
        future.result(5)


@pytest.mark.parametrize('private_first', [False, True])
@pytest.mark.parametrize('private_budget', ['reentry', 'aggregate'])
@pytest.mark.parametrize('pool', [None, 'logical-pool'])
def test_coalesced_waiters_keep_independent_budget_and_delivery_charges(
        runtime, private_first, private_budget, pool):
    from contextlib import nullcontext
    manager, dispatch = runtime
    physical = Future()
    work_started, joined, allow_join = Event(), Event(), Event()
    calls, private = [], []
    class Executor:
        def submit(self, function):
            calls.append(function)
            work_started.set()
            return physical
    def operation(record):
        return manager.work(record, Executor(), lambda: 23)
    def listener():
        assert allow_join.wait(5)
        scope = (manager.synchronous_reentry() if private_budget == 'reentry'
                 else nullcontext('aggregate'))
        with scope as budget:
            future = manager.submit(operation, key='shared-budget',
                                    budget=budget, pool=pool)
            private.append(future)
            with pytest.raises(ManagementContextError):
                future.add_done_callback(lambda f: None)
            joined.set()
            assert future.result(5) == 23
    try:
        if not private_first:
            ordinary = manager.submit(
                operation, key='shared-budget', pool=pool)
            assert work_started.wait(5)
        notification = manager.notify(listener)
        allow_join.set()
        assert joined.wait(5)
        if private_first:
            assert work_started.wait(5)
            ordinary = manager.submit(
                operation, key='shared-budget', pool=pool)
        assert private[0]._record is ordinary._record
        ordinary.add_done_callback(lambda f: None)
        complete_physical(manager, physical, 23)
        assert ordinary.result(5) == 23
        notification.result(5)
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: manager._credits == 1, 5)
            assert manager._reentry_credits == 0
        assert not manager._reentry_active
        if pool is not None:
            assert manager._pool_credits[pool] == 1
        assert len(calls) == 1
        assert len(dispatch.queue) == 1
        dispatch.drain()
        wait_for_retirement(manager, ordinary)
        assert manager.counts()['ordinary'] == manager.counts()['reentry'] == 0
        assert not manager._pool_credits
    finally:
        allow_join.set()
        if not physical.done():
            complete_physical(manager, physical, 23)


def test_foreign_callback_uses_ordinary_admission(runtime):
    first, dispatch = runtime
    other = ManagementRuntime(dispatch, limit=1)
    gate, entered = Event(), Event()
    try:
        other.submit(lambda record: (entered.set(), gate.wait(5)))
        assert entered.wait(5)
        def listener():
            with other.synchronous_reentry() as budget:
                assert budget == 'ordinary'
                with pytest.raises(ManagementOverloaded):
                    other.submit(lambda record: None, budget=budget)
            assert not other._reentry_active
        first.notify(listener).result(5)
    finally:
        gate.set()
        other.close()
        other.join(5)


def test_standard_wait_rejects_owner_self_wait_and_allows_external_wait(
        runtime):
    manager, dispatch = runtime
    following = []
    def handler(record):
        future = manager.submit(lambda record: 41)
        following.append(future)
        with pytest.raises(ManagementContextError):
            wait([future], timeout=0)
        return 40
    completed = manager.submit(handler)
    assert completed.result(5) == 40
    assert not wait([completed, following[0]], timeout=5).not_done
    assert following[0].result(5) == 41


def test_external_standard_wait_wakes_when_pending_operation_is_cancelled(
        runtime):
    manager, dispatch = runtime
    entered, release, installed = Event(), Event(), Event()
    blocker = manager.submit(lambda record: (entered.set(), release.wait(5)))
    assert entered.wait(5)
    future = manager.submit(lambda record: 42)
    class ObservedWaiters(list):
        def append(self, waiter):
            super().append(waiter)
            installed.set()
    future._waiters = ObservedWaiters()
    results = []
    thread = Thread(target=lambda: results.append(wait([future], timeout=5)))
    thread.start()
    try:
        assert installed.wait(5)
        assert future.cancel()
        thread.join(5)
        assert not thread.is_alive()
        assert results[0].done == {future}
        assert not results[0].not_done
    finally:
        release.set()
        thread.join(5)
    blocker.result(5)


def test_close_handles_retire_history_but_preserve_live_duplicate_joins(
        runtime):
    import gc
    import weakref
    manager, dispatch = runtime
    references, retired = [], []
    for i in range(128):
        handle = manager.register_lifetime()
        references.append(weakref.ref(handle))
        done = manager.close_lifetime(handle, lambda: retired.append(i))
        assert done.is_set()
        assert manager.close_lifetime(
            handle, lambda: pytest.fail('duplicate')) is done
    del handle
    gc.collect()
    assert all(reference() is None for reference in references)
    assert retired == list(range(128))
    assert manager.counts()['close'] == 0
    first, second = manager.register_lifetime(), manager.register_lifetime()
    manager.close_lifetime(first, lambda: None)
    assert not second.closed.is_set()
    manager.close()
    assert second.closed.is_set()
    assert not manager._lifetimes
    assert manager.close_lifetime(
        second, lambda: pytest.fail('after close')) is second.closed
    with pytest.raises(ManagementClosed):
        manager.register_lifetime()


@pytest.mark.parametrize('terminal_first', [False, True])
def test_bulk_owner_rejection_unwinds_all_conditions(runtime, terminal_first):
    manager, dispatch = runtime
    already = manager.submit(lambda record: 1)
    assert already.result(5) == 1
    following = []
    def handler(record):
        pair = [manager.submit(lambda record: 2) for _ in range(2)]
        terminal, pending = sorted(pair, key=id, reverse=not terminal_first)
        assert terminal.cancel()
        assert wait([already, terminal], timeout=0).done == {already, terminal}
        with pytest.raises(ManagementContextError):
            wait([terminal, pending], timeout=0)
        # Rejection must not contaminate this thread's ordinary status access.
        assert not pending.done()
        following.extend(pair)
    manager.submit(handler).result(5)
    assert not wait(following, timeout=5).not_done
    for future in following:
        acquired = future._condition.acquire(timeout=1)
        try:
            assert acquired, (
                'bulk wait rejection retained an earlier condition')
        finally:
            if acquired:
                future._condition.release()


@pytest.mark.parametrize(
    'outcome', ['return', 'error', 'base_error', 'rejected_reacquire'])
def test_policy_phase_restores_admission_after_policy_exit(
        runtime, outcome):
    from cassandra import _management
    manager, dispatch = runtime
    class PolicyAbort(BaseException):
        pass
    @_management.policy_call
    def policy():
        assert _management._context.call is None
        assert manager.counts()['ordinary'] == 0
        if outcome == 'error':
            raise ValueError('policy failed')
        if outcome == 'base_error':
            raise PolicyAbort()
        if outcome == 'rejected_reacquire':
            manager.close()
        return 13
    with _management.compatibility_call(manager):
        call = _management._context.call
        original = call.record
        if outcome == 'return':
            assert policy() == 13
            assert call.record is not original
        else:
            error = {'error': ValueError, 'base_error': PolicyAbort,
                     'rejected_reacquire': ManagementClosed}[outcome]
            with pytest.raises(error):
                policy()
            if outcome == 'rejected_reacquire':
                assert call.record is None
            else:
                assert call.record is not None
                assert call.record is not original
                assert manager.counts()['ordinary'] == 1
        assert _management._context.call is call
    assert getattr(_management._context, 'call', None) is None
    assert manager.counts()['ordinary'] == 0


def test_policy_phase_waits_for_its_native_sentinel_frame(monkeypatch):
    manager = ManagementRuntime(Dispatcher(), limit=1)
    entered, release, waiting, finished = Event(), Event(), Event(), Event()
    observations, errors = [], []
    handle, wait_condition = manager._handle, manager._condition.wait

    def delayed_handler(record):
        handle(record)
        entered.set()
        assert release.wait(5)

    def observe_wait(timeout=None):
        if current_thread() is caller:
            waiting.set()
        return wait_condition(timeout)

    @management.policy_call
    def policy():
        observations.append(manager.counts()['ordinary'])
        raise ValueError('do not reacquire')

    def invoke():
        try:
            with management.compatibility_call(manager):
                with pytest.raises(ValueError, match='do not reacquire'):
                    policy()
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    caller = Thread(target=invoke, daemon=True)
    monkeypatch.setattr(manager, '_handle', delayed_handler)
    monkeypatch.setattr(manager._condition, 'wait', observe_wait)
    try:
        caller.start()
        assert entered.wait(5)
        assert waiting.wait(5), 'phase release did not fence native execution'
        assert not observations
        release.set()
        assert finished.wait(5)
        assert not errors
        assert observations == [0]
    finally:
        release.set()
        try:
            caller.join(5)
        finally:
            manager.close()
            manager.join(5)
    assert not caller.is_alive()


@pytest.mark.parametrize('pool', [None, 'logical-pool'])
def test_compatibility_work_rejects_owner_before_admission(
        runtime, monkeypatch, pool):
    from cassandra._management import compatibility_work
    manager, dispatch = runtime
    submitted = []
    original = manager.submit
    def submit(*args, **kwargs):
        future = original(*args, **kwargs)
        submitted.append(future)
        def forbidden_wait(*args, **kwargs):
            raise AssertionError('compatibility work attempted an owner wait')
        monkeypatch.setattr(future._record.started, 'wait', forbidden_wait)
        return future
    def handler(record):
        monkeypatch.setattr(manager, 'submit', submit)
        try:
            with pytest.raises(ManagementContextError):
                with compatibility_work(manager):
                    pytest.fail('owner entered physical compatibility work')
            assert submitted == []
        finally:
            # Retire any synthetic physical obligation admitted before a
            # forbidden owner wait, including when the assertion fails.
            for future in submitted:
                with manager._condition:
                    manager._finish(future._record)
                    future._record.physical -= 1
                    manager._release_locked(future._record)
    original(handler, pool=pool).result(5)


def test_health_registration_crossing_final_close_rejects_without_retention(
        runtime, monkeypatch):
    manager, dispatch = runtime
    started, resume = Event(), Event()
    original = manager.start
    def start():
        original()
        started.set()
        assert resume.wait(5)
    monkeypatch.setattr(manager, 'start', start)
    incarnation = object()
    errors = []
    def register():
        try:
            manager.register_health(incarnation)
        except BaseException as exc:
            errors.append(exc)
    thread = Thread(target=register)
    thread.start()
    try:
        assert started.wait(5)
        manager.close()
        manager.join(5)
        resume.set()
        thread.join(5)
        assert not thread.is_alive()
        assert len(errors) == 1 and isinstance(errors[0], ManagementClosed)
        assert manager.counts()['health'] == 0
    finally:
        resume.set()
        thread.join(5)
        manager.unregister_health(incarnation)


def test_cleanup_cannot_join_an_already_started_async_finalization():
    detached = []
    manager = ManagementRuntime(Dispatcher(), on_finalized=detached.append)
    early = manager.retain_cleanup()
    late = None
    try:
        manager.close()
        assert not detached
        early()
        assert len(detached) == 1 and manager._finalizing
        assert not manager.finalized.is_set()
        with pytest.raises(ManagementClosed):
            late = manager.retain_cleanup()
        assert manager.counts()['close'] == 0
        detached[0]()
        manager.join(5)
    finally:
        early()
        if late is not None:
            late()
        manager.close()
        if detached:
            detached[0]()
        manager.join(5)


@pytest.mark.parametrize('entry', ['call', 'work'])
@pytest.mark.parametrize('drain_queued', [False, True])
def test_interrupted_initial_compatibility_admission_discharges_reservation(
        runtime, monkeypatch, entry, drain_queued):
    from cassandra._management import compatibility_call, compatibility_work
    manager, dispatch = runtime
    entered, resume = Event(), Event()
    manager.submit(lambda record: (entered.set(), resume.wait(5)))
    assert entered.wait(5)
    admitted = []
    original = manager.submit
    def submit(*args, **kwargs):
        future = original(*args, **kwargs)
        admitted.append(future)
        def interrupt(*args, **kwargs):
            assert not future._record.started.is_set()
            raise KeyboardInterrupt()
        monkeypatch.setattr(future._record.started, 'wait', interrupt)
        return future
    monkeypatch.setattr(manager, 'submit', submit)
    try:
        scope = compatibility_call if entry == 'call' else compatibility_work
        with pytest.raises(KeyboardInterrupt):
            with scope(manager):
                pytest.fail('interrupted admission entered its body')
        assert len(admitted) == 1
        assert admitted[0]._record.physical == 0
        if drain_queued:
            monkeypatch.setattr(manager, 'submit', original)
            resume.set()
            manager.submit(lambda record: None).result(5)
            with manager._condition:
                assert manager._condition.wait_for(
                    lambda: not manager._records, 5)
            assert admitted[0]._record.released
            assert admitted[0]._record.physical == 0
        else:
            manager.close()
            assert admitted[0]._record.released
    finally:
        resume.set()
        manager.close()
        # A failed interruption assertion must not leave daemon threads or
        # synthetic physical reservations behind.
        for future in admitted:
            with manager._condition:
                if future._record.physical:
                    future._record.physical -= 1
                    manager._release_locked(future._record)
        manager.join(5)


@pytest.mark.parametrize('installed', [False, True])
@pytest.mark.parametrize('outcome', ['result', 'exception'])
def test_interrupted_compatibility_attachment_retains_work_until_completion(
        runtime, installed, outcome):
    from cassandra._management import compatibility_work
    manager, dispatch = runtime
    physical = Future()
    assert physical.set_running_or_notify_cancel()
    register = physical.add_done_callback
    attaching, waiting, finished = Event(), Event(), Event()
    result = physical.result
    def wait_for_result(timeout=None):
        waiting.set()
        return result(5)
    physical.result = wait_for_result
    errors, records = [], []
    def interrupted(callback):
        if installed:
            register(callback)
        attaching.set()
        raise KeyboardInterrupt('compatibility attachment interrupted')
    physical.add_done_callback = interrupted
    def caller():
        try:
            with compatibility_work(manager) as attach:
                with manager._condition:
                    records.extend(manager._records)
                attach(physical)
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()
    thread = Thread(target=caller, daemon=True)
    thread.start()
    try:
        assert attaching.wait(5)
        assert waiting.wait(5), 'attachment did not retain its physical wait'
        with manager._condition:
            assert len(records) == 1
            assert records[0].physical == 1
        assert not physical.done() and not finished.is_set()
    finally:
        if outcome == 'result':
            physical.set_result(23)
        else:
            physical.set_exception(ValueError('physical work failed'))
        thread.join(5)
        assert not thread.is_alive()
        # The caller can finish while the capture-free native sentinel still
        # unwinds. Fence that frame before inspecting reservation retirement.
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: all(not record.executing for record in records), 5)
            debt_after_completion = sum(record.physical for record in records)
            credits_after_completion = manager.counts()['ordinary']
            for record in records:
                if record.physical:
                    manager._finish(record)
                    record.physical -= 1
                    manager._release_locked(record)
    assert len(errors) == 1
    assert isinstance(errors[0], KeyboardInterrupt)
    assert debt_after_completion == credits_after_completion == 0
    assert records[0].physical == 0
    assert manager.counts()['ordinary'] == 0


@pytest.mark.parametrize('pool', [None, 'logical-pool'])
@pytest.mark.parametrize(
    'ending', ['normal', 'close', 'fatal', 'close_fatal'])
def test_owner_callback_handoff_yields_reactor_while_caller_callback_runs(
        runtime, monkeypatch, pool, ending):
    from cassandra._management import ManagementDeliveryFailed
    manager, dispatch = runtime
    old = manager.submit(lambda record: 3)
    assert old.result(5) == 3
    with manager._condition:
        assert manager._condition.wait_for(lambda: old._record.released, 5)
    entered, resume, io_progress = Event(), Event(), Event()
    callback_parked = Event()
    condition_wait = manager._condition.wait
    def observe_wait(timeout=None):
        if execution_context() == 'callback' and manager._closing:
            callback_parked.set()
        return condition_wait(timeout)
    monkeypatch.setattr(manager._condition, 'wait', observe_wait)
    calls = []
    def caller_callback(future):
        calls.append(('caller-enter', execution_context()))
        entered.set()
        assert resume.wait(5)
        calls.append(('caller-exit', execution_context()))
    caller = Thread(target=lambda: old.add_done_callback(caller_callback))
    reactor = Thread(target=dispatch.drain)
    caller.start()
    try:
        assert entered.wait(5)
        def owner(record):
            old.add_done_callback(
                lambda f: calls.append(('owner', execution_context())))
        current = manager.submit(owner, pool=pool)
        current.result(5)
        dispatch(io_progress.set)
        reactor.start()
        assert io_progress.wait(5), (
            'callback handoff blocked shared reactor progress')
        reactor.join(5)
        assert not reactor.is_alive()
        assert calls == [('caller-enter', None)]
        assert manager.counts()['ordinary'] == 1
        if ending in ('close', 'close_fatal'):
            manager.close()
            assert not manager.finalized.is_set()
            if ending == 'close_fatal':
                # L must remain available for disposal if the deferred batch
                # loses its completion service only after shutdown begins.
                assert callback_parked.wait(5)
                def reject(ticket):
                    raise RuntimeError('reactor lost after close')
                monkeypatch.setattr(manager, '_dispatch', reject)
        elif ending == 'fatal':
            def reject(ticket):
                raise RuntimeError('validated reactor lost during handoff')
            monkeypatch.setattr(manager, '_dispatch', reject)
            with manager._condition:
                failed_delivery = manager.submit(lambda record: 9)
                failed_delivery.add_done_callback(
                    lambda f: pytest.fail('undeliverable callback ran'))
            assert failed_delivery.result(5) == 9
            assert manager.closed.wait(5)
            manager.join(5)
            assert isinstance(old._delivery.error, ManagementDeliveryFailed)
            assert manager.counts()['ordinary'] == 0
    finally:
        resume.set()
        caller.join(5)
        if reactor.ident is not None:
            reactor.join(5)
        assert not caller.is_alive() and not reactor.is_alive()
    dispatch.drain()
    expected = [('caller-enter', None), ('caller-exit', None)]
    if ending not in ('fatal', 'close_fatal'):
        expected.append(('owner', 'bridge'))
    assert calls == expected
    wait_for_retirement(manager, current)
    assert manager.counts()['ordinary'] == 0
    assert not old._delivery.waiters
    if ending == 'close_fatal':
        manager.join(5)
        assert isinstance(old._delivery.error, ManagementDeliveryFailed)
        assert not manager._callback_garbage
        assert all(value == 0
                   for value in assert_runtime_counts(manager).values())


@pytest.mark.parametrize('one_record', [False, True])
def test_deferred_callback_handoffs_keep_one_ticket_per_charged_record(
        runtime, one_record):
    manager, dispatch = runtime
    old = [manager.submit(lambda record: 1) for _ in range(3)]
    assert all(f.result(5) == 1 for f in old)
    with manager._condition:
        assert manager._condition.wait_for(
            lambda: not manager._records, 5)
    count = 1 if one_record else 3
    manager.limit = count
    entered = [Event() for _ in old]
    resume, io_progress = Event(), Event()
    calls, owners = [], []
    def caller_callback(future, ready):
        ready.set()
        assert resume.wait(5)
    callers = [Thread(target=lambda f=f, ready=ready:
                      f.add_done_callback(lambda f: caller_callback(f, ready)))
               for f, ready in zip(old, entered)]
    reactor = Thread(target=dispatch.drain)
    for thread in callers:
        thread.start()
    try:
        assert all(ready.wait(5) for ready in entered)
        def owner(futures):
            for future in futures:
                future.add_done_callback(
                    lambda f: calls.append(execution_context()))
        if one_record:
            owners.append(manager.submit(lambda record: owner(old)))
            owners[-1].result(5)
        else:
            for future in old:
                owners.append(manager.submit(
                    lambda record, f=future: owner([f])))
                owners[-1].result(5)
        dispatch(io_progress.set)
        reactor.start()
        assert io_progress.wait(5)
        reactor.join(5)
        assert not reactor.is_alive()
        with manager._condition:
            assert len(manager._records) == count
            assert assert_runtime_counts(manager)['delivery'] == count
            assert all(r.delivery and r.delivery_ticket is None
                       for r in manager._records)
        assert not dispatch.queue
        with pytest.raises(ManagementOverloaded):
            manager.submit(lambda record: None)
        resume.set()
        for thread in callers:
            thread.join(5)
            assert not thread.is_alive()
        assert len(dispatch.queue) == count
        dispatch.drain()
        assert calls == ['bridge'] * 3
        wait_for_retirement(manager, *owners)
        assert manager.counts()['ordinary'] == 0
        assert all(not f._delivery.waiters for f in old)
    finally:
        resume.set()
        for thread in callers:
            thread.join(5)
        if reactor.ident is not None:
            reactor.join(5)


def test_callback_handoff_completion_during_active_delivery_cannot_lose_wakeup(
        runtime, monkeypatch):
    manager, dispatch = runtime
    future = manager.submit(lambda record: 1)
    assert future.result(5) == 1
    with manager._condition:
        assert manager._condition.wait_for(lambda: future._record.released, 5)
    entered, resume, deferred, return_delivery = [Event() for _ in range(4)]
    calls = []
    def callback(future):
        entered.set()
        assert resume.wait(5)
    caller = Thread(target=lambda: future.add_done_callback(callback))
    reactor = Thread(target=dispatch.drain)
    original = future._delivery.drain
    def drain(record=None):
        result = original(record)
        if record is not None and not result:
            deferred.set()
            assert return_delivery.wait(5)
        return result
    monkeypatch.setattr(future._delivery, 'drain', drain)
    caller.start()
    try:
        assert entered.wait(5)
        owner = manager.submit(lambda record: future.add_done_callback(
            lambda f: calls.append(execution_context())))
        owner.result(5)
        reactor.start()
        assert deferred.wait(5)
        resume.set()
        caller.join(5)
        assert not caller.is_alive()
        with manager._condition:
            assert assert_runtime_counts(manager)['delivery'] == 1
            assert owner._record.delivery_again
            assert owner._record.delivery_ticket is not None
        return_delivery.set()
        reactor.join(5)
        assert not reactor.is_alive()
        assert calls == ['bridge']
        assert not dispatch.queue
        wait_for_retirement(manager, owner)
        assert manager.counts()['ordinary'] == 0
    finally:
        resume.set()
        return_delivery.set()
        caller.join(5)
        if reactor.ident is not None:
            reactor.join(5)


@pytest.mark.parametrize('budget', ['reentry', 'aggregate'])
def test_private_owner_record_cannot_borrow_an_old_future_callback_batch(
        runtime, budget):
    manager, dispatch = runtime
    future = manager.submit(lambda record: 1)
    assert future.result(5) == 1
    with manager._condition:
        assert manager._condition.wait_for(lambda: future._record.released, 5)
    def handler(record):
        with pytest.raises(ManagementContextError):
            future.add_done_callback(
                lambda f: pytest.fail('uncharged callback'))
    manager.submit(handler, budget=budget).result(5)
    assert future._delivery.record is future._record
    assert not dispatch.queue


@pytest.mark.parametrize('outcome', ['result', 'error'])
def test_coalesced_settlement_and_bulk_wait_use_compatible_lock_order(outcome):
    import subprocess
    import sys
    # The broken lock order also blocks close, so isolate and reap the child.
    # Preallocate real futures in reverse ID order rather than assume an
    # allocator order. Admission and stdlib bulk waiting remain unchanged.
    script = r'''import faulthandler
faulthandler.dump_traceback_later(7)
"""Bounded child probe; parent terminates a genuine bulk-lock deadlock."""
from concurrent.futures import wait
from threading import Event, Thread, get_ident
import sys

from cassandra import _management

print('management-loader', _management.__file__, flush=True)
manager = _management.ManagementRuntime(lambda callback: None)
gate, owner_held, waiter_held = Event(), Event(), Event()
original = _management.OperationFuture
available = []


def factory(record):
    if not available:
        available.extend(sorted([original(record), original(record)],
                                key=id, reverse=True))
    return available.pop(0)


def handler(record):
    assert gate.wait(5)
    if sys.argv[1] == 'error':
        raise ValueError('controlled failure')
    return 17


_management.OperationFuture = factory
try:
    first = manager.submit(handler, key='shared')
    second = manager.submit(handler, key='shared')
finally:
    _management.OperationFuture = original
assert first._record is second._record and id(second) < id(first)
setter = 'set_exception' if sys.argv[1] == 'error' else 'set_result'
original_setter = getattr(first, setter)


def publish(value):
    owner_held.set()
    assert waiter_held.wait(5)
    original_setter(value)


setattr(first, setter, publish)
original_acquire = second._condition.acquire


def acquire(*args, **kwargs):
    result = original_acquire(*args, **kwargs)
    if get_ident() == waiter.ident:
        waiter_held.set()
    return result


second._condition.acquire = acquire
results = []


def bulk_wait():
    assert owner_held.wait(5)
    results.append(wait([first, second], timeout=5))


waiter = Thread(target=bulk_wait)
waiter.start()
gate.set()
waiter.join(6)
assert not waiter.is_alive(), 'coalesced settlement deadlocked bulk wait'
assert results and results[0].done == {first, second}
assert not results[0].not_done
for future in (first, second):
    if sys.argv[1] == 'error':
        assert isinstance(future.exception(1), ValueError)
    else:
        assert future.result(1) == 17
assert manager.submit(lambda record: 18).result(5) == 18
manager.close()
manager.join(5)
assert manager.finalized.is_set()
print('bulk-wait-and-close-complete', flush=True)

faulthandler.cancel_dump_traceback_later()
'''
    result = subprocess.run(
        [sys.executable, '-c', script, outcome], capture_output=True,
        text=True, timeout=12)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('plain', [False, True])
@pytest.mark.parametrize('inherited_first', [False, True])
def test_mixed_bulk_fork_rejection_preserves_fresh_locks(
        runtime, plain, inherited_first):
    import os
    import select
    import signal
    from concurrent.futures import as_completed
    from concurrent.futures._base import _AcquireFutures
    if not hasattr(os, 'fork'):
        pytest.skip('fork unavailable')
    manager, dispatch = runtime
    inherited = manager.submit(lambda record: 1)
    assert inherited.result(5) == 1
    locked, release = Event(), Event()

    def hold_parent_condition():
        with inherited._condition:
            locked.set()
            assert release.wait(25)

    holder = Thread(target=hold_parent_condition)
    holder.start()
    try:
        assert locked.wait(5)
        read_fd, write_fd = os.pipe()
        try:
            pid = os.fork()
        except BaseException:
            os.close(read_fd)
            os.close(write_fd)
            raise
    except BaseException:
        release.set()
        holder.join(5)
        raise
    if pid == 0:
        os.close(read_fd)
        try:
            fresh_manager = None
            gate = Event()
            if plain:
                fresh = Future()
            else:
                fresh_manager = ManagementRuntime(lambda callback: None)
                fresh = fresh_manager.submit(lambda record: gate.wait(5))

            def inherited_lock_access(*args, **kwargs):
                raise AssertionError('touched an inherited condition lock')

            inherited._condition._acquire = inherited_lock_access
            inherited._condition._release = inherited_lock_access
            try:
                with inherited._condition:
                    raise AssertionError('entered inherited condition context')
            except ManagementClosed:
                pass
            fresh._condition.acquire()
            for consumer in (
                    lambda: wait([fresh, inherited], timeout=0),
                    lambda: next(as_completed([fresh, inherited], timeout=0))):
                try:
                    consumer()
                except ManagementClosed:
                    pass
                else:
                    raise AssertionError('bulk wait accepted inherited future')
            # Exercise both possible ID-sorted acquisition permutations without
            # assuming an allocator order or patching any stdlib function.
            ordered = _AcquireFutures([fresh, inherited])
            ordered.futures = ([inherited, fresh] if inherited_first
                               else [fresh, inherited])
            try:
                with ordered:
                    inherited._state
            except ManagementClosed:
                pass
            else:
                raise AssertionError('bulk context accepted inherited state')
            attempts = []

            def probe_outer_depth():
                acquired = fresh._condition.acquire(blocking=False)
                attempts.append(acquired)
                if acquired:
                    fresh._condition.release()

            probe = Thread(target=probe_outer_depth)
            probe.start()
            probe.join(5)
            assert attempts == [False], 'bulk rejection released outer lock'
            fresh._condition.release()
            settled = Event()

            def settle():
                fresh.set_result(23)
                settled.set()

            worker = Thread(target=settle)
            worker.start()
            assert settled.wait(5), 'bulk rejection leaked fresh condition'
            worker.join(5)
            assert fresh.result(0) == 23
            if fresh_manager is not None:
                gate.set()
                fresh_manager.close()
                fresh_manager.join(5)
            os.write(write_fd, b'ok')
        except BaseException as exc:
            os.write(write_fd, repr(exc).encode()[:4000])
        finally:
            os._exit(0)
    os.close(write_fd)
    try:
        ready, _, _ = select.select([read_fd], [], [], 20)
        if not ready:
            os.kill(pid, signal.SIGKILL)
        assert ready, 'mixed bulk wait entered a parent-held condition'
        outcome = os.read(read_fd, 4096)
        assert outcome == b'ok', outcome.decode()
    finally:
        release.set()
        holder.join(5)
        os.close(read_fd)
        os.waitpid(pid, 0)


def test_count_snapshots_do_not_enumerate_retained_records():
    class NoIteration(set):
        def __iter__(self):
            pytest.fail('telemetry enumerated admitted records')

    dispatch = Dispatcher()
    manager = ManagementRuntime(dispatch)
    manager._started = True
    futures = [manager.submit(lambda record: None) for _ in range(64)]
    records = manager._records
    try:
        manager._records = NoIteration(records)
        assert manager.counts()['mailbox'] == 64
        assert manager.high_waters['mailbox'] == 64
        for future in futures:
            future.add_done_callback(lambda future: None)
            manager._finish(future._record)
        assert manager.counts()['delivery'] == 64
        assert manager.high_waters['delivery'] == 64
    finally:
        manager._records = records
        manager.close()
        dispatch.drain()
        drain_controlled_callbacks(manager)
        manager.join(5)
    assert all(future.done() for future in futures)
    assert all(value == 0 for value in assert_runtime_counts(manager).values())


@pytest.mark.parametrize('pool', [None, 'pool'])
def test_shutdown_prunes_handlers_but_retains_result_mailbox_counts(pool):
    from cassandra._management import _PENDING

    dispatch = Dispatcher()
    manager = ManagementRuntime(dispatch)
    manager._started = True
    results = []
    future = manager.submit(lambda record: _PENDING, pool=pool)
    record = future._record
    future.add_done_callback(lambda future: results.append('callback'))
    with manager._condition:
        manager._enqueue_locked(record, lambda: results.append('result'),
                                retain_on_close=True)
        assert assert_runtime_counts(manager)['mailbox'] == 2
    manager.close()
    counts = assert_runtime_counts(manager)
    assert counts['mailbox'] == counts['delivery'] == 1
    queue = manager._cluster if pool is None else manager._pools[pool]
    with manager._condition:
        entry = manager._dequeue_locked(queue)
        assert entry.retain_on_close
    manager._execute(entry.record, entry.callback,
                     'cluster' if pool is None else 'pool')
    with manager._condition:
        manager._release_locked(record)
    assert assert_runtime_counts(manager)['mailbox'] == 0
    dispatch.drain()
    drain_controlled_callbacks(manager)
    manager.join(5)
    assert results == ['result', 'callback']
    assert all(value == 0 for value in assert_runtime_counts(manager).values())


def test_delivery_counts_retain_active_ticket_after_dispatch_failure(
        runtime, monkeypatch):
    manager, dispatch = runtime
    entered, release = Event(), Event()
    with manager._condition:
        first = manager.submit(lambda record: 1)
        first.add_done_callback(
            lambda future: (entered.set(), release.wait(5)))
    assert first.result(5) == 1
    with manager._condition:
        assert first._record.delivery_ticket is not None
    reactor = Thread(target=dispatch.drain)
    reactor.start()
    try:
        assert entered.wait(5)
        assert assert_runtime_counts(manager)['delivery'] == 1

        def reject(ticket):
            raise RuntimeError('reactor lost with an active delivery')
        monkeypatch.setattr(manager, '_dispatch', reject)
        with manager._condition:
            second = manager.submit(lambda record: 2)
            second.add_done_callback(
                lambda future: pytest.fail('delivery ran'))
        assert second.result(5) == 2
        assert manager.closed.wait(5)
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: second._record.released, 5)
        counts = assert_runtime_counts(manager)
        assert counts['ordinary'] == counts['delivery'] == 1
        assert counts['mailbox'] == 0
        assert manager.high_waters['delivery'] == 2
        assert not manager.finalized.is_set()
    finally:
        release.set()
        reactor.join(5)
        assert not reactor.is_alive()
    manager.join(5)
    assert all(value == 0 for value in assert_runtime_counts(manager).values())


@pytest.mark.parametrize('entry', ['call', 'work'])
def test_inherited_compatibility_cleanup_rejects_before_parent_lock(entry):
    import os
    import signal
    from cassandra._management import compatibility_call, compatibility_work

    if not hasattr(os, 'fork'):
        pytest.skip('fork unavailable')
    manager = ManagementRuntime(Dispatcher())
    scope = (compatibility_call if entry == 'call' else compatibility_work)(
        manager)
    scope.__enter__()
    held, release = Event(), Event()
    def hold():
        with manager._condition:
            held.set()
            assert release.wait(5)
    holder = Thread(target=hold)
    holder.start()
    try:
        assert held.wait(5)
        pid = os.fork()
        if pid == 0:
            # Bound lock acquisition even if inherited-state rejection fails.
            signal.alarm(2)
            try:
                scope.__exit__(None, None, None)
            except ManagementClosed:
                os._exit(0)
            except BaseException:
                os._exit(2)
            os._exit(3)
        _, status = os.waitpid(pid, 0)
        assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
    finally:
        release.set()
        holder.join(5)
        assert not holder.is_alive()
        scope.__exit__(None, None, None)
        manager.close()
        manager.join(5)
    assert all(value == 0 for value in assert_runtime_counts(manager).values())


@pytest.mark.parametrize('charged', [False, True])
def test_delivered_callbacks_release_captured_objects_outside_locks(
        runtime, charged):
    import gc
    import weakref

    manager, dispatch = runtime
    calls, released, references = [], [], []
    class Payload:
        def __del__(self):
            released.append((future._condition._is_owned(),
                             manager._condition._is_owned()))

    def register(index):
        payload = Payload()
        references.append(weakref.ref(payload))
        future.add_done_callback(
            lambda future, payload=payload: calls.append(index))

    if charged:
        with manager._condition:
            future = manager.submit(lambda record: 7)
            for index in range(16):
                register(index)
        assert future.result(5) == 7
        assert all(reference() is not None for reference in references)
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: future._record.delivery_ticket is not None, 5)
        dispatch.drain()
    else:
        future = manager.submit(lambda record: 7)
        assert future.result(5) == 7
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: future._record.released, 5)
        for index in range(16):
            register(index)
    gc.collect()
    assert calls == list(range(16))
    assert all(reference() is None for reference in references)
    assert released == [(False, False)] * 16
    assert not future._done_callbacks
    assert future.result() == 7


def test_runtime_fixture_cleans_up_after_counter_invariant_failure(
        monkeypatch):
    from cassandra._management import _PENDING

    resources = runtime.__wrapped__(monkeypatch)
    manager, dispatch = next(resources)
    delivered = []
    try:
        future = manager.submit(lambda record: _PENDING)
        future.add_done_callback(lambda future: delivered.append(True))
        assert future._record.started.wait(5)
        with manager._condition:
            manager._mailbox_pending += 1
        with pytest.raises(AssertionError):
            next(resources)
        assert manager.closed.is_set() and manager.finalized.is_set()
        assert all(not thread.is_alive() for thread in manager._threads)
        assert not manager._records and not dispatch.queue
        assert delivered == [True]
    finally:
        # Also protect a failed setup/assertion in this regression itself.
        resources.close()


def test_runtime_fixture_reports_owner_counter_failure_without_stranding(
        monkeypatch):
    resources = runtime.__wrapped__(monkeypatch)
    manager, dispatch = next(resources)
    observe = manager._observe_counts_locked
    def inconsistent_counts():
        if execution_context() != 'cluster':
            return observe()
        manager._mailbox_pending += 1
        try:
            return observe()
        finally:
            manager._mailbox_pending -= 1
    monkeypatch.setattr(manager, '_observe_counts_locked', inconsistent_counts)
    delivered = []
    def handler(record):
        manager._observe_counts_locked()
        return 7
    try:
        with manager._condition:
            future = manager.submit(handler)
            future.add_done_callback(
                lambda done: delivered.append(done.result()))
        assert future.result(5) == 7
        dispatch.drain()
        with pytest.raises(AssertionError, match='mailbox count:'):
            next(resources)
        assert delivered == [7]
        assert not manager._records and not dispatch.queue
        assert all(not thread.is_alive() for thread in manager._threads)
    finally:
        resources.close()


def test_callback_rejects_wait_for_its_active_aggregate(runtime):
    manager, dispatch = runtime
    def callback():
        with manager.synchronous_reentry():
            with pytest.raises(ManagementContextError, match='own credit'):
                manager.submit(lambda record: None, budget='aggregate',
                               wait=True, timeout=5)
    manager.notify(callback).result(5)
    assert not manager._reentry_active


@pytest.mark.parametrize('pool', [None, 'pool'])
def test_superseded_handler_retains_admission_until_it_returns(pool):
    manager = ManagementRuntime(Dispatcher(), limit=1, pool_limit=1)
    entered, release = Event(), Event()
    def handler(record):
        entered.set()
        assert release.wait(5)
        return 7
    try:
        future = manager.submit(handler, pool=pool)
        assert entered.wait(5)
        manager.supersede(future)
        with pytest.raises(management.ManagementSuperseded):
            future.result(5)
        assert manager.counts()['ordinary'] == 1
        assert future._record.handler is handler
        with pytest.raises(ManagementOverloaded):
            manager.submit(lambda record: None, pool=pool)
        release.set()
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: not manager._records, 5)
        assert manager.submit(lambda record: 9, pool=pool).result(5) == 9
    finally:
        release.set()
        manager.close()
        manager.join(5)


def test_retired_handler_failure_is_logged(runtime, caplog):
    manager, dispatch = runtime
    entered, release = Event(), Event()
    def handler(record):
        entered.set()
        assert release.wait(5)
        raise ValueError('late handler failure')
    try:
        future = manager.submit(handler)
        assert entered.wait(5)
        manager.supersede(future)
        release.set()
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: not manager._records, 5)
        assert 'Error in retired management handler' in caplog.text
        assert 'late handler failure' in caplog.text
    finally:
        release.set()


def test_health_notification_failure_is_logged(runtime, caplog):
    manager, dispatch = runtime
    incarnation = object()
    def fail():
        raise ValueError('health notification failure')
    manager.register_health(incarnation)
    manager.observe_negative(incarnation, lambda: None, fail)
    manager.submit(lambda record: None).result(5)
    manager.notify(lambda: None).result(5)
    assert 'Error in management notification callback' in caplog.text
    assert 'health notification failure' in caplog.text


@pytest.mark.parametrize('still_alive', [False, True])
def test_join_uses_one_deadline_and_reports_live_runners(
        monkeypatch, still_alive):
    manager = ManagementRuntime(Dispatcher())
    manager.finalized.set()
    observed = []
    class Runner:
        def join(self, timeout):
            observed.append(timeout)

        def is_alive(self):
            return still_alive
    class Clock:
        times = iter((100, 102, 104))
        @classmethod
        def monotonic(cls):
            return next(cls.times)
    manager._threads = [Runner(), Runner()]
    monkeypatch.setattr(management, 'time', Clock)
    if still_alive:
        with pytest.raises(TimeoutError):
            manager.join(5)
        assert observed == [3]
    else:
        manager.join(5)
        assert observed == [3, 1]


def test_retired_foreign_delivery_does_not_retain_later_results(runtime):
    import gc
    import weakref

    manager, dispatch = runtime
    class Payload:
        pass
    first = manager.submit(lambda record: Payload())
    first.result(5)
    previous, references = first, []
    for _ in range(16):
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: previous._record.released, 5)
        following = manager.submit(lambda record, previous=previous: (
            previous.add_done_callback(lambda future: None), Payload())[1])
        following.result(5)
        dispatch.drain()
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: following._record.released, 5)
        references.append(weakref.ref(following.result()))
        previous = following
    del previous, following
    gc.collect()
    assert all(reference() is None for reference in references)
    assert first._delivery.record is first._record


def test_caller_drain_defers_to_owner_adopted_before_claim(
        runtime, monkeypatch):
    manager, dispatch = runtime
    future = manager.submit(lambda record: 7)
    assert future.result(5) == 7
    with manager._condition:
        assert manager._condition.wait_for(lambda: future._record.released, 5)
    before_claim, resume = Event(), Event()
    calls = []
    drain = future._delivery.drain
    def paused(record=None):
        if record is None:
            before_claim.set()
            assert resume.wait(5)
        return drain(record)
    monkeypatch.setattr(future._delivery, 'drain', paused)
    caller = Thread(target=lambda: future.add_done_callback(
        lambda future: calls.append(('caller', execution_context()))))
    caller.start()
    try:
        assert before_claim.wait(5)
        owner = manager.submit(lambda record: future.add_done_callback(
            lambda future: calls.append(('owner', execution_context()))))
        owner.result(5)
        resume.set()
        caller.join(5)
        assert not caller.is_alive()
        assert not calls
        assert assert_runtime_counts(manager)['ordinary'] == 1
        dispatch.drain()
        assert calls == [('caller', 'bridge'), ('owner', 'bridge')]
        wait_for_retirement(manager, owner)
        assert owner._record.released
        assert assert_runtime_counts(manager)['ordinary'] == 0
    finally:
        resume.set()
        caller.join(5)
        assert not caller.is_alive()


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Requires synchronous reference destruction')
def test_failed_delivery_releases_captures_on_callback_lane_outside_locks():
    import gc
    import weakref

    dispatch = Dispatcher()
    manager = ManagementRuntime(dispatch)
    observed, probes = [], []
    released, entered, cleanup_release = Event(), Event(), Event()
    class Payload:
        def __del__(self):
            observed.append((execution_context(),
                             future._condition._is_owned(),
                             manager._condition._is_owned()))
            probe = Thread(target=lambda: (manager.counts(), released.set()))
            probes.append(probe)
            probe.start()
            probe.join(2)
            observed.append(released.is_set())
            entered.set()
            observed.append(cleanup_release.wait(5))

    def reject(ticket):
        raise RuntimeError('reactor failed')
    manager._dispatch = reject
    try:
        with manager._condition:
            future = manager.submit(lambda record: 7)
            payload = Payload()
            reference = weakref.ref(payload)
            future.add_done_callback(lambda future, payload=payload: None)
            del payload
        assert future.result(5) == 7
        assert entered.wait(5)
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: future._record.handler is None, 5)
        manager.close()
        assert not manager.finalized.is_set()
        assert assert_runtime_counts(manager)['close'] == 0
        assert manager._capture_obligations == 2  # Callback and handler.
        assert manager.high_waters['close'] == 0
        cleanup_release.set()
        manager.join(5)
        gc.collect()
        assert reference() is None
        assert observed == [('callback', False, False), True, True]
        assert released.is_set()
        assert not manager._callback_garbage
        assert all(value == 0
                   for value in assert_runtime_counts(manager).values())
    finally:
        cleanup_release.set()
        manager.close()
        dispatch.drain()
        manager.join(5)
        for probe in probes:
            probe.join(5)
            assert not probe.is_alive()


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Requires synchronous reference destruction')
@pytest.mark.parametrize('stage', ['normal', 'queued', 'unadmitted'])
def test_notification_capture_retires_on_callback_lane_outside_lock(stage):
    manager = ManagementRuntime(Dispatcher())
    entered, resume, retired = Event(), Event(), Event()
    observed, calls, probes = [], [], []
    class Callback:
        def __call__(self):
            calls.append(True)
            entered.set()
            assert resume.wait(5)
            return 7

        def __del__(self):
            observed.append((execution_context(),
                             manager._condition._is_owned()))
            if stage == 'normal':
                try:
                    observed.append(manager.submit(lambda record: 9).result(2))
                except BaseException as exc:
                    observed.append(type(exc))
            else:
                progressed = Event()
                probe = Thread(target=lambda: (manager.counts(),
                                               progressed.set()))
                probes.append(probe)
                probe.start()
                probe.join(2)
                observed.append(progressed.is_set())
            retired.set()

    def block(*args):
        entered.set()
        assert resume.wait(5)
    try:
        if stage == 'queued':
            manager.notify(block)
            assert entered.wait(5)
        elif stage == 'unadmitted':
            manager.submit(block)
            assert entered.wait(5)
        future = manager.notify(Callback())
        if stage == 'normal':
            assert entered.wait(5)
        if stage != 'unadmitted':
            # Fence admission before releasing L or pruning its queue.
            assert manager.submit(lambda record: 1).result(5) == 1
        if stage != 'normal':
            manager.close()
        resume.set()
        if stage == 'normal':
            assert future.result(5) == 7
        else:
            with pytest.raises(ManagementClosed):
                future.result(5)
        assert retired.wait(5)
        assert calls == ([True] if stage == 'normal' else [])
        assert observed == [('callback', False),
                            9 if stage == 'normal' else True]
    finally:
        resume.set()
        manager.close()
        manager.join(5)
        for probe in probes:
            probe.join(5)
            assert not probe.is_alive()
    assert all(value == 0 for value in assert_runtime_counts(manager).values())


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Requires synchronous reference destruction')
@pytest.mark.parametrize('mode', [
    'result', 'exception', 'wait', 'as_completed', 'admission'])
def test_open_notification_retirement_guards_same_lane_waits(mode):
    manager = ManagementRuntime(Dispatcher())
    entered, resume, retired = Event(), Event(), Event()
    observed, nested = [], []
    class Callback:
        def __call__(self):
            pytest.fail('superseded notification ran')

        def __del__(self):
            try:
                # One slot retains this capture; one admits its child.
                manager.limit = 2
                nested.append(manager.notify(lambda: 7))
                try:
                    if mode == 'result':
                        nested[0].result(2)
                    elif mode == 'exception':
                        nested[0].exception(2)
                    elif mode == 'wait':
                        wait(nested, timeout=2)
                    elif mode == 'as_completed':
                        next(as_completed(nested, timeout=2))
                    else:
                        manager.submit(lambda record: 1, wait=True, timeout=2)
                except ManagementContextError:
                    observed.append(True)
                except BaseException as exc:
                    observed.append(type(exc))
                else:
                    observed.append(False)
                try:
                    with manager.synchronous_reentry() as budget:
                        observed.append((budget, manager.submit(
                            lambda record: 9, budget=budget).result(2)))
                except BaseException as exc:
                    observed.append(type(exc))
            finally:
                retired.set()

    def block(record):
        entered.set()
        assert resume.wait(5)
    try:
        manager.submit(block)
        assert entered.wait(5)
        future = manager.notify(Callback())
        manager.supersede(future)
        resume.set()
        assert retired.wait(5)
        assert observed == [True, ('reentry', 9)]
        assert nested[0].result(5) == 7
    finally:
        resume.set()
        manager.close()
        manager.join(5)
    assert all(value == 0 for value in assert_runtime_counts(manager).values())


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Requires synchronous reference destruction')
def test_consumed_health_handler_captures_retire_on_cleanup_lane():
    manager = ManagementRuntime(Dispatcher())
    applied, retired = Event(), Event()
    observed = []
    class Apply:
        def __call__(self):
            applied.set()

        def __del__(self):
            observed.append((execution_context(),
                             manager._condition._is_owned()))
            retired.set()

    try:
        incarnation = object()
        manager.register_health(incarnation)
        assert manager.observe_negative(incarnation, Apply())
        assert applied.wait(5)
        assert retired.wait(5)
        assert observed == [('cleanup', False)]
    finally:
        manager.close()
        manager.join(5)


def test_private_capture_retirement_is_bounded_while_callback_is_occupied():
    import gc
    import weakref

    manager = ManagementRuntime(Dispatcher(), reentry_window=1)
    finished, release = Event(), Event()
    references, observations = [], []
    class Payload:
        pass

    def listener():
        with manager.synchronous_reentry() as budget:
            for _ in range(100):
                payload = Payload()
                references.append(weakref.ref(payload))
                future = manager.submit(
                    lambda record, owned=payload: 7, budget=budget, wait=True)
                del payload
                assert future.result(5) == 7
                del future
            gc.collect()
            observations.append(sum(ref() is not None for ref in references))
            with manager._condition:
                observations.append(len(manager._records))
        finished.set()
        assert release.wait(5)

    try:
        future = manager.notify(listener)
        assert finished.wait(5), 'native calls depended on callback retirement'
        assert observations[0] <= 1
        assert observations[1] <= 2  # The listener and its last native record.
    finally:
        release.set()
        manager.close()
        manager.join(5)
    gc.collect()
    assert all(ref() is None for ref in references)


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Requires synchronous reference destruction')
@pytest.mark.parametrize('mode', [
    'result', 'exception', 'wait', 'as_completed', 'admission', 'reentry',
    'join'])
def test_private_cleanup_keeps_credit_and_rejects_self_dependent_waits(mode):
    manager = ManagementRuntime(Dispatcher(), reentry_window=1)
    listener_entered, listener_release = Event(), Event()
    cleanup_entered, cleanup_release = Event(), Event()
    observations, pending = [], []

    class Payload:
        def __del__(self):
            observations.append((execution_context(),
                                 manager._condition._is_owned()))
            child = manager.notify(lambda: 9)
            pending.append(child)
            try:
                if mode == 'result':
                    child.result(2)
                elif mode == 'exception':
                    child.exception(2)
                elif mode == 'wait':
                    wait([child], timeout=2)
                elif mode == 'as_completed':
                    next(as_completed([child], timeout=2))
                elif mode == 'admission':
                    manager.submit(lambda record: 1, wait=True, timeout=2)
                elif mode == 'reentry':
                    with manager.synchronous_reentry():
                        observations.append('unexpected reentry')
                else:
                    manager.join(2)
            except ManagementContextError:
                observations.append('rejected')
            except BaseException as exc:
                observations.append(type(exc))
            else:
                observations.append('unexpected wait')
            observations.append(manager.counts()['reentry'])
            cleanup_entered.set()
            cleanup_release.wait(5)

    def listener():
        listener_entered.set()
        assert listener_release.wait(5)

    try:
        manager.notify(listener)
        assert listener_entered.wait(5)
        payload = Payload()
        future = manager.submit(lambda record, owned=payload: 7,
                                budget='reentry')
        del payload
        assert future.result(5) == 7
        assert cleanup_entered.wait(5)
        assert observations == [('cleanup', False), 'rejected', 1]
        assert manager.counts()['reentry'] == 1
        with pytest.raises(ManagementOverloaded):
            manager.submit(lambda record: 1, budget='reentry')
        # C and the physical executor are not used for disposal.
        assert manager.submit(lambda record: 11).result(5) == 11
        manager.close()
        assert not manager.finalized.is_set()
    finally:
        cleanup_release.set()
        listener_release.set()
        manager.close()
        manager.join(5)
    assert future._record.released
    assert manager.counts()['reentry'] == 0
    assert not manager._handler_garbage


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Requires synchronous reference destruction')
def test_health_cleanup_coalesces_and_survives_unregister_reregister():
    import gc
    import weakref

    manager = ManagementRuntime(Dispatcher())
    listener_entered, listener_release = Event(), Event()
    cleanup_entered, cleanup_release = Event(), Event()
    latest_applied = Event()
    incarnation, references, applied = object(), [], []

    class Apply:
        def __init__(self, value):
            self.value = value

        def __call__(self):
            applied.append(self.value)
            if self.value == 101:
                latest_applied.set()

        def __del__(self):
            if self.value == 0:
                cleanup_entered.set()
                cleanup_release.wait(5)

    def observe(value):
        callback = Apply(value)
        references.append(weakref.ref(callback))
        assert manager.observe_negative(incarnation, callback)

    def listener():
        listener_entered.set()
        assert listener_release.wait(5)

    try:
        manager.notify(listener)
        assert listener_entered.wait(5)
        manager.register_health(incarnation)
        observe(0)
        assert cleanup_entered.wait(5)
        slot = manager._health[id(incarnation)]
        for value in range(1, 101):
            observe(value)
        gc.collect()
        assert sum(ref() is not None for ref in references) == 2
        with manager._condition:
            assert sum(r.health_slot is slot for r in manager._records) == 1
        manager.unregister_health(incarnation)
        assert manager._health[id(incarnation)] is slot
        manager.register_health(incarnation)
        observe(101)
        assert manager._health[id(incarnation)] is slot
        assert not latest_applied.is_set()
        cleanup_release.set()
        assert latest_applied.wait(5)
        with manager._condition:
            assert manager._condition.wait_for(lambda: not slot.active, 5)
        assert manager._health[id(incarnation)] is slot
        assert applied == [0, 101]
        manager.unregister_health(incarnation)
        assert id(incarnation) not in manager._health
    finally:
        cleanup_release.set()
        listener_release.set()
        manager.close()
        manager.join(5)
    gc.collect()
    assert all(ref() is None for ref in references)


def test_private_cleanup_progresses_with_all_physical_workers_waiting():
    manager = ManagementRuntime(Dispatcher(), reentry_window=1)
    entered, release, stop = Event(), Event(), Event()
    callers_ready = Barrier(3)
    executor, callers = ThreadPoolExecutor(2), []

    def listener():
        entered.set()
        assert release.wait(5)

    def synchronous_caller():
        callers_ready.wait(5)
        for _ in range(50):
            if stop.is_set():
                return
            assert manager.submit(lambda record: 11, budget='reentry',
                                  wait=True, timeout=5).result(5) == 11

    try:
        manager.notify(listener)
        assert entered.wait(5)
        callers = [executor.submit(synchronous_caller) for _ in range(2)]
        callers_ready.wait(5)
        for caller in callers:
            caller.result(5)
    finally:
        stop.set()
        release.set()
        try:
            manager.close()
            manager.join(5)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
            assert not wait(callers, timeout=5).not_done


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Requires synchronous reference destruction')
def test_health_incarnation_churn_retains_bounded_cleanup_slots():
    manager = ManagementRuntime(Dispatcher(), limit=2)
    entered, release, second_applied = Event(), Event(), Event()
    applied = []

    class Apply:
        def __init__(self, number):
            self.number = number

        def __call__(self):
            applied.append(self.number)
            if self.number == 1:
                second_applied.set()

        def __del__(self):
            if self.number == 0:
                entered.set()
                release.wait(5)

    try:
        incarnation = object()
        manager.register_health(incarnation)
        manager.observe_negative(incarnation, Apply(0))
        assert entered.wait(5)
        manager.unregister_health(incarnation)
        incarnation = object()
        manager.register_health(incarnation)
        manager.observe_negative(incarnation, Apply(1))
        assert second_applied.wait(5)
        manager.unregister_health(incarnation)
        for number in range(2, 102):
            incarnation = object()
            manager.register_health(incarnation)
            manager.observe_negative(incarnation, Apply(number))
            manager.unregister_health(incarnation)
        assert applied == [0, 1]
        with manager._condition:
            assert manager._health_active == manager.limit
            assert len(manager._health) == manager.limit
            assert len(manager._records) == manager.limit
        manager.close()
        assert not manager.finalized.is_set()
        release.set()
        manager.join(5)
        assert not manager._health
        assert manager._health_active == 0
    finally:
        release.set()
        manager.close()
        manager.join(5)


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Requires synchronous reference destruction')
@pytest.mark.parametrize('same_incarnation', [True, False])
def test_deferred_health_apply_precedes_its_notification(same_incarnation):
    manager = ManagementRuntime(Dispatcher(), limit=1)
    entered, release, applied, notified = Event(), Event(), Event(), Event()
    observations = []

    class First:
        def __call__(self):
            pass

        def __del__(self):
            entered.set()
            release.wait(5)

    def notification():
        observations.append(applied.is_set())
        notified.set()

    try:
        first = object()
        manager.register_health(first)
        manager.observe_negative(first, First())
        assert entered.wait(5)
        target = first if same_incarnation else object()
        manager.register_health(target)
        manager.observe_negative(target, applied.set, notification)
        # A native mailbox fence lets C examine the deferred health demand.
        # With capacity held, it must skip both apply and paired notification.
        fence = manager.submit(lambda record: None)
        fence.result(5)
        wait_for_retirement(manager, fence)
        with manager._condition:
            assert manager._health[id(target)].notification is notification
        assert not notified.is_set()
        release.set()
        assert notified.wait(5)
        assert observations == [True]
    finally:
        release.set()
        manager.close()
        manager.join(5)


def test_consumed_health_captures_do_not_accumulate_across_incarnations():
    import gc
    import weakref

    manager = ManagementRuntime(Dispatcher(), limit=2)
    entered, release = Event(), Event()
    references = []

    class Payload:
        pass

    def listener():
        entered.set()
        assert release.wait(5)

    try:
        manager.notify(listener)
        assert entered.wait(5)
        for _ in range(32):
            incarnation, payload, applied = object(), Payload(), Event()
            references.append(weakref.ref(payload))
            manager.register_health(incarnation)
            manager.observe_negative(
                incarnation, lambda owned=payload, event=applied: event.set())
            del payload
            assert applied.wait(5)
            manager.unregister_health(incarnation)
        gc.collect()
        assert sum(ref() is not None for ref in references) <= manager.limit
        with manager._condition:
            assert len(manager._records) <= manager.limit + 1
    finally:
        release.set()
        manager.close()
        manager.join(5)
    gc.collect()
    assert all(ref() is None for ref in references)


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Requires synchronous reference destruction')
def test_private_cleanup_weakref_callback_allows_terminal_reads_and_close():
    import weakref

    manager = ManagementRuntime(Dispatcher(), limit=2, reentry_window=1)
    entered, release, run, retired = Event(), Event(), Event(), Event()
    observations, children = [], []

    class Payload:
        pass

    def listener():
        entered.set()
        assert release.wait(5)

    def retirement(reference):
        try:
            observations.append((execution_context(),
                                 manager._condition._is_owned(),
                                 future._condition._is_owned()))
            observations.append(future.result(0))
            children.append(manager.submit(lambda record: 9))
            manager.close()
            observations.append(manager.finalized.is_set())
        except BaseException as exc:
            observations.append(type(exc))
        finally:
            retired.set()

    try:
        manager.notify(listener)
        assert entered.wait(5)
        payload = Payload()
        reference = weakref.ref(payload, retirement)
        future = manager.submit(lambda record, owned=payload: run.wait(5),
                                budget='reentry')
        del payload
        run.set()
        assert future.result(5) is True
        assert retired.wait(5)
        assert reference() is None
        assert observations == [('cleanup', False, False), True, False]
    finally:
        run.set()
        release.set()
        manager.close()
        manager.join(5)


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Requires synchronous reference destruction')
@pytest.mark.parametrize('action', ['replace', 'unregister', 'close'])
def test_health_capture_retirement_releases_outside_runtime_lock(action):
    manager = ManagementRuntime(Dispatcher())
    entered, resume, retired = Event(), Event(), Event()
    cleanup_release = Event()
    incarnation = object()
    observed, probes = [], []
    class Callback:
        def __call__(self):
            pytest.fail('retired health notification ran')

        def __del__(self):
            observed.append((execution_context(),
                             manager._condition._is_owned()))
            progressed = Event()
            probe = Thread(target=lambda: (manager.counts(), progressed.set()))
            probes.append(probe)
            probe.start()
            probe.join(2)
            observed.append(progressed.is_set())
            retired.set()
            if action == 'close':
                observed.append(cleanup_release.wait(5))

    def block(record):
        entered.set()
        assert resume.wait(5)
    try:
        manager.register_health(incarnation)
        manager.submit(block)
        assert entered.wait(5)
        assert manager.observe_negative(incarnation, lambda: None, Callback())
        if action == 'replace':
            assert manager.observe_negative(incarnation, lambda: None)
        elif action == 'unregister':
            manager.unregister_health(incarnation)
        else:
            manager.close()
        assert retired.wait(5)
        if action == 'close':
            assert observed == [('callback', False), True]
            assert manager._capture_obligations == 1
            assert manager.counts()['close'] == 0
            assert manager.high_waters['close'] == 0
            assert not manager.finalized.is_set()
        else:
            assert observed == [(None, False), True]
    finally:
        cleanup_release.set()
        resume.set()
        manager.close()
        manager.join(5)
        for probe in probes:
            probe.join(5)
            assert not probe.is_alive()
    if action == 'close':
        assert observed == [('callback', False), True, True]
        assert manager._capture_obligations == 0
    assert all(value == 0 for value in assert_runtime_counts(manager).values())


@pytest.mark.parametrize('failed_phase', [1, 2, 3, 4, 5])
def test_interrupt_after_thread_start_return_keeps_runner_owned(
        monkeypatch, failed_phase):
    import sys
    manager = ManagementRuntime(Dispatcher())
    real_start = Thread.start
    starts = []
    entered, release = Event(), Event()
    run = manager._run
    def held_run(kind, target):
        if len(starts) == failed_phase and current_thread() is starts[-1]:
            entered.set()
            release.wait(5)
        run(kind, target)
    manager._run = held_run
    old_trace = sys.gettrace()
    def interrupt(frame, event, arg):
        if (frame.f_code is ManagementRuntime.start.__code__ and
                event == 'line'):
            sys.settrace(old_trace)
            frame.f_trace = old_trace
            raise KeyboardInterrupt('after native start returned')
        return interrupt
    def start(thread):
        starts.append(thread)
        real_start(thread)
        if len(starts) == failed_phase:
            assert entered.wait(5)
            frame = sys._getframe(1)
            frame.f_trace = interrupt
            sys.settrace(interrupt)
    try:
        with monkeypatch.context() as patcher:
            patcher.setattr(Thread, 'start', start)
            with pytest.raises(KeyboardInterrupt,
                               match='after native start returned'):
                manager.start()
        assert not manager.finalized.is_set()
        with pytest.raises(TimeoutError):
            manager.join(0.01)
        release.set()
        manager.join(5)
        assert all(not thread.is_alive() for thread in starts)
    finally:
        sys.settrace(old_trace)
        release.set()
        manager.close()
        manager.join(5)
        for thread in starts:
            thread.join(5)


@pytest.mark.parametrize('newer', [False, True])
def test_failed_health_apply_logs_and_suppresses_its_notification(
        runtime, caplog, newer):
    manager, dispatch = runtime
    incarnation = object()
    entered, release = Event(), Event()
    applied, notifications = [], []
    def fail():
        entered.set()
        assert release.wait(5)
        raise ValueError('health state was not applied')
    manager.register_health(incarnation)
    try:
        manager.observe_negative(incarnation, fail,
                                 lambda: notifications.append('failed'))
        assert entered.wait(5)
        if newer:
            manager.observe_negative(
                incarnation, lambda: applied.append('newer'),
                lambda: notifications.append(tuple(applied)))
        release.set()
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: not manager._health_active and
                manager._health[id(incarnation)].apply is None, 5)
        manager.submit(lambda record: None).result(5)
        manager.notify(lambda: None).result(5)
        assert notifications == ([('newer',)] if newer else [])
        assert 'Error applying management health update' in caplog.text
        assert 'health state was not applied' in caplog.text
    finally:
        release.set()


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Requires synchronous reference destruction')
@pytest.mark.parametrize('pool', [None, 'physical-pool'])
@pytest.mark.parametrize('budget', ['ordinary', 'reentry'])
@pytest.mark.parametrize('order', ['worker-first', 'mailbox-first'])
@pytest.mark.parametrize('ending', ['accept', 'discard', 'error', 'reject',
                                    'close', 'closed'])
def test_physical_captures_retire_on_charged_safe_lane(
        monkeypatch, pool, budget, order, ending):
    import weakref
    from cassandra._management import ManagementSuperseded

    manager = ManagementRuntime(Dispatcher(), limit=8, reentry_window=1)
    executor = ThreadPoolExecutor(1)
    work_entered, work_release = Event(), Event()
    blocker_entered, blocker_release = Event(), Event()
    owner_returned, owner_release = Event(), Event()
    worker_returned, worker_release = Event(), Event()
    capture_entered, capture_release = Event(), Event()
    listener_entered, listener_release = Event(), Event()
    captures_done = Event()
    records, references, observed, disposed = [], [], [], []
    weak_observed = []
    expected_count = 3 if ending in ('error', 'reject') else 4
    execute = manager._execute

    class Capture:
        def __init__(self, kind):
            self.kind = kind
            def retired(reference, name=kind):
                weak_observed.append((name, execution_context(),
                                      current_thread() in manager._threads,
                                      manager._condition._is_owned()))
            references.append(weakref.ref(self, retired))

        def __del__(self):
            observed.append((self.kind, execution_context(),
                             manager._condition._is_owned(),
                             not records[0].released,
                             current_thread() in manager._threads))
            if self.kind == 'accept':
                capture_entered.set()
                capture_release.wait(5)
            if len(observed) == expected_count:
                captures_done.set()

    class ControlledExecutor:
        def submit(self, function):
            if ending == 'reject':
                raise RuntimeError('executor rejected before launch')
            physical, start = Future(), Event()
            register = physical.add_done_callback

            def add_callback(callback):
                def completed(future):
                    callback(future)
                    worker_returned.set()
                    if order == 'mailbox-first':
                        worker_release.wait(5)
                register(completed)
                del physical.add_done_callback
                start.set()

            physical.add_done_callback = add_callback

            def run():
                assert start.wait(5)
                work_entered.set()
                assert work_release.wait(5)
                if ending == 'error':
                    # Infrastructure may fail before invoking the callable.
                    physical.set_exception(ValueError('executor failed'))
                else:
                    physical.set_result(function())
            executor.submit(run)
            return physical

    def hold_owner(record, callback, kind):
        execute(record, callback, kind)
        with manager._condition:
            returned = (records and record is records[0] and
                        record.handler_done and not record.mailbox_pending)
        if returned:
            owner_returned.set()
            if order == 'worker-first':
                owner_release.wait(5)

    def handler(record):
        records.append(record)
        # These defaults are created here; the retained handler has no
        # reference to them after its first native frame returns.
        def function(owned=Capture('function')):
            return Capture('value')
        def accept(value, owned=Capture('accept')):
            return 17
        def dispose(value, owned=Capture('dispose')):
            disposed.append((value.kind, execution_context()))
        outcome = manager.work(record, ControlledExecutor(), function,
                               accept=accept, dispose=dispose)
        if ending == 'reject':
            # Discard the caller-owned traceback while the native frame and
            # attempt still retain every callback. Its frame-back aliases
            # would otherwise outlive the runtime's ownership under test.
            record.futures[0].exception().__traceback__ = None
        return outcome

    monkeypatch.setattr(manager, '_execute', hold_owner)
    try:
        if budget == 'reentry':
            manager.notify(lambda: (listener_entered.set(),
                                    listener_release.wait(5)))
            assert listener_entered.wait(5)
        future = manager.submit(handler, pool=pool, budget=budget)
        if ending != 'reject':
            assert work_entered.wait(5)
            manager.submit(lambda record: None, pool=pool).result(5)
            if ending == 'closed':
                # No result mailbox can be accepted after native shutdown.
                manager.close()
                work_release.set()
            else:
                manager.submit(lambda record: (
                    blocker_entered.set(), blocker_release.wait(5)), pool=pool)
                assert blocker_entered.wait(5)
                work_release.set()
                with manager._condition:
                    assert manager._condition.wait_for(
                        lambda: future._record.mailbox_pending == 1, 5)
                if ending == 'discard':
                    manager.supersede(future)
                elif ending == 'close':
                    manager.close()
                blocker_release.set()
        if ending != 'closed':
            assert owner_returned.wait(5)
        if ending != 'reject':
            assert worker_returned.wait(5)
        if ending == 'accept':
            assert future.result(5) == 17
        else:
            error = future.exception(5)
            expected = {'discard': ManagementSuperseded,
                        'close': ManagementClosed,
                        'closed': ManagementClosed,
                        'error': ValueError, 'reject': RuntimeError}[ending]
            assert isinstance(error, expected)
            # The caller owns the published exception. Remove its traceback
            # alias so this test isolates the runtime's callback ownership.
            error.__traceback__ = None
        if order == 'worker-first':
            executor.submit(lambda: None).result(5)
            owner_release.set()
        else:
            if ending in ('close', 'closed'):
                for thread in manager._threads:
                    if thread.name in ('cassandra-management-cluster',
                                       'cassandra-management-pool'):
                        thread.join(5)
                        assert not thread.is_alive()
            else:
                manager.submit(lambda record: None, pool=pool).result(5)
            worker_release.set()
            executor.submit(lambda: None).result(5)
        assert capture_entered.wait(5)
        assert not future._record.released
        assert manager.counts()[budget] >= 1
        capture_release.set()
        assert captures_done.wait(5)
        wait_for_retirement(manager, future)
        expected_context = 'callback' if budget == 'ordinary' else 'cleanup'
        expected_names = ['accept', 'dispose', 'function']
        if expected_count == 4:
            expected_names.append('value')
        assert sorted(kind for kind, *_ in observed) == expected_names
        assert all(not locked and charged and
                   (context == expected_context or
                    (kind == 'value' and not managed))
                   for kind, context, locked, charged, managed in observed)
        assert sorted(kind for kind, *_ in weak_observed) == expected_names
        assert all(not locked and
                   (context == expected_context or
                    (kind == 'value' and not managed))
                   for kind, context, managed, locked in weak_observed)
        assert all(reference() is None for reference in references)
        assert disposed == ([('value', 'worker')]
                            if ending in ('discard', 'close', 'closed')
                            else [])
    finally:
        for event in (work_release, blocker_release, owner_release,
                      worker_release, capture_release, listener_release):
            event.set()
        manager.close()
        executor.shutdown(wait=False, cancel_futures=True)
        manager.join(5)
        for thread in executor._threads:
            thread.join(5)
            assert not thread.is_alive()


def test_completed_physical_attempt_keeps_its_unused_reservation(runtime):
    manager, dispatch = runtime
    physical = Future()
    physical.set_exception(ValueError('failed before execution'))
    class Executor:
        def submit(self, function):
            return physical
    def handler(record):
        manager.work(record, Executor(), lambda: None)
        assert record.physical == 0
        with pytest.raises(ManagementContextError):
            manager.work(record, Executor(), lambda: None)
        return management._PENDING
    future = manager.submit(handler)
    with pytest.raises(ValueError, match='failed before execution'):
        future.result(5)
    wait_for_retirement(manager, future)


@pytest.mark.parametrize('budget', ['ordinary', 'reentry', 'aggregate'])
@pytest.mark.parametrize('bad', ['identity', 'key', 'interrupted-identity'])
def test_rejected_metadata_keeps_admission(budget, bad):
    manager = management.ManagementRuntime(
        lambda callback: callback(), limit=1, reentry_window=1
    )

    class Interrupted:
        def __iter__(self):
            raise KeyboardInterrupt('metadata conversion')

    options = (
        {'identity': None}
        if bad == 'identity'
        else {'key': []}
        if bad == 'key'
        else {'identity': Interrupted()}
    )
    try:
        with pytest.raises((TypeError, KeyboardInterrupt)):
            manager.submit(lambda record: None, budget=budget, **options)
        assert not manager._records
        assert manager._credits == manager._reentry_credits == 0
        assert not manager._reentry_active
        assert manager.submit(lambda record: 17, budget=budget).result(5) == 17
    finally:
        manager.close()
        manager.join(5)


def test_interrupted_final_release_restores_scope(monkeypatch):
    manager = management.ManagementRuntime(
        lambda callback: callback(), limit=1
    )
    entered, release = (Event(), Event())
    original = manager._handle

    def hold(record):
        result = original(record)
        entered.set()
        assert release.wait(5)
        return result

    monkeypatch.setattr(manager, '_handle', hold)
    previous = getattr(management._context, 'call', None)
    retired = None
    stale = False
    leaked = False
    original_wait = manager._condition.wait_for

    def interrupt(predicate, timeout=None):
        raise KeyboardInterrupt('phase fence')

    try:
        try:
            with management.compatibility_call(manager):
                retired = management._context.call
                assert entered.wait(5)
                monkeypatch.setattr(manager._condition, 'wait_for', interrupt)
        except KeyboardInterrupt:
            stale = getattr(management._context, 'call', None) is not previous
        finally:
            monkeypatch.setattr(manager._condition, 'wait_for', original_wait)
            release.set()
        with manager._condition:
            assert manager._condition.wait_for(lambda: not manager._records, 5)
        monkeypatch.setattr(manager, '_handle', original)

        @management.policy_call
        def outside():
            return 19

        assert outside() == 19
        leaked = retired.record is not None
    finally:
        if retired is not None:
            retired.release()
        management._context.call = previous
        manager.close()
        manager.join(5)
    assert not stale
    assert not leaked


@pytest.mark.parametrize('budget', ['ordinary', 'reentry', 'aggregate'])
def test_key_insertion_failure_retires_its_charged_capture(budget):
    manager = management.ManagementRuntime(
        lambda callback: callback(), limit=1, reentry_window=1
    )

    class Key:
        calls = 0

        def __hash__(self):
            self.calls += 1
            if self.calls == 2:
                raise ValueError('key insertion')
            return 17

    try:
        with pytest.raises(ValueError, match='key insertion'):
            manager.submit(lambda record: None, budget=budget, key=Key())
        with manager._condition:
            assert manager._condition.wait_for(lambda: not manager._records, 5)
        assert manager._credits == manager._reentry_credits == 0
        assert not manager._reentry_active
        assert manager.submit(lambda record: 21, budget=budget).result(5) == 21
    finally:
        manager.close()
        manager.join(5)


@pytest.mark.parametrize('coalesced', [False, True])
def test_future_preparation_failure_keeps_admission(monkeypatch, coalesced):
    manager = management.ManagementRuntime(
        lambda callback: callback(), limit=2
    )
    entered, release = (Event(), Event())
    first = None
    try:
        if coalesced:
            first = manager.submit(
                lambda record: (entered.set(), release.wait(5)), key='key'
            )
            assert entered.wait(5)
        with monkeypatch.context() as patch:

            def fail(record):
                raise KeyboardInterrupt('future allocation')

            patch.setattr(management, 'OperationFuture', fail)
            with pytest.raises(KeyboardInterrupt, match='future allocation'):
                manager.submit(lambda record: None, key='key')
        assert manager._credits == int(coalesced)
        assert len(manager._records) == int(coalesced)
        release.set()
        if first is not None:
            first.result(5)
        assert manager.submit(lambda record: 22).result(5) == 22
    finally:
        release.set()
        manager.close()
        manager.join(5)


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Requires synchronous reference destruction')
@pytest.mark.parametrize('budget', ['ordinary', 'reentry', 'aggregate'])
def test_key_insertion_rejection_keeps_credit_through_safe_disposal(budget):
    manager = management.ManagementRuntime(
        lambda callback: callback(), limit=2, reentry_window=1
    )
    listening, release_listener, disposing, release_capture = (
        Event(),
        Event(),
        Event(),
        Event(),
    )
    seen = []

    class Key:
        calls = 0

        def __hash__(self):
            self.calls += 1
            if self.calls == 2:
                raise ValueError('key insertion')
            return 19

    class Handler:
        def __call__(self, record):
            pytest.fail('rejected handler executed')

        def __del__(self):
            seen.append(
                (
                    management.execution_context(),
                    manager._condition._is_owned(),
                    manager._credits,
                    manager._reentry_credits,
                    manager._reentry_active,
                )
            )
            disposing.set()
            assert release_capture.wait(5)

    try:
        manager.notify(lambda: (listening.set(), release_listener.wait(5)))
        assert listening.wait(5)
        try:
            manager.submit(Handler(), key=Key(), budget=budget)
        except ValueError as error:
            assert str(error) == 'key insertion'
            error.__traceback__ = None
        else:
            pytest.fail('key insertion did not reject')
        if budget == 'ordinary':
            assert manager._credits == 2
            assert not disposing.is_set()
        else:
            assert disposing.wait(5)
        release_listener.set()
        assert disposing.wait(5)
        assert seen[0][0] == (
            'callback' if budget == 'ordinary' else 'cleanup'
        )
        assert seen[0][1] is False
        assert (
            seen[0][2] > 0
            if budget == 'ordinary'
            else seen[0][3] == 1
            if budget == 'reentry'
            else seen[0][4]
        )
        release_capture.set()
        with manager._condition:
            assert manager._condition.wait_for(lambda: not manager._records, 5)
    finally:
        release_capture.set()
        release_listener.set()
        manager.close()
        manager.join(5)


@pytest.mark.parametrize('budget', ['ordinary', 'reentry', 'aggregate'])
def test_unhashable_pool_rejection_does_not_charge_admission(runtime, budget):
    manager, dispatch = runtime
    with pytest.raises(TypeError):
        manager.submit(lambda record: None, pool=[], budget=budget)
    assert not manager._records
    assert manager.counts()['mailbox'] == 0
    assert manager._credits == manager._reentry_credits == 0
    assert not manager._reentry_active
    assert manager.submit(lambda record: 23, budget=budget).result(5) == 23


@pytest.mark.parametrize('budget', ['ordinary', 'reentry', 'aggregate'])
@pytest.mark.parametrize('failure', range(1, 7))
def test_pool_hash_failure_cannot_orphan_admission(runtime, budget, failure):
    manager, dispatch = runtime
    caller = get_ident()

    class Pool:
        calls = 0
        failed = False
        injecting = True

        def __hash__(self):
            if self.injecting and get_ident() == caller:
                self.calls += 1
                if self.calls == failure:
                    self.failed = True
                    raise KeyboardInterrupt('pool metadata')
            return 29

    pool = Pool()
    future = None
    # Fault only the admission transaction. Accepted metadata stays stable
    # before any native owner can use it or teardown can retire it.
    with manager._condition:
        try:
            future = manager.submit(lambda record: 24, pool=pool, budget=budget)
        except KeyboardInterrupt as error:
            assert str(error) == 'pool metadata'
            assert pool.failed
        else:
            assert not pool.failed
        finally:
            pool.injecting = False
    if future is not None:
        assert future.result(5) == 24
    with manager._condition:
        assert manager._condition.wait_for(lambda: not manager._records, 5)
    assert manager.counts()['mailbox'] == 0
    assert manager._credits == manager._reentry_credits == 0
    assert not manager._reentry_active
    assert not manager._pool_credits
    assert not manager._pools


@pytest.mark.parametrize('budget', ['ordinary', 'reentry', 'aggregate'])
def test_rejected_reserved_pool_has_no_physical_debt(runtime, budget):
    manager, dispatch = runtime
    caller = get_ident()
    failure = 5 if budget == 'ordinary' else 3

    class Pool:
        calls = 0

        def __hash__(self):
            if get_ident() == caller:
                self.calls += 1
                if self.calls == failure:
                    raise KeyboardInterrupt('reserved pool metadata')
            return 31

    with pytest.raises(KeyboardInterrupt, match='reserved pool metadata'):
        manager.submit(
            lambda record: pytest.fail('rejected work ran'),
            pool=Pool(),
            budget=budget,
            reserve=1,
        )
    with manager._condition:
        assert manager._condition.wait_for(lambda: not manager._records, 5)
    assert manager._credits == manager._reentry_credits == 0
    assert not manager._reentry_active
    assert manager.counts()['mailbox'] == 0


@pytest.mark.parametrize('budget', ['ordinary', 'reentry', 'aggregate'])
def test_pool_equality_failure_preserves_existing_operation(runtime, budget):
    manager, dispatch = runtime
    entered, release = Event(), Event()

    class Pool:
        def __hash__(self):
            return 37

        def __eq__(self, other):
            raise ValueError('pool equality')

    try:
        first = manager.submit(
            lambda record: (entered.set(), release.wait(5)), pool=Pool()
        )
        assert entered.wait(5)
        with pytest.raises(ValueError, match='pool equality'):
            manager.submit(
                lambda record: pytest.fail('rejected work ran'),
                pool=Pool(),
                budget=budget,
            )
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: len(manager._records) == 1, 5
            )
        assert manager._credits == 1
        assert manager._reentry_credits == 0
        assert not manager._reentry_active
        assert manager.counts()['mailbox'] == 0
    finally:
        release.set()
    first.result(5)


@pytest.mark.parametrize('budget', ['ordinary', 'reentry', 'aggregate'])
def test_key_equality_insertion_failure_retires_charge(runtime, budget):
    manager, dispatch = runtime
    entered, release = Event(), Event()

    class Key:
        comparisons = 0

        def __hash__(self):
            return 41

        def __eq__(self, other):
            self.comparisons += 1
            if self.comparisons == 2:
                raise ValueError('key equality insertion')
            return False

    try:
        first = manager.submit(
            lambda record: (entered.set(), release.wait(5)), key=Key()
        )
        assert entered.wait(5)
        with pytest.raises(ValueError, match='key equality insertion'):
            manager.submit(
                lambda record: pytest.fail('rejected work ran'),
                key=Key(),
                budget=budget,
            )
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: len(manager._records) == 1, 5
            )
        assert manager._credits == 1
        assert manager._reentry_credits == 0
        assert not manager._reentry_active
        assert manager.counts()['mailbox'] == 0
        assert len(manager._keys) == 1
    finally:
        release.set()
    first.result(5)


class _PrimitiveRuntime:
    def __init__(self):
        from threading import Condition

        self._condition = Condition()
        self._pid = management.os.getpid()
        self.callbacks = []

    def _check_pid(self):
        if self._pid != management.os.getpid():
            raise management.ManagementClosed()

    def _schedule_callbacks(self, future):
        self.callbacks.append(future)

    def _queue_delivery_locked(self, record, future):
        self.callbacks.append(future)

    def flush(self):
        while self.callbacks:
            f = self.callbacks.pop(0)
            f._delivery.drain(f._record)


def _primitive_future():
    runtime = _PrimitiveRuntime()
    record = management._Record(
        runtime,
        management.Envelope(1, 'operation', None, object(), ()),
        None,
        None,
        None,
    )
    return (management.OperationFuture(record), runtime)


def test_native_terminal_read_and_deferred_callbacks():
    f, r = _primitive_future()
    seen = []
    f.add_done_callback(lambda settled: seen.append(settled.result()))
    with management._entered('cluster', f._record):
        with pytest.raises(management.ManagementContextError):
            f.result(0)
        f.set_result(42)
        assert f.result() == 42
        assert seen == []
    r.flush()
    assert seen == [42]


def test_cancel_notifies_standard_bulk_waiter():
    f, r = _primitive_future()
    ready, finished = Event(), Event()
    registered, result = [], []

    class RegisteredWaiters(list):
        def append(self, waiter):
            super().append(waiter)
            registered.append(waiter)
            ready.set()

    f._waiters = RegisteredWaiters()

    def waiting():
        try:
            result.append(wait([f]))
        finally:
            finished.set()

    t = Thread(target=waiting, name='primitive-cancel-waiter')
    t.start()
    try:
        assert ready.wait(5)
        assert f.cancel()
        assert finished.wait(5)
    finally:
        if not f.done():
            f.set_result(None)
        for waiter in registered:
            waiter.event.set()
        t.join(5)
    assert not t.is_alive()
    assert result[0].done == {f}
    assert not result[0].not_done


def test_late_callbacks_use_serial_reentrant_drainer():
    f, r = _primitive_future()
    f.set_result(1)
    r.flush()
    f._record.released = True
    seen = []

    def first(settled):
        seen.append('first-enter')
        settled.add_done_callback(lambda _: seen.append('second'))
        seen.append('first-exit')

    f.add_done_callback(first)
    assert seen == ['first-enter', 'first-exit', 'second']


def test_bulk_owner_rejection_unwinds_conditions():
    a, r = _primitive_future()
    b, _ = _primitive_future()
    with management._entered('pool', a._record):
        with pytest.raises(management.ManagementContextError):
            wait([a, b], timeout=0)
    acquired = []

    def check_from_another_thread():
        for pending in (a, b):
            available = pending._condition.acquire(False)
            acquired.append(available)
            if available:
                pending._condition.release()

    checker = Thread(target=check_from_another_thread)
    checker.start()
    checker.join(5)
    assert not checker.is_alive()
    assert acquired == [True, True]


def test_unentered_thread_target_revocation_is_terminal():
    called = []
    entry = management._ThreadStart(lambda: called.append(True))
    assert not entry.revoke()
    entry()
    assert not entry.revoke()
    assert not called


def test_active_delivery_ticket_cannot_be_cancelled():
    cancelled = management._DeliveryTicket(
        lambda: pytest.fail('cancelled delivery ran')
    )
    assert cancelled.cancel()
    cancelled()
    entered, release = Event(), Event()

    def deliver():
        entered.set()
        assert release.wait(5)

    ticket = management._DeliveryTicket(deliver)
    runner = Thread(target=ticket)
    runner.start()
    try:
        assert entered.wait(5)
        assert not ticket.cancel()
    finally:
        release.set()
        runner.join(5)
    assert not runner.is_alive()
    assert ticket.cancel()




@pytest.mark.skipif(
    platform.python_implementation() != 'CPython',
    reason='Requires synchronous reference destruction',
)
@pytest.mark.parametrize('pool', [None, 'pool'])
@pytest.mark.parametrize('action', ['replace', 'unregister'])
def test_pending_health_owner_captures_retire_on_cleanup(
    runtime, pool, action
):
    manager, dispatch = runtime
    blocked, release, retired = Event(), Event(), Event()
    incarnation, observations, references = object(), [], []
    terminal = Future()
    terminal.set_result('done')

    def observe_finalizer(label, mechanism):
        observations.append(
            (
                label,
                mechanism,
                execution_context(),
                manager._condition._is_owned(),
                terminal._condition._is_owned(),
                terminal.result(),
            )
        )
        if len(observations) == 4:
            retired.set()

    class Capture:
        def __init__(self, label):
            self.label = label

        def __call__(self):
            pytest.fail('retired pending capture was invoked')

        def __del__(self):
            observe_finalizer(self.label, 'del')

    def capture(label):
        callback = Capture(label)
        references.append(
            weakref.ref(
                callback, lambda ref: observe_finalizer(label, 'weakref')
            )
        )
        return callback

    def block(record):
        blocked.set()
        assert release.wait(5)

    def owner(record):
        assert manager.observe_negative(
            incarnation, capture('apply'), capture('notification')
        )
        if action == 'replace':
            assert manager.observe_negative(incarnation, lambda: None)
        else:
            manager.unregister_health(incarnation)

    try:
        manager.register_health(incarnation)
        if pool is not None:
            manager.submit(block)
            assert blocked.wait(5)
        future = manager.submit(owner, pool=pool)
        future.result(5)
        assert retired.wait(5)
        assert sorted(
            (label, mechanism) for label, mechanism, *_ in observations
        ) == [
            ('apply', 'del'),
            ('apply', 'weakref'),
            ('notification', 'del'),
            ('notification', 'weakref'),
        ]
        assert all(
            item[2:] == ('cleanup', False, False, 'done')
            for item in observations
        )
        assert all(ref() is None for ref in references)
    finally:
        release.set()
        dispatch.drain()


@pytest.mark.skipif(
    platform.python_implementation() != 'CPython',
    reason='Requires synchronous reference destruction',
)
@pytest.mark.parametrize('pool', [None, 'pool'])
@pytest.mark.parametrize('action', ['replace', 'unregister'])
def test_pending_health_owner_disposal_bounds_preserve_retry_state(
    runtime, pool, action
):
    manager, dispatch = runtime
    blocked, release = Event(), Event()
    cleanup_entered, cleanup_release = Event(), Event()
    incarnations = [object() for _ in range(manager.limit + 1)]
    slots, references, observations, applied = [], [], [], []
    owner_ready, retry_allowed = Event(), Event()
    current = [
        (lambda number=number: applied.append(number), lambda: None)
        for number in range(manager.limit + 1)
    ]
    rejected_apply, rejected_notification = lambda: None, lambda: None

    class Capture:
        def __init__(self, number):
            self.number = number

        def __call__(self):
            pytest.fail('retired capture was invoked')

        def __del__(self):
            observations.append(
                (execution_context(), manager._condition._is_owned())
            )
            if self.number == 0:
                cleanup_entered.set()
                cleanup_release.wait(5)

    def block(record):
        blocked.set()
        assert release.wait(5)

    def owner(record):
        for number, incarnation in enumerate(incarnations):
            manager.register_health(incarnation)
            old = Capture(number)
            references.append(weakref.ref(old))
            assert manager.observe_negative(incarnation, old)
            old = None
            slot = manager._health[id(incarnation)]
            slots.append(slot)
            if number == manager.limit:
                with pytest.raises(ManagementOverloaded):
                    if action == 'replace':
                        manager.observe_negative(incarnation, *current[number])
                    else:
                        manager.unregister_health(incarnation)
                assert slot.apply is references[number]()
                assert slot.notification is None
                assert slot.registered and not slot.removed
                assert not slot.pending_disposal
                continue
            if action == 'replace':
                assert manager.observe_negative(incarnation, *current[number])
            else:
                manager.unregister_health(incarnation)
                assert not slot.registered
                assert manager._health[id(incarnation)] is slot
                # Empty repeated unregister/register never creates another slot.
                manager.unregister_health(incarnation)
                manager.register_health(incarnation)
                assert manager._health[id(incarnation)] is slot
                assert manager.observe_negative(incarnation, *current[number])
            if number == 0:
                assert cleanup_entered.wait(5)
            for attempt in range(5):
                with pytest.raises(ManagementOverloaded):
                    if action == 'replace':
                        manager.observe_negative(
                            incarnation,
                            rejected_apply,
                            rejected_notification,
                            removed=True,
                        )
                    else:
                        manager.unregister_health(incarnation)
                assert manager._health[id(incarnation)] is slot
                assert (slot.apply, slot.notification) == current[number]
                assert slot.registered and not slot.removed
                assert slot.pending_disposal
            assert manager._pending_health_disposals == number + 1
        assert manager._pending_health_disposals == manager.limit
        assert manager._health_active == 0
        assert len(manager._handler_garbage) == manager.limit - 1
        assert manager._capture_obligations == manager.limit
        owner_ready.set()
        assert retry_allowed.wait(5)
        # Retry serially after each reservation has actually drained. Keep C
        # occupied throughout, so every old pending pair remains unconsumed.
        for incarnation in incarnations:
            with manager._condition:
                assert manager._condition.wait_for(
                    lambda: manager._pending_health_disposals == 0, 5
                )
            manager.unregister_health(incarnation)
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: manager._pending_health_disposals == 0, 5
            )

    try:
        if pool is not None:
            manager.submit(block)
            assert blocked.wait(5)
        future = manager.submit(owner, pool=pool)
        assert owner_ready.wait(5)
        assert manager._pending_health_disposals == manager.limit
        # First destructor is active, all other old captures remain queued.
        assert (
            sum(ref() is not None for ref in references) == manager.limit + 1
        )
        assert observations == [('cleanup', False)]
        cleanup_release.set()
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: manager._pending_health_disposals == 0, 5
            )
        retry_allowed.set()
        future.result(5)
        assert all(not slot.pending_disposal for slot in slots)
    finally:
        cleanup_release.set()
        retry_allowed.set()
        release.set()
        dispatch.drain()
    with manager._condition:
        assert manager._condition.wait_for(
            lambda: manager._pending_health_disposals == 0
            and manager._health_active == 0,
            5,
        )
    gc.collect()
    assert all(ref() is None for ref in references)
    assert all(
        context == 'cleanup' and not locked for context, locked in observations
    )
    assert not manager._health


@pytest.mark.skipif(
    platform.python_implementation() != 'CPython',
    reason='Requires synchronous reference destruction',
)
@pytest.mark.parametrize('pool', [None, 'pool'])
def test_pending_health_cleanup_close_holds_debt_and_guards_reentry(
    runtime, pool
):
    manager, dispatch = runtime
    blocked, release = Event(), Event()
    cleanup_entered, cleanup_release = Event(), Event()
    incarnation, observations = object(), []
    weakrefs = []

    class Capture:
        def __call__(self):
            pytest.fail('retired pending handler was invoked')

        def __del__(self):
            observations.append(
                (execution_context(), manager._condition._is_owned())
            )
            for operation in (
                lambda: manager.join(0),
                lambda: manager.submit(lambda r: None, wait=True),
                management.assert_can_wait,
            ):
                try:
                    operation()
                except ManagementContextError:
                    observations.append('rejected')
                else:
                    observations.append('allowed')
            # Closing during disposal retains this very reservation.
            manager.close()
            observations.append(manager._pending_health_disposals)
            cleanup_entered.set()
            cleanup_release.wait(5)

    def block(record):
        blocked.set()
        assert release.wait(5)

    def owner(record):
        old = Capture()
        weakrefs.append(weakref.ref(old))
        assert manager.observe_negative(incarnation, old)
        old = None
        manager.unregister_health(incarnation)

    try:
        manager.register_health(incarnation)
        slot = manager._health[id(incarnation)]
        if pool is not None:
            manager.submit(block)
            assert blocked.wait(5)
        future = manager.submit(owner, pool=pool)
        assert cleanup_entered.wait(5)
        assert observations == [
            ('cleanup', False),
            'rejected',
            'rejected',
            'rejected',
            1,
        ]
        assert slot.pending_disposal
        assert manager.closed.is_set()
        assert not manager.finalized.is_set()
        assert manager._capture_obligations >= 1
        assert manager._pending_health_disposals == 1
        cleanup_release.set()
        release.set()
        dispatch.drain()
        manager.join(5)
        assert not slot.pending_disposal
        assert manager._pending_health_disposals == 0
        assert manager._capture_obligations == 0
        assert manager._health_active == 0
        assert all(ref() is None for ref in weakrefs)
    finally:
        cleanup_release.set()
        release.set()
        dispatch.drain()


@pytest.mark.skipif(
    platform.python_implementation() != 'CPython',
    reason='Requires synchronous reference destruction',
)
def test_pending_and_consumed_health_disposals_keep_the_same_slot(
    runtime, monkeypatch
):
    manager, dispatch = runtime
    cleanup_started, start_cleanup = Event(), Event()
    pending_entered, release_pending = Event(), Event()
    latest_entered, release_latest = Event(), Event()
    first_applied, latest_applied, latest_notified = Event(), Event(), Event()
    incarnation, observations, applied = object(), [], []
    cleanup = manager._run_handler_cleanup

    def held_cleanup():
        cleanup_started.set()
        assert start_cleanup.wait(5)
        cleanup()

    monkeypatch.setattr(manager, '_run_handler_cleanup', held_cleanup)

    class Apply:
        def __init__(self, number):
            self.number = number

        def __call__(self):
            applied.append(self.number)
            if self.number == 0:
                first_applied.set()
            elif self.number == 3:
                latest_applied.set()

        def __del__(self):
            observations.append(
                (
                    self.number,
                    execution_context(),
                    manager._condition._is_owned(),
                )
            )
            if self.number == 1:
                pending_entered.set()
                release_pending.wait(5)
            elif self.number == 3:
                latest_entered.set()
                release_latest.wait(5)

    def owner(record):
        assert manager.observe_negative(incarnation, Apply(1))
        assert manager.observe_negative(incarnation, lambda: None)
        with pytest.raises(ManagementOverloaded):
            manager.unregister_health(incarnation)

    try:
        manager.register_health(incarnation)
        assert cleanup_started.wait(5)
        assert manager.observe_negative(incarnation, Apply(0))
        assert first_applied.wait(5)
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: bool(manager._handler_garbage), 5
            )
        manager.submit(owner, pool='pool').result(5)
        slot = manager._health[id(incarnation)]
        assert slot.active and slot.pending_disposal
        assert manager._health_active == 1
        assert manager._pending_health_disposals == 1
        # A nonowner keeps immediate outside-lock disposal even when this
        # incarnation already has both independently charged batches.
        manager.unregister_health(incarnation)
        assert not slot.registered
        assert manager._health[id(incarnation)] is slot
        start_cleanup.set()
        assert pending_entered.wait(5)
        # The consumed record drained first; its release must retain the
        # unregistered tombstone while the pending destructor still runs.
        assert not slot.active and slot.pending_disposal
        assert manager._health[id(incarnation)] is slot
        assert manager._health_active == 0
        assert manager._pending_health_disposals == 1
        manager.register_health(incarnation)
        assert manager._health[id(incarnation)] is slot
        assert manager.observe_negative(
            incarnation, Apply(3), latest_notified.set
        )
        assert latest_applied.wait(5)
        assert latest_notified.wait(5)
        assert applied == [0, 3]
        manager.unregister_health(incarnation)
        release_pending.set()
        assert latest_entered.wait(5)
        # Pending disposal drained; the separately charged consumed record
        # now keeps the same tombstone until its destructor returns.
        assert slot.active and not slot.pending_disposal
        assert manager._health[id(incarnation)] is slot
        assert manager._health_active == 1
        assert manager._pending_health_disposals == 0
        manager.close()
        assert not manager.finalized.is_set()
        release_latest.set()
        dispatch.drain()
        manager.join(5)
        assert manager._capture_obligations == 0
        assert manager._health_active == 0
        assert manager._pending_health_disposals == 0
        assert observations == [
            (0, 'cleanup', False),
            (1, 'cleanup', False),
            (3, 'cleanup', False),
        ]
    finally:
        start_cleanup.set()
        release_pending.set()
        release_latest.set()
        dispatch.drain()


@pytest.mark.parametrize('pool', [None, 'pool'])
def test_pending_health_owner_overload_is_independent_of_gc(
    runtime, monkeypatch, pool
):
    manager, dispatch = runtime
    cleanup_started, cleanup_release = Event(), Event()
    blocked, release = Event(), Event()
    cleanup = manager._run_handler_cleanup
    incarnation, first, second = object(), lambda: None, lambda: None

    def held_cleanup():
        cleanup_started.set()
        assert cleanup_release.wait(5)
        cleanup()

    monkeypatch.setattr(manager, '_run_handler_cleanup', held_cleanup)

    def block(record):
        blocked.set()
        assert release.wait(5)

    def owner(record):
        assert manager.observe_negative(incarnation, first)
        manager.unregister_health(incarnation)
        slot = manager._health[id(incarnation)]
        assert not slot.registered and slot.pending_disposal
        for number in range(20):
            manager.register_health(incarnation)
            assert manager._health[id(incarnation)] is slot
            if number == 0:
                assert manager.observe_negative(incarnation, second)
            else:
                with pytest.raises(ManagementOverloaded):
                    manager.observe_negative(incarnation, second)
            with pytest.raises(ManagementOverloaded):
                manager.unregister_health(incarnation)
            assert slot.registered and slot.apply is second
            assert manager._pending_health_disposals == 1
        assert not slot.active
        assert manager._capture_obligations == 1

    try:
        manager.register_health(incarnation)
        assert cleanup_started.wait(5)
        if pool is not None:
            manager.submit(block)
            assert blocked.wait(5)
        manager.submit(owner, pool=pool).result(5)
        cleanup_release.set()
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: manager._pending_health_disposals == 0, 5
            )
        manager.unregister_health(incarnation)
    finally:
        cleanup_release.set()
        release.set()
        dispatch.drain()


@pytest.mark.skipif(
    platform.python_implementation() != 'CPython',
    reason='Requires synchronous reference destruction',
)
@pytest.mark.parametrize('pool', [None, 'pool'])
@pytest.mark.parametrize('action', ['replace', 'unregister'])
def test_pending_health_overload_traceback_does_not_retain_retired_pair(
    runtime, monkeypatch, pool, action
):
    manager, dispatch = runtime
    cleanup_started, cleanup_release = Event(), Event()
    blocked, release, retry, rejected, retired = (
        Event(),
        Event(),
        Event(),
        Event(),
        Event(),
    )
    incarnation, errors, observations = object(), [], []
    cleanup = manager._run_handler_cleanup

    def held_cleanup():
        cleanup_started.set()
        assert cleanup_release.wait(5)
        cleanup()

    monkeypatch.setattr(manager, '_run_handler_cleanup', held_cleanup)

    class Capture:
        def __call__(self):
            pytest.fail('pending capture should have been unregistered')

        def __del__(self):
            observations.append(
                (execution_context(), manager._condition._is_owned())
            )
            retired.set()

    def block(record):
        blocked.set()
        assert release.wait(5)

    def owner(record):
        assert manager.observe_negative(incarnation, lambda: None)
        assert manager.observe_negative(incarnation, Capture())
        try:
            if action == 'replace':
                manager.observe_negative(incarnation, lambda: None)
            else:
                manager.unregister_health(incarnation)
        except ManagementOverloaded as error:
            errors.append(error)
        else:
            pytest.fail('second pending retirement was accepted')
        rejected.set()
        assert retry.wait(5)
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: manager._pending_health_disposals == 0, 5
            )
        manager.unregister_health(incarnation)
        # The retained exception still has its original traceback here.
        # Disposal must not depend on the producer dropping that traceback.
        assert retired.wait(5)

    try:
        manager.register_health(incarnation)
        assert cleanup_started.wait(5)
        if pool is not None:
            manager.submit(block)
            assert blocked.wait(5)
        future = manager.submit(owner, pool=pool)
        assert rejected.wait(5)
        assert len(errors) == 1 and errors[0].__traceback__ is not None
        cleanup_release.set()
        retry.set()
        future.result(5)
        assert observations == [('cleanup', False)]
        assert errors[0].__traceback__ is not None
    finally:
        cleanup_release.set()
        retry.set()
        release.set()
        errors.clear()
        dispatch.drain()


@pytest.mark.parametrize('action', ['replace', 'unregister'])
def test_pending_health_disposal_append_failure_preserves_retry_state(
    runtime, monkeypatch, action
):
    from collections import deque

    manager, dispatch = runtime
    cleanup_started, cleanup_release = Event(), Event()
    incarnation = object()
    old_apply, old_notification = lambda: None, lambda: None
    new_apply, new_notification = lambda: None, lambda: None
    cleanup = manager._run_handler_cleanup
    failures = []

    class FailOnce(deque):
        fail = True

        def append(self, entry):
            if entry[2] is not None and self.fail:
                self.fail = False
                raise RuntimeError('pending-health append failed')
            super().append(entry)

    garbage = FailOnce()
    monkeypatch.setattr(manager, '_handler_garbage', garbage)

    def held_cleanup():
        cleanup_started.set()
        assert cleanup_release.wait(5)
        cleanup()

    monkeypatch.setattr(manager, '_run_handler_cleanup', held_cleanup)

    def change():
        if action == 'replace':
            manager.observe_negative(
                incarnation, new_apply, new_notification, removed=True
            )
        else:
            manager.unregister_health(incarnation)

    def owner(record):
        assert manager.observe_negative(
            incarnation, old_apply, old_notification
        )
        slot = manager._health[id(incarnation)]
        try:
            change()
        except RuntimeError as error:
            failures.append(error)
        else:
            pytest.fail('injected queue append did not fail')
        assert (slot.apply, slot.notification) == (old_apply, old_notification)
        assert slot.registered and not slot.removed
        assert not slot.pending_disposal
        assert manager._pending_health_disposals == 0
        assert manager._capture_obligations == 0
        assert not garbage
        change()
        assert slot.pending_disposal
        assert manager._pending_health_disposals == 1
        assert manager._capture_obligations == 1
        assert len(garbage) == 1
        if action == 'replace':
            assert slot.registered and slot.removed
            assert (slot.apply, slot.notification) == (
                new_apply,
                new_notification,
            )
        else:
            assert not slot.registered
            assert slot.apply is slot.notification is None

    try:
        manager.register_health(incarnation)
        assert cleanup_started.wait(5)
        manager.submit(owner).result(5)
        assert len(failures) == 1
        assert failures[0].__traceback__ is not None
        cleanup_release.set()
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: manager._pending_health_disposals == 0, 5
            )
    finally:
        cleanup_release.set()
        failures.clear()
        dispatch.drain()


def test_reactor_channel_returns_only_its_borrow_after_detach():
    order, queued, retired, complete = [], [], [], []

    class Connection:
        @staticmethod
        def initialize_reactor():
            order.append('initialized')

        @staticmethod
        def _management_dispatch(callback):
            queued.append(callback)

        @staticmethod
        def _management_in_context():
            return False

        @staticmethod
        def _management_borrow():
            order.append('borrowed')
            return retired.append

    channel = management.ReactorChannel(Connection)
    assert order == ['initialized', 'borrowed']
    channel.dispatch(lambda: order.append('delivered'))
    channel.close(lambda: complete.append(True))
    assert not complete
    assert len(retired) == 1
    with pytest.raises(ManagementClosed):
        channel.dispatch(lambda: pytest.fail('detached dispatch ran'))
    queued.pop()()
    assert order[-1] == 'delivered'
    retired.pop()()
    assert complete == [True]


@pytest.mark.parametrize('in_context', [False, True])
def test_reactor_channel_legacy_startup_is_safe_on_the_reactor(in_context):
    initialized, dispatched, complete = [], [], []

    class Connection:
        @staticmethod
        def initialize_reactor():
            initialized.append(True)

        @staticmethod
        def _management_dispatch(callback):
            dispatched.append(callback)
            callback()

        @staticmethod
        def _management_in_context():
            return in_context

    channel = management.ReactorChannel(Connection)
    assert initialized == [True]
    assert len(dispatched) == int(not in_context)
    channel.close(lambda: complete.append(True))
    assert complete == [True]


@pytest.mark.parametrize('missing', ['dispatch', 'context'])
def test_reactor_channel_missing_adapter_does_not_start_a_service(missing):
    attributes = {
        'initialize_reactor': staticmethod(
            lambda: pytest.fail('unsupported adapter started a reactor')
        )
    }
    if missing != 'dispatch':
        attributes['_management_dispatch'] = staticmethod(
            lambda callback: None
        )
    if missing != 'context':
        attributes['_management_in_context'] = staticmethod(lambda: False)
    connection = type('Connection', (), attributes)
    with pytest.raises(ManagementContextError, match='no management'):
        management.ReactorChannel(connection)


def test_reactor_channel_legacy_startup_failure_is_reported(monkeypatch):
    class NotReady:
        def set(self):
            pytest.fail('unscheduled startup completed')

        def wait(self, timeout):
            assert timeout == 5
            return False

    class Connection:
        initialize_reactor = staticmethod(lambda: None)
        _management_dispatch = staticmethod(lambda callback: None)
        _management_in_context = staticmethod(lambda: False)

    monkeypatch.setattr(management, 'Event', NotReady)
    with pytest.raises(
        ManagementContextError, match='startup did not complete'
    ):
        management.ReactorChannel(Connection)


@pytest.mark.parametrize('factory, operation, expected', [
    (management._DeliveryTicket, 'cancel', True),
    (management._ThreadStart, 'revoke', False),
], ids=['delivery-ticket', 'thread-start'])
def test_handoff_target_disposal_allows_reentry(factory, operation, expected):
    observed = []
    entry = None

    class Target:
        def __call__(self):
            observed.append('invoked')

        def __del__(self):
            unlocked = entry._lock.acquire(False)
            if not unlocked:
                observed.append(('locked', None))
                return
            entry._lock.release()
            observed.append(('released', getattr(entry, operation)()))

    entry = factory(Target())
    assert getattr(entry, operation)() is expected
    gc.collect()
    assert observed == [('released', expected)]


def test_callback_pool_fanout_uses_bounded_reserved_capacity():
    from cassandra._management import compatibility_work
    dispatch = Dispatcher()
    manager = ManagementRuntime(dispatch, limit=1, reentry_window=2)
    release, started = Event(), Event()
    submitted = []
    errors = []
    executor = ThreadPoolExecutor(2)
    def work():
        assert release.wait(5)
        return True
    def fanout():
        try:
            for _ in range(5):
                with compatibility_work(manager) as attach:
                    future = executor.submit(work)
                    submitted.append(future)
                    attach(future)
            started.set()
        except BaseException as exc:
            errors.append(exc)
    notification = manager.notify(fanout)
    try:
        with manager._condition:
            assert manager._condition.wait_for(
                lambda: manager._reentry_credits == 2, 5)
        assert manager.counts()['ordinary'] == 1
        assert not started.is_set()
        release.set()
        notification.result(5)
        assert started.is_set()
        assert not errors
        assert len(submitted) == 5
        assert all(future.result(5) is True for future in submitted)
        assert manager.high_waters['reentry'] == 2
    finally:
        release.set()
        executor.shutdown()
        manager.close()
        dispatch.drain()
        manager.join(5)
