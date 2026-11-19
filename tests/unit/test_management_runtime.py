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
