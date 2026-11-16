from collections import deque
from concurrent.futures import (
    Future, ThreadPoolExecutor, as_completed, wait, TimeoutError)
from threading import Barrier, Event, Lock, Thread, get_ident

import pytest

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
    """Bound a controlled Future's synchronous owner-handoff callback."""
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


def assert_runtime_counts(manager):
    with manager._condition:
        counts = manager.counts()
        assert counts['mailbox'] == sum(
            record.mailbox_pending for record in manager._records)
        assert counts['delivery'] == sum(
            record.delivery for record in manager._records)
        assert all(value >= 0 for value in counts.values())
        return counts


@pytest.fixture
def runtime(monkeypatch):
    dispatch = Dispatcher()
    runtime = ManagementRuntime(dispatch, limit=4, pool_limit=2)
    observe = runtime._observe_counts_locked
    def checked_counts():
        counts = observe()
        assert counts['mailbox'] == sum(
            record.mailbox_pending for record in runtime._records)
        assert counts['delivery'] == sum(
            record.delivery for record in runtime._records)
        return counts
    monkeypatch.setattr(runtime, '_observe_counts_locked', checked_counts)
    yield runtime, dispatch
    assert_runtime_counts(runtime)
    runtime.close()
    assert_runtime_counts(runtime)
    dispatch.drain()
    runtime.join(5)
    assert all(value == 0 for value in assert_runtime_counts(runtime).values())


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
            assert manager.submit(lambda record: 18, budget=budget).result(5) == 18
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
        assert finished.wait(1), 'callback waited behind its own credit'
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
    assert manager._reentry_credits == 0


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
            assert manager.submit(lambda record: 23).result(5) == 23
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
    completed = Event()
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
            dispose=lambda value: disposal_threads.append(get_ident())))
        with pytest.raises(ManagementClosed):
            future.result(5)
        # Shutdown publishes the native Future before executor submission has
        # returned on the owner. Keep the executor open through that handoff.
        assert completed.wait(5)
    manager.join(5)
    assert len(disposal_threads) == 1
    assert disposal_threads[0] not in {
        thread.ident for thread in manager._threads}
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
    assert manager.submit(lambda record: 25).result(5) == 25
    assert physical.cancelled()
    with manager._condition:
        assert future._record.physical == 0
        assert manager.counts()['ordinary'] == 0


@pytest.mark.parametrize('failed_phase', [1, 2, 3, 4])
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


@pytest.mark.parametrize('failed_phase', [1, 2, 3, 4])
@pytest.mark.parametrize('failure_point', ['construct', 'before', 'after'])
@pytest.mark.parametrize('startup_error', [RuntimeError, KeyboardInterrupt])
def test_start_failure_retires_registered_lifetimes(
        monkeypatch, failed_phase, failure_point, startup_error):
    import cassandra._management as management

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
        assert manager.close() is manager.closed
    assert finalized == [(True, True, 0)]
    assert not manager._lifetimes
    assert all(not thread.is_alive() for thread in manager._threads)


def test_delivery_failure_reentering_close_waits_for_lifetime_retirement():
    from cassandra._management import ManagementDeliveryFailed

    finalized = []
    def reject(callback):
        # The outer close has begun, but its retirement is still in progress.
        assert manager._closing and not manager.closed.is_set()
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
    manager.join(5)
    manager.close()
    assert isinstance(manager.completion_error, ManagementDeliveryFailed)
    assert len(finalized) == 1
    closed, retired, counts = finalized[0]
    assert closed and retired and all(value == 0 for value in counts.values())


@pytest.mark.parametrize('bootstrap_pending', [False, True])
def test_start_interruption_after_launch_tracks_the_runner(
        monkeypatch, bootstrap_pending):
    manager = ManagementRuntime(Dispatcher())
    launched, release = Event(), Event()
    threads = []
    real_start = Thread.start
    real_bootstrap = Thread._bootstrap_inner
    def bootstrap(thread):
        if bootstrap_pending:
            launched.set()
            assert release.wait(5)
        real_bootstrap(thread)
    def start(thread):
        threads.append(thread)
        if not bootstrap_pending:
            real_start(thread)
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
                assert not manager.finalized.is_set()
            assert manager._threads == threads
    finally:
        release.set()
        for thread in threads:
            assert thread._started.wait(5)
            thread.join(5)
            assert not thread.is_alive()
        manager.close()
        manager.join(5)
    assert manager._exited == len(threads) == 1


def test_post_terminal_owner_callbacks_use_current_charged_batch(runtime):
    manager, dispatch = runtime
    old = manager.submit(lambda record: 3)
    assert old.result(5) == 3
    calls = []
    def handler(record):
        for i in range(20):
            old.add_done_callback(
                lambda f, i=i: calls.append((i, execution_context())))
        assert calls == []
    current = manager.submit(handler)
    current.result(5)
    assert manager._credits == 1
    assert len(dispatch.queue) == 1
    dispatch.drain()
    assert calls == [(i, 'bridge') for i in range(20)]
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
                    handled.append(r.envelope.operation), owners.add(get_ident())))
                with producer_lock:
                    admitted.append((future._record.envelope.operation, future))
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
    completion = Thread(target=lambda: worker.set_result(object()), daemon=True)
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
                return any(item[2] for item in queue)
            assert manager._condition.wait_for(result_queued, 5)
        manager.close()
        for pending in (future, blocker, queued):
            with pytest.raises(ManagementClosed):
                pending.result(5)
        with manager._condition:
            queue = manager._cluster if pool is None else manager._pools[pool]
            assert len(queue) == 1 and queue[0][2]
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
    assert completion.ident not in {thread.ident for thread in manager._threads}


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
        assert len(manager._threads) == 4
    finally:
        release.set()
        manager.close()
        manager.join(5)


def test_worker_close_and_join_guard(runtime):
    manager, dispatch = runtime
    with ThreadPoolExecutor(1) as executor:
        def physical():
            assert execution_context() == 'worker'
            manager.close()
            with pytest.raises(ManagementContextError):
                manager.join()
            return object()
        future = manager.submit(lambda r: manager.work(r, executor, physical))
        with pytest.raises(ManagementClosed):
            future.result(5)
    manager.join(5)


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
def test_policy_phase_restores_context_and_only_reacquires_after_success(
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
            assert call.record is None
        assert _management._context.call is call
    assert getattr(_management._context, 'call', None) is None
    assert manager.counts()['ordinary'] == 0


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
            # Keep the regression's old-source failure safe: it admits a
            # synthetic physical obligation before reaching the forbidden wait.
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
        # Unwind the old-source counterexample without leaving daemon threads
        # or a synthetic physical reservation behind after the failed
        # assertion.
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
    attaching, finished = Event(), Event()
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
        with manager._condition:
            assert len(records) == 1
            assert records[0].physical == 1
        assert not finished.wait(0.05)
    finally:
        if outcome == 'result':
            physical.set_result(23)
        else:
            physical.set_exception(ValueError('physical work failed'))
        thread.join(5)
        assert not thread.is_alive()
        # Keep the original-source failure bounded without masking the defect.
        with manager._condition:
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
@pytest.mark.parametrize('ending', ['normal', 'close', 'fatal'])
def test_owner_callback_handoff_yields_reactor_while_caller_callback_runs(
        runtime, monkeypatch, pool, ending):
    from cassandra._management import ManagementDeliveryFailed
    manager, dispatch = runtime
    old = manager.submit(lambda record: 3)
    assert old.result(5) == 3
    with manager._condition:
        assert manager._condition.wait_for(lambda: old._record.released, 5)
    entered, resume, io_progress = Event(), Event(), Event()
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
        manager.submit(owner, pool=pool).result(5)
        dispatch(io_progress.set)
        reactor.start()
        assert io_progress.wait(1), (
            'callback handoff blocked shared reactor progress')
        reactor.join(5)
        assert not reactor.is_alive()
        assert calls == [('caller-enter', None)]
        assert manager.counts()['ordinary'] == 1
        if ending == 'close':
            manager.close()
            assert not manager.finalized.is_set()
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
    if ending != 'fatal':
        expected.append(('owner', 'bridge'))
    assert calls == expected
    assert manager.counts()['ordinary'] == 0
    assert not old._delivery.waiters


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
    calls = []
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
            manager.submit(lambda record: owner(old)).result(5)
        else:
            for future in old:
                manager.submit(lambda record, f=future: owner([f])).result(5)
        dispatch(io_progress.set)
        reactor.start()
        assert io_progress.wait(1)
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
            second.add_done_callback(lambda future: pytest.fail('delivery ran'))
        assert second.result(5) == 2
        assert manager.closed.wait(5)
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
