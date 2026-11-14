from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor, wait, TimeoutError
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


@pytest.fixture
def runtime():
    dispatch = Dispatcher()
    runtime = ManagementRuntime(dispatch, limit=4, pool_limit=2)
    yield runtime, dispatch
    runtime.close()
    dispatch.drain()
    runtime.join(5)


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


def test_owner_wait_fails_and_pool_handlers_are_serial(runtime):
    manager, dispatch = runtime
    pending = Future()
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
        assert manager._credits == 0


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
    assert manager.close_lifetime(
        child_one, lambda: pytest.fail('duplicate')) is one
    assert manager.close_lifetime(
        child_two, lambda: effects.append(2)).is_set()
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


@pytest.mark.parametrize('failed_phase', [1, 2, 3, 4])
def test_start_failure_and_repeated_close_leave_no_threads(
        monkeypatch, failed_phase):
    import cassandra._management as management
    real_start = Thread.start
    starts = []
    def start(thread):
        starts.append(thread)
        if len(starts) == failed_phase:
            raise RuntimeError('thread startup failed')
        return real_start(thread)
    dispatch = Dispatcher()
    manager = ManagementRuntime(dispatch)
    try:
        with monkeypatch.context() as patcher:
            patcher.setattr(management.Thread, 'start', start)
            with pytest.raises(RuntimeError, match='thread startup failed'):
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
            assert manager.close() is first
        finally:
            manager.close()
            manager.join(5)
        assert not manager._records
        assert all(not thread.is_alive() for thread in manager._threads)


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
    admitted, handled, owners = [], [], set()
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
        assert start.wait(5)
        for _ in range(8):
            future = manager.submit(lambda r: (
                handled.append(r.envelope.operation), owners.add(get_ident())))
            with producer_lock:
                admitted.append((future._record.envelope.operation, future))
    threads = [Thread(target=producer) for _ in range(8)]
    try:
        for thread in threads:
            thread.start()
        start.set()
        for thread in threads:
            thread.join(5)
            assert not thread.is_alive()
        for _, future in admitted:
            future.result(5)
        assert handled == sorted(number for number, _ in admitted)
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
    def initial(record):
        started.set()
        return manager.work(record, ControlledExecutor(), object,
                            dispose=lambda value: disposed.set())
    manager.pool_limit = 3
    future = manager.submit(initial, pool=pool)
    try:
        assert started.wait(5)
        blocker = manager.submit(
            lambda record: (blocked.set(), release.wait(5)), pool=pool)
        assert blocked.wait(5)
        queued = manager.submit(
            lambda record: pytest.fail('closed ordinary work ran'),
            pool=pool)
        # The reserved result sits behind ordinary work.
        worker.set_result(object())
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
    assert disposed.wait(5)


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
        physical.set_result(object())
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
            assert isinstance(future._delivery_error, ManagementDeliveryFailed)
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
    assert isinstance(first._delivery_error, ManagementDeliveryFailed)
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
        physical.set_result(23)
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
            physical.set_result(23)


def test_foreign_callback_uses_ordinary_admission(runtime):
    first, dispatch = runtime
    other = ManagementRuntime(dispatch, limit=1)
    gate, entered = Event(), Event()
    try:
        occupied = other.submit(lambda record: (entered.set(), gate.wait(5)))
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
            assert isinstance(old._delivery_error, ManagementDeliveryFailed)
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
    assert not old._callback_waiters


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
        assert all(not f._callback_waiters for f in old)
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
    original = future._drain_callbacks
    def drain(record=None):
        result = original(record)
        if record is not None and not result:
            deferred.set()
            assert return_delivery.wait(5)
        return result
    monkeypatch.setattr(future, '_drain_callbacks', drain)
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
    assert future._delivery_record is future._record
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
