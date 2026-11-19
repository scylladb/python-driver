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
    try:
        t.start()
        assert ready.wait(5)
        assert f.cancel()
        assert finished.wait(5)
    finally:
        if not f.done():
            f.set_result(None)
        for waiter in registered:
            waiter.event.set()
        if t.is_alive():
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
