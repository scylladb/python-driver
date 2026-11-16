"""Test reactor completion adapters and ownership of real local descriptors."""

import asyncio
import importlib
import sys
from collections import deque
from contextlib import contextmanager
from concurrent.futures import CancelledError, Future
from threading import Event, Thread, current_thread, get_ident

import pytest

from cassandra import DependencyException
from cassandra._management import (
    ManagementRuntime, ReactorChannel, execution_context)


def reactor_module(reactor):
    try:
        return importlib.import_module('cassandra.io.' + reactor + 'reactor')
    except (ImportError, DependencyException) as exc:
        pytest.skip(str(exc))


@pytest.mark.parametrize('failure', [ImportError, DependencyException])
def test_unavailable_reactor_import_is_skipped(monkeypatch, failure):
    def unavailable(name):
        raise failure('reactor dependency unavailable')
    monkeypatch.setattr(importlib, 'import_module', unavailable)
    with pytest.raises(pytest.skip.Exception, match='reactor dependency unavailable'):
        reactor_module('libev')


@pytest.mark.parametrize('failure', [RuntimeError, AssertionError])
def test_reactor_import_errors_are_not_hidden(monkeypatch, failure):
    def broken(name):
        raise failure('supported reactor is broken')
    monkeypatch.setattr(importlib, 'import_module', broken)
    with pytest.raises(failure, match='supported reactor is broken'):
        reactor_module('asyncore')


@pytest.fixture(autouse=True)
def close_borrowed_channels(monkeypatch):
    channels = []
    initialize = ReactorChannel.__init__
    def tracked(self, *args, **kwargs):
        initialize(self, *args, **kwargs)
        channels.append(self)
    monkeypatch.setattr(ReactorChannel, '__init__', tracked)
    yield
    for channel in channels:
        detached = Event()
        channel.close(detached.set)
        assert detached.wait(5)


@pytest.fixture(params=['asyncio', 'libev', 'asyncore'])
def connection_class(request):
    module = reactor_module(request.param)
    return getattr(module, {'asyncio': 'AsyncioConnection',
                            'libev': 'LibevConnection',
                            'asyncore': 'AsyncoreConnection'}[request.param])


def test_idle_reactor_completion_runs_on_io_thread(connection_class):
    channel = ReactorChannel(connection_class)
    for _ in range(3):
        finished = Event()
        observations = []
        def on_io():
            assert execution_context() is None  # Ordinary ResponseFuture context.
            assert connection_class._management_in_context()
            observations.append(get_ident())
            finished.set()
        channel.dispatch(on_io)
        assert finished.wait(5)
        assert observations[0] != get_ident()


def test_clusters_borrow_reactor_without_closing_other_channel(connection_class):
    channel = ReactorChannel(connection_class)
    first = ManagementRuntime(channel.dispatch, on_finalized=channel.close)
    second_channel = ReactorChannel(connection_class)
    second = ManagementRuntime(second_channel.dispatch, on_finalized=second_channel.close)
    callback_entered, release, other_done = Event(), Event(), Event()
    handler_entered, handler_release = Event(), Event()
    try:
        def handler(record):
            handler_entered.set()
            assert handler_release.wait(5)
            return 3
        future = first.submit(handler)
        assert handler_entered.wait(5)
        def hold(future):
            callback_entered.set()
            assert release.wait(5)
        future.add_done_callback(hold)
        handler_release.set()
        assert future.result(5) == 3
        assert callback_entered.wait(5)
        first.close()
        assert not first.finalized.is_set()
        # Shared reactor may be blocked by application code; native state still
        # progresses and the second channel works as soon as that code returns.
        other = second.submit(lambda record: 8)
        other.add_done_callback(lambda f: other_done.set())
        assert other.result(5) == 8
        release.set()
        assert other_done.wait(5)
        first.join(5)
        assert second.submit(lambda record: 9).result(5) == 9
    finally:
        handler_release.set()
        release.set()
        first.close()
        second.close()
        first.join(5)
        second.join(5)


@pytest.mark.parametrize('terminal', ['result', 'error', 'cancel'])
@pytest.mark.parametrize('registered', ['before', 'after'])
def test_asyncio_chaining_progresses_while_listener_waits(connection_class, terminal, registered):
    channel = ReactorChannel(connection_class)
    manager = ManagementRuntime(channel.dispatch, on_finalized=channel.close)
    application_loop = asyncio.new_event_loop()
    app_thread = Thread(target=application_loop.run_forever)
    app_thread.start()
    entered, complete = Event(), Event()
    release_handler = Event()
    outcomes = []
    try:
        def listener():
            def handler(record):
                assert release_handler.wait(5)
                if terminal == 'error':
                    raise ValueError('expected')
                return 12
            source = manager.submit(handler)
            if registered == 'after':
                if terminal == 'cancel':
                    source.cancel()
                release_handler.set()
                try:
                    source.result(5)
                except (ValueError, CancelledError):
                    pass
            async def consume():
                wrapped = asyncio.wrap_future(source)
                entered.set()
                return await wrapped
            chained = asyncio.run_coroutine_threadsafe(consume(), application_loop)
            if registered == 'before':
                assert entered.wait(5)
                if terminal == 'cancel':
                    source.cancel()
                release_handler.set()
            try:
                outcomes.append(chained.result(5))
            except (ValueError, CancelledError) as exc:
                outcomes.append(type(exc))
            complete.set()
        notification = manager.notify(listener)
        assert complete.wait(5)
        notification.result(5)
        assert outcomes == [{'result': 12, 'error': ValueError, 'cancel': CancelledError}[terminal]]
    finally:
        release_handler.set()
        try:
            manager.close()
            manager.join(5)
        finally:
            application_loop.call_soon_threadsafe(application_loop.stop)
            app_thread.join(5)
            assert not app_thread.is_alive()
            application_loop.close()


def test_libev_dispatch_enqueued_during_idle_exit_is_not_lost():
    module = reactor_module('libev')
    loop = module.LibevLoop()
    native = loop._loop
    returned, release, first, second = Event(), Event(), Event(), Event()
    class PausedExit:
        paused = False
        def start(self):
            native.start()
            if not self.paused:
                self.paused = True
                returned.set()
                assert release.wait(5)
        def __getattr__(self, name):
            return getattr(native, name)
    loop._loop = PausedExit()
    try:
        loop.call_soon(first.set)
        assert first.wait(5)
        assert returned.wait(5)
        loop.call_soon(second.set)
        release.set()
        assert second.wait(5), 'idle exit lost a completion queued before exit committed'
        loop._thread.join(5)
        assert not loop._thread.is_alive()
    finally:
        release.set()
        loop._cleanup_unpublished()


@pytest.mark.parametrize('cleanup_before_exit', [False, True])
def test_libev_retired_runner_does_not_claim_replacement_work(monkeypatch, cleanup_before_exit):
    module = reactor_module('libev')
    loop = module.LibevLoop()
    native, drain = loop._loop, loop._drain_deferred
    retired, release_old, replacement, release_new, delivered = [Event() for _ in range(5)]
    old_threads, observations = [], []
    def pause_retired():
        drain()
        if not loop._started and not retired.is_set():
            old_threads.append(current_thread())
            retired.set()
            assert release_old.wait(5)
    class PausedReplacement:
        def start(self):
            if retired.is_set() and current_thread() is not old_threads[0]:
                replacement.set()
                assert release_new.wait(15 if cleanup_before_exit else 5)
            # A failing ownership assertion must not re-enter a disposed native loop.
            if loop._loop is not None:
                native.start()
        def __getattr__(self, name):
            return getattr(native, name)
    monkeypatch.setattr(loop, '_drain_deferred', pause_retired)
    loop._loop = PausedReplacement()
    native_fields = ('_loop', '_preparer', '_loop_timer', '_management_keeper', '_notifier')
    native_state = [getattr(loop, name) for name in native_fields]
    def completion():
        observations.append((get_ident(), current_thread() is loop._thread))
        delivered.set()
    try:
        loop.call_soon(lambda: None)
        assert retired.wait(5), 'runner did not publish its idle exit'
        loop.call_soon(completion)
        assert replacement.wait(5)
        new_thread = loop._thread
        assert new_thread is not old_threads[0]
        release_old.set()
        old_threads[0].join(5)
        assert not old_threads[0].is_alive()
        assert observations == [], 'retired runner consumed new-generation work'
        if cleanup_before_exit:
            loop._cleanup_unpublished()
            assert new_thread.is_alive()
            assert all(getattr(loop, name) is value
                       for name, value in zip(native_fields, native_state)), (
                'retired runner allowed disposal before current native execution exited')
            assert not loop._runner_exited
        release_new.set()
        assert delivered.wait(5)
        new_thread.join(5)
        assert observations == [(new_thread.ident, True)]
        assert not new_thread.is_alive()
        if cleanup_before_exit:
            assert all(getattr(loop, name) is None for name in native_fields)
    finally:
        release_old.set()
        release_new.set()
        loop._cleanup()
        for thread in old_threads:
            thread.join(5)
            assert not thread.is_alive()
        loop._cleanup_unpublished()


@pytest.mark.parametrize('arming_path', ['acquire', 'keepalive'])
def test_libev_keeper_arming_cannot_outlive_cleanup(monkeypatch, arming_path):
    module = reactor_module('libev')
    loop = module.LibevLoop()
    native = loop._management_keeper
    arming, resume, stopped, armed = [Event() for _ in range(4)]
    releases, errors = [], []
    class PausedKeeper:
        active = False
        def start(self, timeout):
            arming.set()
            assert resume.wait(5)
            native.start(timeout)
            self.active = True
            armed.set()
        def stop(self):
            native.stop()
            self.active = False
            stopped.set()
    keeper = PausedKeeper()
    loop._management_keeper = keeper
    def borrow():
        try:
            releases.append(loop.borrow_management())
        except BaseException as exc:
            errors.append(exc)
    borrower = Thread(target=borrow) if arming_path == 'acquire' else None
    stopper = None
    try:
        if borrower is not None:
            borrower.start()
        else:
            loop._management_users.add(object())
            loop.call_soon(loop._keep_management_alive)
        assert arming.wait(5)
        runner = loop._thread
        stopper = Thread(target=loop._cleanup)
        stopper.start()
        assert stopped.wait(5), 'cleanup did not stop the in-flight keeper'
        assert loop._shutdown
        resume.set()
        assert armed.wait(5)
        runner.join(5)
        assert not runner.is_alive(), 'keeper was resurrected after cleanup'
        assert not keeper.active
        stopper.join(5)
        assert not stopper.is_alive()
        if borrower is not None:
            borrower.join(5)
            assert not borrower.is_alive()
            assert errors == []
            assert len(releases) == 1
    finally:
        resume.set()
        keeper.stop()
        loop.notify()
        if borrower is not None:
            borrower.join(5)
        for release in releases:
            release(lambda: None)
        if stopper is not None:
            stopper.join(5)
        loop._cleanup_unpublished()


@pytest.mark.parametrize('cleanup_order', ['during-release', 'before-release'])
def test_libev_borrow_release_completes_during_cleanup(monkeypatch, cleanup_order):
    module = reactor_module('libev')
    loop = module.LibevLoop()
    release_borrow = loop.borrow_management()
    entered, finish, admission_attempted, stopped, completed = [Event() for _ in range(5)]
    errors, observations, blocked = [], [], []
    admission_lock, keeper = loop._lock_thread, loop._management_keeper
    dispatch = loop.call_soon
    def hold():
        entered.set()
        assert finish.wait(5)
    def cleanup():
        try:
            loop._cleanup()
        except BaseException as exc:
            errors.append(exc)
    stopper = Thread(target=cleanup)
    class ObservedLock:
        def __enter__(self):
            if current_thread() is stopper:
                acquired = admission_lock.acquire(False)
                blocked.append(not acquired)
                admission_attempted.set()
                if acquired:
                    return self
            admission_lock.acquire()
            return self
        def __exit__(self, *args):
            admission_lock.release()
    class ObservedKeeper:
        def stop(self):
            keeper.stop()
            stopped.set()
        def __getattr__(self, name):
            return getattr(keeper, name)
    def racing_dispatch(callback):
        stopper.start()
        assert admission_attempted.wait(5)
        if not blocked[0]:
            # The unprotected release allows cleanup to commit shutdown here.
            assert stopped.wait(5)
        dispatch(callback)
    def detached():
        observations.append(current_thread())
        completed.set()
    try:
        dispatch(hold)
        assert entered.wait(5)
        runner = loop._thread
        monkeypatch.setattr(loop, '_lock_thread', ObservedLock())
        loop._management_keeper = ObservedKeeper()
        if cleanup_order == 'during-release':
            loop.call_soon = racing_dispatch
        else:
            stopper.start()
            assert stopped.wait(5)
            assert loop._shutdown
        release_borrow(detached)
        assert stopped.wait(5)
        finish.set()
        assert completed.wait(5)
        stopper.join(5)
        runner.join(5)
        assert not stopper.is_alive()
        assert not runner.is_alive()
        assert errors == []
        assert not loop._management_users
        assert not loop._management_callbacks
        assert len(observations) == 1
        if cleanup_order == 'during-release':
            assert blocked[0], 'cleanup passed the release admission boundary'
            assert observations == [runner]
        else:
            assert observations == [current_thread()]
    finally:
        finish.set()
        if stopper.ident is not None:
            stopper.join(5)
        loop._cleanup()
        try:
            release_borrow(lambda: None)
        finally:
            if loop.call_soon is racing_dispatch:
                del loop.call_soon
            loop._cleanup_unpublished()


def test_chaining_application_loop_closes_when_management_join_raises(monkeypatch, connection_class):
    # Exercise the actual chaining test's failure cleanup, then supply our own
    # fallback cleanup so a negative control cannot leak a non-daemon thread.
    ReactorChannel(connection_class)
    app_threads, app_loops = [], []
    make_loop, thread_class, join = asyncio.new_event_loop, Thread, ManagementRuntime.join
    def new_loop():
        loop = make_loop()
        app_loops.append(loop)
        return loop
    def new_thread(*args, **kwargs):
        thread = thread_class(*args, **kwargs)
        app_threads.append(thread)
        return thread
    def failed_join(manager, timeout):
        join(manager, timeout)
        raise RuntimeError('forced finalization failure')
    monkeypatch.setattr(asyncio, 'new_event_loop', new_loop)
    monkeypatch.setattr(__name__ + '.Thread', new_thread)
    monkeypatch.setattr(ManagementRuntime, 'join', failed_join)
    try:
        with pytest.raises(RuntimeError, match='forced finalization failure'):
            test_asyncio_chaining_progresses_while_listener_waits(connection_class, 'result', 'before')
        assert len(app_threads) == len(app_loops) == 1
        assert not app_threads[0].is_alive()
        assert app_loops[0].is_closed()
    finally:
        for loop, thread in zip(app_loops, app_threads):
            if thread.is_alive():
                loop.call_soon_threadsafe(loop.stop)
                thread.join(5)
            if not thread.is_alive() and not loop.is_closed():
                loop.close()


def test_reactor_startup_lock_is_reset_in_fork_child(connection_class):
    import os
    import select
    import signal
    if not hasattr(os, 'fork'):
        pytest.skip('fork unavailable')
    channel = ReactorChannel(connection_class)
    module = importlib.import_module(connection_class.__module__)
    lock = (connection_class._lock if module.__name__.endswith('asyncioreactor')
            else module._initialization_lock)
    entered, release = Event(), Event()
    def holder():
        with lock:
            entered.set()
            assert release.wait(10)
    thread = Thread(target=holder)
    thread.start()
    assert entered.wait(5)
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(read_fd)
            child_channel = ReactorChannel(connection_class)
            os.write(write_fd, b'started')
        finally:
            os._exit(0)
    os.close(write_fd)
    try:
        ready, _, _ = select.select([read_fd], [], [], 5)
        if not ready:
            os.kill(pid, signal.SIGKILL)
        assert ready, 'new child reactor entered inherited startup lock'
        assert os.read(read_fd, 8) == b'started'
    finally:
        release.set()
        thread.join(5)
        os.close(read_fd)
        os.waitpid(pid, 0)


def test_libev_shutdown_drain_rejects_inline_channel_borrow(monkeypatch):
    module = reactor_module('libev')
    loop = module.LibevLoop()
    monkeypatch.setattr(module, '_global_loop', loop)
    entered, release, stopped, finished = [Event() for _ in range(4)]
    errors, rejected, observed, channels = [], [], [], []
    native_keeper = loop._management_keeper
    class ObservedKeeper:
        def stop(self):
            native_keeper.stop()
            stopped.set()
        def __getattr__(self, name):
            return getattr(native_keeper, name)
    def hold():
        entered.set()
        assert release.wait(5)
    def attempt():
        try:
            channels.append(ReactorChannel(module.LibevConnection))
        except RuntimeError as exc:
            rejected.append(str(exc))
        finally:
            observed.append(current_thread())
            finished.set()
    def cleanup():
        try:
            loop._cleanup()
        except BaseException as exc:
            errors.append(exc)
    stopper = Thread(target=cleanup)
    try:
        channels.append(ReactorChannel(module.LibevConnection))
        with monkeypatch.context() as keeper_patch:
            keeper_patch.setattr(loop, '_management_keeper', ObservedKeeper())
            before = set(loop._management_users)
            loop.call_soon(hold)
            assert entered.wait(5)
            runner = loop._thread
            loop.call_soon(attempt)
            stopper.start()
            assert stopped.wait(5)
            with loop._lock:
                assert loop._shutdown
            release.set()
            assert finished.wait(5)
            stopper.join(5)
            runner.join(5)
            assert not stopper.is_alive()
            assert not runner.is_alive()
            assert errors == []
            assert observed == [runner]
            assert rejected == ['Libev reactor is shutting down'], {
                'channels': len(channels), 'tokens': len(loop._management_users),
                'previous_tokens': len(before)}
            assert set(loop._management_users) == before
            assert not loop._management_callbacks
    finally:
        release.set()
        if stopper.ident is not None:
            stopper.join(5)
        try:
            for channel in channels:
                detached = Event()
                channel.close(detached.set)
                assert detached.wait(5)
        finally:
            loop._cleanup_unpublished()


def test_failed_libev_startup_releases_unpublished_native_callbacks(monkeypatch):
    import gc
    import weakref
    module = reactor_module('libev')
    original_factory = module.LibevLoop
    candidates = []
    def candidate():
        loop = original_factory()
        candidates.append(weakref.ref(loop))
        return loop
    class FailedStart:
        def start(self):
            raise RuntimeError('native runner was not started')
        def is_alive(self):
            return False
    monkeypatch.setattr(module, '_global_loop', None)
    monkeypatch.setattr(module, 'LibevLoop', candidate)
    monkeypatch.setattr(module, 'Thread', lambda **kwargs: FailedStart())
    for _ in range(3):
        with pytest.raises(RuntimeError, match='native runner was not started'):
            module.LibevConnection.initialize_reactor()
        assert module._global_loop is None
    gc.collect()
    assert len(candidates) == 3
    assert all(candidate() is None for candidate in candidates)


def test_unpublished_libev_disposal_waits_for_actual_native_exit(monkeypatch):
    module = reactor_module('libev')
    original_factory = module.LibevLoop
    entered, release = Event(), Event()
    candidates = []
    class PausedNativeLoop:
        def __init__(self, native):
            self.native = native
        def __getattr__(self, name):
            return getattr(self.native, name)
        def start(self):
            entered.set()
            assert release.wait(15)
            self.native.start()
    def candidate():
        loop = original_factory()
        loop._loop = PausedNativeLoop(loop._loop)
        candidates.append(loop)
        return loop
    class ReadyTimeout(Event):
        def wait(self, timeout=None):
            assert entered.wait(5)
            return False
    monkeypatch.setattr(module, '_global_loop', None)
    monkeypatch.setattr(module, 'LibevLoop', candidate)
    monkeypatch.setattr(module, 'Event', ReadyTimeout)
    try:
        with pytest.raises(RuntimeError, match='startup did not complete'):
            module.LibevConnection.initialize_reactor()
        loop = candidates[0]
        assert loop._thread.is_alive()
        assert loop._loop is not None
        assert loop._preparer is not None
        # A repeated request must also retain native state until actual exit.
        loop._cleanup_unpublished()
        assert loop._loop is not None
    finally:
        release.set()
        for loop in candidates:
            loop._thread.join(5)
            assert not loop._thread.is_alive()
    assert module._global_loop is None
    assert loop._loop is None
    assert loop._preparer is None
    assert loop._loop_timer is None
    assert loop._management_keeper is None
    assert loop._notifier is None


@pytest.mark.parametrize('reactor', ['libev', 'asyncore'])
def test_startup_failure_does_not_acknowledge_concurrent_dispatch(monkeypatch, reactor):
    module = reactor_module(reactor)
    if reactor == 'asyncore':
        monkeypatch.setattr(module, '_dispatcher_map', {})
        class Dispatcher:
            def validate(self):
                pass
            def notify_loop(self):
                pass
            def loop(self, timeout):
                Event().wait(0.001)
            def close(self):
                pass
        monkeypatch.setattr(module.AsyncoreLoop, '_loop_dispatch_class', Dispatcher)
    loop = getattr(module, {'libev': 'LibevLoop', 'asyncore': 'AsyncoreLoop'}[reactor])()
    starting, fail, second_entered, accepted, delivered = [Event() for _ in range(5)]
    admission_attempted = Event()
    original_thread = module.Thread
    calls = []
    class FailedStart:
        def start(self):
            starting.set()
            assert fail.wait(5)
            raise RuntimeError('thread creation failed')
        def is_alive(self):
            return False
    def factory(*args, **kwargs):
        calls.append(True)
        return FailedStart() if len(calls) == 1 else original_thread(*args, **kwargs)
    monkeypatch.setattr(module, 'Thread', factory)
    errors, rejected_callbacks = [], []
    def first():
        try:
            loop.call_soon(lambda: rejected_callbacks.append(True))
        except RuntimeError as exc:
            errors.append(str(exc))
    def second():
        second_entered.set()
        loop.call_soon(delivered.set)
        accepted.set()
    first_thread, second_thread = Thread(target=first), Thread(target=second)
    lock_name = '_lock_thread' if reactor == 'libev' else '_state_lock'
    admission_lock = getattr(loop, lock_name)
    blocked = []
    class ObservedLock:
        def __enter__(self):
            if get_ident() == second_thread.ident:
                acquired = admission_lock.acquire(False)
                blocked.append(not acquired)
                admission_attempted.set()
                if acquired:
                    return self
            admission_lock.acquire()
            return self
        def __exit__(self, *args):
            admission_lock.release()
    setattr(loop, lock_name, ObservedLock())
    try:
        first_thread.start()
        assert starting.wait(5)
        second_thread.start()
        assert second_entered.wait(5)
        assert admission_attempted.wait(5)
        assert blocked == [True], 'second admission did not block on provisional startup'
        assert not accepted.wait(0.05)
        fail.set()
        first_thread.join(5)
        second_thread.join(5)
        assert accepted.is_set()
        assert delivered.wait(5)
        assert errors == ['thread creation failed']
        assert rejected_callbacks == []
    finally:
        fail.set()
        first_thread.join(5)
        if second_thread.ident is not None:
            second_thread.join(5)
        if reactor == 'libev':
            loop._cleanup_unpublished()
        else:
            loop._cleanup()


def test_asyncore_terminal_drain_survives_closed_pipe_dispatcher(monkeypatch):
    import os
    if os.name == 'nt':
        pytest.skip('pipe dispatch is not supported by Windows select')
    module = reactor_module('asyncore')
    monkeypatch.setattr(module, '_dispatcher_map', {})
    monkeypatch.setattr(module.AsyncoreLoop, '_loop_dispatch_class',
                        module._AsyncorePipeDispatcher)
    loop = module.AsyncoreLoop()
    dispatcher = loop._loop_dispatcher
    polling, resume_poll, timing, resume_timer, stopping, closed = [Event() for _ in range(6)]
    observations, errors = [], []
    poll, notify, close, run = dispatcher.loop, dispatcher.notify_loop, dispatcher.close, loop._run_loop
    def held_poll(timeout):
        polling.set()
        assert resume_poll.wait(5)
        poll(timeout)
    def notified():
        notify()
        if loop._shutdown:
            stopping.set()
    def closed_dispatcher():
        close()
        closed.set()
    def run_observed():
        try:
            run()
        except BaseException as exc:
            errors.append(exc)
    def expired():
        # Cleanup also services timers after its bounded join. It must be able
        # to remove this timer while its original callback is still running.
        timer.cancel()
        timing.set()
        assert resume_timer.wait(10)
        raise SystemExit('timer stopped the reactor before normal dispatch')
    timer = module.Timer(0, expired)
    monkeypatch.setattr(dispatcher, 'loop', held_poll)
    monkeypatch.setattr(dispatcher, 'notify_loop', notified)
    monkeypatch.setattr(dispatcher, 'close', closed_dispatcher)
    monkeypatch.setattr(loop, '_run_loop', run_observed)
    stopper = Thread(target=loop._cleanup)
    try:
        loop.add_timer(timer)
        loop.maybe_start()
        runner = loop._thread
        assert polling.wait(5)
        for _ in range(130):
            loop.call_soon(lambda: observations.append(current_thread()))
        assert len(loop._management_callbacks) == 130
        stopper.start()
        assert stopping.wait(5)
        resume_poll.set()
        assert timing.wait(5)
        assert not dispatcher._notified
        assert observations == []
        assert closed.wait(5), 'cleanup did not close the dispatcher after its bounded join'
        stopper.join(5)
        assert not stopper.is_alive()
        assert dispatcher.write_fd is None
        resume_timer.set()
        runner.join(5)
        assert not runner.is_alive()
        assert observations == [runner] * 130, 'terminal drain stranded accepted callbacks'
        assert not loop._management_callbacks
        assert loop._started is False, 'terminal drain did not relinquish runner ownership'
        assert loop._management_failed
        assert errors == []
        with pytest.raises(RuntimeError):
            loop.call_soon(lambda: None)
    finally:
        resume_poll.set()
        resume_timer.set()
        if stopper.ident is not None:
            stopper.join(5)
        loop._cleanup()
        if loop._thread is not None:
            loop._thread.join(5)
            assert not loop._thread.is_alive()


def test_asyncore_normal_dispatch_keeps_full_batches_bounded(monkeypatch):
    module = reactor_module('asyncore')
    monkeypatch.setattr(module, '_dispatcher_map', {})
    loop = module.AsyncoreLoop()
    delivered, wakes = [], []
    monkeypatch.setattr(loop, 'wake_loop', lambda: wakes.append(len(delivered)))
    try:
        loop._management_callbacks.extend(lambda: delivered.append(True) for _ in range(130))
        loop._dispatch_management()
        assert len(delivered) == 64
        assert len(loop._management_callbacks) == 66
        assert wakes == [64]
        loop._dispatch_management()
        assert len(delivered) == 128
        assert wakes == [64, 128]
        loop._dispatch_management()
        assert len(delivered) == 130
        assert not loop._management_callbacks
        assert wakes == [64, 128]
    finally:
        loop._cleanup()


def test_asyncore_dispatch_accepted_during_runner_failure_is_delivered(monkeypatch):
    module = reactor_module('asyncore')
    monkeypatch.setattr(module, '_dispatcher_map', {})
    entered, fail, delivered = Event(), Event(), Event()
    class Dispatcher:
        def validate(self):
            pass
        def notify_loop(self):
            pass
        def loop(self, timeout):
            entered.set()
            assert fail.wait(5)
            raise RuntimeError('dispatcher failed')
        def close(self):
            pass
    monkeypatch.setattr(module.AsyncoreLoop, '_loop_dispatch_class', Dispatcher)
    loop = module.AsyncoreLoop()
    observations = []
    try:
        loop.maybe_start()
        assert entered.wait(5)
        def completion():
            observations.append(get_ident())
            delivered.set()
        loop.call_soon(completion)
        fail.set()
        loop._thread.join(5)
        assert delivered.is_set()
        assert observations == [loop._thread.ident]
        with pytest.raises(RuntimeError, match='shutting down'):
            loop.call_soon(lambda: None)
        assert not loop._management_callbacks
    finally:
        fail.set()
        loop._cleanup()


def test_asyncore_ordinary_connection_recovers_without_reviving_management(monkeypatch):
    import socket
    module = reactor_module('asyncore')
    monkeypatch.setattr(module, '_dispatcher_map', {})
    entered, fail, finish_failure, recovered = [Event() for _ in range(4)]
    class Dispatcher:
        failed = False
        def validate(self):
            pass
        def notify_loop(self):
            pass
        def loop(self, timeout):
            if fail.is_set() and not self.failed:
                entered.set()
                assert finish_failure.wait(5)
                self.failed = True
                raise OSError('transient poll failure')
            if self.failed:
                recovered.set()
            Event().wait(0.001)
        def close(self):
            pass
    monkeypatch.setattr(module.AsyncoreLoop, '_loop_dispatch_class', Dispatcher)
    loop = module.AsyncoreLoop()
    monkeypatch.setattr(module, '_global_loop', loop)
    # This channel predates service loss and must never become usable again.
    channel = None
    ours, peer = socket.socketpair()
    connection = None
    try:
        channel = ReactorChannel(module.AsyncoreConnection)
        old_runner = loop._thread
        fail.set()
        assert entered.wait(5)
        delivered = Event()
        channel.dispatch(delivered.set)
        finish_failure.set()
        old_runner.join(5)
        assert not old_runner.is_alive()
        assert delivered.is_set()
        with pytest.raises(RuntimeError):
            channel.dispatch(lambda: None)
        monkeypatch.setattr(module.AsyncoreConnection, '_connect_socket',
                            lambda self: setattr(self, '_socket', ours))
        monkeypatch.setattr(module.AsyncoreConnection, '_send_options_message', lambda self: None)
        connection = module.AsyncoreConnection(connect_timeout=1)
        assert recovered.wait(5)
        assert loop._thread is not old_runner
        assert loop._thread.is_alive()
        assert module._dispatcher_map[ours.fileno()] is connection
        assert connection.socket is ours
        with pytest.raises(RuntimeError):
            channel.dispatch(lambda: None)
        with pytest.raises(RuntimeError):
            ReactorChannel(module.AsyncoreConnection)
        assert not loop._management_callbacks
    finally:
        fail.set()
        finish_failure.set()
        if channel is not None:
            channel.close()
        if connection is not None:
            connection.close()
        loop._cleanup()
        ours.close()
        peer.close()


@pytest.mark.parametrize('reactor', ['asyncio', 'libev', 'asyncore'])
@pytest.mark.parametrize('count', [1, 130])
def test_reactor_shutdown_delivers_already_accepted_callbacks(monkeypatch, reactor, count):
    module = reactor_module(reactor)
    entered, release, delivered, stopping = [Event() for _ in range(4)]
    if reactor == 'asyncore':
        monkeypatch.setattr(module, '_dispatcher_map', {})
    if reactor == 'asyncio':
        connection = type('IsolatedConnection', (module.AsyncioConnection,),
                          {'_loop': None, '_loop_thread': None})
        connection.initialize_reactor()
        dispatch, runner = connection._management_dispatch, connection._loop_thread
        def stop():
            stopping.set()
        def cleanup():
            if runner.is_alive():
                connection._loop.call_soon_threadsafe(connection._loop.stop)
            runner.join(5)
            assert not runner.is_alive()
            connection._loop.close()
    else:
        loop = getattr(module, {'libev': 'LibevLoop', 'asyncore': 'AsyncoreLoop'}[reactor])()
        dispatch = loop.call_soon
        def stop():
            stopping.set()
            loop._cleanup()
        def cleanup():
            if reactor == 'libev':
                loop._cleanup_unpublished()
            else:
                loop._cleanup()
        runner = None
    observations = []
    def hold():
        entered.set()
        assert release.wait(5)
        if reactor == 'asyncio':
            # Stop in the current ready-queue turn, before any of the drain
            # handles queued while this ordinary loop callback was held.
            connection._loop.stop()
    stopper = None
    try:
        if reactor == 'asyncio':
            connection._loop.call_soon_threadsafe(hold)
        else:
            dispatch(hold)
        assert entered.wait(5)
        def completion():
            observations.append(get_ident())
            if len(observations) == count:
                delivered.set()
        for _ in range(count):
            dispatch(completion)
        if reactor == 'asyncio':
            stop()
        else:
            runner = loop._thread
            stopper = Thread(target=stop)
            stopper.start()
            assert stopping.wait(5)
            # Cleanup commits shutdown under the admission lock before join.
            for _ in range(500):
                with (loop._lock if reactor == 'libev' else loop._state_lock):
                    if loop._shutdown:
                        break
                Event().wait(0.001)
            assert loop._shutdown
        release.set()
        assert delivered.wait(5)
        runner.join(5)
        assert not runner.is_alive()
        if stopper is not None:
            stopper.join(5)
            assert not stopper.is_alive()
        assert observations == [runner.ident] * count
        with pytest.raises(RuntimeError):
            dispatch(lambda: None)
    finally:
        release.set()
        if stopper is not None:
            stopper.join(5)
        cleanup()


@pytest.mark.parametrize('case,args,fail_after_delivery', [
    ('test_libev_dispatch_enqueued_during_idle_exit_is_not_lost', (), False),
    ('test_libev_retired_runner_does_not_claim_replacement_work', (False,), False),
    ('test_libev_keeper_arming_cannot_outlive_cleanup', ('acquire',), False),
    ('test_libev_keeper_arming_cannot_outlive_cleanup', ('keepalive',), False),
    ('test_libev_borrow_release_completes_during_cleanup', ('during-release',), False),
    ('test_libev_borrow_release_completes_during_cleanup', ('before-release',), False),
    ('test_startup_failure_does_not_acknowledge_concurrent_dispatch', ('libev',), False),
    ('test_reactor_shutdown_delivers_already_accepted_callbacks', ('libev', 1), False),
    ('test_reactor_shutdown_delivers_already_accepted_callbacks', ('libev', 130), False),
    ('test_libev_dispatch_enqueued_during_idle_exit_is_not_lost', (), True),
])
def test_private_libev_fixture_teardown_releases_native_callbacks(monkeypatch, case, args, fail_after_delivery):
    import gc
    import weakref
    module = reactor_module('libev')
    factory = module.LibevLoop
    references = []
    def tracked():
        loop = factory()
        references.append(weakref.ref(loop))
        if fail_after_delivery:
            dispatch = loop.call_soon
            def fail(callback):
                delivered = Event()
                def observed():
                    callback()
                    delivered.set()
                dispatch(observed)
                assert delivered.wait(5)
                raise AssertionError('forced fixture failure after native delivery')
            fixture_patches.setattr(loop, 'call_soon', fail)
        return loop
    try:
        with monkeypatch.context() as fixture_patches:
            fixture_patches.setattr(module, 'LibevLoop', tracked)
            test = globals()[case]
            if fail_after_delivery:
                with pytest.raises(AssertionError, match='forced fixture failure after native delivery'):
                    test(*args)
            elif case == 'test_libev_dispatch_enqueued_during_idle_exit_is_not_lost':
                test(*args)
            else:
                test(fixture_patches, *args)
        gc.collect()
        assert references
        assert all(reference() is None for reference in references), 'private native reactor retained after fixture teardown'
    finally:
        # Keep a failing ownership assertion from retaining its native loop in
        # this test process; this fallback runs after the assertion above.
        for reference in references:
            loop = reference()
            if loop is not None:
                loop._cleanup_unpublished()


@contextmanager
def inherited_descriptor(module, reactor, sock):
    from types import SimpleNamespace
    inherited = module._global_loop
    if reactor == 'libev':
        class Connection:
            _socket = sock
            _read_watcher = _write_watcher = None
            _write_watcher_is_active = False
            deque = deque()
        conn = Connection()
        with inherited._lock:
            inherited._live_conns = inherited._live_conns | {conn}
        try:
            yield conn
        finally:
            with inherited._lock:
                inherited._live_conns = inherited._live_conns - {conn}
    else:
        descriptor = sock.fileno()
        module._dispatcher_map[descriptor] = SimpleNamespace(
            socket=sock, readable=lambda: False, writable=lambda: False)
        try:
            yield
        finally:
            module._dispatcher_map.pop(descriptor, None)


@pytest.mark.parametrize('dispatcher_name', [
    pytest.param('_AsyncorePipeDispatcher', marks=pytest.mark.skipif(
        sys.platform == 'win32', reason='pipe dispatch is not supported by Windows select')),
    '_AsyncoreUDPDispatcher', '_BusyWaitDispatcher'])
def test_inherited_asyncore_descriptor_survives_actual_poll(dispatcher_name):
    import subprocess
    reactor_module('asyncore')
    # A running shared runner also consults the process-global map. Keep the
    # real polling/cleanup regression in its own process, including failures.
    script = (
        'import sys, pytest; '
        'from tests.unit.test_management_reactors import '
        '_assert_inherited_asyncore_descriptor_survives_poll as check; '
        'patch = pytest.MonkeyPatch(); '
        '\ntry: check(patch, sys.argv[1])'
        '\nfinally: patch.undo()')
    result = subprocess.run(
        [sys.executable, '-c', script, dispatcher_name],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30)
    assert result.returncode == 0, result.stdout


def _assert_inherited_asyncore_descriptor_survives_poll(monkeypatch, dispatcher_name):
    import socket
    module = reactor_module('asyncore')
    monkeypatch.setattr(module, '_dispatcher_map', {})
    dispatcher_class = getattr(module, dispatcher_name)
    if dispatcher_name == '_AsyncoreUDPDispatcher':
        class LocalUDPDispatcher(dispatcher_class):
            bind_address = ('127.0.0.1', 0)

            def __init__(self):
                super().__init__()
                # validate() wakes this socket after construction, so keep
                # its actual OS-assigned endpoint as the notify destination.
                self.bind_address = self._socket.getsockname()
        dispatcher_class = LocalUDPDispatcher
    monkeypatch.setattr(module.AsyncoreLoop, '_loop_dispatch_class',
                        dispatcher_class)
    loop = module.AsyncoreLoop()
    monkeypatch.setattr(module, '_global_loop', loop)
    entered, release, polled, initial, delivered = [Event() for _ in range(5)]
    actual_poll = loop._loop_dispatcher.loop
    errors, snapshots = [], []
    def observed_poll(timeout):
        entered.set()
        assert release.wait(5)
        snapshots.append(set(module._dispatcher_map))
        try:
            actual_poll(timeout)
        except BaseException as exc:
            errors.append(exc)
            raise
        finally:
            polled.set()
    monkeypatch.setattr(loop._loop_dispatcher, 'loop', observed_poll)
    ours, peer = socket.socketpair()
    descriptor = ours.fileno()
    try:
        assert isinstance(loop._loop_dispatcher, dispatcher_class)
        loop.call_soon(initial.set)
        assert entered.wait(5)
        with inherited_descriptor(module, 'asyncore', ours):
            assert descriptor in module._dispatcher_map
            release.set()
            assert polled.wait(5)
            assert descriptor in snapshots[0]
            assert initial.wait(5)
            assert errors == [], {
                'errors': errors, 'management_failed': loop._management_failed}
            assert not loop._management_failed
            loop.call_soon(delivered.set)
            assert delivered.wait(5)
    finally:
        release.set()
        try:
            loop._cleanup()
            assert loop._thread is None or not loop._thread.is_alive()
        finally:
            ours.close()
            peer.close()


def test_inherited_libev_descriptor_is_safe_during_parent_prepare(monkeypatch):
    import socket
    module = reactor_module('libev')
    entered, release, prepared = Event(), Event(), Event()
    errors = []
    original_prepare = module.LibevLoop._loop_will_run
    def observed_prepare(loop, watcher):
        try:
            original_prepare(loop, watcher)
        except BaseException as exc:
            errors.append(exc)
            raise
        finally:
            prepared.set()
    monkeypatch.setattr(module.LibevLoop, '_loop_will_run', observed_prepare)
    loop = module.LibevLoop()
    monkeypatch.setattr(module, '_global_loop', loop)
    ours, peer = socket.socketpair()
    def hold():
        entered.set()
        assert release.wait(5)
    try:
        loop.call_soon(hold)
        assert entered.wait(5)
        previous = loop._live_conns
        with inherited_descriptor(module, 'libev', ours) as conn:
            installed = loop._live_conns
            assert conn not in previous
            assert conn in installed
            release.set()
            assert prepared.wait(5)
            assert not errors
        assert conn in installed
        assert conn not in loop._live_conns
    finally:
        release.set()
        try:
            loop._cleanup_unpublished()
            assert loop._thread is None or not loop._thread.is_alive()
        finally:
            ours.close()
            peer.close()


@pytest.mark.parametrize('reactor', ['libev', 'asyncore'])
def test_fork_reinitialization_closes_inherited_descriptors(monkeypatch, reactor):
    import os
    import select
    import signal
    import socket
    if not hasattr(os, 'fork'):
        pytest.skip('fork unavailable')
    module = reactor_module(reactor)
    connection = getattr(module, {'libev': 'LibevConnection',
                                  'asyncore': 'AsyncoreConnection'}[reactor])
    connection.initialize_reactor()
    inherited = module._global_loop
    ours, peer = socket.socketpair()
    try:
        with inherited_descriptor(module, reactor, ours):
            original_factory = getattr(module, {'libev': 'LibevLoop', 'asyncore': 'AsyncoreLoop'}[reactor])
            def checked_factory():
                assert ours.fileno() == -1
                if reactor == 'libev':
                    assert inherited._loop is None
                    assert inherited._notifier is None
                else:
                    assert inherited._loop_dispatcher.write_fd is None
                return original_factory()
            monkeypatch.setattr(module, original_factory.__name__, checked_factory)
            read_fd, write_fd = os.pipe()
            pid = os.fork()
            if pid == 0:
                try:
                    os.close(read_fd)
                    connection.initialize_reactor()
                    os.write(write_fd, b'closed')
                finally:
                    os._exit(0)
            os.close(write_fd)
            try:
                ready, _, _ = select.select([read_fd], [], [], 5)
                if not ready:
                    os.kill(pid, signal.SIGKILL)
                assert ready
                assert os.read(read_fd, 6) == b'closed'
                # The child closed only its descriptor copies.
                ours.send(b'x')
                assert peer.recv(1) == b'x'
            finally:
                os.close(read_fd)
                os.waitpid(pid, 0)
    finally:
        ours.close()
        peer.close()
