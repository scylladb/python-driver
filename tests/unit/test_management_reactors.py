"""Socket-free tests of the actual supported reactor completion adapters."""

import asyncio
import importlib
from concurrent.futures import CancelledError, Future
from threading import Event, Thread, get_ident

import pytest

from cassandra import DependencyException
from cassandra._management import (
    ManagementRuntime, ReactorChannel, execution_context)


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
    try:
        module = importlib.import_module('cassandra.io.' + {
            'asyncio': 'asyncioreactor', 'libev': 'libevreactor',
            'asyncore': 'asyncorereactor'}[request.param])
    except (ImportError, DependencyException) as exc:
        pytest.skip(str(exc))
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
        manager.close()
        manager.join(5)
        application_loop.call_soon_threadsafe(application_loop.stop)
        app_thread.join(5)
        application_loop.close()


def test_libev_dispatch_enqueued_during_idle_exit_is_not_lost():
    try:
        module = importlib.import_module('cassandra.io.libevreactor')
    except (ImportError, DependencyException) as exc:
        pytest.skip(str(exc))
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
        loop._cleanup()


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
