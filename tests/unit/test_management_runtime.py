from concurrent.futures import wait
from threading import Event, Thread

import pytest

from cassandra import _management as management


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
    ready = Event()
    result = []

    def waiting():
        ready.set()
        result.append(wait([f], timeout=2))

    t = Thread(target=waiting)
    t.start()
    assert ready.wait(1)
    assert f.cancel()
    t.join(3)
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
