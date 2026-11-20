# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import gc
import platform
import weakref
from unittest.mock import Mock

import pytest

from cassandra.connection import Timer, TimerManager
from tests.unit.io import utils


class Clock:
    now = 100

    def time(self):
        return self.now

    monotonic = time


def test_timer_validation_accepts_delayed_scheduling(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(utils, 'time', clock)

    def create_timer(timeout, callback):
        # Model a descheduled reactor without sleeping or CPU load.
        clock.now += timeout + .75
        callback()
        return Mock()

    utils.submit_and_wait_for_completion(None, create_timer, 0, 3, 1, 10)


def test_timer_validation_rejects_early_callbacks():
    def create_timer(timeout, callback):
        callback()
        return Mock()

    with pytest.raises(AssertionError):
        utils.submit_and_wait_for_completion(None, create_timer, 1, 2, 1, 1)


def test_timer_validation_rejects_callback_after_batch_deadline(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(utils, 'time', clock)

    def create_timer(timeout, callback):
        clock.now += 10
        callback()
        return Mock()

    with pytest.raises(AssertionError, match='after batch deadline'):
        utils.submit_and_wait_for_completion(None, create_timer, 0, 1, 1, 1)


def test_timer_validation_rejects_duplicate_callbacks():
    def create_timer(timeout, callback):
        callback()
        callback()
        return Mock()

    with pytest.raises(AssertionError, match='more than once'):
        utils.submit_and_wait_for_completion(None, create_timer, 0, 1, 1, 1)


def test_timer_validation_bounds_missing_callback_wait(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(utils, 'time', clock)
    timer = Mock()

    def create_timer(timeout, callback):
        clock.now += 10
        return timer

    with pytest.raises(pytest.fail.Exception, match='1 callbacks pending'):
        utils.submit_and_wait_for_completion(None, create_timer, 0, 1, 1, 1)
    timer.cancel.assert_called_once_with()


@pytest.mark.parametrize('timeouts', [(0, 1, 2), (2, 1, 0), (2, 0, 1)])
def test_timer_manager_expiration_and_order(monkeypatch, timeouts):
    # Exact deadline behavior uses a controlled clock. The smoke test
    # must not depend on how quickly runners schedule their threads.
    clock = Clock()
    monkeypatch.setattr('cassandra.connection.time', clock)
    manager = TimerManager()
    completed = []
    for timeout in timeouts:
        manager.add_timer(Timer(
            timeout, lambda t=timeout: completed.append(t)))

    assert manager.service_timeouts() == 101
    assert completed == [0]
    clock.now = 100.999
    assert manager.service_timeouts() == 101
    assert completed == [0]
    clock.now = 101
    assert manager.service_timeouts() == 102
    assert completed == [0, 1]
    clock.now = 102
    assert manager.service_timeouts() is None
    assert completed == [0, 1, 2]
    manager.service_timeouts()
    assert completed == [0, 1, 2]


@pytest.mark.skipif(
    platform.python_implementation() != 'CPython',
    reason='needs reference counting to free the owner when it is deleted')
def test_canceled_timer_does_not_keep_its_owner_alive():
    # A ResponseFuture's timeout timer calls back one of the future's own
    # methods, and the future cancels the timer when the request completes.
    # Check that the canceled timer doesn't leave the owner in a reference
    # cycle: such cycles are freed only by the cyclic garbage collector, and
    # a long run of requests piled up so many of them that a single full
    # collection paused all the driver's threads for seconds.
    class Owner:
        def __init__(self):
            self.timer = Timer(10, self.on_timeout)

        def on_timeout(self):
            pass

    owner = Owner()
    owner.timer.cancel()
    ref = weakref.ref(owner)
    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        del owner
        # Check that reference counting alone freed the owner.
        assert ref() is None
    finally:
        if gc_was_enabled:
            gc.enable()


def test_timer_canceled_while_finishing_does_not_fail():
    # cancel() may run on another thread between finish()'s check of
    # canceled and its call of the callback. Check that finish() then
    # neither fails nor calls the callback.
    callback = Mock()
    timer = Timer(0, callback)
    timer.callback = None
    assert timer.finish(timer.end)
    callback.assert_not_called()


def test_timer_canceled_after_finish_read_callback_does_not_fail():
    # cancel() may also clear the callback right after finish() read it.
    # Check that finish() then calls the callback it read, instead of
    # reading the attribute again and calling None.
    class RacingTimer(Timer):
        @property
        def callback(self):
            callback = self._callback
            # Model a concurrent cancel() right after this read.
            self._callback = None
            return callback

        @callback.setter
        def callback(self, callback):
            self._callback = callback

    callback = Mock()
    timer = RacingTimer(0, callback)
    assert timer.finish(timer.end)
    callback.assert_called_once_with()
