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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
        # Model a descheduled reactor without sleeping or requiring CPU load.
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
    # Exact deadline behavior is checked with a controlled clock; the reactor
    # smoke test must not depend on how quickly a runner schedules its threads.
    clock = Clock()
    monkeypatch.setattr('cassandra.connection.time', clock)
    manager = TimerManager()
    completed = []
    for timeout in timeouts:
        manager.add_timer(Timer(timeout, lambda t=timeout: completed.append(t)))

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
