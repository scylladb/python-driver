# Copyright DataStax, Inc.
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


import unittest

from concurrent.futures import ThreadPoolExecutor
from itertools import cycle
from unittest.mock import Mock, patch
import time
import threading
from queue import PriorityQueue
import sys
import platform
import uuid

from cassandra.cluster import Cluster, Session
from cassandra.concurrent import (execute_concurrent,
                                  execute_concurrent_async,
                                  execute_concurrent_with_args)
from cassandra.pool import Host
from cassandra.policies import SimpleConvictionPolicy
from tests.unit.utils import mock_session_pools
import pytest


class MockResponseResponseFuture():
    """
    This is a mock ResponseFuture. It is used to allow us to hook into the underlying session
    and invoke callback with various timing.
    """

    _query_trace = None
    _col_names = None
    _col_types = None

    # a list pending callbacks, these will be prioritized in reverse or normal orderd
    pending_callbacks = PriorityQueue()

    def __init__(self, reverse):

        # if this is true invoke callback in the reverse order then what they were insert
        self.reverse = reverse
        # hardcoded to avoid paging logic
        self.has_more_pages = False

        if(reverse):
            self.priority = 100
        else:
            self.priority = 0

    def add_callback(self, fn, *args, **kwargs):
        """
        This is used to add a callback our pending list of callbacks.
        If reverse is specified we will invoke the callback in the opposite order that we added it
        """
        time_added = time.time()
        self.pending_callbacks.put((self.priority, (fn, args, kwargs, time_added)))
        if not reversed:
            self.priority += 1
        else:
            self.priority -= 1

    def add_callbacks(self, callback, errback,
                      callback_args=(), callback_kwargs=None,
                      errback_args=(), errback_kwargs=None):

        self.add_callback(callback, *callback_args, **(callback_kwargs or {}))

    def get_next_callback(self):
        return self.pending_callbacks.get()

    def has_next_callback(self):
        return not self.pending_callbacks.empty()

    def has_more_pages(self):
        return False

    def clear_callbacks(self):
        return


class DeferredResponseFuture(object):
    """Minimal ResponseFuture whose outcome is controlled by a test."""

    _query_trace = None
    _col_names = None
    _col_types = None
    has_more_pages = False

    def __init__(self):
        self._callback = None
        self._errback = None

    def add_callbacks(self, callback, errback,
                      callback_args=(), callback_kwargs=None,
                      errback_args=(), errback_kwargs=None):
        self._callback = (
            callback, callback_args, callback_kwargs or {})
        self._errback = (
            errback, errback_args, errback_kwargs or {})

    def succeed(self, result):
        callback, args, kwargs = self._callback
        callback(result, *args, **kwargs)

    def fail(self, exc):
        errback, args, kwargs = self._errback
        errback(exc, *args, **kwargs)

    def clear_callbacks(self):
        self._callback = None
        self._errback = None


class ImmediateFailedResponseFuture(object):

    def __init__(self, error):
        self.error = error

    def add_callbacks(self, callback, errback,
                      callback_args=(), callback_kwargs=None,
                      errback_args=(), errback_kwargs=None):
        errback(self.error, *errback_args, **(errback_kwargs or {}))


class TimedCallableInvoker(threading.Thread):
    """
    This is a local thread which is runs and invokes all the callbacks on the pending callback queue.
    The slowdown flag can used to invoke random slowdowns in our simulate queries.
    """
    def __init__(self, handler, slowdown=False):
        super(TimedCallableInvoker, self).__init__()
        self.slowdown = slowdown
        self._stopper = threading.Event()
        self.handler = handler

    def stop(self):
        self._stopper.set()

    def stopped(self):
        return self._stopper.isSet()

    def run(self):
        while(not self.stopped()):
            if(self.handler.has_next_callback()):
                pending_callback = self.handler.get_next_callback()
                priority_num = pending_callback[0]
                if (priority_num % 10) == 0 and self.slowdown:
                    self._stopper.wait(.1)
                callback_args = pending_callback[1]
                fn, args, kwargs, time_added = callback_args
                fn([time_added], *args, **kwargs)
            self._stopper.wait(.001)
        return

class ConcurrencyTest((unittest.TestCase)):

    def enable_async_submit(self, session, max_workers=2):
        executor = ThreadPoolExecutor(max_workers=max_workers)
        self.addCleanup(executor.shutdown, wait=True)
        session.submit.side_effect = executor.submit
        return executor

    @staticmethod
    def wait_for_response_futures(response_futures, count, timeout=1.0):
        deadline = time.monotonic() + timeout
        while len(response_futures) < count and time.monotonic() < deadline:
            time.sleep(0.001)
        assert len(response_futures) == count

    def deferred_session(self):
        response_futures = []

        def execute_async(*args, **kwargs):
            response_future = DeferredResponseFuture()
            response_futures.append(response_future)
            return response_future

        session = Mock()
        session.execute_async.side_effect = execute_async
        self.enable_async_submit(session)
        return session, response_futures

    def test_async_returns_before_requests_complete(self):
        session, response_futures = self.deferred_session()

        result_future = execute_concurrent_async(
            session, [("SELECT value FROM test WHERE key=?", (1,))])

        self.wait_for_response_futures(response_futures, 1)
        assert len(response_futures) == 1
        assert not result_future.done()

        response_futures[0].succeed(["value"])
        results = result_future.result(timeout=1.0)
        assert len(results) == 1
        assert results[0].success
        assert results[0].result_or_exc.one() == "value"

    def test_async_results_retain_input_order(self):
        session, response_futures = self.deferred_session()
        statements_and_params = [
            ("SELECT value FROM test WHERE key=?", (i,)) for i in range(3)
        ]

        result_future = execute_concurrent_async(
            session, statements_and_params, concurrency=3)
        self.wait_for_response_futures(response_futures, 3)
        assert len(response_futures) == 3

        response_futures[2].succeed(["two"])
        response_futures[0].succeed(["zero"])
        assert not result_future.done()
        response_futures[1].succeed(["one"])

        results = result_future.result(timeout=1.0)
        assert [result.success for result in results] == [True, True, True]
        assert [result.result_or_exc.one() for result in results] == [
            "zero", "one", "two"]

    def test_async_empty_input_returns_completed_future(self):
        session = Mock()
        self.enable_async_submit(session)

        result_future = execute_concurrent_async(session, iter(()))

        assert result_future.result(timeout=1.0) == []
        assert result_future.done()
        session.execute_async.assert_not_called()

    def test_async_rejects_non_positive_concurrency(self):
        session = Mock()

        for concurrency in (0, -1):
            with self.subTest(concurrency=concurrency):
                with pytest.raises(ValueError, match="greater than 0"):
                    execute_concurrent_async(
                        session, [("SELECT value FROM test", None)],
                        concurrency=concurrency)

        session.execute_async.assert_not_called()

    def test_async_rejects_non_integer_concurrency(self):
        session = Mock()

        for concurrency in (1.5, float('nan'), "1"):
            with self.subTest(concurrency=concurrency):
                with pytest.raises(TypeError):
                    execute_concurrent_async(
                        session, [("SELECT value FROM test", None)],
                        concurrency=concurrency)

        session.submit.assert_not_called()
        session.execute_async.assert_not_called()

    def test_async_rejects_shutdown_session(self):
        session = Mock()
        session.submit.return_value = None

        result_future = execute_concurrent_async(
            session, [("SELECT value FROM test", None)])

        with pytest.raises(RuntimeError, match="shut down session"):
            result_future.result(timeout=1.0)
        session.execute_async.assert_not_called()

    def test_async_preserves_success_when_session_shuts_down_after_dispatch(self):
        session, response_futures = self.deferred_session()
        result_future = execute_concurrent_async(
            session, [("SELECT value FROM test", None)])
        self.wait_for_response_futures(response_futures, 1)

        completion_thread = []
        callback_done = threading.Event()

        def on_done(completed):
            completion_thread.append(threading.get_ident())
            callback_done.set()

        result_future.add_done_callback(on_done)
        session.submit.side_effect = None
        session.submit.return_value = None

        response_thread = threading.get_ident()
        response_futures[0].succeed(["value"])

        assert result_future.result(timeout=1.0)[0].success
        assert callback_done.wait(1.0)
        assert completion_thread != [response_thread]

    def test_async_reports_shutdown_off_response_thread_when_work_remains(self):
        session, response_futures = self.deferred_session()
        result_future = execute_concurrent_async(
            session,
            [("SELECT value FROM test", None)] * 2,
            concurrency=1)
        self.wait_for_response_futures(response_futures, 1)

        completion_thread = []
        callback_done = threading.Event()

        def on_done(completed):
            completion_thread.append(threading.get_ident())
            callback_done.set()

        result_future.add_done_callback(on_done)
        session.submit.side_effect = None
        session.submit.return_value = None

        response_thread = threading.get_ident()
        response_futures[0].succeed(["value"])

        with pytest.raises(RuntimeError, match="shut down session"):
            result_future.result(timeout=1.0)
        assert callback_done.wait(1.0)
        assert completion_thread != [response_thread]

    def test_async_fail_fast_completes_once(self):
        session, response_futures = self.deferred_session()
        statements_and_params = [
            ("SELECT value FROM test WHERE key=?", (i,)) for i in range(4)
        ]
        error = RuntimeError("first failure")
        late_error = RuntimeError("late failure")

        result_future = execute_concurrent_async(
            session, statements_and_params, concurrency=3,
            raise_on_first_error=True)
        self.wait_for_response_futures(response_futures, 3)
        completions = []
        callback_threads = []
        callback_done = threading.Event()

        def on_done(completed):
            completions.append(completed.exception())
            callback_threads.append(threading.get_ident())
            callback_done.set()

        result_future.add_done_callback(on_done)

        response_thread = threading.get_ident()
        response_futures[0].fail(error)
        assert result_future.exception(timeout=1.0) is error
        assert callback_done.wait(1.0)
        assert completions == [error]
        assert callback_threads != [response_thread]

        # In-flight callbacks are allowed to finish, but cannot complete the
        # aggregate Future again or schedule the remaining statement.
        response_futures[1].succeed(["late success"])
        response_futures[2].fail(late_error)
        assert completions == [error]
        assert result_future.exception() is error
        assert session.execute_async.call_count == 3

    def test_async_broken_iterator_during_initial_fill(self):
        session, response_futures = self.deferred_session()
        error = RuntimeError("broken input")

        def statements():
            yield "SELECT value FROM test", None
            raise error

        result_future = execute_concurrent_async(
            session, statements(), concurrency=2)
        completions = []
        result_future.add_done_callback(
            lambda completed: completions.append(completed.exception()))

        assert result_future.exception(timeout=1.0) is error
        assert completions == [error]

        # Request scheduled before iteration failed may still finish.
        response_futures[0].succeed(["late success"])
        assert completions == [error]
        assert result_future.exception() is error

    def test_async_broken_iterator_while_replenishing(self):
        session, response_futures = self.deferred_session()
        error = RuntimeError("broken input")

        def statements():
            yield "SELECT value FROM test WHERE key=?", (0,)
            yield "SELECT value FROM test WHERE key=?", (1,)
            raise error

        result_future = execute_concurrent_async(
            session, statements(), concurrency=2)
        self.wait_for_response_futures(response_futures, 2)
        completions = []
        result_future.add_done_callback(
            lambda completed: completions.append(completed.exception()))
        assert not result_future.done()

        response_futures[0].succeed(["zero"])
        assert result_future.exception(timeout=1.0) is error
        assert completions == [error]

        # Second request was already in flight when iteration failed.
        response_futures[1].succeed(["late success"])
        assert completions == [error]
        assert result_future.exception() is error

    def test_async_aggregate_future_cannot_be_cancelled(self):
        session, response_futures = self.deferred_session()

        result_future = execute_concurrent_async(
            session, [("SELECT value FROM test", None)])

        assert result_future.running()
        assert not result_future.cancel()
        self.wait_for_response_futures(response_futures, 1)
        response_futures[0].succeed(["value"])
        assert result_future.result(timeout=1.0)[0].success

    def test_async_no_recursion_on_synchronous_errback(self):
        error = RuntimeError("immediate failure")

        class AlreadyFailedFuture(object):
            def add_callbacks(self, callback, errback,
                              callback_args=(), callback_kwargs=None,
                              errback_args=(), errback_kwargs=None):
                errback(error, *errback_args, **(errback_kwargs or {}))

        session = Mock()
        session.execute_async.return_value = AlreadyFailedFuture()
        self.enable_async_submit(session)
        count = sys.getrecursionlimit()

        result_future = execute_concurrent_async(
            session, [("SELECT value FROM test", None)] * count,
            concurrency=1, raise_on_first_error=False)

        results = result_future.result(timeout=2.0)
        assert len(results) == count
        assert all(not result.success for result in results)
        assert all(result.result_or_exc is error for result in results)

    def test_async_returns_before_input_iterator_unblocks(self):
        iterator_blocked = threading.Event()
        release_iterator = threading.Event()
        call_returned = threading.Event()
        error = RuntimeError("immediate failure")
        stopped = RuntimeError("input stopped")
        result_holder = {}

        def statements():
            yield "SELECT value FROM test", None
            iterator_blocked.set()
            release_iterator.wait(2.0)
            raise stopped

        session = Mock()
        session.execute_async.return_value = ImmediateFailedResponseFuture(error)
        self.enable_async_submit(session, max_workers=1)

        def invoke():
            result_holder['future'] = execute_concurrent_async(
                session, statements(), concurrency=1,
                raise_on_first_error=False)
            call_returned.set()

        caller = threading.Thread(target=invoke)
        caller.start()
        try:
            assert call_returned.wait(1.0)
            assert iterator_blocked.wait(1.0)
        finally:
            release_iterator.set()
            caller.join(1.0)

        assert not caller.is_alive()
        assert result_holder['future'].exception(timeout=1.0) is stopped

    def test_async_pump_yields_during_unbounded_synchronous_failures(self):
        stop = threading.Event()
        marker = threading.Event()
        call_returned = threading.Event()
        error = RuntimeError("immediate failure")
        stopped = RuntimeError("input stopped")
        result_holder = {}

        def statements():
            while not stop.is_set():
                yield "SELECT value FROM test", None
            raise stopped

        session = Mock()
        session.execute_async.return_value = ImmediateFailedResponseFuture(error)
        executor = self.enable_async_submit(session, max_workers=1)

        def invoke():
            result_holder['future'] = execute_concurrent_async(
                session, statements(), concurrency=1,
                raise_on_first_error=False)
            call_returned.set()

        caller = threading.Thread(target=invoke)
        caller.start()
        try:
            assert call_returned.wait(1.0)
            executor.submit(marker.set)
            # A bounded pump turn lets unrelated cluster-executor work run
            # even when every child request completes synchronously.
            assert marker.wait(1.0)
        finally:
            stop.set()
            caller.join(1.0)

        assert not caller.is_alive()
        assert result_holder['future'].exception(timeout=1.0) is stopped

    def test_session_concurrent_methods_delegate(self):
        session = Mock()
        statements_and_params = [("SELECT value FROM test", None)]
        profile = object()
        expected = object()

        with patch("cassandra.concurrent.execute_concurrent",
                   return_value=expected) as execute:
            result = Session.execute_concurrent(
                session, statements_and_params, 7, False, True, profile)
            assert result is expected
            execute.assert_called_once_with(
                session, statements_and_params, 7, False, True, profile)

        with patch("cassandra.concurrent.execute_concurrent_with_args",
                   return_value=expected) as execute_with_args:
            result = Session.execute_concurrent_with_args(
                session, "statement", [(1,)], concurrency=7)
            assert result is expected
            execute_with_args.assert_called_once_with(
                session, "statement", [(1,)], concurrency=7)

        with patch("cassandra.concurrent.execute_concurrent_async",
                   return_value=expected) as execute_async:
            result = Session.execute_concurrent_async(
                session, statements_and_params, 7, True, profile)
            assert result is expected
            execute_async.assert_called_once_with(
                session, statements_and_params, 7, True, profile)

    def test_fail_fast_stops_scheduling_after_late_in_flight_success(self):
        for results_generator in (False, True):
            with self.subTest(results_generator=results_generator):
                session, response_futures = self.deferred_session()
                error = RuntimeError("first failure")
                completed = threading.Event()
                observed = []

                def invoke():
                    try:
                        results = execute_concurrent(
                            session,
                            [("INSERT INTO test (key) VALUES (?)", (i,))
                             for i in range(5)],
                            concurrency=2,
                            raise_on_first_error=True,
                            results_generator=results_generator)
                        if results_generator:
                            list(results)
                    except Exception as exc:
                        observed.append(exc)
                    finally:
                        completed.set()

                caller = threading.Thread(target=invoke)
                caller.start()
                self.wait_for_response_futures(response_futures, 2)

                response_futures[0].fail(error)
                assert completed.wait(1.0)
                assert observed == [error]

                # A request already in flight may still complete, but it must
                # not schedule another statement after fail-fast termination.
                response_futures[1].succeed(["late success"])
                assert session.execute_async.call_count == 2
                caller.join(1.0)
                assert not caller.is_alive()

    def test_results_ordering_forward(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorListResults
        when queries complete in the order they were executed.
        """
        self.insert_and_validate_list_results(False, False)

    def test_results_ordering_reverse(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorListResults
        when queries complete in the reverse order they were executed.
        """
        self.insert_and_validate_list_results(True, False)

    def test_results_ordering_forward_slowdown(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorListResults
        when queries complete in the order they were executed, with slow queries mixed in.
        """
        self.insert_and_validate_list_results(False, True)

    def test_results_ordering_reverse_slowdown(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorListResults
        when queries complete in the reverse order they were executed, with slow queries mixed in.
        """
        self.insert_and_validate_list_results(True, True)

    def test_results_ordering_forward_generator(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorGenResults
        when queries complete in the order they were executed.
        """
        self.insert_and_validate_list_generator(False, False)

    def test_results_ordering_reverse_generator(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorGenResults
        when queries complete in the reverse order they were executed.
        """
        self.insert_and_validate_list_generator(True, False)

    def test_results_ordering_forward_generator_slowdown(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorGenResults
        when queries complete in the order they were executed, with slow queries mixed in.
        """
        self.insert_and_validate_list_generator(False, True)

    def test_results_ordering_reverse_generator_slowdown(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorGenResults
        when queries complete in the reverse order they were executed, with slow queries mixed in.
        """
        self.insert_and_validate_list_generator(True, True)

    def insert_and_validate_list_results(self, reverse, slowdown):
        """
        This utility method will execute submit various statements for execution using the ConcurrentExecutorListResults,
        then invoke a separate thread to execute the callback associated with the futures registered
        for those statements. The parameters will toggle various timing, and ordering changes.
        Finally it will validate that the results were returned in the order they were submitted
        :param reverse: Execute the callbacks in the opposite order that they were submitted
        :param slowdown: Cause intermittent queries to perform slowly
        """
        our_handler = MockResponseResponseFuture(reverse=reverse)
        mock_session = Mock()
        statements_and_params = zip(cycle(["INSERT INTO test3rf.test (k, v) VALUES (%s, 0)"]),
                                    [(i, ) for i in range(100)])
        mock_session.execute_async.return_value = our_handler

        t = TimedCallableInvoker(our_handler, slowdown=slowdown)
        t.start()
        results = execute_concurrent(mock_session, statements_and_params)

        while(not our_handler.pending_callbacks.empty()):
            time.sleep(.01)
        t.stop()
        self.validate_result_ordering(results)

    def insert_and_validate_list_generator(self, reverse, slowdown):
        """
        This utility method will execute submit various statements for execution using the ConcurrentExecutorGenResults,
        then invoke a separate thread to execute the callback associated with the futures registered
        for those statements. The parameters will toggle various timing, and ordering changes.
        Finally it will validate that the results were returned in the order they were submitted
        :param reverse: Execute the callbacks in the opposite order that they were submitted
        :param slowdown: Cause intermittent queries to perform slowly
        """
        our_handler = MockResponseResponseFuture(reverse=reverse)
        mock_session = Mock()
        statements_and_params = zip(cycle(["INSERT INTO test3rf.test (k, v) VALUES (%s, 0)"]),
                                    [(i, ) for i in range(100)])
        mock_session.execute_async.return_value = our_handler

        t = TimedCallableInvoker(our_handler, slowdown=slowdown)
        t.start()
        try:
            results = execute_concurrent(mock_session, statements_and_params, results_generator=True)
            self.validate_result_ordering(results)
        finally:
            t.stop()

    def validate_result_ordering(self, results):
        """
        This method will validate that the timestamps returned from the result are in order. This indicates that the
        results were returned in the order they were submitted for execution
        :param results:
        """
        last_time_added = 0
        for success, result in results:
            assert success
            current_time_added = list(result)[0]

            #Windows clock granularity makes this equal most of the times
            if "Windows" in platform.system():
                assert last_time_added <= current_time_added
            else:
                assert last_time_added < current_time_added
            last_time_added = current_time_added

    @mock_session_pools
    def test_recursion_limited(self):
        """
        Verify that recursion is controlled when raise_on_first_error=False and something is wrong with the query.

        PYTHON-585
        """
        max_recursion = sys.getrecursionlimit()
        s = Session(Cluster(), [Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())])
        with pytest.raises(TypeError):
            execute_concurrent_with_args(s, "doesn't matter", [('param',)] * max_recursion, raise_on_first_error=True)

        results = execute_concurrent_with_args(s, "doesn't matter", [('param',)] * max_recursion, raise_on_first_error=False)  # previously
        assert len(results) == max_recursion
        for r in results:
            assert not r[0]
            assert isinstance(r[1], TypeError)

    def test_no_recursion_on_synchronous_errback(self):
        """
        Verify that execute_concurrent does not blow the stack when every
        future completes with an error *before* add_callbacks is called
        (i.e. the errback fires synchronously inside add_callbacks).

        This exercises a different code path from test_recursion_limited:
        that test covers execute_async raising an exception, while this one
        covers execute_async returning a future whose errback fires inline.
        """
        count = sys.getrecursionlimit()
        error = Exception("immediate failure")

        class AlreadyFailedFuture:
            """A future that already has _final_exception set."""
            _query_trace = None
            _col_names = None
            _col_types = None
            has_more_pages = False

            def add_callback(self, fn, *args, **kwargs):
                pass

            def add_errback(self, fn, *args, **kwargs):
                # Fire errback synchronously, mimicking a future that
                # completed before add_callbacks was called.
                fn(error, *args, **kwargs)

            def add_callbacks(self, callback, errback,
                              callback_args=(), callback_kwargs=None,
                              errback_args=(), errback_kwargs=None):
                self.add_callback(callback, *callback_args, **(callback_kwargs or {}))
                self.add_errback(errback, *errback_args, **(errback_kwargs or {}))

            def clear_callbacks(self):
                pass

        mock_session = Mock()
        mock_session.execute_async.return_value = AlreadyFailedFuture()

        statements_and_params = [("SELECT 1", ())] * count
        results = execute_concurrent(mock_session, statements_and_params,
                                     raise_on_first_error=False)

        assert len(results) == count
        for success, result in results:
            assert not success
            assert result is error
