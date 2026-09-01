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

from itertools import cycle
from unittest.mock import Mock
import time
import threading
from queue import PriorityQueue
import sys
import platform
import uuid

from cassandra.cluster import Cluster, Session
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args
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

def _call_with_hang_guard(fn, *args, deadline=15, **kwargs):
    """
    Run ``fn(*args, **kwargs)`` on a background thread and return/raise
    whatever it returns/raises, but never block the test suite forever if
    it hangs.

    This replaces ``@pytest.mark.timeout`` for tests exercising the
    submitter thread: the tests themselves are fully deterministic (no
    sleeps or polling), and ``deadline`` is only a diagnostic upper bound
    for a genuine deadlock/hang, not a synchronization mechanism -- it
    does not affect what passes or fails otherwise. Doing this ourselves,
    rather than relying on pytest-timeout's platform-specific hang-killing
    (signal-based on some platforms/interpreters, thread-based with
    ``os._exit`` elsewhere), gives a plain, portable ``AssertionError``
    with a clear message instead of interpreter-dependent behavior.
    """
    outcome = {}

    def target():
        try:
            outcome['result'] = fn(*args, **kwargs)
        except BaseException as exc:
            outcome['exception'] = exc

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(deadline)
    assert not t.is_alive(), (
        "%r did not complete within %ss -- the submitter thread likely "
        "hung instead of surfacing an error" % (fn, deadline)
    )
    if 'exception' in outcome:
        raise outcome['exception']
    return outcome.get('result')


class ConcurrencyTest((unittest.TestCase)):

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

    def test_submitter_thread_survives_broken_iterable(self):
        """
        ConcurrentExecutorListResults dispatches follow-up requests from a
        dedicated submitter thread that pulls from the caller's
        statements_and_parameters iterable. If that iterable raises
        something other than StopIteration (e.g. a generator that blows up
        mid-stream), the submitter thread must not die silently: nothing
        else ever advances `_current`/`_exec_count` or wakes `_results()`
        after the initial batch, so an unhandled exception there would hang
        execute_concurrent() forever instead of surfacing an error.

        Run through ``_call_with_hang_guard`` instead of
        ``@pytest.mark.timeout``: the test itself is deterministic (the
        mock future resolves synchronously, no sleeps), the guard is only
        there in case a regression reintroduces a hang.
        """
        class ImmediateFuture:
            _query_trace = None
            _col_names = None
            _col_types = None
            has_more_pages = False

            def add_callbacks(self, callback, errback,
                              callback_args=(), callback_kwargs=None,
                              errback_args=(), errback_kwargs=None):
                # Fire the success callback synchronously and immediately,
                # forcing the submitter thread to keep pulling from the
                # iterable well past the initial batch.
                callback("row", *callback_args, **(callback_kwargs or {}))

            def clear_callbacks(self):
                pass

        def broken_statements():
            for i in range(5):
                yield ("SELECT 1", (i,))
            raise ValueError("boom from user iterable")

        mock_session = Mock()
        mock_session.execute_async.side_effect = lambda *a, **kw: ImmediateFuture()

        with pytest.raises(ValueError, match="boom from user iterable"):
            _call_with_hang_guard(execute_concurrent, mock_session, broken_statements(),
                                  concurrency=2, raise_on_first_error=False)

    def test_submitter_surfaces_base_exception_from_iterable(self):
        """
        The submitter thread only advances ``_current``/``_exec_count`` and
        wakes ``_results()`` after the initial batch, so anything that kills
        it silently hangs execute_concurrent() forever. The iterable is
        pulled with ``next()`` inside the submitter, so a ``BaseException``
        raised there (e.g. GeneratorExit when the caller's generator is
        closed, or KeyboardInterrupt) must be recorded as fatal and
        re-raised by the calling thread rather than vanishing with the
        thread. ``except BaseException`` (not just ``Exception``) keeps the
        failure surfaced to the caller.

        Run through ``_call_with_hang_guard`` instead of
        ``@pytest.mark.timeout`` -- see
        ``test_submitter_thread_survives_broken_iterable`` for why.
        """
        class ImmediateFuture:
            _query_trace = None
            _col_names = None
            _col_types = None
            has_more_pages = False

            def add_callbacks(self, callback, errback,
                              callback_args=(), callback_kwargs=None,
                              errback_args=(), errback_kwargs=None):
                callback("row", *callback_args, **(callback_kwargs or {}))

            def clear_callbacks(self):
                pass

        def raising_statements():
            for i in range(5):
                yield ("SELECT 1", (i,))
            raise GeneratorExit("generator closed mid-stream")

        mock_session = Mock()
        mock_session.execute_async.side_effect = lambda *a, **kw: ImmediateFuture()

        with pytest.raises(GeneratorExit):
            _call_with_hang_guard(execute_concurrent, mock_session, raising_statements(),
                                  concurrency=2, raise_on_first_error=False)

    def test_fail_fast_stops_submitter_promptly(self):
        """
        Regression test for a lost-wakeup + missing-check bug that broke the
        ``raise_on_first_error=True`` ("fail-fast") contract.

        Two bugs combined to break fail-fast:

        1. ``_put_result``'s fail-fast path takes ``self._condition`` and
           calls ``notify()`` to wake the main thread up immediately -- but
           if the failure happens synchronously during the *initial* batch
           dispatched by ``execute()`` (which itself holds
           ``self._condition`` while dispatching), the main thread has not
           reached ``_results()``'s ``wait()`` yet. ``Condition.notify()``
           only wakes threads already parked in ``wait()``, so that
           notification is silently dropped, and nothing else was checking
           for the already-recorded exception before blocking.
        2. ``_submitter_loop``'s dispatch path only checked ``stop_event``
           before pulling more items from the caller's iterable and
           dispatching them -- never ``_fail_fast``/``_exception`` -- so
           even once the main thread noticed the failure, it had to wait
           for the submitter thread to notice ``stop_event`` on its own
           schedule.

        Net effect: fail-fast degraded into consuming (and dispatching)
        the entire iterable instead of stopping right after the first
        failure -- unbounded for a generator input.

        This test fails the very first dispatched statement synchronously
        (so the failure is recorded during the initial batch, guaranteeing
        the dropped-notify scenario rather than racing for it) against a
        large iterable, and asserts that only a small, bounded number of
        statements were ever pulled from it. Pre-fix, this either hangs
        (caught by ``_call_with_hang_guard``) or consumes/dispatches the
        entire iterable; post-fix, dispatch stops right after the first
        failure.
        """
        consumed = []
        TOTAL_STATEMENTS = 20000

        def many_statements():
            for i in range(TOTAL_STATEMENTS):
                consumed.append(i)
                yield ("SELECT 1", (i,))

        class ImmediateFuture:
            _query_trace = None
            _col_names = None
            _col_types = None
            has_more_pages = False

            def __init__(self, idx):
                self._idx = idx

            def add_callbacks(self, callback, errback,
                              callback_args=(), callback_kwargs=None,
                              errback_args=(), errback_kwargs=None):
                # Fail only the very first statement (idx 0); every other
                # statement succeeds synchronously and immediately, so
                # nothing but the fail-fast logic itself would ever stop
                # dispatch.
                if self._idx == 0:
                    errback(ValueError("boom on first statement"), *errback_args,
                            **(errback_kwargs or {}))
                else:
                    callback("row", *callback_args, **(callback_kwargs or {}))

            def clear_callbacks(self):
                pass

        def fake_execute_async(statement, params, timeout=None, execution_profile=None):
            return ImmediateFuture(params[0])

        mock_session = Mock()
        mock_session.execute_async.side_effect = fake_execute_async

        with pytest.raises(ValueError, match="boom on first statement"):
            _call_with_hang_guard(execute_concurrent, mock_session, many_statements(),
                                  concurrency=5, raise_on_first_error=True)

        assert len(consumed) < 100, (
            "fail-fast did not stop dispatch promptly: consumed %d of %d "
            "statements from the iterable after the first failure"
            % (len(consumed), TOTAL_STATEMENTS)
        )
