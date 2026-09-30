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

from cassandra import OperationTimedOut
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


class _ManualFuture(object):
    """Future completed explicitly by the test, from any thread."""
    _query_trace = None
    _col_names = None
    _col_types = None
    has_more_pages = False

    def __init__(self, params, on_registered):
        self.params = params
        self._on_registered = on_registered

    def add_callbacks(self, callback, errback, callback_args=(), callback_kwargs=None,
                      errback_args=(), errback_kwargs=None):
        self.callback = lambda rows: callback(rows, *callback_args)
        self.errback = lambda exc: errback(exc, *errback_args)
        self._on_registered(self)

    def clear_callbacks(self):
        pass


def _session_with(on_registered):
    # on_registered(future) runs once execute_concurrent has attached its callbacks
    session = Mock()
    session.execute_async.side_effect = lambda stmt, params, **kw: _ManualFuture(params, on_registered)
    return session


class ConcurrentExecutorTest(unittest.TestCase):

    def _run(self, fn, *args, **kwargs):
        # Fail instead of hanging the suite on a deadlock regression.
        out = {}

        def target():
            try:
                out['result'] = fn(*args, **kwargs)
            except BaseException as exc:
                out['exc'] = exc
        t = threading.Thread(target=target, daemon=True)
        t.start()
        t.join(10)
        assert not t.is_alive(), "execute_concurrent hung"
        if 'exc' in out:
            raise out['exc']
        return out['result']

    def test_callbacks_do_not_block_on_submitting_thread(self):
        # Completions arriving while the caller is still submitting must not wait for it.
        for results_generator in (False, True):
            futures = []

            def on_execute(future):
                futures.append(future)
                if len(futures) == 2:
                    t = threading.Thread(target=futures[0].callback, args=(['r'],))
                    t.start()
                    t.join(2)
                    assert not t.is_alive(), "IO-thread callback blocked on the submitter"
                    future.callback(['r'])
                elif len(futures) > 2:
                    future.callback(['r'])
                return future

            results = self._run(lambda: list(execute_concurrent(
                _session_with(on_execute), [("q", (i,)) for i in range(10)],
                concurrency=10, results_generator=results_generator)))
            assert [r.success for r in results] == [True] * 10

    def test_no_helper_threads(self):
        before = threading.active_count()

        def on_execute(future):
            assert threading.active_count() == before
            future.callback(['r'])
            return future

        results = execute_concurrent(_session_with(on_execute), [("q", (i,)) for i in range(50)], concurrency=5)
        assert len(results) == 50

    def test_submission_happens_on_calling_thread(self):
        # Submission must run on the thread that called execute_concurrent,
        # never on the IO/callback thread that completes a future.
        caller = threading.current_thread()
        submitter_threads = []

        def on_registered(future):
            # complete from a foreign thread, like a reactor would
            threading.Thread(target=future.callback, args=(['r'],), daemon=True).start()

        def execute_async(stmt, params, **kw):
            submitter_threads.append(threading.current_thread())
            return _ManualFuture(params, on_registered)

        session = Mock()
        session.execute_async.side_effect = execute_async

        results = list(execute_concurrent(session, [("q", (i,)) for i in range(20)],
                                          concurrency=4, results_generator=True))
        assert len(results) == 20
        assert submitter_threads
        assert all(t is caller for t in submitter_threads)

    def test_results_in_order_with_out_of_order_completion(self):
        for results_generator in (False, True):
            pending = []

            def on_execute(future):
                pending.append(future)
                if len(pending) == 4:  # complete the window in reverse
                    while pending:
                        f = pending.pop()
                        f.callback([f.params[0]])
                return future

            results = self._run(lambda: list(execute_concurrent(
                _session_with(on_execute), [("q", (i,)) for i in range(40)],
                concurrency=4, results_generator=results_generator)))
            assert [r.result_or_exc.current_rows for r in results] == [[i] for i in range(40)]

    def test_generator_backpressure_waits_for_consumer(self):
        # In generator mode new requests are submitted only when the consumer
        # asks for the next result, so a slow consumer lowers concurrency.
        submitted = []
        pending = []

        def on_execute(future):
            submitted.append(future.params[0])
            pending.append(future)
            return future

        gen = execute_concurrent(_session_with(on_execute),
                                 [("q", (i,)) for i in range(1000)],
                                 concurrency=3, results_generator=True)
        assert len(submitted) == 3

        # Complete the initial window while the consumer is idle: nothing more
        # may be submitted without demand.
        for f in list(pending):
            f.callback(['r'])
        time.sleep(0.05)
        assert len(submitted) == 3

        # Consuming a result tops the window back up.
        assert next(gen).success
        assert len(submitted) == 6

    def test_duplicate_completion_counted_once(self):
        # e.g. a speculative response arriving after a client timeout
        futures = []

        def on_execute(future):
            futures.append(future)
            if future.params[0] == 0:
                future.errback(OperationTimedOut())
                future.callback(['late'])
            return future

        out = []
        t = threading.Thread(target=lambda: out.append(execute_concurrent(
            _session_with(on_execute), [("q", (i,)) for i in range(2)], raise_on_first_error=False)),
            daemon=True)
        t.start()
        t.join(0.5)
        assert not out, "returned before the second request completed"
        futures[1].callback(['r'])
        t.join(5)
        assert not t.is_alive(), "execute_concurrent hung"
        assert [r.success for r in out[0]] == [False, True]

    def test_late_error_after_success_is_ignored(self):
        # A duplicate (late) error for a statement whose retained result was a
        # success must not be recorded and must not fail-fast, in either mode.
        def on_execute(future):
            future.callback(['ok'])
            if future.params[0] == 0:
                future.errback(OperationTimedOut())
            return future

        for results_generator in (False, True):
            results = self._run(lambda: list(execute_concurrent(
                _session_with(on_execute), [("q", (i,)) for i in range(3)],
                concurrency=1, raise_on_first_error=True,
                results_generator=results_generator)))
            assert [r.success for r in results] == [True, True, True]

    def test_iterable_errors_propagate(self):
        def broken(exc):
            for i in range(5):
                yield ("q", (i,))
            raise exc

        def on_execute(future):
            future.callback(['r'])
            return future

        for results_generator in (False, True):
            for exc in (ValueError("boom"), GeneratorExit()):
                with pytest.raises(type(exc)):
                    self._run(lambda: list(execute_concurrent(
                        _session_with(on_execute), broken(exc), concurrency=2,
                        raise_on_first_error=False, results_generator=results_generator)))

    def test_fail_fast_stops_consuming_input(self):
        consumed = []

        def statements():
            for i in range(20000):
                consumed.append(i)
                yield ("q", (i,))

        def on_execute(future):
            if future.params[0] == 0:
                future.errback(ValueError("first"))
            else:
                future.callback(['r'])
            return future

        for results_generator in (False, True):
            del consumed[:]
            with pytest.raises(ValueError, match="first"):
                self._run(lambda: list(execute_concurrent(
                    _session_with(on_execute), statements(), concurrency=5,
                    raise_on_first_error=True, results_generator=results_generator)))
            assert len(consumed) <= 5

    def test_execute_async_raising_is_recorded(self):
        session = Mock()
        session.execute_async.side_effect = RuntimeError("no hosts")
        results = execute_concurrent(session, [("q", ())] * 3, raise_on_first_error=False)
        assert [(r.success, type(r.result_or_exc)) for r in results] == [(False, RuntimeError)] * 3

    def test_invalid_concurrency_rejected(self):
        session = _session_with(lambda future: future.callback(['r']))
        for bad in (1.5, float('inf'), float('nan'), "10"):
            with pytest.raises(TypeError):
                execute_concurrent(session, [("q", ())], concurrency=bad)
        for bad in (0, -1):
            with pytest.raises(ValueError):
                execute_concurrent(session, [("q", ())], concurrency=bad)
