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


from collections import deque, namedtuple
from concurrent.futures import Future
from heapq import heappush, heappop
from itertools import cycle
from operator import index
from threading import Condition, Thread

from cassandra.cluster import ResultSet, EXEC_PROFILE_DEFAULT

import logging
log = logging.getLogger(__name__)


ExecutionResult = namedtuple('ExecutionResult', ['success', 'result_or_exc'])

def execute_concurrent(session, statements_and_parameters, concurrency=100, raise_on_first_error=True, results_generator=False, execution_profile=EXEC_PROFILE_DEFAULT):
    """
    Executes a sequence of (statement, parameters) tuples concurrently.  Each
    ``parameters`` item must be a sequence or :const:`None`.

    The `concurrency` parameter controls how many statements will be executed
    concurrently. It must be an integer greater than zero.

    If `raise_on_first_error` is left as :const:`True`, execution will stop
    scheduling new statements after the first failed statement and the
    corresponding exception will be raised. With `results_generator`, earlier
    results are yielded first and the exception is raised when iteration
    reaches the failed statement.

    `results_generator` controls how the results are returned.

    * If :const:`False`, the results are returned only after all requests have completed.
    * If :const:`True`, an iterator is returned. Using a generator results in a constrained
      memory footprint when the results set will be large -- results are yielded
      as they return instead of materializing the entire list at once. The trade for lower memory
      footprint is marginal CPU overhead (more thread coordination and sorting out-of-order results
      on-the-fly).

    `execution_profile` argument is the execution profile to use for this
    request, and is passed directly to :meth:`Session.execute_async`. In
    legacy configuration mode, each request uses ``Session.default_timeout``;
    otherwise it uses the selected execution profile's ``request_timeout``.

    A sequence of ``ExecutionResult(success, result_or_exc)`` namedtuples is returned
    in the same order that the statements were passed in.  If ``success`` is :const:`False`,
    there was an error executing the statement, and ``result_or_exc`` will be
    an :class:`Exception`.  If ``success`` is :const:`True`, ``result_or_exc``
    will be the query result.

    Example usage::

        select_statement = session.prepare("SELECT * FROM users WHERE id=?")

        statements_and_params = []
        for user_id in user_ids:
            params = (user_id, )
            statements_and_params.append((select_statement, params))

        results = execute_concurrent(
            session, statements_and_params, raise_on_first_error=False)

        for (success, result) in results:
            if not success:
                handle_error(result)  # result will be an Exception
            else:
                process_user(result[0])  # result will be a list of rows

    Note: in the case that `generators` are used, it is important to ensure the consumers do not
    block or attempt further synchronous requests, because no further IO will be processed until
    the consumer returns. This may also produce a deadlock in the IO event thread.
    """
    concurrency = _validate_concurrency(concurrency)

    if not statements_and_parameters:
        return []

    executor = ConcurrentExecutorGenResults(session, statements_and_parameters, execution_profile) \
        if results_generator else ConcurrentExecutorListResults(session, statements_and_parameters, execution_profile)
    return executor.execute(concurrency, raise_on_first_error)


def _validate_concurrency(concurrency):
    concurrency = index(concurrency)
    if concurrency <= 0:
        raise ValueError("concurrency must be greater than 0")
    return concurrency


class _ConcurrentExecutor(object):

    def __init__(self, session, statements_and_params, execution_profile):
        self.session = session
        try:
            self._input_len = len(statements_and_params)
        except TypeError:
            self._input_len = 0
        self._enum_statements = enumerate(iter(statements_and_params))
        self._execution_profile = execution_profile
        self._condition = Condition()
        self._fail_fast = False
        self._results_queue = []
        self._current = 0
        self._exec_count = 0
        self._executing = False
        self._pending_executions = deque()
        self._stopped = False

    def execute(self, concurrency, fail_fast):
        self._fail_fast = fail_fast
        self._results_queue = []
        self._current = 0
        self._exec_count = 0
        self._stopped = False
        with self._condition:
            for n in range(concurrency):
                if not self._execute_next():
                    break
        return self._results()

    def _execute_next(self):
        # lock must be held
        if self._stopped:
            return False
        try:
            (idx, (statement, params)) = next(self._enum_statements)
            self._exec_count += 1
            self._execute(idx, statement, params)
            return True
        except StopIteration:
            pass

    def _execute(self, idx, statement, params):
        # When execute_async completes synchronously (e.g. immediate timeout),
        # the errback fires inline: _on_error -> _put_result -> _execute_next
        # -> _execute.  Without protection this recurses once per remaining
        # statement and blows the stack.
        #
        # ``_executing`` marks that we are inside this method higher up the
        # stack; re-entrant calls queue their work in ``_pending_executions``
        # and the outermost call drains it in a loop -- no recursion.
        if self._executing:
            self._pending_executions.append((idx, statement, params))
            return

        self._executing = True
        pending = self._pending_executions
        try:
            while True:
                try:
                    future = self.session.execute_async(
                        statement, params,
                        execution_profile=self._execution_profile)
                    # Plain functions + self in args: no bound method per request.
                    args = (self, future, idx)
                    future.add_callbacks(
                        callback=self._on_success, callback_args=args,
                        errback=self._on_error, errback_args=args)
                except Exception as exc:
                    self._put_result(exc, idx, False)
                if not pending:
                    break
                idx, statement, params = pending.popleft()
        finally:
            self._executing = False

    @staticmethod
    def _on_success(result, executor, future, idx):
        future.clear_callbacks()
        executor._put_result(ResultSet(future, result), idx, True)

    @staticmethod
    def _on_error(result, executor, future, idx):
        executor._put_result(result, idx, False)


class ConcurrentExecutorGenResults(_ConcurrentExecutor):

    def execute(self, concurrency, fail_fast):
        # Completed but not yet yielded idxs; bounded like the results heap.
        self._reported = set()
        return _ConcurrentExecutor.execute(self, concurrency, fail_fast)

    def _put_result(self, result, idx, success):
        with self._condition:
            # First completion wins; a future may report again (e.g. late
            # response after a client timeout) and must not be counted twice.
            reported = self._reported
            if idx < self._current or idx in reported:
                return
            reported.add(idx)
            heappush(self._results_queue, (idx, ExecutionResult(success, result)))
            if not success and self._fail_fast:
                self._stopped = True
            else:
                self._execute_next()
            self._condition.notify()

    def _results(self):
        with self._condition:
            while self._current < self._exec_count:
                while not self._results_queue or self._results_queue[0][0] != self._current:
                    self._condition.wait()
                while self._results_queue and self._results_queue[0][0] == self._current:
                    idx, res = heappop(self._results_queue)
                    try:
                        self._condition.release()
                        if self._fail_fast and not res[0]:
                            raise res[1]
                        yield res
                    finally:
                        self._condition.acquire()
                    self._current += 1
                    self._reported.discard(idx)


class ConcurrentExecutorListResults(_ConcurrentExecutor):

    _exception = None
    _input_error = None

    def execute(self, concurrency, fail_fast):
        self._exception = None
        self._input_error = None
        self._result_list = [None] * self._input_len
        self._fail_fast = fail_fast
        self._current = 0
        self._exec_count = 0
        self._stopped = False
        with self._condition:
            try:
                for n in range(concurrency):
                    if not self._execute_next():
                        break
            except BaseException as exc:
                # The caller's iterable failed; surfaced by _results().
                self._stopped = True
                self._input_error = exc
        return self._results()

    def _put_result(self, result, idx, success):
        with self._condition:
            # First completion wins; a future may report again (e.g. late
            # response after a client timeout) and must not be counted twice.
            results = self._result_list
            if idx >= len(results):
                # Unsized input: grow geometrically.
                results.extend([None] * max(idx + 1 - len(results), len(results)))
            if results[idx] is not None:
                return
            results[idx] = ExecutionResult(success, result)
            self._current += 1
            if not success and self._fail_fast:
                self._stopped = True
                if self._exception is None:
                    self._exception = result
                self._condition.notify()
            else:
                try:
                    has_next = self._execute_next()
                except BaseException as exc:
                    # The caller's iterable failed; on an IO thread this would be lost.
                    self._stopped = True
                    self._input_error = exc
                    self._condition.notify()
                    return
                if not has_next and self._current == self._exec_count:
                    self._condition.notify()

    def _results(self):
        with self._condition:
            while self._current < self._exec_count:
                if self._input_error is not None:
                    raise self._input_error
                if self._exception is not None and self._fail_fast:
                    raise self._exception
                self._condition.wait()
        if self._input_error is not None:
            raise self._input_error
        if self._exception is not None and self._fail_fast:  # raise the exception even if there was no wait
            raise self._exception
        del self._result_list[self._exec_count:]
        return self._result_list



def execute_concurrent_with_args(session, statement, parameters, *args, **kwargs):
    """
    Like :meth:`~cassandra.concurrent.execute_concurrent()`, but takes a single
    statement and a sequence of parameters.  Each item in ``parameters``
    should be a sequence or :const:`None`.

    Example usage::

        statement = session.prepare("INSERT INTO mytable (a, b) VALUES (1, ?)")
        parameters = [(x,) for x in range(1000)]
        execute_concurrent_with_args(session, statement, parameters, concurrency=50)
    """
    return execute_concurrent(session, zip(cycle((statement,)), parameters), *args, **kwargs)


class ConcurrentExecutorFutureResults(_ConcurrentExecutor):

    _pump_budget = 100

    def __init__(self, session, statements_and_params, execution_profile, future):
        # Iterating user input belongs on the session executor. Pass a harmless
        # placeholder to the base initializer and retain the real iterable.
        super(ConcurrentExecutorFutureResults, self).__init__(
            session, (), execution_profile)
        self._statements_and_params = statements_and_params
        self._future = future
        self._finished = False
        self._completion = None
        self._pump_scheduled = False
        self._iterator_initialized = False
        self._next_item = None
        self._exhausted = False
        self._concurrency = 0

    def execute(self, concurrency, fail_fast):
        self._fail_fast = fail_fast
        self._concurrency = concurrency
        self._schedule_pump()
        return self._future

    def _schedule_pump(self):
        with self._condition:
            if self._pump_scheduled or (
                    self._finished and self._completion is None):
                return
            self._pump_scheduled = True

        try:
            submitted = self.session.submit(self._pump)
            if submitted is None:
                raise RuntimeError(
                    "cannot execute concurrent statements on a shut down session")
        except Exception as exc:
            self._fail_submission(exc)

    def _pump(self):
        # Only one pump runs at a time (_pump_scheduled), so the input iterator
        # and execute_async are used without holding the lock. Response
        # callbacks never wait behind user iterator code or request submission.
        completion = None
        reschedule = False

        try:
            if not self._iterator_initialized:
                self._enum_statements = enumerate(
                    iter(self._statements_and_params))
                self._iterator_initialized = True

            budget = self._pump_budget
            while True:
                with self._condition:
                    if self._finished or self._exhausted:
                        break

                # Read one item ahead so exhaustion is known even when all
                # capacity is in use. Session shutdown after the final
                # response must still report a completed batch as successful.
                if self._next_item is None:
                    self._next_item = self._next_statement()
                    if self._next_item is None:
                        with self._condition:
                            self._exhausted = True
                        break

                with self._condition:
                    if (self._finished or budget <= 0 or
                            self._exec_count - self._current >= self._concurrency):
                        break
                    idx, statement, params = self._next_item
                    self._next_item = None
                    self._exec_count += 1

                self._execute(idx, statement, params)
                budget -= 1

            with self._condition:
                self._pump_scheduled = False
                if self._finished:
                    completion = self._take_completion()
                elif self._exhausted and self._current == self._exec_count:
                    self._finished = True
                    ordered_results = [
                        result for _, result in sorted(self._results_queue)]
                    completion = (True, ordered_results)
                elif (not self._exhausted and
                      self._exec_count - self._current < self._concurrency):
                    # Synchronous callbacks left capacity available. Yield to
                    # other cluster-executor work before consuming more input.
                    reschedule = True
        except BaseException as exc:
            log.debug("Concurrent execution input or submission failed",
                      exc_info=True)
            with self._condition:
                self._pump_scheduled = False
                if not self._finished:
                    self._finished = True
                    self._completion = (False, exc)
                completion = self._take_completion()

        self._dispatch_completion(completion)
        if reschedule:
            self._schedule_pump()

    def _next_statement(self):
        try:
            idx, (statement, params) = next(self._enum_statements)
        except StopIteration:
            return None
        return idx, statement, params

    def _put_result(self, result, idx, success):
        with self._condition:
            # Requests already in flight may finish after fail-fast completion.
            # They must neither enqueue more work nor complete the Future again.
            if self._finished:
                log.debug("Discarding result of concurrent statement %d "
                          "received after aggregate completion", idx)
                return

            self._results_queue.append((idx, ExecutionResult(success, result)))
            self._current += 1

            if not success and self._fail_fast:
                self._finished = True
                self._completion = (False, result)

        # Submission and aggregate completion stay off the response callback
        # thread. This also coalesces callbacks that arrive close together.
        self._schedule_pump()

    def _take_completion(self):
        completion = self._completion
        self._completion = None
        return completion

    def _fail_submission(self, exc):
        log.debug("Concurrent execution submission rejected: %r", exc)
        with self._condition:
            self._pump_scheduled = False
            if (not self._finished and self._exhausted and
                    self._current < self._exec_count):
                # Nothing is left to submit. The last in-flight response
                # schedules again and completes the batch from here.
                return
            if not self._finished:
                self._finished = True
                if self._exhausted:
                    ordered_results = [
                        result for _, result in sorted(self._results_queue)]
                    self._completion = (True, ordered_results)
                else:
                    self._completion = (False, exc)
            completion = self._take_completion()

        self._dispatch_completion(completion)

    def _dispatch_completion(self, completion):
        if completion is None or self._future.done():
            return

        # Completing a concurrent Future invokes its callbacks synchronously.
        # Always use a short-lived thread so a callback can safely shut down
        # the Cluster without making its executor worker join itself. This also
        # keeps callbacks off response callback/reactor threads when submission
        # fails because Session shutdown has stopped the executor.
        completion_thread = Thread(
            target=self._complete,
            args=(completion,),
            name="cassandra-concurrent-completion",
            daemon=True)
        try:
            completion_thread.start()
        except RuntimeError:
            # A lost completion would leave the aggregate Future pending
            # forever. Completing inline is the only remaining option.
            log.warning("Unable to start concurrent completion thread; "
                        "completing the aggregate Future inline",
                        exc_info=True)
            self._complete(completion)

    def _complete(self, completion):
        if completion is None or self._future.done():
            return

        success, result = completion
        if success:
            self._future.set_result(result)
        else:
            self._future.set_exception(result)


def execute_concurrent_async(session, statements_and_parameters, concurrency=100,
                             raise_on_first_error=False,
                             execution_profile=EXEC_PROFILE_DEFAULT):
    """
    Asynchronously execute statements, returning a Future immediately.

    See :meth:`.Session.execute_concurrent_async`.

    .. versionadded:: 3.29.13
    """
    concurrency = _validate_concurrency(concurrency)

    future = Future()
    # Aggregate work starts before this function returns. Marking the Future
    # running makes its cancellation contract explicit and prevents completion
    # racing with a successful cancel().
    future.set_running_or_notify_cancel()
    try:
        executor = ConcurrentExecutorFutureResults(
            session, statements_and_parameters, execution_profile, future)
        executor.execute(concurrency, raise_on_first_error)
    except Exception as exc:
        future.set_exception(exc)
    return future
