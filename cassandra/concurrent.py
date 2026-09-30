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


from collections import namedtuple
from itertools import cycle
from queue import Empty, SimpleQueue

from cassandra.cluster import ResultSet, EXEC_PROFILE_DEFAULT

import logging
log = logging.getLogger(__name__)


ExecutionResult = namedtuple('ExecutionResult', ['success', 'result_or_exc'])

def execute_concurrent(session, statements_and_parameters, concurrency=100, raise_on_first_error=True, results_generator=False, execution_profile=EXEC_PROFILE_DEFAULT):
    """
    Executes a sequence of (statement, parameters) tuples concurrently.  Each
    ``parameters`` item must be a sequence or :const:`None`.

    The `concurrency` parameter controls how many statements will be executed
    concurrently.

    If `raise_on_first_error` is left as :const:`True`, execution will stop
    after the first failed statement and the corresponding exception will be
    raised.

    `results_generator` controls how the results are returned.

    * If :const:`False`, the results are returned only after all requests have completed.
    * If :const:`True`, a generator expression is returned. Using a generator results in a constrained
      memory footprint when the results set will be large -- results are yielded as they return
      instead of materializing the entire list at once. Results are still returned in the order the
      statements were passed in, so out-of-order completions are held until their turn.

    `execution_profile` argument is the execution profile to use for this
    request, it is passed directly to :meth:`Session.execute_async`.

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

    Requests are submitted from the calling thread, never from the IO event thread. With
    `results_generator`, new requests are only submitted while the consumer is asking for the
    next result, so a slow consumer lowers the effective concurrency.
    """
    if not isinstance(concurrency, int):
        raise TypeError("concurrency must be an integer")
    if concurrency <= 0:
        raise ValueError("concurrency must be greater than 0")

    if not statements_and_parameters:
        return []

    executor = ConcurrentExecutorGenResults(session, statements_and_parameters, execution_profile) \
        if results_generator else ConcurrentExecutorListResults(session, statements_and_parameters, execution_profile)
    return executor.execute(concurrency, raise_on_first_error)


class _ConcurrentExecutor(object):
    # All submission happens on the calling thread. IO-thread callbacks only
    # enqueue the completed result, so they never block on the caller.

    def __init__(self, session, statements_and_params, execution_profile):
        self.session = session
        self._statements = iter(statements_and_params)
        self._execution_profile = execution_profile
        self._done = SimpleQueue()
        self._results = {}
        self._submitted = 0
        self._in_flight = 0
        self._exhausted = False
        self._fail_fast = False
        self._first_error = None

    def _submit(self, concurrency):
        while self._in_flight < concurrency and not self._exhausted \
                and not (self._fail_fast and self._first_error is not None):
            try:
                statement, params = next(self._statements)
            except StopIteration:
                self._exhausted = True
                return
            idx = self._submitted
            self._submitted += 1
            self._in_flight += 1
            try:
                future = self.session.execute_async(statement, params, timeout=None, execution_profile=self._execution_profile)
                future.add_callbacks(
                    callback=self._on_success, callback_args=(future, idx),
                    errback=self._on_error, errback_args=(idx,))
            except Exception as exc:
                self._on_error(exc, idx)

    def _reap(self, concurrency, block):
        # Runs on the calling thread. Each statement is recorded and counted
        # exactly once, so a future that reports twice (e.g. a late speculative
        # response) can neither advance the window nor fail fast on a result
        # that was already superseded.
        while True:
            try:
                idx, result = self._done.get(block)
            except Empty:
                break
            block = False
            if idx in self._results:
                continue
            self._results[idx] = result
            self._in_flight -= 1
            if self._fail_fast and not result.success and self._first_error is None:
                self._first_error = result.result_or_exc
        self._submit(concurrency)

    def _on_success(self, result, future, idx):
        future.clear_callbacks()
        self._complete(idx, ExecutionResult(True, ResultSet(future, result)))

    def _on_error(self, exc, idx):
        self._complete(idx, ExecutionResult(False, exc))

    def _complete(self, idx, result):
        # Runs on an IO thread. Only enqueue the completion; the calling thread
        # does the bookkeeping (dedup, fail-fast) when it reaps.
        self._done.put((idx, result))


class ConcurrentExecutorGenResults(_ConcurrentExecutor):

    def execute(self, concurrency, fail_fast):
        self._fail_fast = fail_fast
        self._submit(concurrency)
        return self._results_gen(concurrency)

    def _results_gen(self, concurrency):
        results = self._results
        current = 0
        while current < self._submitted:
            while current not in results:
                self._reap(concurrency, block=True)
            res = results.pop(current)
            current += 1
            if self._fail_fast and not res.success:
                raise res.result_or_exc
            # Keep the window full while the consumer works on this result.
            self._reap(concurrency, block=False)
            yield res


class ConcurrentExecutorListResults(_ConcurrentExecutor):

    def execute(self, concurrency, fail_fast):
        self._fail_fast = fail_fast
        self._submit(concurrency)
        while True:
            if fail_fast and self._first_error is not None:
                raise self._first_error
            if not self._in_flight:
                break
            self._reap(concurrency, block=True)
        return [self._results[i] for i in range(self._submitted)]


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
