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
from heapq import heappush, heappop
from itertools import cycle
from threading import Condition, Event, Thread

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
      memory footprint when the results set will be large -- results are yielded
      as they return instead of materializing the entire list at once. The trade for lower memory
      footprint is marginal CPU overhead (more thread coordination and sorting out-of-order results
      on-the-fly).

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

    Note: in the case that `generators` are used, it is important to ensure the consumers do not
    block or attempt further synchronous requests, because no further IO will be processed until
    the consumer returns. This may also produce a deadlock in the IO event thread.
    """
    if concurrency <= 0:
        raise ValueError("concurrency must be greater than 0")

    if not statements_and_parameters:
        return []

    executor = ConcurrentExecutorGenResults(session, statements_and_parameters, execution_profile) \
        if results_generator else ConcurrentExecutorListResults(session, statements_and_parameters, execution_profile)
    return executor.execute(concurrency, raise_on_first_error)


class _ConcurrentExecutor(object):

    def __init__(self, session, statements_and_params, execution_profile):
        self.session = session
        self._enum_statements = enumerate(iter(statements_and_params))
        self._execution_profile = execution_profile
        self._condition = Condition()
        self._fail_fast = False
        self._results_queue = []
        self._current = 0
        self._exec_count = 0
        self._executing = False

    def execute(self, concurrency, fail_fast):
        self._fail_fast = fail_fast
        self._results_queue = []
        self._current = 0
        self._exec_count = 0
        with self._condition:
            for n in range(concurrency):
                if not self._execute_next():
                    break
        return self._results()

    def _execute_next(self):
        # lock must be held
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
        # ``_executing`` marks that we are already inside this method higher up
        # the call stack.  When a synchronous callback re-enters, we just stash
        # the pending work in ``_pending_executions`` and let the outermost
        # invocation drain it in a loop -- no recursion.
        if self._executing:
            self._pending_executions.append((idx, statement, params))
            return

        self._executing = True
        self._pending_executions = [(idx, statement, params)]
        try:
            while self._pending_executions:
                p_idx, p_statement, p_params = self._pending_executions.pop(0)
                try:
                    future = self.session.execute_async(p_statement, p_params, timeout=None, execution_profile=self._execution_profile)
                    args = (future, p_idx)
                    future.add_callbacks(
                        callback=self._on_success, callback_args=args,
                        errback=self._on_error, errback_args=args)
                except Exception as exc:
                    self._put_result(exc, p_idx, False)
        finally:
            self._executing = False

    def _on_success(self, result, future, idx):
        future.clear_callbacks()
        self._put_result(ResultSet(future, result), idx, True)

    def _on_error(self, result, future, idx):
        self._put_result(result, idx, False)


class ConcurrentExecutorGenResults(_ConcurrentExecutor):

    def _put_result(self, result, idx, success):
        with self._condition:
            heappush(self._results_queue, (idx, ExecutionResult(success, result)))
            self._execute_next()
            self._condition.notify()

    def _results(self):
        with self._condition:
            while self._current < self._exec_count:
                while not self._results_queue or self._results_queue[0][0] != self._current:
                    self._condition.wait()
                while self._results_queue and self._results_queue[0][0] == self._current:
                    _, res = heappop(self._results_queue)
                    try:
                        self._condition.release()
                        if self._fail_fast and not res[0]:
                            raise res[1]
                        yield res
                    finally:
                        self._condition.acquire()
                    self._current += 1


class ConcurrentExecutorListResults(_ConcurrentExecutor):

    _exception = None
    _fatal_exception = None

    def execute(self, concurrency, fail_fast):
        self._exception = None
        self._fatal_exception = None
        self._submit_ready = deque()
        self._submit_event = Event()
        self._stop_event = Event()
        self._exhausted = False
        # Submit the initial batch from the calling thread (no contention
        # yet -- the submitter thread is not started until afterward).
        # Track whether the initial batch consumed all statements.
        self._fail_fast = fail_fast
        self._results_queue = []
        self._current = 0
        self._exec_count = 0
        with self._condition:
            for n in range(concurrency):
                if not self._execute_next():
                    self._exhausted = True
                    break
                # A statement can fail synchronously (e.g. execute_async
                # raising, or its errback firing inline) while we're still
                # dispatching this initial batch -- see ``_execute``.
                # ``_put_result``'s fail-fast path records ``_exception``
                # right here (reentrantly, same thread/lock), so stop
                # dispatching the rest of the batch immediately instead of
                # continuing up to ``concurrency`` more statements the
                # caller doesn't want. This check is race-free: it runs on
                # the same thread that just set ``_exception``, under the
                # same lock, with no other thread involved yet (the
                # submitter is not started until ``_results()``).
                if self._fail_fast and self._exception:
                    break
        return self._results()

    def _results(self):
        # Always start the submitter thread: it owns ``_current`` accounting
        # (incrementing from drained completion signals) so the event-loop
        # callback path can stay lock-free in the success case.  Even when
        # the iterator was fully consumed by the initial batch, the
        # submitter still needs to run to record completions.
        self._submitter = Thread(target=self._submitter_loop,
                                 daemon=True, name="concurrent-submitter")
        self._submitter.start()

        try:
            with self._condition:
                while not self._exhausted or self._current < self._exec_count:
                    # Check for an already-recorded fail-fast exception
                    # *before* waiting, not only after. ``_put_result``'s
                    # cold path may have set ``_exception`` and called
                    # ``notify()`` while we were still inside ``execute()``
                    # dispatching the initial batch (i.e. before this
                    # thread ever reached ``wait()``) -- ``notify()`` only
                    # wakes threads already parked in ``wait()``, so that
                    # notification is silently dropped. Without this
                    # pre-check we would then block in ``wait()`` with no
                    # guarantee of another wakeup any time soon (the
                    # submitter only notifies again once the whole
                    # iterable is exhausted), so fail-fast would degrade
                    # into consuming the entire -- possibly unbounded --
                    # iterable. Reading ``_exception``/``_fail_fast`` here
                    # is safe: both are only ever written while holding
                    # this same ``_condition`` lock, which we hold
                    # continuously in this loop except while inside
                    # ``wait()`` itself.
                    if self._exception and self._fail_fast:
                        break
                    self._condition.wait()
        finally:
            self._stop_event.set()
            self._submit_event.set()  # wake submitter so it sees the stop
            self._submitter.join()
        # A fatal error in the submitter thread itself (as opposed to a
        # per-statement failure) means we can no longer trust that the
        # remaining statements were ever dispatched or accounted for.
        # Raise unconditionally -- silently returning a truncated/incomplete
        # results list would violate the "one result per input statement"
        # contract, and hanging (the alternative if this were left
        # unraised) is worse. See _submitter_loop for what can trigger this.
        if self._fatal_exception:
            raise self._fatal_exception
        if self._exception and self._fail_fast:
            raise self._exception
        return [r[1] for r in sorted(self._results_queue)]

    def _put_result(self, result, idx, success):
        """Record a completion and signal the submitter thread.

        Called from the event-loop callback thread (or from the submitter
        thread when execute_async raises synchronously).

        Hot path (success, not fail-fast): NO lock acquisition.  We rely on
        the submitter thread to bump ``_current`` from the drained signal
        count under the same lock acquisition that bumps ``_exec_count``.
        This removes ~0.5-1us of lock cost from every callback on the
        event-loop thread.

        Note: ``self._results_queue.append`` and ``self._submit_ready.append``
        are individually safe under both the GIL and free-threaded builds
        (PEP 703) -- CPython's list/deque append is atomic either way (a
        per-object critical section protects it in free-threaded builds).
        What actually needs to hold across threads is that the submitter
        thread, once it observes a drained ``_submit_ready`` entry, also
        observes the matching ``_results_queue`` append that happened
        before it in this method. That ordering comes from
        ``self._submit_event.set()``/``.wait()``: both are backed by a real
        lock, so the ``set()`` here happens-after every write above it in
        this function, and the submitter's ``wait()`` happens-after that
        ``set()``. This holds under free-threaded Python too, which the
        driver's CI exercises (see the "3.14t" jobs).
        """
        self._results_queue.append((idx, ExecutionResult(success, result)))
        if not success and self._fail_fast:
            # Cold path: take the lock to record the exception and wake
            # the main thread immediately so it can stop waiting.
            with self._condition:
                if not self._exception:
                    self._exception = result
                self._condition.notify()
        # Signal the submitter thread.  It will:
        #   1) bump _current under the lock from the drained signal count,
        #   2) submit a replacement request,
        #   3) notify _results() if all completions have arrived.
        self._submit_ready.append(1)
        self._submit_event.set()

    def _submitter_loop(self):
        """Drain completion signals and submit follow-up requests.

        Runs on a dedicated thread so that the libev event-loop thread
        only needs to do the lightweight ``deque.append`` + ``Event.set``
        in ``_put_result`` rather than the full execute_async cycle
        (query-plan, borrow connection, serialise, enqueue).

        Owns ``_current`` accounting: each drained completion signal
        increments ``_current`` by one under the same lock acquisition
        that bumps ``_exec_count`` for the new batch.  This keeps the
        event-loop callback path lock-free in the success case.
        """
        ready = self._submit_ready
        ready_event = self._submit_event
        stop_event = self._stop_event
        enum_stmts = self._enum_statements
        session = self.session
        profile = self._execution_profile
        on_success = self._on_success
        on_error = self._on_error
        condition = self._condition
        try:
            while not stop_event.is_set():
                ready_event.wait()
                ready_event.clear()
                # Drain all pending completion signals.
                count = 0
                while True:
                    try:
                        ready.popleft()
                        count += 1
                    except IndexError:
                        break
                if count == 0:
                    continue
                # Treat an already-recorded fail-fast exception exactly
                # like a stop request: keep doing the accounting for
                # completions that already happened, but stop pulling
                # further statements from the caller's iterable and stop
                # dispatching new requests. This must not rely on the
                # main thread reaching ``_results()``'s ``finally`` and
                # setting ``stop_event`` -- that can be delayed (or, pre
                # fix, missed) -- so this loop checks ``_exception``
                # itself. ``_exception`` is only ever written under
                # ``condition`` (see ``_put_result``'s cold path), so we
                # read it under the same lock here to avoid racing that
                # write (this matters on free-threaded builds too).
                with condition:
                    fail_fast_stop = self._fail_fast and self._exception is not None
                if stop_event.is_set() or fail_fast_stop:
                    # Main thread is shutting down (e.g. fail-fast).  Do the
                    # accounting for already-completed requests but skip
                    # dispatching new ones.
                    with condition:
                        self._current += count
                        if self._exhausted and self._current >= self._exec_count:
                            condition.notify()
                    continue
                if self._exhausted:
                    # No more statements to dispatch -- just account for the
                    # completions we just drained and notify the waiter if
                    # everything has caught up.
                    with condition:
                        self._current += count
                        if self._current >= self._exec_count:
                            condition.notify()
                    continue
                # Submit follow-up requests directly (fast path).
                # The iterator is only consumed from this thread (the initial
                # batch was fully dispatched before this thread started).
                #
                # Pull statements from the iterator first, then bump _current
                # and _exec_count for the entire batch in one lock acquisition,
                # then dispatch.  This avoids per-request lock overhead while
                # ensuring _results() never sees _current >= _exec_count
                # prematurely.
                batch = []
                iterator_done = False
                for _ in range(count):
                    try:
                        batch.append(next(enum_stmts))
                    except StopIteration:
                        iterator_done = True
                        break
                # Single lock acquisition: bump both _current (from the
                # drained completion count) and _exec_count (from the new
                # batch size) atomically.  Setting _exhausted in the same
                # critical section ensures the main thread never sees
                # _exhausted=True with a stale _exec_count.
                with condition:
                    self._current += count
                    self._exec_count += len(batch)
                    if iterator_done:
                        self._exhausted = True
                    # Wake the waiter if all completions have caught up.
                    if self._exhausted and self._current >= self._exec_count:
                        condition.notify()
                    fail_fast_stop = self._fail_fast and self._exception is not None
                # Re-check after the lock release: a stop request or a
                # fail-fast exception may have arrived while we were
                # holding the lock; avoid dispatching requests we know
                # will be discarded (they were already accounted for
                # above, so skipping them here does not lose the
                # accounting -- see the docstring note on _results()
                # about the pre-wait exception check for why the main
                # thread does not need this batch to ever "complete").
                if stop_event.is_set() or fail_fast_stop:
                    continue
                for idx, (statement, params) in batch:
                    try:
                        future = session.execute_async(statement, params,
                                                       timeout=None,
                                                       execution_profile=profile)
                        args = (future, idx)
                        future.add_callbacks(
                            callback=on_success, callback_args=args,
                            errback=on_error, errback_args=args)
                    except BaseException as exc:
                        # Record the failure directly.  _put_result handles
                        # _current accounting and will enqueue another signal
                        # to _submit_ready -- but that is fine because the
                        # next drain will attempt another next(enum_stmts).
                        self._put_result(exc, idx, False)
        except BaseException as exc:
            # Anything escaping the loop above (most plausibly the caller's
            # statements_and_parameters iterable raising something other
            # than StopIteration out of next(enum_stmts), including
            # BaseException like GeneratorExit or KeyboardInterrupt) would
            # otherwise kill this thread silently -- and since this thread is
            # the only thing that ever advances _current/_exec_count or wakes
            # _results() after the initial batch, the caller would then
            # block in _results() forever. Record the failure and force
            # the waiter's predicate to become true so it stops waiting on
            # state we can no longer maintain, instead of hanging.
            log.exception("concurrent-submitter thread aborted execute_concurrent early")
            with condition:
                if self._fatal_exception is None:
                    self._fatal_exception = exc
                self._exhausted = True
                self._current = self._exec_count
                condition.notify()



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
