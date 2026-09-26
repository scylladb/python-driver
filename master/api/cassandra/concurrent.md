# cassandra.concurrent

Utilities for Concurrent Statement Execution

<a id="module-cassandra.concurrent"></a>

### cassandra.concurrent.execute_concurrent(session, statements_and_parameters, concurrency=100, raise_on_first_error=True, results_generator=False, execution_profile=<object object>)

Executes a sequence of (statement, parameters) tuples concurrently.  Each
`parameters` item must be a sequence or `None`.

The concurrency parameter controls how many statements will be executed
concurrently.

If raise_on_first_error is left as `True`, execution will stop
after the first failed statement and the corresponding exception will be
raised.

results_generator controls how the results are returned.

* If `False`, the results are returned only after all requests have completed.
* If `True`, a generator expression is returned. Using a generator results in a constrained
  memory footprint when the results set will be large – results are yielded
  as they return instead of materializing the entire list at once. The trade for lower memory
  footprint is marginal CPU overhead (more thread coordination and sorting out-of-order results
  on-the-fly).

execution_profile argument is the execution profile to use for this
request, it is passed directly to `Session.execute_async()`.

A sequence of `ExecutionResult(success, result_or_exc)` namedtuples is returned
in the same order that the statements were passed in.  If `success` is `False`,
there was an error executing the statement, and `result_or_exc` will be
an `Exception`.  If `success` is `True`, `result_or_exc`
will be the query result.

Example usage:

```default
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
```

Note: in the case that generators are used, it is important to ensure the consumers do not
block or attempt further synchronous requests, because no further IO will be processed until
the consumer returns. This may also produce a deadlock in the IO event thread.

### cassandra.concurrent.execute_concurrent_with_args(session, statement, parameters, \*args, \*\*kwargs)

Like [`execute_concurrent()`](#cassandra.concurrent.execute_concurrent), but takes a single
statement and a sequence of parameters.  Each item in `parameters`
should be a sequence or `None`.

Example usage:

```default
statement = session.prepare("INSERT INTO mytable (a, b) VALUES (1, ?)")
parameters = [(x,) for x in range(1000)]
execute_concurrent_with_args(session, statement, parameters, concurrency=50)
```
