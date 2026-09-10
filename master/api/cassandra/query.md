# cassandra.query

Prepared Statements, Batch Statements, Tracing, and Row Factories

<a id="module-cassandra.query"></a>

### cassandra.query.tuple_factory(colnames, rows)

Returns each row as a tuple

Example:

```default
>>> from cassandra.query import tuple_factory
>>> session = cluster.connect('mykeyspace')
>>> session.row_factory = tuple_factory
>>> rows = session.execute("SELECT name, age FROM users LIMIT 1")
>>> print(rows[0])
('Bob', 42)
```

#### Versionchanged
Changed in version 2.0.0: moved from `cassandra.decoder` to `cassandra.query`

### cassandra.query.named_tuple_factory(colnames, rows)

Returns each row as a [namedtuple](https://docs.python.org/2/library/collections.html#collections.namedtuple).
This is the default row factory.

Example:

```default
>>> from cassandra.query import named_tuple_factory
>>> session = cluster.connect('mykeyspace')
>>> session.row_factory = named_tuple_factory
>>> rows = session.execute("SELECT name, age FROM users LIMIT 1")
>>> user = rows[0]

>>> # you can access field by their name:
>>> print("name: %s, age: %d" % (user.name, user.age))
name: Bob, age: 42

>>> # or you can access fields by their position (like a tuple)
>>> name, age = user
>>> print("name: %s, age: %d" % (name, age))
name: Bob, age: 42
>>> name = user[0]
>>> age = user[1]
>>> print("name: %s, age: %d" % (name, age))
name: Bob, age: 42
```

#### Versionchanged
Changed in version 2.0.0: moved from `cassandra.decoder` to `cassandra.query`

### cassandra.query.dict_factory(colnames, rows)

Returns each row as a dict.

Example:

```default
>>> from cassandra.query import dict_factory
>>> session = cluster.connect('mykeyspace')
>>> session.row_factory = dict_factory
>>> rows = session.execute("SELECT name, age FROM users LIMIT 1")
>>> print(rows[0])
{u'age': 42, u'name': u'Bob'}
```

#### Versionchanged
Changed in version 2.0.0: moved from `cassandra.decoder` to `cassandra.query`

### cassandra.query.ordered_dict_factory(colnames, rows)

Like [`dict_factory()`](#cassandra.query.dict_factory), but returns each row as an OrderedDict,
so the order of the columns is preserved.

#### Versionchanged
Changed in version 2.0.0: moved from `cassandra.decoder` to `cassandra.query`

### *class* cassandra.query.SimpleStatement(query_string, retry_policy=None, consistency_level=None, routing_key=None, serial_consistency_level=None, fetch_size=<object object>, keyspace=None, custom_payload=None, is_idempotent=False)

A simple, un-prepared query.

query_string should be a literal CQL statement with the exception
of parameter placeholders that will be filled through the
parameters argument of [`Session.execute()`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.execute).

See [`Statement`](#cassandra.query.Statement) attributes for a description of the other parameters.

### *class* cassandra.query.PreparedStatement

A statement that has been prepared against at least one Cassandra node.
Instances of this class should not be created directly, but through
[`Session.prepare()`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.prepare).

A [`PreparedStatement`](#cassandra.query.PreparedStatement) should be prepared only once. Re-preparing a statement
may affect performance (as the operation requires a network roundtrip).

<b>A note about <code>\*</code> in prepared statements</b>: Do not use `*` in prepared statements if you might
change the schema of the table being queried. The driver and server each
maintain a map between metadata for a schema and statements that were
prepared against that schema. When a user changes a schema, e.g. by adding
or removing a column, the server invalidates its mappings involving that
schema. However, there is currently no way to propagate that invalidation
to drivers. Thus, after a schema change, the driver will incorrectly
interpret the results of `SELECT *` queries prepared before the schema
change. This is currently being addressed in [CASSANDRA-10786](https://issues.apache.org/jira/browse/CASSANDRA-10786).

#### *property* result_metadata_and_id

The cached result metadata and its metadata id as one immutable
`(result_metadata, result_metadata_id)` pair.

Read this property when both values are needed together: the tuple is
replaced atomically by [`update_result_metadata()`](#cassandra.query.PreparedStatement.update_result_metadata), so a single read
can never observe the metadata of one schema version paired with the
metadata id of another.

#### *property* result_metadata

Cached result metadata (column definitions) from PREPARE. Read-only:
[`update_result_metadata()`](#cassandra.query.PreparedStatement.update_result_metadata) is the only way to replace it, so it can
never be assigned separately from the id it belongs to.

#### *property* result_metadata_id

Cached result metadata id (hash) from PREPARE. Read-only:
[`update_result_metadata()`](#cassandra.query.PreparedStatement.update_result_metadata) is the only way to replace it, so it can
never be assigned separately from the metadata it describes.

#### update_result_metadata(result_metadata, result_metadata_id)

Replace the cached result metadata and metadata id together, in a single
atomic attribute store. Response callbacks may update a statement while
request threads read it; updating the pair in one step (rather than the
two fields separately) prevents a reader from pairing a fresh metadata id
with stale metadata — a state in which the server would skip sending
metadata and rows would be decoded against the wrong columns.

Also re-arms `_warned_missing_column_metadata`, so an anomaly that
recurs after the metadata was recovered is logged again.

#### bind(values)

Creates and returns a [`BoundStatement`](#cassandra.query.BoundStatement) instance using values.

See [`BoundStatement.bind()`](#cassandra.query.BoundStatement.bind) for rules on input `values`.

### *class* cassandra.query.BoundStatement(prepared_statement, retry_policy=None, consistency_level=None, routing_key=None, serial_consistency_level=None, fetch_size=<object object>, keyspace=None, custom_payload=None)

A prepared statement that has been bound to a particular set of values.
These may be created directly or through [`PreparedStatement.bind()`](#cassandra.query.PreparedStatement.bind).

prepared_statement should be an instance of [`PreparedStatement`](#cassandra.query.PreparedStatement).

See [`Statement`](#cassandra.query.Statement) attributes for a description of the other parameters.

#### prepared_statement *= None*

The [`PreparedStatement`](#cassandra.query.PreparedStatement) instance that this was created from.

#### values *= None*

The sequence of values that were bound to the prepared statement.

#### bind(values)

Binds a sequence of values for the prepared statement parameters
and returns this instance.  Note that values *must* be:

* a sequence, even if you are only binding one value, or
* a dict that relates 1-to-1 between dict keys and columns

#### Versionchanged
Changed in version 2.6.0: [`UNSET_VALUE`](#cassandra.query.UNSET_VALUE) was introduced. These can be bound as positional parameters
in a sequence, or by name in a dict. Additionally, when using protocol v4+:

* short sequences will be extended to match bind parameters with UNSET_VALUE
* names may be omitted from a dict with UNSET_VALUE implied.

#### Versionchanged
Changed in version 3.0.0: method will not throw if extra keys are present in bound dict (PYTHON-178)

#### *property* routing_key

The [`partition_key`](https://python-driver.docs.scylladb.com/master/api/cassandra/metadata.md#cassandra.metadata.TableMetadata.partition_key) portion of the primary key,
which can be used to determine which nodes are replicas for the query.

If the partition key is a composite, a list or tuple must be passed in.
Each key component should be in its packed (binary) format, so all
components should be strings.

### *class* cassandra.query.Statement

An abstract class representing a single query. There are three subclasses:
[`SimpleStatement`](#cassandra.query.SimpleStatement), [`BoundStatement`](#cassandra.query.BoundStatement), and [`BatchStatement`](#cassandra.query.BatchStatement).
These can be passed to [`Session.execute()`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.execute).

#### retry_policy *= None*

An instance of a [`cassandra.policies.RetryPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.RetryPolicy) or one of its
subclasses.  This controls when a query will be retried and how it
will be retried.

#### consistency_level *= None*

The [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) to be used for this operation.  Defaults
to `None`, which means that the default consistency level for
the Session this is executed in will be used.

#### fetch_size *= <object object>*

How many rows will be fetched at a time.  This overrides the default
of [`Session.default_fetch_size`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.default_fetch_size)

This only takes effect when protocol version 2 or higher is used.
See [`Cluster.protocol_version`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster.protocol_version) for details.

#### Versionadded
Added in version 2.0.0.

#### keyspace *= None*

The string name of the keyspace this query acts on. This is used when
[`TokenAwarePolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.TokenAwarePolicy) is configured in the profile load balancing policy.

It is set implicitly on [`BoundStatement`](#cassandra.query.BoundStatement), and [`BatchStatement`](#cassandra.query.BatchStatement),
but must be set explicitly on [`SimpleStatement`](#cassandra.query.SimpleStatement).

#### Versionadded
Added in version 2.1.3.

#### table *= None*

The string name of the table this query acts on. This is used when the tablet
feature is enabled and in the same time :class\`~.TokenAwarePolicy\` is configured
in the profile load balancing policy.

#### custom_payload *= None*

[Custom Payloads](https://python-driver.docs.scylladb.com/master/api/cassandra/protocol.md#custom-payload) to be passed to the server.

These are only allowed when using protocol version 4 or higher.

#### Versionadded
Added in version 2.6.0.

#### is_idempotent *= False*

Flag indicating whether this statement is safe to run multiple times in speculative execution.

#### *property* routing_key

The [`partition_key`](https://python-driver.docs.scylladb.com/master/api/cassandra/metadata.md#cassandra.metadata.TableMetadata.partition_key) portion of the primary key,
which can be used to determine which nodes are replicas for the query.

If the partition key is a composite, a list or tuple must be passed in.
Each key component should be in its packed (binary) format, so all
components should be strings.

#### *property* serial_consistency_level

The serial consistency level is only used by conditional updates
(`INSERT`, `UPDATE` and `DELETE` with an `IF` condition).  For
those, the `serial_consistency_level` defines the consistency level of
the serial phase (or “paxos” phase) while the normal
[`consistency_level`](#cassandra.query.Statement.consistency_level) defines the consistency for the “learn” phase,
i.e. what type of reads will be guaranteed to see the update right away.
For example, if a conditional write has a [`consistency_level`](#cassandra.query.Statement.consistency_level) of
[`QUORUM`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel.QUORUM) (and is successful), then a
[`QUORUM`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel.QUORUM) read is guaranteed to see that write.
But if the regular [`consistency_level`](#cassandra.query.Statement.consistency_level) of that write is
[`ANY`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel.ANY), then only a read with a
[`consistency_level`](#cassandra.query.Statement.consistency_level) of [`SERIAL`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel.SERIAL) is
guaranteed to see it (even a read with consistency
[`ALL`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel.ALL) is not guaranteed to be enough).

The serial consistency can only be one of [`SERIAL`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel.SERIAL)
or [`LOCAL_SERIAL`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel.LOCAL_SERIAL). While `SERIAL` guarantees full
linearizability (with other `SERIAL` updates), `LOCAL_SERIAL` only
guarantees it in the local data center.

The serial consistency level is ignored for any query that is not a
conditional update. Serial reads should use the regular
[`consistency_level`](#cassandra.query.Statement.consistency_level).

Serial consistency levels may only be used against Cassandra 2.0+
and the [`protocol_version`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster.protocol_version) must be set to 2 or higher.

See [Lightweight Transactions (Compare-and-set)](https://python-driver.docs.scylladb.com/master/lwt.md) for a discussion on how to work with results returned from
conditional statements.

#### Versionadded
Added in version 2.0.0.

### cassandra.query.UNSET_VALUE

Specifies an unset value when binding a prepared statement.

Unset values are ignored, allowing prepared statements to be used without specify

See [https://issues.apache.org/jira/browse/CASSANDRA-7304](https://issues.apache.org/jira/browse/CASSANDRA-7304) for further details on semantics.

#### Versionadded
Added in version 2.6.0.

Only valid when using native protocol v4+

### *class* cassandra.query.BatchStatement(batch_type=BatchType.LOGGED, retry_policy=None, consistency_level=None)

A protocol-level batch of operations which are applied atomically
by default.

#### Versionadded
Added in version 2.0.0.

batch_type specifies The [`BatchType`](#cassandra.query.BatchType) for the batch operation.
Defaults to [`BatchType.LOGGED`](#cassandra.query.BatchType.LOGGED).

retry_policy should be a [`RetryPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.RetryPolicy) instance for
controlling retries on the operation.

consistency_level should be a [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) value
to be used for all operations in the batch.

custom_payload is a [Custom Payloads](https://python-driver.docs.scylladb.com/master/api/cassandra/protocol.md#custom-payload) passed to the server.
Note: as Statement objects are added to the batch, this map is
updated with any values found in their custom payloads. These are
only allowed when using protocol version 4 or higher.

Example usage:

```python
insert_user = session.prepare("INSERT INTO users (name, age) VALUES (?, ?)")
batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

for (name, age) in users_to_insert:
    batch.add(insert_user, (name, age))

session.execute(batch)
```

You can also mix different types of operations within a batch:

```python
batch = BatchStatement()
batch.add(SimpleStatement("INSERT INTO users (name, age) VALUES (%s, %s)"), (name, age))
batch.add(SimpleStatement("DELETE FROM pending_users WHERE name=%s"), (name,))
session.execute(batch)
```

#### Versionadded
Added in version 2.0.0.

#### Versionchanged
Changed in version 2.1.0: Added serial_consistency_level as a parameter

#### Versionchanged
Changed in version 2.6.0: Added custom_payload as a parameter

#### serial_consistency_level *= None*

The same as [`Statement.serial_consistency_level`](#cassandra.query.Statement.serial_consistency_level), but is only
supported when using protocol version 3 or higher.

#### batch_type *= None*

The [`BatchType`](#cassandra.query.BatchType) for the batch operation.  Defaults to
[`BatchType.LOGGED`](#cassandra.query.BatchType.LOGGED).

#### clear()

This is a convenience method to clear a batch statement for reuse.

*Note:* it should not be used concurrently with uncompleted execution futures executing the same
`BatchStatement`.

#### add(statement, parameters=None)

Adds a [`Statement`](#cassandra.query.Statement) and optional sequence of parameters
to be used with the statement to the batch.

Like with other statements, parameters must be a sequence, even
if there is only one item.

#### add_all(statements, parameters)

Adds a sequence of [`Statement`](#cassandra.query.Statement) objects and a matching sequence
of parameters to the batch. Statement and parameter sequences must be of equal length or
one will be truncated. `None` can be used in the parameters position where are needed.

### *class* cassandra.query.BatchType

A BatchType is used with [`BatchStatement`](#cassandra.query.BatchStatement) instances to control
the atomicity of the batch operation.

#### Versionadded
Added in version 2.0.0.

#### LOGGED *= BatchType.LOGGED*

Atomic batch operation.

#### UNLOGGED *= BatchType.UNLOGGED*

Non-atomic batch operation.

#### COUNTER *= BatchType.COUNTER*

Batches of counter operations.

### *class* cassandra.query.ValueSequence(iterable=(),)

A wrapper class that is used to specify that a sequence of values should
be treated as a CQL list of values instead of a single column collection when used
as part of the parameters argument for [`Session.execute()`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.execute).

This is typically needed when supplying a list of keys to select.
For example:

```default
>>> my_user_ids = ('alice', 'bob', 'charles')
>>> query = "SELECT * FROM users WHERE user_id IN %s"
>>> session.execute(query, parameters=[ValueSequence(my_user_ids)])
```

A wrapper class that is used to specify that a sequence of values should
be treated as a CQL list of values instead of a single column collection when used
as part of the parameters argument for [`Session.execute()`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.execute).

This is typically needed when supplying a list of keys to select.
For example:

```default
>>> my_user_ids = ('alice', 'bob', 'charles')
>>> query = "SELECT * FROM users WHERE user_id IN %s"
>>> session.execute(query, parameters=[ValueSequence(my_user_ids)])
```

### *class* cassandra.query.QueryTrace

A trace of the duration and events that occurred when executing
an operation.

#### request_type *= None*

A string that very generally describes the traced operation.

#### duration *= None*

A `datetime.timedelta` measure of the duration of the query.

#### client *= None*

The IP address of the client that issued this request

This is only available when using Cassandra 2.2+

#### coordinator *= None*

The IP address of the host that acted as coordinator for this request.

#### parameters *= None*

A `dict` of parameters for the traced operation, such as the
specific query string.

#### started_at *= None*

A UTC `datetime.datetime` object describing when the operation
was started.

#### events *= None*

A chronologically sorted list of [`TraceEvent`](#cassandra.query.TraceEvent) instances
representing the steps the traced operation went through.  This
corresponds to the rows in `system_traces.events` for this tracing
session.

#### trace_id *= None*

`uuid.UUID` unique identifier for this tracing session.  Matches
the `session_id` column in `system_traces.sessions` and
`system_traces.events`.

#### populate(max_wait=2.0, wait_for_complete=True, query_cl=None)

Retrieves the actual tracing details from Cassandra and populates the
attributes of this instance.  Because tracing details are stored
asynchronously by Cassandra, this may need to retry the session
detail fetch.  If the trace is still not available after max_wait
seconds, [`TraceUnavailable`](#cassandra.query.TraceUnavailable) will be raised; if max_wait is
`None`, this will retry forever.

wait_for_complete=False bypasses the wait for duration to be populated.
This can be used to query events from partial sessions.

query_cl specifies a consistency level to use for polling the trace tables,
if it should be different than the session default.

### *class* cassandra.query.TraceEvent

Representation of a single event within a query trace.

#### description *= None*

A brief description of the event.

#### datetime *= None*

A UTC `datetime.datetime` marking when the event occurred.

#### source *= None*

The IP address of the node this event occurred on.

#### source_elapsed *= None*

A `datetime.timedelta` measuring the amount of time until
this event occurred starting from when [`source`](#cassandra.query.TraceEvent.source) first
received the query.

#### thread_name *= None*

The name of the thread that this event occurred on.

### *exception* cassandra.query.TraceUnavailable

Raised when complete trace details cannot be fetched from Cassandra.
