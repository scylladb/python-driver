# cassandra.metrics

Performance Metrics

<a id="module-cassandra.metrics"></a>

### *class* cassandra.metrics.Metrics

A collection of timers and counters for various performance metrics.

Timer metrics are represented as floating point seconds.

#### request_timer *= None*

A `PmfStat` timer for requests. This is a dict-like
object with the following keys:

* count - number of requests that have been timed
* min - min latency
* max - max latency
* mean - mean latency
* stddev - standard deviation for latencies
* median - median latency
* 75percentile - 75th percentile latencies
* 95percentile - 95th percentile latencies
* 98percentile - 98th percentile latencies
* 99percentile - 99th percentile latencies
* 999percentile - 99.9th percentile latencies

#### connection_errors *= None*

A `IntStat` count of the number of times that a
request to a Cassandra node has failed due to a connection problem.

#### write_timeouts *= None*

A `IntStat` count of write requests that resulted
in a timeout.

#### read_timeouts *= None*

A `IntStat` count of read requests that resulted
in a timeout.

#### unavailables *= None*

A `IntStat` count of write or read requests that
failed due to an insufficient number of replicas being alive to meet
the requested [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel).

#### other_errors *= None*

A `IntStat` count of all other request failures,
including failures caused by invalid requests, bootstrapping nodes,
overloaded nodes, etc.

#### retries *= None*

A `IntStat` count of the number of times a
request was retried based on the [`RetryPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.RetryPolicy) decision.

#### ignores *= None*

A `IntStat` count of the number of times a
failed request was ignored based on the [`RetryPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.RetryPolicy) decision.

#### known_hosts *= None*

A `IntStat` count of the number of nodes in
the cluster that the driver is aware of, regardless of whether any
connections are opened to those nodes.

#### connected_to *= None*

A `IntStat` count of the number of nodes that
the driver currently has at least one connection open to.

#### open_connections *= None*

A `IntStat` count of the number connections
the driver currently has open.

#### get_stats()

Returns the metrics for the registered cluster instance.

#### set_stats_name(stats_name)

Set the metrics stats name.
The stats_name is a string used to access the metrics through getStats(): getStats()[<stats_name>]
Default is ‘cassandra-<num>’.

#### shutdown()

Remove this metrics instance from the global registry.
Called when the cluster is shutdown to prevent stale references.
