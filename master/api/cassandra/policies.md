# cassandra.policies

Load balancing and Failure Handling Policies

<a id="module-cassandra.policies"></a>

## Load Balancing

### *class* cassandra.policies.HostDistance

A measure of how “distant” a node is from the client, which
may influence how the load balancer distributes requests
and how many connections are opened to the node.

#### IGNORED *= -1*

A node with this distance should never be queried or have
connections opened to it.

#### LOCAL_RACK *= 0*

Nodes with `LOCAL_RACK` distance will be preferred for operations
under some load balancing policies (such as [`RackAwareRoundRobinPolicy`](#cassandra.policies.RackAwareRoundRobinPolicy))
and will have a greater number of connections opened against
them by default.

This distance is typically used for nodes within the same
datacenter and the same rack as the client.

#### LOCAL *= 1*

Nodes with `LOCAL` distance will be preferred for operations
under some load balancing policies (such as [`DCAwareRoundRobinPolicy`](#cassandra.policies.DCAwareRoundRobinPolicy))
and will have a greater number of connections opened against
them by default.

This distance is typically used for nodes within the same
datacenter as the client.

#### REMOTE *= 2*

Nodes with `REMOTE` distance will be treated as a last resort
by some load balancing policies (such as [`DCAwareRoundRobinPolicy`](#cassandra.policies.DCAwareRoundRobinPolicy)
and [`RackAwareRoundRobinPolicy`](#cassandra.policies.RackAwareRoundRobinPolicy))and will have a smaller number of
connections opened against them by default.

This distance is typically used for nodes outside of the
datacenter that the client is running in.

### *class* cassandra.policies.LoadBalancingPolicy

Load balancing policies are used to decide how to distribute
requests among all possible coordinator nodes in the cluster.

In particular, they may focus on querying “near” nodes (those
in a local datacenter) or on querying nodes who happen to
be replicas for the requested data.

You may also use subclasses of [`LoadBalancingPolicy`](#cassandra.policies.LoadBalancingPolicy) for
custom behavior.

You should always use immutable collections (e.g., tuples or
frozensets) to store information about hosts to prevent accidental
modification. When there are changes to the hosts (e.g., a host is
down or up), the old collection should be replaced with a new one.

#### distance(host)

Returns a measure of how remote a [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) is in
terms of the [`HostDistance`](#cassandra.policies.HostDistance) enums.

#### populate(cluster, hosts)

This method is called to initialize the load balancing
policy with a set of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) instances before its
first use.  The cluster parameter is an instance of
[`Cluster`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster).

#### make_query_plan(working_keyspace=None, query=None)

Given a [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) instance, return a iterable
of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) instances which should be queried in that
order.  A generator may work well for custom implementations
of this method.

Note that the query argument may be `None` when preparing
statements.

working_keyspace should be the string name of the current keyspace,
as set through [`Session.set_keyspace()`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.set_keyspace) or with a `USE`
statement.

#### check_supported()

This will be called after the cluster Metadata has been initialized.
If the load balancing policy implementation cannot be supported for
some reason (such as a missing C extension), this is the point at
which it should raise an exception.

### *class* cassandra.policies.RoundRobinPolicy

A subclass of [`LoadBalancingPolicy`](#cassandra.policies.LoadBalancingPolicy) which evenly
distributes queries across all nodes in the cluster,
regardless of what datacenter the nodes may be in.

#### populate(cluster, hosts)

This method is called to initialize the load balancing
policy with a set of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) instances before its
first use.  The cluster parameter is an instance of
[`Cluster`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster).

#### distance(host)

Returns a measure of how remote a [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) is in
terms of the [`HostDistance`](#cassandra.policies.HostDistance) enums.

#### make_query_plan(working_keyspace=None, query=None)

Given a [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) instance, return a iterable
of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) instances which should be queried in that
order.  A generator may work well for custom implementations
of this method.

Note that the query argument may be `None` when preparing
statements.

working_keyspace should be the string name of the current keyspace,
as set through [`Session.set_keyspace()`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.set_keyspace) or with a `USE`
statement.

#### on_up(host)

Called when a node is marked up.

#### on_down(host)

Called when a node is marked down.

#### on_add(host)

Called when a node is added to the cluster.  The newly added node
should be considered up.

#### on_remove(host)

Called when a node is removed from the cluster.

### *class* cassandra.policies.DCAwareRoundRobinPolicy(local_dc='', used_hosts_per_remote_dc=0)

Similar to [`RoundRobinPolicy`](#cassandra.policies.RoundRobinPolicy), but prefers hosts
in the local datacenter and only uses nodes in remote
datacenters as a last resort.

The local_dc parameter should be the name of the datacenter
(such as is reported by `nodetool ring`) that should
be considered local. If not specified, the driver will choose
a local_dc based on the first host among [`Cluster.contact_points`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster.contact_points)
having a valid DC. If relying on this mechanism, all specified
contact points should be nodes in a single, local DC.

used_hosts_per_remote_dc controls how many nodes in
each remote datacenter will have connections opened
against them. In other words, used_hosts_per_remote_dc hosts
will be considered [`REMOTE`](#cassandra.policies.HostDistance.REMOTE) and the
rest will be considered [`IGNORED`](#cassandra.policies.HostDistance.IGNORED).
By default, all remote hosts are ignored.

#### populate(cluster, hosts)

This method is called to initialize the load balancing
policy with a set of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) instances before its
first use.  The cluster parameter is an instance of
[`Cluster`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster).

#### distance(host)

Returns a measure of how remote a [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) is in
terms of the [`HostDistance`](#cassandra.policies.HostDistance) enums.

#### make_query_plan(working_keyspace=None, query=None)

Given a [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) instance, return a iterable
of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) instances which should be queried in that
order.  A generator may work well for custom implementations
of this method.

Note that the query argument may be `None` when preparing
statements.

working_keyspace should be the string name of the current keyspace,
as set through [`Session.set_keyspace()`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.set_keyspace) or with a `USE`
statement.

#### on_up(host)

Called when a node is marked up.

#### on_down(host)

Called when a node is marked down.

#### on_add(host)

Called when a node is added to the cluster.  The newly added node
should be considered up.

#### on_remove(host)

Called when a node is removed from the cluster.

### *class* cassandra.policies.RackAwareRoundRobinPolicy(local_dc, local_rack, used_hosts_per_remote_dc=0)

Similar to [`DCAwareRoundRobinPolicy`](#cassandra.policies.DCAwareRoundRobinPolicy), but prefers hosts
in the local rack, before hosts in the local datacenter but a
different rack, before hosts in all other datercentres

The local_dc and local_rack parameters should be the name of the
datacenter and rack (such as is reported by `nodetool ring`) that
should be considered local.

used_hosts_per_remote_dc controls how many nodes in
each remote datacenter will have connections opened
against them. In other words, used_hosts_per_remote_dc hosts
will be considered [`REMOTE`](#cassandra.policies.HostDistance.REMOTE) and the
rest will be considered [`IGNORED`](#cassandra.policies.HostDistance.IGNORED).
By default, all remote hosts are ignored.

#### populate(cluster, hosts)

This method is called to initialize the load balancing
policy with a set of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) instances before its
first use.  The cluster parameter is an instance of
[`Cluster`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster).

#### distance(host)

Returns a measure of how remote a [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) is in
terms of the [`HostDistance`](#cassandra.policies.HostDistance) enums.

#### make_query_plan(working_keyspace=None, query=None)

Given a [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) instance, return a iterable
of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) instances which should be queried in that
order.  A generator may work well for custom implementations
of this method.

Note that the query argument may be `None` when preparing
statements.

working_keyspace should be the string name of the current keyspace,
as set through [`Session.set_keyspace()`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.set_keyspace) or with a `USE`
statement.

#### on_up(host)

Called when a node is marked up.

#### on_down(host)

Called when a node is marked down.

#### on_add(host)

Called when a node is added to the cluster.  The newly added node
should be considered up.

#### on_remove(host)

Called when a node is removed from the cluster.

### *class* cassandra.policies.WhiteListRoundRobinPolicy(hosts)

A subclass of [`RoundRobinPolicy`](#cassandra.policies.RoundRobinPolicy) which evenly
distributes queries across all nodes in the cluster,
regardless of what datacenter the nodes may be in, but
only if that node exists in the list of allowed nodes

This policy is addresses the issue described in
[https://datastax-oss.atlassian.net/browse/JAVA-145](https://datastax-oss.atlassian.net/browse/JAVA-145)
Where connection errors occur when connection
attempts are made to private IP addresses remotely

The hosts parameter should be a sequence of hosts to permit
connections to.

#### populate(cluster, hosts)

This method is called to initialize the load balancing
policy with a set of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) instances before its
first use.  The cluster parameter is an instance of
[`Cluster`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster).

#### distance(host)

Returns a measure of how remote a [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) is in
terms of the [`HostDistance`](#cassandra.policies.HostDistance) enums.

#### on_up(host)

Called when a node is marked up.

#### on_add(host)

Called when a node is added to the cluster.  The newly added node
should be considered up.

### *class* cassandra.policies.TokenAwarePolicy(child_policy, shuffle_replicas=True)

A [`LoadBalancingPolicy`](#cassandra.policies.LoadBalancingPolicy) wrapper that adds token awareness to
a child policy.

This alters the child policy’s behavior so that it first attempts to
send queries to [`LOCAL`](#cassandra.policies.HostDistance.LOCAL) replicas (as determined
by the child policy) based on the [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement)’s
[`routing_key`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement.routing_key). If [`shuffle_replicas`](#cassandra.policies.TokenAwarePolicy.shuffle_replicas) is
truthy, these replicas will be yielded in a random order. Once those
hosts are exhausted, the remaining hosts in the child policy’s query
plan will be used in the order provided by the child policy.

If no [`routing_key`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement.routing_key) is set on the query, the child
policy’s query plan will be used as is.

#### shuffle_replicas *= True*

Yield local replicas in a random order.

#### populate(cluster, hosts)

This method is called to initialize the load balancing
policy with a set of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) instances before its
first use.  The cluster parameter is an instance of
[`Cluster`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster).

#### check_supported()

This will be called after the cluster Metadata has been initialized.
If the load balancing policy implementation cannot be supported for
some reason (such as a missing C extension), this is the point at
which it should raise an exception.

#### distance(\*args, \*\*kwargs)

Returns a measure of how remote a [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) is in
terms of the [`HostDistance`](#cassandra.policies.HostDistance) enums.

#### make_query_plan(working_keyspace=None, query=None)

Given a [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) instance, return a iterable
of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) instances which should be queried in that
order.  A generator may work well for custom implementations
of this method.

Note that the query argument may be `None` when preparing
statements.

working_keyspace should be the string name of the current keyspace,
as set through [`Session.set_keyspace()`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.set_keyspace) or with a `USE`
statement.

#### on_up(\*args, \*\*kwargs)

Called when a node is marked up.

#### on_down(\*args, \*\*kwargs)

Called when a node is marked down.

#### on_add(\*args, \*\*kwargs)

Called when a node is added to the cluster.  The newly added node
should be considered up.

#### on_remove(\*args, \*\*kwargs)

Called when a node is removed from the cluster.

### *class* cassandra.policies.HostFilterPolicy(child_policy, predicate)

A [`LoadBalancingPolicy`](#cassandra.policies.LoadBalancingPolicy) subclass configured with a child policy,
and a single-argument predicate. This policy defers to the child policy for
hosts where `predicate(host)` is truthy. Hosts for which
`predicate(host)` is falsy will be considered [`IGNORED`](#cassandra.policies.HostDistance.IGNORED), and will
not be used in a query plan.

This can be used in the cases where you need a whitelist or blacklist
policy, e.g. to prepare for decommissioning nodes or for testing:

```python
def address_is_ignored(host):
    return host.address in [ignored_address0, ignored_address1]

blacklist_filter_policy = HostFilterPolicy(
    child_policy=RoundRobinPolicy(),
    predicate=address_is_ignored
)

cluster = Cluster(
    primary_host,
    load_balancing_policy=blacklist_filter_policy,
)
```

See the note in the [`make_query_plan()`](#cassandra.policies.HostFilterPolicy.make_query_plan) documentation for a caveat on
how wrapping ordering polices (e.g. [`RoundRobinPolicy`](#cassandra.policies.RoundRobinPolicy)) may break
desirable properties of the wrapped policy.

Please note that whitelist and blacklist policies are not recommended for
general, day-to-day use. You probably want something like
[`DCAwareRoundRobinPolicy`](#cassandra.policies.DCAwareRoundRobinPolicy), which prefers a local DC but has
fallbacks, over a brute-force method like whitelisting or blacklisting.

* **Parameters:**
  * **child_policy** – an instantiated [`LoadBalancingPolicy`](#cassandra.policies.LoadBalancingPolicy)
    that this one will defer to.
  * **predicate** – a one-parameter function that takes a [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host).
    If it returns a falsy value, the [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) will
    be [`IGNORED`](#cassandra.policies.HostDistance.IGNORED) and not returned in query plans.

<!-- we document these methods manually so we can specify a param to predicate -->

#### predicate(host)

A predicate, set on object initialization, that takes a [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host)
and returns a value. If the value is falsy, the [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) is
[`IGNORED`](#cassandra.policies.HostDistance.IGNORED). If the value is truthy,
[`HostFilterPolicy`](#cassandra.policies.HostFilterPolicy) defers to the child policy to determine the
host’s distance.

This is a read-only value set in `__init__`, implemented as a
`property`.

#### distance(host)

Checks if `predicate(host)`, then returns
[`IGNORED`](#cassandra.policies.HostDistance.IGNORED) if falsy, and defers to the child policy
otherwise.

#### make_query_plan(working_keyspace=None, query=None)

Defers to the child policy’s
[`LoadBalancingPolicy.make_query_plan()`](#cassandra.policies.LoadBalancingPolicy.make_query_plan) and filters the results.

Note that this filtering may break desirable properties of the wrapped
policy in some cases. For instance, imagine if you configure this
policy to filter out `host2`, and to wrap a round-robin policy that
rotates through three hosts in the order `host1, host2, host3`,
`host2, host3, host1`, `host3, host1, host2`, repeating. This
policy will yield `host1, host3`, `host3, host1`, `host3, host1`,
disproportionately favoring `host3`.

### *class* cassandra.policies.DefaultLoadBalancingPolicy(child_policy)

A [`LoadBalancingPolicy`](#cassandra.policies.LoadBalancingPolicy) wrapper that adds the ability to target a specific host first.

If no host is set on the query, the child policy’s query plan will be used as is.

#### populate(cluster, hosts)

This method is called to initialize the load balancing
policy with a set of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) instances before its
first use.  The cluster parameter is an instance of
[`Cluster`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster).

#### make_query_plan(working_keyspace=None, query=None)

Given a [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) instance, return a iterable
of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) instances which should be queried in that
order.  A generator may work well for custom implementations
of this method.

Note that the query argument may be `None` when preparing
statements.

working_keyspace should be the string name of the current keyspace,
as set through [`Session.set_keyspace()`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.set_keyspace) or with a `USE`
statement.

### *class* cassandra.policies.DSELoadBalancingPolicy(\*args, \*\*kwargs)

*Deprecated:* This will be removed in the next major release,
consider using [`DefaultLoadBalancingPolicy`](#cassandra.policies.DefaultLoadBalancingPolicy).

## Translating Server Node Addresses

### *class* cassandra.policies.AddressTranslator

Interface for translating cluster-defined endpoints.

The driver discovers nodes using server metadata and topology change events. Normally,
the endpoint defined by the server is the right way to connect to a node. In some environments,
these addresses may not be reachable, or not preferred (public vs. private IPs in cloud environments,
suboptimal routing, etc). This interface allows for translating from server defined endpoints to
preferred addresses for driver connections.

*Note:* `contact_points` provided while creating the [`Cluster`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster) instance are not
translated using this mechanism – only addresses received from Cassandra nodes are.

#### translate(addr)

Accepts the node ip address, and returns a translated address to be used connecting to this node.

### *class* cassandra.policies.IdentityTranslator

Returns the endpoint with no translation

#### translate(addr)

Accepts the node ip address, and returns a translated address to be used connecting to this node.

### *class* cassandra.policies.EC2MultiRegionTranslator

Resolves private ips of the hosts in the same datacenter as the client, and public ips of hosts in other datacenters.

#### translate(addr)

Reverse DNS the public broadcast_address, then lookup that hostname to get the AWS-resolved IP, which
will point to the private IP address within the same datacenter.

## Marking Hosts Up or Down

### *class* cassandra.policies.ConvictionPolicy(host)

A policy which decides when hosts should be considered down
based on the types of failures and the number of failures.

If custom behavior is needed, this class may be subclassed.

host is an instance of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host).

#### add_failure(connection_exc)

Implementations should return `True` if the host should be
convicted, `False` otherwise.

#### reset()

Implementations should clear out any convictions or state regarding
the host.

### *class* cassandra.policies.SimpleConvictionPolicy(host)

The default implementation of [`ConvictionPolicy`](#cassandra.policies.ConvictionPolicy),
which simply marks a host as down after the first failure
of any kind.

host is an instance of [`Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host).

#### add_failure(connection_exc)

Implementations should return `True` if the host should be
convicted, `False` otherwise.

#### reset()

Implementations should clear out any convictions or state regarding
the host.

## Reconnecting to Dead Hosts

### *class* cassandra.policies.ReconnectionPolicy

This class and its subclasses govern how frequently an attempt is made
to reconnect to nodes that are marked as dead.

If custom behavior is needed, this class may be subclassed.

#### new_schedule()

This should return a finite or infinite iterable of delays (each as a
floating point number of seconds) in-between each failed reconnection
attempt.  Note that if the iterable is finite, reconnection attempts
will cease once the iterable is exhausted.

### *class* cassandra.policies.ConstantReconnectionPolicy(delay, max_attempts=None)

A [`ReconnectionPolicy`](#cassandra.policies.ReconnectionPolicy) subclass which sleeps for a fixed delay
in-between each reconnection attempt.

delay should be a floating point number of seconds to wait in-between
each attempt.

max_attempts should be a total number of attempts to be made before
giving up, or `None` to continue reconnection attempts forever.
The default is 64.

#### new_schedule()

This should return a finite or infinite iterable of delays (each as a
floating point number of seconds) in-between each failed reconnection
attempt.  Note that if the iterable is finite, reconnection attempts
will cease once the iterable is exhausted.

### *class* cassandra.policies.ExponentialReconnectionPolicy(base_delay, max_delay, max_attempts=None)

A [`ReconnectionPolicy`](#cassandra.policies.ReconnectionPolicy) subclass which exponentially increases
the length of the delay in-between each reconnection attempt up to
a set maximum delay.

A random amount of jitter (+/- 15%) will be added to the pure exponential
delay value to avoid the situations where many reconnection handlers are
trying to reconnect at exactly the same time.

base_delay and max_delay should be in floating point units of
seconds.

max_attempts should be a total number of attempts to be made before
giving up, or `None` to continue reconnection attempts forever.
The default is 64.

#### new_schedule()

This should return a finite or infinite iterable of delays (each as a
floating point number of seconds) in-between each failed reconnection
attempt.  Note that if the iterable is finite, reconnection attempts
will cease once the iterable is exhausted.

## Retrying Failed Operations

### *class* cassandra.policies.WriteType

For usage with [`RetryPolicy`](#cassandra.policies.RetryPolicy), this describe a type
of write operation.

#### SIMPLE *= 0*

A write to a single partition key. Such writes are guaranteed to be atomic
and isolated.

#### BATCH *= 1*

A write to multiple partition keys that used the distributed batch log to
ensure atomicity.

#### UNLOGGED_BATCH *= 2*

A write to multiple partition keys that did not use the distributed batch
log. Atomicity for such writes is not guaranteed.

#### COUNTER *= 3*

A counter write (for one or multiple partition keys). Such writes should
not be replayed in order to avoid overcount.

#### BATCH_LOG *= 4*

The initial write to the distributed batch log that Cassandra performs
internally before a BATCH write.

#### CAS *= 5*

A lighweight-transaction write, such as “DELETE … IF EXISTS”.

#### VIEW *= 6*

This WriteType is only seen in results for requests that were unable to
complete MV operations.

#### CDC *= 7*

This WriteType is only seen in results for requests that were unable to
complete CDC operations.

### *class* cassandra.policies.RetryPolicy

A policy that describes whether to retry, rethrow, or ignore coordinator
timeout and unavailable failures. These are failures reported from the
server side. Timeouts are configured by
[settings in cassandra.yaml](https://github.com/apache/cassandra/blob/cassandra-2.1.4/conf/cassandra.yaml#L568-L584).
Unavailable failures occur when the coordinator cannot achieve the consistency
level for a request. For further information see the method descriptions
below.

To specify a default retry policy, set the
[`Cluster.default_retry_policy`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster.default_retry_policy) attribute to an instance of this
class or one of its subclasses.

To specify a retry policy per query, set the [`Statement.retry_policy`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement.retry_policy)
attribute to an instance of this class or one of its subclasses.

If custom behavior is needed for retrying certain operations,
this class may be subclassed.

#### RETRY *= 0*

This should be returned from the below methods if the operation
should be retried on the same connection.

#### RETHROW *= 1*

This should be returned from the below methods if the failure
should be propagated and no more retries attempted.

#### IGNORE *= 2*

This should be returned from the below methods if the failure
should be ignored but no more retries should be attempted.

#### RETRY_NEXT_HOST *= 3*

This should be returned from the below methods if the operation
should be retried on another connection.

#### on_read_timeout(query, consistency, required_responses, received_responses, data_retrieved, retry_num)

This is called when a read operation times out from the coordinator’s
perspective (i.e. a replica did not respond to the coordinator in time).
It should return a tuple with two items: one of the class enums (such
as [`RETRY`](#cassandra.policies.RetryPolicy.RETRY)) and a [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) to retry the
operation at or `None` to keep the same consistency level.

query is the [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) that timed out.

consistency is the [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) that the operation was
attempted at.

The required_responses and received_responses parameters describe
how many replicas needed to respond to meet the requested consistency
level and how many actually did respond before the coordinator timed
out the request. data_retrieved is a boolean indicating whether
any of those responses contained data (as opposed to just a digest).

retry_num counts how many times the operation has been retried, so
the first time this method is called, retry_num will be 0.

By default, operations will be retried at most once, and only if
a sufficient number of replicas responded (with data digests).

#### on_write_timeout(query, consistency, write_type, required_responses, received_responses, retry_num)

This is called when a write operation times out from the coordinator’s
perspective (i.e. a replica did not respond to the coordinator in time).

query is the [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) that timed out.

consistency is the [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) that the operation was
attempted at.

write_type is one of the [`WriteType`](#cassandra.policies.WriteType) enums describing the
type of write operation.

The required_responses and received_responses parameters describe
how many replicas needed to acknowledge the write to meet the requested
consistency level and how many replicas actually did acknowledge the
write before the coordinator timed out the request.

retry_num counts how many times the operation has been retried, so
the first time this method is called, retry_num will be 0.

By default, failed write operations will retried at most once, and
they will only be retried if the write_type was
[`BATCH_LOG`](#cassandra.policies.WriteType.BATCH_LOG).

#### on_unavailable(query, consistency, required_replicas, alive_replicas, retry_num)

This is called when the coordinator node determines that a read or
write operation cannot be successful because the number of live
replicas are too low to meet the requested [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel).
This means that the read or write operation was never forwarded to
any replicas.

query is the [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) that failed.

consistency is the [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) that the operation was
attempted at.

required_replicas is the number of replicas that would have needed to
acknowledge the operation to meet the requested consistency level.
alive_replicas is the number of replicas that the coordinator
considered alive at the time of the request.

retry_num counts how many times the operation has been retried, so
the first time this method is called, retry_num will be 0.

By default, if this is the first retry, it triggers a retry on the next
host in the query plan with the same consistency level. If this is not the
first retry, no retries will be attempted and the error will be re-raised.

#### on_request_error(query, consistency, error, retry_num)

This is called when an unexpected error happens. This can be in the
following situations:

* On a connection error
* On server errors: overloaded, isBootstrapping, serverError, etc.

query is the [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) that timed out.

consistency is the [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) that the operation was
attempted at.

error the instance of the exception.

retry_num counts how many times the operation has been retried, so
the first time this method is called, retry_num will be 0.

By default, it triggers a retry on the next host in the query plan
with the same consistency level.

### *class* cassandra.policies.FallthroughRetryPolicy

A retry policy that never retries and always propagates failures to
the application.

#### on_read_timeout(\*args, \*\*kwargs)

This is called when a read operation times out from the coordinator’s
perspective (i.e. a replica did not respond to the coordinator in time).
It should return a tuple with two items: one of the class enums (such
as [`RETRY`](#cassandra.policies.RetryPolicy.RETRY)) and a [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) to retry the
operation at or `None` to keep the same consistency level.

query is the [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) that timed out.

consistency is the [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) that the operation was
attempted at.

The required_responses and received_responses parameters describe
how many replicas needed to respond to meet the requested consistency
level and how many actually did respond before the coordinator timed
out the request. data_retrieved is a boolean indicating whether
any of those responses contained data (as opposed to just a digest).

retry_num counts how many times the operation has been retried, so
the first time this method is called, retry_num will be 0.

By default, operations will be retried at most once, and only if
a sufficient number of replicas responded (with data digests).

#### on_write_timeout(\*args, \*\*kwargs)

This is called when a write operation times out from the coordinator’s
perspective (i.e. a replica did not respond to the coordinator in time).

query is the [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) that timed out.

consistency is the [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) that the operation was
attempted at.

write_type is one of the [`WriteType`](#cassandra.policies.WriteType) enums describing the
type of write operation.

The required_responses and received_responses parameters describe
how many replicas needed to acknowledge the write to meet the requested
consistency level and how many replicas actually did acknowledge the
write before the coordinator timed out the request.

retry_num counts how many times the operation has been retried, so
the first time this method is called, retry_num will be 0.

By default, failed write operations will retried at most once, and
they will only be retried if the write_type was
[`BATCH_LOG`](#cassandra.policies.WriteType.BATCH_LOG).

#### on_unavailable(\*args, \*\*kwargs)

This is called when the coordinator node determines that a read or
write operation cannot be successful because the number of live
replicas are too low to meet the requested [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel).
This means that the read or write operation was never forwarded to
any replicas.

query is the [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) that failed.

consistency is the [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) that the operation was
attempted at.

required_replicas is the number of replicas that would have needed to
acknowledge the operation to meet the requested consistency level.
alive_replicas is the number of replicas that the coordinator
considered alive at the time of the request.

retry_num counts how many times the operation has been retried, so
the first time this method is called, retry_num will be 0.

By default, if this is the first retry, it triggers a retry on the next
host in the query plan with the same consistency level. If this is not the
first retry, no retries will be attempted and the error will be re-raised.

#### on_request_error(\*args, \*\*kwargs)

This is called when an unexpected error happens. This can be in the
following situations:

* On a connection error
* On server errors: overloaded, isBootstrapping, serverError, etc.

query is the [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) that timed out.

consistency is the [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) that the operation was
attempted at.

error the instance of the exception.

retry_num counts how many times the operation has been retried, so
the first time this method is called, retry_num will be 0.

By default, it triggers a retry on the next host in the query plan
with the same consistency level.

### *class* cassandra.policies.DowngradingConsistencyRetryPolicy(\*args, \*\*kwargs)

*Deprecated:* This retry policy will be removed in the next major release.

A retry policy that sometimes retries with a lower consistency level than
the one initially requested.

**BEWARE**: This policy may retry queries using a lower consistency
level than the one initially requested. By doing so, it may break
consistency guarantees. In other words, if you use this retry policy,
there are cases (documented below) where a read at [`QUORUM`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel.QUORUM)
*may not* see a preceding write at [`QUORUM`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel.QUORUM). Do not use this
policy unless you have understood the cases where this can happen and
are ok with that. It is also recommended to subclass this class so
that queries that required a consistency level downgrade can be
recorded (so that repairs can be made later, etc).

This policy implements the same retries as [`RetryPolicy`](#cassandra.policies.RetryPolicy),
but on top of that, it also retries in the following cases:

* On a read timeout: if the number of replicas that responded is
  greater than one but lower than is required by the requested
  consistency level, the operation is retried at a lower consistency
  level.
* On a write timeout: if the operation is an [`UNLOGGED_BATCH`](#cassandra.policies.WriteType.UNLOGGED_BATCH)
  and at least one replica acknowledged the write, the operation is
  retried at a lower consistency level.  Furthermore, for other
  write types, if at least one replica acknowledged the write, the
  timeout is ignored.
* On an unavailable exception: if at least one replica is alive, the
  operation is retried at a lower consistency level.

The reasoning behind this retry policy is as follows: if, based
on the information the Cassandra coordinator node returns, retrying the
operation with the initially requested consistency has a chance to
succeed, do it. Otherwise, if based on that information we know the
initially requested consistency level cannot be achieved currently, then:

* For writes, ignore the exception (thus silently failing the
  consistency requirement) if we know the write has been persisted on at
  least one replica.
* For reads, try reading at a lower consistency level (thus silently
  failing the consistency requirement).

In other words, this policy implements the idea that if the requested
consistency level cannot be achieved, the next best thing for writes is
to make sure the data is persisted, and that reading something is better
than reading nothing, even if there is a risk of reading stale data.

#### on_read_timeout(query, consistency, required_responses, received_responses, data_retrieved, retry_num)

This is called when a read operation times out from the coordinator’s
perspective (i.e. a replica did not respond to the coordinator in time).
It should return a tuple with two items: one of the class enums (such
as [`RETRY`](#cassandra.policies.RetryPolicy.RETRY)) and a [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) to retry the
operation at or `None` to keep the same consistency level.

query is the [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) that timed out.

consistency is the [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) that the operation was
attempted at.

The required_responses and received_responses parameters describe
how many replicas needed to respond to meet the requested consistency
level and how many actually did respond before the coordinator timed
out the request. data_retrieved is a boolean indicating whether
any of those responses contained data (as opposed to just a digest).

retry_num counts how many times the operation has been retried, so
the first time this method is called, retry_num will be 0.

By default, operations will be retried at most once, and only if
a sufficient number of replicas responded (with data digests).

#### on_write_timeout(query, consistency, write_type, required_responses, received_responses, retry_num)

This is called when a write operation times out from the coordinator’s
perspective (i.e. a replica did not respond to the coordinator in time).

query is the [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) that timed out.

consistency is the [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) that the operation was
attempted at.

write_type is one of the [`WriteType`](#cassandra.policies.WriteType) enums describing the
type of write operation.

The required_responses and received_responses parameters describe
how many replicas needed to acknowledge the write to meet the requested
consistency level and how many replicas actually did acknowledge the
write before the coordinator timed out the request.

retry_num counts how many times the operation has been retried, so
the first time this method is called, retry_num will be 0.

By default, failed write operations will retried at most once, and
they will only be retried if the write_type was
[`BATCH_LOG`](#cassandra.policies.WriteType.BATCH_LOG).

#### on_unavailable(query, consistency, required_replicas, alive_replicas, retry_num)

This is called when the coordinator node determines that a read or
write operation cannot be successful because the number of live
replicas are too low to meet the requested [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel).
This means that the read or write operation was never forwarded to
any replicas.

query is the [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) that failed.

consistency is the [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) that the operation was
attempted at.

required_replicas is the number of replicas that would have needed to
acknowledge the operation to meet the requested consistency level.
alive_replicas is the number of replicas that the coordinator
considered alive at the time of the request.

retry_num counts how many times the operation has been retried, so
the first time this method is called, retry_num will be 0.

By default, if this is the first retry, it triggers a retry on the next
host in the query plan with the same consistency level. If this is not the
first retry, no retries will be attempted and the error will be re-raised.

## Retrying Idempotent Operations

### *class* cassandra.policies.SpeculativeExecutionPolicy

Interface for specifying speculative execution plans

#### new_plan(keyspace, statement)

Returns

* **Parameters:**
  * **keyspace**
  * **statement**
* **Returns:**

### *class* cassandra.policies.ConstantSpeculativeExecutionPolicy(delay, max_attempts)

A speculative execution policy that sends a new query every X seconds (**delay**) for a maximum of Y attempts (**max_attempts**).

#### new_plan(keyspace, statement)

Returns

* **Parameters:**
  * **keyspace**
  * **statement**
* **Returns:**
