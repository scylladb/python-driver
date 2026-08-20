# cassandra.cluster

Clusters and Sessions

<a id="module-cassandra.cluster"></a>

### *class* cassandra.cluster.Cluster([contact_points=('127.0.0.1',)][, port=9042][, executor_threads=2], \*\*attr_kwargs)

The main class to use when interacting with a Cassandra cluster.
Typically, one instance of this class will be created for each
separate Cassandra cluster that your application interacts with.

Example usage:

```default
>>> from cassandra.cluster import Cluster
>>> cluster = Cluster(['192.168.1.1', '192.168.1.2'])
>>> session = cluster.connect()
>>> session.execute("CREATE KEYSPACE ...")
>>> ...
>>> cluster.shutdown()
```

`Cluster` and `Session` also provide context management functions
which implicitly handle shutdown when leaving scope.

`executor_threads` defines the number of threads in a pool for handling asynchronous tasks such as
extablishing connection pools or refreshing metadata.

Any of the mutable Cluster attributes may be set as keyword arguments to the constructor.

#### contact_points *= ['127.0.0.1']*

The list of contact points to try connecting for cluster discovery. A
contact point can be a string (ip or hostname), a tuple (ip/hostname, port) or a
[`connection.EndPoint`](https://python-driver.docs.scylladb.com/master/api/cassandra/connection.md#cassandra.connection.EndPoint) instance.

Defaults to loopback interface.

Note: When using [`DCAwareRoundRobinPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.DCAwareRoundRobinPolicy) with no explicit
local_dc set (as is the default), the DC is chosen from an arbitrary
host in contact_points. In this case, contact_points should contain
only nodes from a single, local DC.

Note: In the next major version, if you specify contact points, you will
also be required to also explicitly specify a load-balancing policy. This
change will help prevent cases where users had hard-to-debug issues
surrounding unintuitive default load-balancing policy behavior.

#### port *= 9042*

The server-side port to open connections to. Defaults to 9042.

#### cql_version *= None*

If a specific version of CQL should be used, this may be set to that
string version.  Otherwise, the highest CQL version supported by the
server will be automatically used.

#### protocol_version *= 5*

The maximum version of the native protocol to use.

See [`ProtocolVersion`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ProtocolVersion) for more information about versions.

If not set in the constructor, the driver will automatically downgrade
version based on a negotiation with the server, but it is most efficient
to set this to the maximum supported by your version of ScyllaDB.
Setting this will also prevent conflicting versions negotiated if your
cluster is upgraded.

#### compression *: bool | str | None* *= True*

Controls compression for communications between the driver and Cassandra.
If left as the default of `True`, either lz4 or snappy compression
may be used, depending on what is supported by both the driver
and Cassandra.  If both are fully supported, lz4 will be preferred.

You may also set this to ‘snappy’ or ‘lz4’ to request that specific
compression type.

Setting this to `False` or `None` disables compression.

#### auth_provider

When [`protocol_version`](#cassandra.cluster.Cluster.protocol_version) is 2 or higher, this should
be an instance of a subclass of [`AuthProvider`](https://python-driver.docs.scylladb.com/master/api/cassandra/auth.md#cassandra.auth.AuthProvider),
such as [`PlainTextAuthProvider`](https://python-driver.docs.scylladb.com/master/api/cassandra/auth.md#cassandra.auth.PlainTextAuthProvider).

When not using authentication, this should be left as `None`.

#### load_balancing_policy

An instance of [`policies.LoadBalancingPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.LoadBalancingPolicy) or
one of its subclasses.

#### Versionchanged
Changed in version 2.6.0.

Defaults to [`TokenAwarePolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.TokenAwarePolicy) ([`DCAwareRoundRobinPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.DCAwareRoundRobinPolicy)).
when using CPython (where the murmur3 extension is available). [`DCAwareRoundRobinPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.DCAwareRoundRobinPolicy)
otherwise. Default local DC will be chosen from contact points.

**Please see** [`DCAwareRoundRobinPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.DCAwareRoundRobinPolicy) **for a discussion on default behavior with respect to
DC locality and remote nodes.**

#### reconnection_policy *= <cassandra.policies.ExponentialReconnectionPolicy object>*

An instance of [`policies.ReconnectionPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.ReconnectionPolicy). Defaults to an instance
of [`ExponentialReconnectionPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.ExponentialReconnectionPolicy) with a base delay of one second and
a max delay of ten minutes.

#### default_retry_policy *= <cassandra.policies.RetryPolicy object>*

A default [`policies.RetryPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.RetryPolicy) instance to use for all
[`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) objects which do not have a [`retry_policy`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement.retry_policy)
explicitly set.

#### conviction_policy_factory *= <class 'cassandra.policies.SimpleConvictionPolicy'>*

A factory function which creates instances of
[`policies.ConvictionPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.ConvictionPolicy).  Defaults to
[`policies.SimpleConvictionPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.SimpleConvictionPolicy).

#### address_translator *= <cassandra.policies.IdentityTranslator object>*

[`policies.AddressTranslator`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.AddressTranslator) instance to be used in translating server node addresses
to driver connection addresses.

#### session_id

A `uuid.UUID` identifying this `Cluster`, generated when it is
created and never changing afterwards.

Every connection this `Cluster` opens – the control connection as
well as the pools of each of its [`Session`](#cassandra.cluster.Session) objects – reports
it to the cluster as the `SESSION_ID` startup option, where ScyllaDB
exposes it in the `client_options` column of its clients table.
Logging it, or attaching it to a support bundle, is what allows
client-side observations to be matched against those rows instead of
correlating them by address and port.

The option is named after the convention shared with the other ScyllaDB
drivers, where a “session” is what this driver calls a `Cluster`. It is
unrelated to `Session.session_id`, which identifies a `Session`
within the client and is never reported to the cluster.

#### driver_config_reporting_enabled *= True*

A boolean indicating whether the driver describes its effective
configuration to the cluster while setting up the control connection, so
that operators can inspect the settings of a client while investigating an
incident. The description is sent as the `DRIVER_CONFIG` startup option
and ScyllaDB exposes it in the `client_options` column of its clients
table.

[`session_id`](#cassandra.cluster.Cluster.session_id), which every connection reports, is not
affected by this setting.

Read when a connection is opened, so changing it takes effect on the
connections established afterwards and leaves the open ones alone.

Defaults to `True`.

#### metrics_enabled *= False*

Whether or not metric collection is enabled.  If enabled, [`metrics`](#cassandra.cluster.Cluster.metrics)
will be an instance of [`Metrics`](https://python-driver.docs.scylladb.com/master/api/cassandra/metrics.md#cassandra.metrics.Metrics).

#### metrics *= None*

An instance of [`cassandra.metrics.Metrics`](https://python-driver.docs.scylladb.com/master/api/cassandra/metrics.md#cassandra.metrics.Metrics) if [`metrics_enabled`](#cassandra.cluster.Cluster.metrics_enabled) is
`True`, else `None`.

#### ssl_context *= None*

An optional `ssl.SSLContext` instance which will be used when new sockets are created.
This should be used when client encryption is enabled in Cassandra.

`wrap_socket` options can be set using [`ssl_options`](#cassandra.cluster.Cluster.ssl_options). ssl_options will
be used as kwargs for `ssl.SSLContext.wrap_socket`.

#### Versionadded
Added in version 3.17.0.

#### ssl_options *= None*

Using ssl_options without ssl_context is deprecated and will be removed in the
next major release.

An optional dict which will be used as kwargs for `ssl.SSLContext.wrap_socket`
when new sockets are created. This should be used when client encryption is enabled
in Cassandra.

The following documentation only applies when ssl_options is used without ssl_context.

By default, a `ca_certs` value should be supplied (the value should be
a string pointing to the location of the CA certs file), and you probably
want to specify `ssl_version` as `ssl.PROTOCOL_TLS` to match
Cassandra’s default protocol.

#### Versionchanged
Changed in version 3.3.0.

In addition to `wrap_socket` kwargs, clients may also specify `'check_hostname': True` to verify the cert hostname
as outlined in RFC 2818 and RFC 6125. Note that this requires the certificate to be transferred, so
should almost always require the option `'cert_reqs': ssl.CERT_REQUIRED`. Note also that this functionality was not built into
Python standard library until (2.7.9, 3.2). To enable this mechanism in earlier versions, patch `ssl.match_hostname`
with a custom or [back-ported function](https://pypi.org/project/backports.ssl_match_hostname/).

#### Versionchanged
Changed in version 3.29.0.

`ssl.match_hostname` has been deprecated since Python 3.7 (and removed in Python 3.12).  This functionality is now implemented
via `ssl.SSLContext.check_hostname`.  All options specified above (including `check_hostname`) should continue to behave in a
way that is consistent with prior implementations.

#### sockopts *= None*

An optional list of tuples which will be used as arguments to
`socket.setsockopt()` for all created sockets.

Note: some drivers find setting TCPNODELAY beneficial in the context of
their execution model. It was not found generally beneficial for this driver.
To try with your own workload, set `sockopts = [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)]`

#### max_schema_agreement_wait *= 10*

The maximum duration (in seconds) that the driver will wait for schema
agreement across the cluster. Defaults to ten seconds.
If set <= 0, the driver will bypass schema agreement waits altogether.

#### metadata *= None*

An instance of [`cassandra.metadata.Metadata`](https://python-driver.docs.scylladb.com/master/api/cassandra/metadata.md#cassandra.metadata.Metadata).

#### connection_class *= <class 'cassandra.io.libevreactor.LibevConnection'>*

This determines what event loop system will be used for managing
I/O with Cassandra.  These are the current options:

* [`cassandra.io.asyncorereactor.AsyncoreConnection`](https://python-driver.docs.scylladb.com/master/api/cassandra/io/asyncorereactor.md#cassandra.io.asyncorereactor.AsyncoreConnection)
* [`cassandra.io.libevreactor.LibevConnection`](https://python-driver.docs.scylladb.com/master/api/cassandra/io/libevreactor.md#cassandra.io.libevreactor.LibevConnection)
* EXPERIMENTAL: [`cassandra.io.asyncioreactor.AsyncioConnection`](https://python-driver.docs.scylladb.com/master/api/cassandra/io/asyncioreactor.md#cassandra.io.asyncioreactor.AsyncioConnection)

By default, `AsyncoreConnection` will be used, which uses
the `asyncore` module in the Python standard library.

If `libev` is installed, `LibevConnection` will be used instead.

`AsyncioConnection`, which uses the `asyncio` module in the Python
standard library, is also available, but currently experimental. Note that
it requires `asyncio` features that were only introduced in the 3.4 line
in 3.4.6, and in the 3.5 line in 3.5.1.

#### control_connection_timeout *= 2.0*

A timeout, in seconds, for queries made by the control connection, such
as querying the current schema and information about nodes in the cluster.
If set to `None`, there will be no timeout for these queries.

#### allow_control_connection_query_fallback *: [ControlConnectionQueryFallback](#cassandra.cluster.ControlConnectionQueryFallback)* *= 'Disabled'*

Controls whether application queries may fall back to the control connection.

`Disabled` keeps the old behavior.
`Fallback` enables control-connection fallback when no usable node pools exist.
`SkipPoolCreation` skips node-pool creation and uses the control connection fallback path.
This fallback is still not used for requests targeted to an explicit host.

#### idle_heartbeat_interval *= 30*

Interval, in seconds, on which to heartbeat idle connections. This helps
keep connections open through network devices that expire idle connections.
It also helps discover bad connections early in low-traffic scenarios.
Setting to zero disables heartbeats.

#### idle_heartbeat_timeout *= 30*

Timeout, in seconds, on which the heartbeat wait for idle connection responses.
Lowering this value can help to discover bad connections earlier.

#### schema_event_refresh_window *= 2*

Window, in seconds, within which a schema component will be refreshed after
receiving a schema_change event.

The driver delays a random amount of time in the range [0.0, window)
before executing the refresh. This serves two purposes:

1.) Spread the refresh for deployments with large fanout from C\* to client tier,
preventing a ‘thundering herd’ problem with many clients refreshing simultaneously.

2.) Remove redundant refreshes. Redundant events arriving within the delay period
are discarded, and only one refresh is executed.

Setting this to zero will execute refreshes immediately.

Setting this negative will disable schema refreshes in response to push events
(refreshes will still occur in response to schema change responses to DDL statements
executed by Sessions of this Cluster).

#### topology_event_refresh_window *= 10*

Window, in seconds, within which the node and token list will be refreshed after
receiving a topology_change event.

Setting this to zero will execute refreshes immediately.

Setting this negative will disable node refreshes in response to push events.

See [`schema_event_refresh_window`](#cassandra.cluster.Cluster.schema_event_refresh_window) for discussion of rationale

#### status_event_refresh_window *= 2*

Window, in seconds, within which the driver will start the reconnect after
receiving a status_change event.

Setting this to zero will connect immediately.

This is primarily used to avoid ‘thundering herd’ in deployments with large fanout from cluster to clients.
When nodes come up, clients attempt to reprepare prepared statements (depending on [`reprepare_on_up`](#cassandra.cluster.Cluster.reprepare_on_up)), and
establish connection pools. This can cause a rush of connections and queries if not mitigated with this factor.

#### prepare_on_all_hosts *= True*

Specifies whether statements should be prepared on all hosts, or just one.

This can reasonably be disabled on long-running applications with numerous clients preparing statements on startup,
where a randomized initial condition of the load balancing policy can be expected to distribute prepares from
different clients across the cluster.

#### reprepare_on_up *= True*

Specifies whether all known prepared statements should be prepared on a node when it comes up.

May be used to avoid overwhelming a node on return, or if it is supposed that the node was only marked down due to
network. If statements are not reprepared, they are prepared on the first execution, causing
an extra roundtrip for one or more client requests.

#### connect_timeout *= 5*

Timeout, in seconds, for creating new connections.

This timeout covers the entire connection negotiation, including TCP
establishment, options passing, and authentication.

#### schema_metadata_enabled *= True*

Flag indicating whether internal schema metadata is updated.

When disabled, the driver does not populate Cluster.metadata.keyspaces on connect, or on schema change events. This
can be used to speed initial connection, and reduce load on client and server during operation. Turning this off
gives away token aware request routing, and programmatic inspection of the metadata model.

#### token_metadata_enabled *= True*

Flag indicating whether internal token metadata is updated.

When disabled, the driver does not query node token information on connect, or on topology change events. This
can be used to speed initial connection, and reduce load on client and server during operation. It is most useful
in large clusters using vnodes, where the token map can be expensive to compute. Turning this off
gives away token aware request routing, and programmatic inspection of the token ring.

#### timestamp_generator *= None*

An object, shared between all sessions created by this cluster instance,
that generates timestamps when client-side timestamp generation is enabled.
By default, each [`Cluster`](#cassandra.cluster.Cluster) uses a new
[`MonotonicTimestampGenerator`](https://python-driver.docs.scylladb.com/master/api/cassandra/timestamps.md#cassandra.timestamps.MonotonicTimestampGenerator).

Applications can set this value for custom timestamp behavior. See the
documentation for [`Session.timestamp_generator()`](#cassandra.cluster.Session.timestamp_generator).

#### endpoint_factory *= None*

An [`EndPointFactory`](https://python-driver.docs.scylladb.com/master/api/cassandra/connection.md#cassandra.connection.EndPointFactory) instance to use internally when creating
a socket connection to a node. You can ignore this unless you need a special
connection mechanism.

#### cloud *= None*

A dict of the cloud configuration. Example:

```default
{
    # path to the secure connect bundle
    'secure_connect_bundle': '/path/to/secure-connect-dbname.zip',

    # optional config options
    'use_default_tempdir': True  # use the system temp dir for the zip extraction
}
```

The zip file will be temporarily extracted in the same directory to
load the configuration and certificates.

#### connect(keyspace=None, wait_for_all_pools=False)

Creates and returns a new [`Session`](#cassandra.cluster.Session) object.

If keyspace is specified, that keyspace will be the default keyspace for
operations on the `Session`.

wait_for_all_pools specifies whether this call should wait for all connection pools to be
established or attempted. Default is False, which means it will return when the first
successful connection is established. Remaining pools are added asynchronously.

#### shutdown()

Closes all sessions and connection associated with this Cluster.
To ensure all connections are properly closed, **you should always
call shutdown() on a Cluster instance when you are done with it**.

Once shutdown, a Cluster should not be used for any purpose.

#### register_user_type(keyspace, user_type, klass)

Registers a class to use to represent a particular user-defined type.
Query parameters for this user-defined type will be assumed to be
instances of klass.  Result sets for this user-defined type will
be instances of klass.  If no class is registered for a user-defined
type, a namedtuple will be used for result sets, and non-prepared
statements may not encode parameters for this type correctly.

keyspace is the name of the keyspace that the UDT is defined in.

user_type is the string name of the UDT to register the mapping
for.

klass should be a class with attributes whose names match the
fields of the user-defined type.  The constructor must accepts kwargs
for each of the fields in the UDT.

This method should only be called after the type has been created
within Cassandra.

Example:

```default
cluster = Cluster(protocol_version=3)
session = cluster.connect()
session.set_keyspace('mykeyspace')
session.execute("CREATE TYPE address (street text, zipcode int)")
session.execute("CREATE TABLE users (id int PRIMARY KEY, location address)")

# create a class to map to the "address" UDT
class Address(object):

    def __init__(self, street, zipcode):
        self.street = street
        self.zipcode = zipcode

cluster.register_user_type('mykeyspace', 'address', Address)

# insert a row using an instance of Address
session.execute("INSERT INTO users (id, location) VALUES (%s, %s)",
                (0, Address("123 Main St.", 78723)))

# results will include Address instances
results = session.execute("SELECT * FROM users")
row = results[0]
print(row.id, row.location.street, row.location.zipcode)
```

#### register_listener(listener)

Adds a `cassandra.policies.HostStateListener` subclass instance to
the list of listeners to be notified when a host is added, removed,
marked up, or marked down.

#### unregister_listener(listener)

Removes a registered listener.

#### add_execution_profile(name, profile, pool_wait_timeout=5)

Adds an [`ExecutionProfile`](#cassandra.cluster.ExecutionProfile) to the cluster. This makes it available for use by `name` in [`Session.execute()`](#cassandra.cluster.Session.execute)
and [`Session.execute_async()`](#cassandra.cluster.Session.execute_async). This method will raise if the profile already exists.

Normally profiles will be injected at cluster initialization via `Cluster(execution_profiles)`. This method
provides a way of adding them dynamically.

Adding a new profile updates the connection pools according to the specified `load_balancing_policy`. By default,
this method will wait up to five seconds for the pool creation to complete, so the profile can be used immediately
upon return. This behavior can be controlled using `pool_wait_timeout` (see
[concurrent.futures.wait](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.wait)
for timeout semantics).

#### get_control_connection_host()

Returns the control connection host metadata.

#### refresh_schema_metadata(max_schema_agreement_wait=None)

Synchronously refresh all schema metadata.

By default, the timeout for this operation is governed by [`max_schema_agreement_wait`](#cassandra.cluster.Cluster.max_schema_agreement_wait)
and [`control_connection_timeout`](#cassandra.cluster.Cluster.control_connection_timeout).

Passing max_schema_agreement_wait here overrides [`max_schema_agreement_wait`](#cassandra.cluster.Cluster.max_schema_agreement_wait).

Setting max_schema_agreement_wait <= 0 will bypass schema agreement and refresh schema immediately.

An Exception is raised if schema refresh fails for any reason.

#### refresh_keyspace_metadata(keyspace, max_schema_agreement_wait=None)

Synchronously refresh keyspace metadata. This applies to keyspace-level information such as replication
and durability settings. It does not refresh tables, types, etc. contained in the keyspace.

See [`refresh_schema_metadata()`](#cassandra.cluster.Cluster.refresh_schema_metadata) for description of `max_schema_agreement_wait` behavior

#### refresh_table_metadata(keyspace, table, max_schema_agreement_wait=None)

Synchronously refresh table metadata. This applies to a table, and any triggers or indexes attached
to the table.

See [`refresh_schema_metadata()`](#cassandra.cluster.Cluster.refresh_schema_metadata) for description of `max_schema_agreement_wait` behavior

#### refresh_user_type_metadata(keyspace, user_type, max_schema_agreement_wait=None)

Synchronously refresh user defined type metadata.

See [`refresh_schema_metadata()`](#cassandra.cluster.Cluster.refresh_schema_metadata) for description of `max_schema_agreement_wait` behavior

#### refresh_user_function_metadata(keyspace, function, max_schema_agreement_wait=None)

Synchronously refresh user defined function metadata.

`function` is a [`cassandra.UserFunctionDescriptor`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.UserFunctionDescriptor).

See [`refresh_schema_metadata()`](#cassandra.cluster.Cluster.refresh_schema_metadata) for description of `max_schema_agreement_wait` behavior

#### refresh_user_aggregate_metadata(keyspace, aggregate, max_schema_agreement_wait=None)

Synchronously refresh user defined aggregate metadata.

`aggregate` is a [`cassandra.UserAggregateDescriptor`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.UserAggregateDescriptor).

See [`refresh_schema_metadata()`](#cassandra.cluster.Cluster.refresh_schema_metadata) for description of `max_schema_agreement_wait` behavior

#### refresh_nodes(force_token_rebuild=False)

Synchronously refresh the node list and token metadata

force_token_rebuild can be used to rebuild the token map metadata, even if no new nodes are discovered.

An Exception is raised if node refresh fails for any reason.

#### set_meta_refresh_enabled(enabled)

*Deprecated:* set [`schema_metadata_enabled`](#cassandra.cluster.Cluster.schema_metadata_enabled) [`token_metadata_enabled`](#cassandra.cluster.Cluster.token_metadata_enabled) instead

Sets a flag to enable (True) or disable (False) all metadata refresh queries.
This applies to both schema and node topology.

Disabling this is useful to minimize refreshes during multiple changes.

Meta refresh must be enabled for the driver to become aware of any cluster
topology changes or schema updates.

### *class* cassandra.cluster.ControlConnectionQueryFallback(\*values)

Controls how application queries use the control connection when node pools
are unavailable.

`Disabled` requires a usable node pool for application queries. If the
driver cannot establish one during session startup, it raises
[`NoHostAvailable`](#cassandra.cluster.NoHostAvailable).

`Fallback` still attempts to create node pools, but allows application
queries to fall back to the control connection when no usable node pool is
available. Session startup is allowed to proceed even if the initial pool
attempts all fail.

`SkipPoolCreation` disables node-pool creation for the session and uses
the control-connection fallback path for application queries.

The fallback path is not used for requests targeted to an explicit host.

### *class* cassandra.cluster.ExecutionProfile(load_balancing_policy=<object object>, retry_policy=None, consistency_level=ConsistencyLevel.LOCAL_ONE, serial_consistency_level=None, request_timeout=10.0, row_factory=<function named_tuple_factory>, speculative_execution_policy=None)

#### consistency_level *= LOCAL_ONE*

[`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) used when not specified on a [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement).

#### load_balancing_policy *= None*

An instance of [`policies.LoadBalancingPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.LoadBalancingPolicy) or one of its subclasses.

Used in determining host distance for establishing connections, and routing requests.

Defaults to `TokenAwarePolicy(DCAwareRoundRobinPolicy())` if not specified

#### retry_policy *= None*

An instance of [`policies.RetryPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.RetryPolicy) instance used when [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) objects do not have a
[`retry_policy`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement.retry_policy) explicitly set.

Defaults to [`RetryPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.RetryPolicy) if not specified

#### serial_consistency_level *= None*

Serial [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) used when not specified on a [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) (for LWT conditional statements).

#### request_timeout *= 10.0*

Request timeout used when not overridden in [`Session.execute()`](#cassandra.cluster.Session.execute)

#### *static* row_factory(colnames, rows)

A callable to format results, accepting `(colnames, rows)` where `colnames` is a list of column names, and
`rows` is a list of tuples, with each tuple representing a row of parsed values.

Some example implementations:

- [`cassandra.query.tuple_factory()`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.tuple_factory) - return a result row as a tuple
- [`cassandra.query.named_tuple_factory()`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.named_tuple_factory) - return a result row as a named tuple
- [`cassandra.query.dict_factory()`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.dict_factory) - return a result row as a dict
- [`cassandra.query.ordered_dict_factory()`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.ordered_dict_factory) - return a result row as an OrderedDict

#### speculative_execution_policy *= None*

An instance of [`policies.SpeculativeExecutionPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.SpeculativeExecutionPolicy)

Defaults to `NoSpeculativeExecutionPolicy` if not specified

#### continuous_paging_options *= None*

*Note:* This feature is implemented to facilitate server integration testing. It is not intended for general use in the Python driver.
See [`Statement.fetch_size`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement.fetch_size) or [`Session.default_fetch_size`](#cassandra.cluster.Session.default_fetch_size) for configuring normal paging.

When set, requests will use DSE’s continuous paging, which streams multiple pages without
intermediate requests.

This has the potential to materialize all results in memory at once if the consumer cannot keep up. Use options
to constrain page size and rate.

This is only available for DSE clusters.

### *class* cassandra.cluster.GraphExecutionProfile(load_balancing_policy=\_NOT_SET, retry_policy=None, consistency_level=ConsistencyLevel.LOCAL_ONE, serial_consistency_level=None, request_timeout=30.0, row_factory=None, graph_options=None, continuous_paging_options=\_NOT_SET)

Default execution profile for graph execution.

See [`ExecutionProfile`](#cassandra.cluster.ExecutionProfile) for base attributes. Note that if not explicitly set,
the row_factory and graph_options.graph_protocol are resolved during the query execution.
These options will resolve to graph_graphson3_row_factory and GraphProtocol.GRAPHSON_3_0
for the core graph engine (DSE 6.8+), otherwise graph_object_row_factory and GraphProtocol.GRAPHSON_1_0

In addition to default parameters shown in the signature, this profile also defaults `retry_policy` to
`cassandra.policies.NeverRetryPolicy`.

#### graph_options *= None*

`cassandra.graph.GraphOptions` to use with this execution

Default options for graph queries, initialized as follows by default:

```default
GraphOptions(graph_language=b'gremlin-groovy')
```

See cassandra.graph.GraphOptions

### *class* cassandra.cluster.GraphAnalyticsExecutionProfile(load_balancing_policy=None, retry_policy=None, consistency_level=ConsistencyLevel.LOCAL_ONE, serial_consistency_level=None, request_timeout=3600. \* 24. \* 7., row_factory=None, graph_options=None)

Execution profile with timeout and load balancing appropriate for graph analytics queries.

See also `GraphExecutionPolicy`.

In addition to default parameters shown in the signature, this profile also defaults `retry_policy` to
`cassandra.policies.NeverRetryPolicy`, and `load_balancing_policy` to one that targets the current Spark
master.

Note: The graph_options.graph_source is set automatically to b’a’ (analytics)
when using GraphAnalyticsExecutionProfile. This is mandatory to target analytics nodes.

### cassandra.cluster.EXEC_PROFILE_DEFAULT

Key for the `Cluster` default execution profile, used when no other profile is selected in
`Session.execute(execution_profile)`.

Use this as the key in `Cluster(execution_profiles)` to override the default profile.

### cassandra.cluster.EXEC_PROFILE_GRAPH_DEFAULT

Key for the default graph execution profile, used when no other profile is selected in
`Session.execute_graph(execution_profile)`.

Use this as the key in [Cluster(execution_profiles)](https://python-driver.docs.scylladb.com/master/execution-profiles.md)
to override the default graph profile.

### cassandra.cluster.EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT

Key for the default graph system execution profile. This can be used for graph statements using the DSE graph
system API.

Selected using `Session.execute_graph(execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)`.

### cassandra.cluster.EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT

Key for the default graph analytics execution profile. This can be used for graph statements intended to
use Spark/analytics as the traversal source.

Selected using `Session.execute_graph(execution_profile=EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT)`.

### *class* cassandra.cluster.Session

A collection of connection pools for each host in the cluster.
Instances of this class should not be created directly, only
using [`Cluster.connect()`](#cassandra.cluster.Cluster.connect).

Queries and statements can be executed through `Session` instances
using the [`execute()`](#cassandra.cluster.Session.execute) and [`execute_async()`](#cassandra.cluster.Session.execute_async)
methods.

Example usage:

```default
>>> session = cluster.connect()
>>> session.set_keyspace("mykeyspace")
>>> session.execute("SELECT * FROM mycf")
```

#### default_timeout *= 10.0*

A default timeout, measured in seconds, for queries executed through
[`execute()`](#cassandra.cluster.Session.execute) or [`execute_async()`](#cassandra.cluster.Session.execute_async).  This default may be
overridden with the timeout parameter for either of those methods.

Setting this to `None` will cause no timeouts to be set by default.

Please see [`ResponseFuture.result()`](#cassandra.cluster.ResponseFuture.result) for details on the scope and
effect of this timeout.

#### Versionadded
Added in version 2.0.0.

#### default_consistency_level *= LOCAL_ONE*

*Deprecated:* use execution profiles instead
The default `ConsistencyLevel` for operations executed through
this session.  This default may be overridden by setting the
[`consistency_level`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement.consistency_level) on individual statements.

#### Versionadded
Added in version 1.2.0.

#### Versionchanged
Changed in version 3.0.0: default changed from ONE to LOCAL_ONE

#### default_serial_consistency_level *= None*

The default `ConsistencyLevel` for serial phase of  conditional updates executed through
this session.  This default may be overridden by setting the
[`serial_consistency_level`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement.serial_consistency_level) on individual statements.

Only valid for `protocol_version >= 2`.

#### row_factory *= <function named_tuple_factory>*

The format to return row results in.  By default, each
returned row will be a named tuple.  You can alternatively
use any of the following:

- [`cassandra.query.tuple_factory()`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.tuple_factory) - return a result row as a tuple
- [`cassandra.query.named_tuple_factory()`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.named_tuple_factory) - return a result row as a named tuple
- [`cassandra.query.dict_factory()`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.dict_factory) - return a result row as a dict
- [`cassandra.query.ordered_dict_factory()`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.ordered_dict_factory) - return a result row as an OrderedDict

#### default_fetch_size *= 5000*

By default, this many rows will be fetched at a time. Setting
this to `None` will disable automatic paging for large query
results.  The fetch size can be also specified per-query through
[`Statement.fetch_size`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement.fetch_size).

This only takes effect when protocol version 2 or higher is used.
See [`Cluster.protocol_version`](#cassandra.cluster.Cluster.protocol_version) for details.

#### Versionadded
Added in version 2.0.0.

#### use_client_timestamp *= True*

When using protocol version 3 or higher, write timestamps may be supplied
client-side at the protocol level.  (Normally they are generated
server-side by the coordinator node.)  Note that timestamps specified
within a CQL query will override this timestamp.

#### Versionadded
Added in version 2.1.0.

#### timestamp_generator *= None*

When [`use_client_timestamp`](#cassandra.cluster.Session.use_client_timestamp) is set, sessions call this object and use
the result as the timestamp.  (Note that timestamps specified within a CQL
query will override this timestamp.)  By default, a new
[`MonotonicTimestampGenerator`](https://python-driver.docs.scylladb.com/master/api/cassandra/timestamps.md#cassandra.timestamps.MonotonicTimestampGenerator) is created for
each [`Cluster`](#cassandra.cluster.Cluster) instance.

Applications can set this value for custom timestamp behavior.  For
example, an application could share a timestamp generator across
[`Cluster`](#cassandra.cluster.Cluster) objects to guarantee that the application will use unique,
increasing timestamps across clusters, or set it to to `lambda:
int(time.time() * 1e6)` if losing records over clock inconsistencies is
acceptable for the application. Custom [`timestamp_generator`](#cassandra.cluster.Session.timestamp_generator) s should
be callable, and calling them should return an integer representing microseconds
since some point in time, typically UNIX epoch.

#### Versionadded
Added in version 3.8.0.

#### encoder *= None*

A [`Encoder`](https://python-driver.docs.scylladb.com/master/api/cassandra/encoder.md#cassandra.encoder.Encoder) instance that will be used when
formatting query parameters for non-prepared statements.  This is not used
for prepared statements (because prepared statements give the driver more
information about what CQL types are expected, allowing it to accept a
wider range of python types).

The encoder uses a mapping from python types to encoder methods (for
specific CQL types).  This mapping can be be modified by users as they see
fit.  Methods of [`Encoder`](https://python-driver.docs.scylladb.com/master/api/cassandra/encoder.md#cassandra.encoder.Encoder) should be used for mapping
values if possible, because they take precautions to avoid injections and
properly sanitize data.

Example:

```default
cluster = Cluster()
session = cluster.connect("mykeyspace")
session.encoder.mapping[tuple] = session.encoder.cql_encode_tuple

session.execute("CREATE TABLE mytable (k int PRIMARY KEY, col tuple<int, ascii>)")
session.execute("INSERT INTO mytable (k, col) VALUES (%s, %s)", [0, (123, 'abc')])
```

#### Versionadded
Added in version 2.1.0.

#### client_protocol_handler *= <class 'cassandra.protocol._ProtocolHandler'>*

Specifies a protocol handler that will be used for client-initiated requests (i.e. no
internal driver requests). This can be used to override or extend features such as
message or type ser/des.

The default pure python implementation is `cassandra.protocol.ProtocolHandler`.

When compiled with Cython, there are also built-in faster alternatives. See [Faster Deserialization](https://python-driver.docs.scylladb.com/master/api/cassandra/protocol.md#faster-deser)

#### execute(statement[, parameters][, timeout][, trace][, custom_payload][, paging_state][, host][, execute_as])

Execute the given query and synchronously wait for the response.

If an error is encountered while executing the query, an Exception
will be raised.

query may be a query string or an instance of [`cassandra.query.Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement).

parameters may be a sequence or dict of parameters to bind.  If a
sequence is used, `%s` should be used the placeholder for each
argument.  If a dict is used, `%(name)s` style placeholders must
be used.

timeout should specify a floating-point timeout (in seconds) after
which an [`OperationTimedOut`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.OperationTimedOut) exception will be raised if the query
has not completed.  If not set, the timeout defaults to the request_timeout of the selected `execution_profile`.
If set to `None`, there is no timeout. Please see [`ResponseFuture.result()`](#cassandra.cluster.ResponseFuture.result) for details on
the scope and effect of this timeout.

If trace is set to `True`, the query will be sent with tracing enabled.
The trace details can be obtained using the returned [`ResultSet`](#cassandra.cluster.ResultSet) object.

custom_payload is a [Custom Payloads](https://python-driver.docs.scylladb.com/master/api/cassandra/protocol.md#custom-payload) dict to be passed to the server.
If query is a Statement with its own custom_payload. The message payload
will be a union of the two, with the values specified here taking precedence.

execution_profile is the execution profile to use for this request. It can be a key to a profile configured
via [`Cluster.add_execution_profile()`](#cassandra.cluster.Cluster.add_execution_profile) or an instance (from [`Session.execution_profile_clone_update()`](#cassandra.cluster.Session.execution_profile_clone_update),
for example

paging_state is an optional paging state, reused from a previous [`ResultSet`](#cassandra.cluster.ResultSet).

host is the [`cassandra.pool.Host`](https://python-driver.docs.scylladb.com/master/api/cassandra/pool.md#cassandra.pool.Host) that should handle the query. If the host specified is down or
not yet connected, the query will fail with [`NoHostAvailable`](#cassandra.cluster.NoHostAvailable). Using this is
discouraged except in a few cases, e.g., querying node-local tables and applying schema changes.

execute_as the user that will be used on the server to execute the request. This is only available
on a DSE cluster.

#### execute_async(statement[, parameters][, trace][, custom_payload][, paging_state][, host][, execute_as])

Execute the given query and return a [`ResponseFuture`](#cassandra.cluster.ResponseFuture) object
which callbacks may be attached to for asynchronous response
delivery.  You may also call [`result()`](#cassandra.cluster.ResponseFuture.result)
on the [`ResponseFuture`](#cassandra.cluster.ResponseFuture) to synchronously block for results at
any time.

See [`Session.execute()`](#cassandra.cluster.Session.execute) for parameter definitions.

Example usage:

```default
>>> session = cluster.connect()
>>> future = session.execute_async("SELECT * FROM mycf")

>>> def log_results(results):
...     for row in results:
...         log.info("Results: %s", row)

>>> def log_error(exc):
>>>     log.error("Operation failed: %s", exc)

>>> future.add_callbacks(log_results, log_error)
```

Async execution with blocking wait for results:

```default
>>> future = session.execute_async("SELECT * FROM mycf")
>>> # do other stuff...

>>> try:
...     results = future.result()
... except Exception:
...     log.exception("Operation failed:")
```

#### execute_graph(statement[, parameters][, trace][, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT][, execute_as])

Executes a Gremlin query string or GraphStatement synchronously,
and returns a ResultSet from this execution.

parameters is dict of named parameters to bind. The values must be
JSON-serializable.

execution_profile: Selects an execution profile for the request.

execute_as the user that will be used on the server to execute the request.

#### execute_graph_async(statement[, parameters][, trace][, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT][, execute_as])

Execute the graph query and return a [`ResponseFuture`](#cassandra.cluster.ResponseFuture)
object which callbacks may be attached to for asynchronous response delivery. You may also call `ResponseFuture.result()` to synchronously block for
results at any time.

#### prepare(statement)

Prepares a query string, returning a [`PreparedStatement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.PreparedStatement)
instance which can be used as follows:

```default
>>> session = cluster.connect("mykeyspace")
>>> query = "INSERT INTO users (id, name, age) VALUES (?, ?, ?)"
>>> prepared = session.prepare(query)
>>> session.execute(prepared, (user.id, user.name, user.age))
```

Or you may bind values to the prepared statement ahead of time:

```default
>>> prepared = session.prepare(query)
>>> bound_stmt = prepared.bind((user.id, user.name, user.age))
>>> session.execute(bound_stmt)
```

Of course, prepared statements may (and should) be reused:

```default
>>> prepared = session.prepare(query)
>>> for user in users:
...     bound = prepared.bind((user.id, user.name, user.age))
...     session.execute(bound)
```

Alternatively, if [`protocol_version`](#cassandra.cluster.Cluster.protocol_version) is 5 or higher
(requires Cassandra 4.0+), the keyspace can be specified as a
parameter. This will allow you to avoid specifying the keyspace in the
query without specifying a keyspace in [`connect()`](#cassandra.cluster.Cluster.connect). It
even will let you prepare and use statements against a keyspace other
than the one originally specified on connection:

```pycon
>>> analyticskeyspace_prepared = session.prepare(
...     "INSERT INTO user_activity id, last_activity VALUES (?, ?)",
...     keyspace="analyticskeyspace")  # note the different keyspace
```

**Important**: PreparedStatements should be prepared only once.
Preparing the same query more than once will likely affect performance.

custom_payload is a key value map to be passed along with the prepare
message. See [Custom Payloads](https://python-driver.docs.scylladb.com/master/api/cassandra/protocol.md#custom-payload).

#### shutdown()

Close all connections.  `Session` instances should not be used
for any purpose after being shutdown.

#### set_keyspace(keyspace)

Set the default keyspace for all queries made through this Session.
This operation blocks until complete.

#### wait_for_schema_agreement(wait_time: float | None = None, scope: SchemaAgreementScope = SchemaAgreementScope.CLUSTER) → bool

Wait for connected hosts in the selected scope to report the same
schema version from `system.local`.

By default, the timeout for this operation is governed by
[`max_schema_agreement_wait`](#cassandra.cluster.Cluster.max_schema_agreement_wait) and
[`control_connection_timeout`](#cassandra.cluster.Cluster.control_connection_timeout).

Passing `wait_time` here overrides
[`max_schema_agreement_wait`](#cassandra.cluster.Cluster.max_schema_agreement_wait). If provided, `wait_time`
must be greater than 0.

`scope` determines which connected hosts participate in the check.
Pass `SchemaAgreementScope.RACK`, `SchemaAgreementScope.DC`,
or `SchemaAgreementScope.CLUSTER`.
The default is `SchemaAgreementScope.CLUSTER`. `RACK` narrows
the check to connected hosts in the local rack only. `DC` checks
connected hosts in the local datacenter. `CLUSTER` queries every
connected host across all datacenters.

* **Parameters:**
  * **wait_time** – Override for
    [`max_schema_agreement_wait`](#cassandra.cluster.Cluster.max_schema_agreement_wait), should be positive
    number.
  * **scope** – Restricts the check to connected hosts in the local rack,
    local datacenter, or whole connected cluster.
* **Returns:**
  `True` when the selected connected hosts agree on schema,
  otherwise `False`.
* **Raises:**
  * **ValueError** – If `wait_time` is provided and is not greater
    than 0.
  * **ValueError** – If `scope` is not one of the schema agreement
    scope values.

#### get_execution_profile(name)

Returns the execution profile associated with the provided `name`.

* **Parameters:**
  **name** – The name (or key) of the execution profile.

#### execution_profile_clone_update(ep, \*\*kwargs)

Returns a clone of the `ep` profile.  `kwargs` can be specified to update attributes
of the returned profile.

This is a shallow clone, so any objects referenced by the profile are shared. This means Load Balancing Policy
is maintained by inclusion in the active profiles. It also means updating any other rich objects will be seen
by the active profile. In cases where this is not desirable, be sure to replace the instance instead of manipulating
the shared object.

#### add_request_init_listener(fn, \*args, \*\*kwargs)

Adds a callback with arguments to be called when any request is created.

It will be invoked as fn(response_future, \*args, \*\*kwargs) after each client request is created,
and before the request is sent. This can be used to create extensions by adding result callbacks to the
response future.

response_future is the [`ResponseFuture`](#cassandra.cluster.ResponseFuture) for the request.

Note that the init callback is done on the client thread creating the request, so you may need to consider
synchronization if you have multiple threads. Any callbacks added to the response future will be executed
on the event loop thread, so the normal advice about minimizing cycles and avoiding blocking apply (see Note in
[`ResponseFuture.add_callbacks()`](#cassandra.cluster.ResponseFuture.add_callbacks).

See [this example](https://github.com/datastax/python-driver/blob/master/examples/request_init_listener.py) in the
source tree for an example.

#### remove_request_init_listener(fn, \*args, \*\*kwargs)

Removes a callback and arguments from the list.

See [`Session.add_request_init_listener()`](#cassandra.cluster.Session.add_request_init_listener).

### *class* cassandra.cluster.ResponseFuture

An asynchronous response delivery mechanism that is returned from calls
to [`Session.execute_async()`](#cassandra.cluster.Session.execute_async).

There are two ways for results to be delivered:
: - Synchronously, by calling [`result()`](#cassandra.cluster.ResponseFuture.result)
  - Asynchronously, by attaching callback and errback functions via
    [`add_callback()`](#cassandra.cluster.ResponseFuture.add_callback), [`add_errback()`](#cassandra.cluster.ResponseFuture.add_errback), and
    [`add_callbacks()`](#cassandra.cluster.ResponseFuture.add_callbacks).

#### query *= None*

The [`Statement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.Statement) instance that is being executed through this
[`ResponseFuture`](#cassandra.cluster.ResponseFuture).

#### result()

Return the final result or raise an Exception if errors were
encountered.  If the final result or error has not been set
yet, this method will block until it is set, or the timeout
set for the request expires.

Timeout is specified in the Session request execution functions.
If the timeout is exceeded, an [`cassandra.OperationTimedOut`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.OperationTimedOut) will be raised.
This is a client-side timeout. For more information
about server-side coordinator timeouts, see [`policies.RetryPolicy`](https://python-driver.docs.scylladb.com/master/api/cassandra/policies.md#cassandra.policies.RetryPolicy).

Example usage:

```default
>>> future = session.execute_async("SELECT * FROM mycf")
>>> # do other stuff...

>>> try:
...     rows = future.result()
...     for row in rows:
...         ... # process results
... except Exception:
...     log.exception("Operation failed:")
```

#### get_query_trace()

Fetches and returns the query trace of the last response, or None if tracing was
not enabled.

Note that this may raise an exception if there are problems retrieving the trace
details from Cassandra. If the trace is not available after max_wait,
[`cassandra.query.TraceUnavailable`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.TraceUnavailable) will be raised.

If the ResponseFuture is not done (async execution) and you try to retrieve the trace,
[`cassandra.query.TraceUnavailable`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.TraceUnavailable) will be raised.

query_cl is the consistency level used to poll the trace tables.

#### get_all_query_traces()

Fetches and returns the query traces for all query pages, if tracing was enabled.

See note in [`get_query_trace()`](#cassandra.cluster.ResponseFuture.get_query_trace) regarding possible exceptions.

#### custom_payload

The custom payload returned from the server, if any. This will only be
set by Cassandra servers implementing a custom QueryHandler, and only
for protocol_version 4+.

Ensure the future is complete before trying to access this property
(call [`result()`](#cassandra.cluster.ResponseFuture.result), or after callback is invoked).
Otherwise it may throw if the response has not been received.

* **Returns:**
  [Custom Payloads](https://python-driver.docs.scylladb.com/master/api/cassandra/protocol.md#custom-payload).

#### is_schema_agreed *= True*

For DDL requests, this may be set `False` if the schema agreement poll after the response fails.

Always `True` for non-DDL requests.

#### has_more_pages

Returns `True` if there are more pages left in the
query results, `False` otherwise.  This should only
be checked after the first page has been returned.

#### Versionadded
Added in version 2.0.0.

#### warnings

Warnings returned from the server, if any. This will only be
set for protocol_version 4+.

Warnings may be returned for such things as oversized batches,
or too many tombstones in slice queries.

Ensure the future is complete before trying to access this property
(call [`result()`](#cassandra.cluster.ResponseFuture.result), or after callback is invoked).
Otherwise it may throw if the response has not been received.

#### start_fetching_next_page()

If there are more pages left in the query result, this asynchronously
starts fetching the next page.  If there are no pages left, [`QueryExhausted`](#cassandra.cluster.QueryExhausted)
is raised.  Also see [`has_more_pages`](#cassandra.cluster.ResponseFuture.has_more_pages).

This should only be called after the first page has been returned.

#### Versionadded
Added in version 2.0.0.

#### add_callback(fn, \*args, \*\*kwargs)

Attaches a callback function to be called when the final results arrive.

By default, fn will be called with the results as the first and only
argument.  If \*args or \*\*kwargs are supplied, they will be passed
through as additional positional or keyword arguments to fn.

If an error is hit while executing the operation, a callback attached
here will not be called.  Use [`add_errback()`](#cassandra.cluster.ResponseFuture.add_errback) or [`add_callbacks()`](#cassandra.cluster.ResponseFuture.add_callbacks)
if you wish to handle that case.

If the final result has already been seen when this method is called,
the callback will be called immediately (before this method returns).

Note: in the case that the result is not available when the callback is added,
the callback is executed by IO event thread. This means that the callback
should not block or attempt further synchronous requests, because no further
IO will be processed until the callback returns.

**Important**: if the callback you attach results in an exception being
raised, **the exception will be ignored**, so please ensure your
callback handles all error cases that you care about.

Usage example:

```default
>>> session = cluster.connect("mykeyspace")

>>> def handle_results(rows, start_time, should_log=False):
...     if should_log:
...         log.info("Total time: %f", time.time() - start_time)
...     ...

>>> future = session.execute_async("SELECT * FROM users")
>>> future.add_callback(handle_results, time.time(), should_log=True)
```

#### add_errback(fn, \*args, \*\*kwargs)

Like [`add_callback()`](#cassandra.cluster.ResponseFuture.add_callback), but handles error cases.
An Exception instance will be passed as the first positional argument
to fn.

#### add_callbacks(callback, errback, callback_args=(), callback_kwargs=None, errback_args=(), errback_kwargs=None)

A convenient combination of [`add_callback()`](#cassandra.cluster.ResponseFuture.add_callback) and
[`add_errback()`](#cassandra.cluster.ResponseFuture.add_errback).

Example usage:

```default
>>> session = cluster.connect()
>>> query = "SELECT * FROM mycf"
>>> future = session.execute_async(query)

>>> def log_results(results, level='debug'):
...     for row in results:
...         log.log(level, "Result: %s", row)

>>> def log_error(exc, query):
...     log.error("Query '%s' failed: %s", query, exc)

>>> future.add_callbacks(
...     callback=log_results, callback_kwargs={'level': 'info'},
...     errback=log_error, errback_args=(query,))
```

### *class* cassandra.cluster.ResultSet

An iterator over the rows from a query result. Also supplies basic equality
and indexing methods for backward-compatability. These methods materialize
the entire result set (loading all pages), and should only be used if the
total result size is understood. Warnings are emitted when paged results
are materialized in this fashion.

You can treat this as a normal iterator over rows:

```default
>>> from cassandra.query import SimpleStatement
>>> statement = SimpleStatement("SELECT * FROM users", fetch_size=10)
>>> for user_row in session.execute(statement):
...     process_user(user_row)
```

Whenever there are no more rows in the current page, the next page will
be fetched transparently.  However, note that it *is* possible for
an `Exception` to be raised while fetching the next page, just
like you might see on a normal call to `session.execute()`.

#### *property* has_more_pages

True if the last response indicated more pages; False otherwise

#### *property* current_rows

The list of current page rows. May be empty; this does not mean
there is no more data. Use has_more_pages() for that.

#### all()

Returns all the remaining rows as a list. This is basically
a convenient shortcut to list(result_set).

This function is not recommended for queries that return a large number of elements.

#### one()

Return a single row of the results or None if empty. This is basically
a shortcut to result_set.current_rows[0] and should only be used when
you know a query returns a single row. Consider using an iterator if the
ResultSet contains more than one row.

#### fetch_next_page()

Manually, synchronously fetch the next page. Supplied for manually retrieving pages
and inspecting `current_page()`. It is not necessary to call this when iterating
through results; paging happens implicitly in iteration.

#### get_query_trace(max_wait_sec=None)

Gets the last query trace from the associated future.
See [`ResponseFuture.get_query_trace()`](#cassandra.cluster.ResponseFuture.get_query_trace) for details.

#### get_all_query_traces(max_wait_sec_per=None)

Gets all query traces from the associated future.
See [`ResponseFuture.get_all_query_traces()`](#cassandra.cluster.ResponseFuture.get_all_query_traces) for details.

#### *property* was_applied

For LWT results, returns whether the transaction was applied.

Result is indeterminate if called on a result that was not an LWT request or on
a [`query.BatchStatement`](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.BatchStatement) containing LWT. In the latter case either all the batch
succeeds or fails.

Only valid when one of the of the internal row factories is in use.

#### *property* paging_state

Server paging state of the query. Can be None if the query was not paged.

The driver treats paging state as opaque, but it may contain primary key data, so applications may want to
avoid sending this to untrusted parties.

### *exception* cassandra.cluster.QueryExhausted

Raised when [`ResponseFuture.start_fetching_next_page()`](#cassandra.cluster.ResponseFuture.start_fetching_next_page) is called and
there are no more pages.  You can check [`ResponseFuture.has_more_pages`](#cassandra.cluster.ResponseFuture.has_more_pages)
before calling to avoid this.

#### Versionadded
Added in version 2.0.0.

### *exception* cassandra.cluster.NoHostAvailable

Raised when an operation is attempted but all connections are
busy, defunct, closed, or resulted in errors when used.

#### errors *= None*

A map of the form `{ip: exception}` which details the particular
Exception that was caught for each host the operation was attempted
against.

### *exception* cassandra.cluster.UserTypeDoesNotExist

An attempt was made to use a user-defined type that does not exist.

#### Versionadded
Added in version 2.1.0.
