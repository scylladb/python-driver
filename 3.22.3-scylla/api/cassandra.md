# [`cassandra`](#module-cassandra) - Exceptions and Enums

### cassandra.\_\_version_info_\_

The version of the driver in a tuple format

### cassandra.\_\_version_\_

The version of the driver in a string format

### *class* cassandra.ConsistencyLevel

Spcifies how many replicas must respond for an operation to be considered
a success.  By default, `ONE` is used for all operations.

#### ANY *= 0*

Only requires that one replica receives the write *or* the coordinator
stores a hint to replay later. Valid only for writes.

#### ONE *= 1*

Only one replica needs to respond to consider the operation a success

#### TWO *= 2*

Two replicas must respond to consider the operation a success

#### THREE *= 3*

Three replicas must respond to consider the operation a success

#### QUORUM *= 4*

`ceil(RF/2)` replicas must respond to consider the operation a success

#### ALL *= 5*

All replicas must respond to consider the operation a success

#### LOCAL_QUORUM *= 6*

Requires a quorum of replicas in the local datacenter

#### EACH_QUORUM *= 7*

Requires a quorum of replicas in each datacenter

#### SERIAL *= 8*

For conditional inserts/updates that utilize Cassandra’s lightweight
transactions, this requires consensus among all replicas for the
modified data.

#### LOCAL_SERIAL *= 9*

Like [`SERIAL`](#cassandra.ConsistencyLevel.SERIAL), but only requires consensus
among replicas in the local datacenter.

#### LOCAL_ONE *= 10*

Sends a request only to replicas in the local datacenter and waits for
one response.

### *class* cassandra.ProtocolVersion

Defines native protocol versions supported by this driver.

#### V1 *= 1*

v1, supported in Cassandra 1.2–>2.2

#### V2 *= 2*

v2, supported in Cassandra 2.0–>2.2;
added support for lightweight transactions, batch operations, and automatic query paging.

#### V3 *= 3*

v3, supported in Cassandra 2.1–>3.x+;
added support for protocol-level client-side timestamps (see `Session.use_client_timestamp`),
serial consistency levels for `BatchStatement`, and an improved connection pool.

#### V4 *= 4*

v4, supported in Cassandra 2.2–>3.x+;
added a number of new types, server warnings, new failure messages, and custom payloads. Details in the
[project docs](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec)

#### V5 *= 5*

v5, in beta from 3.x+

#### DSE_V1 *= 65*

DSE private protocol v1, supported in DSE 5.1+

#### DSE_V2 *= 66*

DSE private protocol v2, supported in DSE 6.0+

#### SUPPORTED_VERSIONS *= (66, 65, 5, 4, 3, 2, 1)*

A tuple of all supported protocol versions

#### BETA_VERSIONS *= (5,)*

A tuple of all beta protocol versions

#### MIN_SUPPORTED *= 1*

Minimum protocol version supported by this driver.

#### MAX_SUPPORTED *= 66*

Maximum protocol version supported by this driver.

#### *classmethod* get_lower_supported(previous_version)

Return the lower supported protocol version. Beta versions are omitted.

### *class* cassandra.UserFunctionDescriptor(name, argument_types)

Describes a User function by name and argument signature

#### name *= None*

name of the function

#### argument_types *= None*

Ordered list of CQL argument type names comprising the type signature

#### *property* signature

function signature string in the form ‘name([type0[,type1[…]]])’

can be used to uniquely identify overloaded function names within a keyspace

### *class* cassandra.UserAggregateDescriptor(name, argument_types)

Describes a User aggregate function by name and argument signature

#### name *= None*

name of the aggregate

#### argument_types *= None*

Ordered list of CQL argument type names comprising the type signature

#### *property* signature

function signature string in the form ‘name([type0[,type1[…]]])’

can be used to uniquely identify overloaded function names within a keyspace

### *exception* cassandra.DriverException

Base for all exceptions explicitly raised by the driver.

### *exception* cassandra.RequestExecutionException

Base for request execution exceptions returned from the server.

### *exception* cassandra.Unavailable

There were not enough live replicas to satisfy the requested consistency
level, so the coordinator node immediately failed the request without
forwarding it to any replicas.

#### consistency *= None*

The requested [`ConsistencyLevel`](#cassandra.ConsistencyLevel)

#### required_replicas *= None*

The number of replicas that needed to be live to complete the operation

#### alive_replicas *= None*

The number of replicas that were actually alive

### *exception* cassandra.Timeout

Replicas failed to respond to the coordinator node before timing out.

#### consistency *= None*

The requested [`ConsistencyLevel`](#cassandra.ConsistencyLevel)

#### required_responses *= None*

The number of required replica responses

#### received_responses *= None*

The number of replicas that responded before the coordinator timed out
the operation

### *exception* cassandra.ReadTimeout

A subclass of [`Timeout`](#cassandra.Timeout) for read operations.

This indicates that the replicas failed to respond to the coordinator
node before the configured timeout. This timeout is configured in
`cassandra.yaml` with the `read_request_timeout_in_ms`
and `range_request_timeout_in_ms` options.

#### data_retrieved *= None*

A boolean indicating whether the requested data was retrieved
by the coordinator from any replicas before it timed out the
operation

### *exception* cassandra.WriteTimeout

A subclass of [`Timeout`](#cassandra.Timeout) for write operations.

This indicates that the replicas failed to respond to the coordinator
node before the configured timeout. This timeout is configured in
`cassandra.yaml` with the `write_request_timeout_in_ms`
option.

#### write_type *= None*

The type of write operation, enum on [`WriteType`](cassandra/policies.md#cassandra.policies.WriteType)

### *exception* cassandra.CoordinationFailure

Replicas sent a failure to the coordinator.

#### consistency *= None*

The requested [`ConsistencyLevel`](#cassandra.ConsistencyLevel)

#### required_responses *= None*

The number of required replica responses

#### received_responses *= None*

The number of replicas that responded before the coordinator timed out
the operation

#### failures *= None*

The number of replicas that sent a failure message

#### error_code_map *= None*

A map of inet addresses to error codes representing replicas that sent
a failure message.  Only set when protocol_version is 5 or higher.

### *exception* cassandra.ReadFailure

A subclass of [`CoordinationFailure`](#cassandra.CoordinationFailure) for read operations.

This indicates that the replicas sent a failure message to the coordinator.

#### data_retrieved *= None*

A boolean indicating whether the requested data was retrieved
by the coordinator from any replicas before it timed out the
operation

### *exception* cassandra.WriteFailure

A subclass of [`CoordinationFailure`](#cassandra.CoordinationFailure) for write operations.

This indicates that the replicas sent a failure message to the coordinator.

#### write_type *= None*

The type of write operation, enum on [`WriteType`](cassandra/policies.md#cassandra.policies.WriteType)

### *exception* cassandra.FunctionFailure

User Defined Function failed during execution

#### keyspace *= None*

Keyspace of the function

#### function *= None*

Name of the function

#### arg_types *= None*

List of argument type names of the function

### *exception* cassandra.RequestValidationException

Server request validation failed

### *exception* cassandra.ConfigurationException

Server indicated request errro due to current configuration

### *exception* cassandra.AlreadyExists

An attempt was made to create a keyspace or table that already exists.

#### keyspace *= None*

The name of the keyspace that already exists, or, if an attempt was
made to create a new table, the keyspace that the table is in.

#### table *= None*

The name of the table that already exists, or, if an attempt was
make to create a keyspace, `None`.

### *exception* cassandra.InvalidRequest

A query was made that was invalid for some reason, such as trying to set
the keyspace for a connection to a nonexistent keyspace.

### *exception* cassandra.Unauthorized

The current user is not authorized to perform the requested operation.

### *exception* cassandra.AuthenticationFailed

Failed to authenticate.

### *exception* cassandra.OperationTimedOut

The operation took longer than the specified (client-side) timeout
to complete.  This is not an error generated by Cassandra, only
the driver.

#### errors *= None*

A dict of errors keyed by the `Host` against which they occurred.

#### last_host *= None*

The last `Host` this operation was attempted against.
