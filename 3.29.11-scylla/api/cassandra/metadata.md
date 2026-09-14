# cassandra.metadata

Schema and Ring Topology

<a id="module-cassandra.metadata"></a>

### cassandra.metadata.cql_keywords

Set of keywords in CQL.

Derived from …/cassandra/src/java/org/apache/cassandra/cql3/Cql.g

### cassandra.metadata.cql_keywords_unreserved

Set of unreserved keywords in CQL.

Derived from …/cassandra/src/java/org/apache/cassandra/cql3/Cql.g

### cassandra.metadata.cql_keywords_reserved

Set of reserved keywords in CQL.

### *class* cassandra.metadata.Metadata

Holds a representation of the cluster schema and topology.

#### cluster_name *= None*

The string name of the cluster.

#### partitioner *= None*

The string name of the partitioner for the cluster.

#### token_map *= None*

A [`TokenMap`](#cassandra.metadata.TokenMap) instance describing the ring topology.

#### keyspaces *= None*

A map from keyspace names to matching [`KeyspaceMetadata`](#cassandra.metadata.KeyspaceMetadata) instances.

#### dbaas *= False*

A boolean indicating if connected to a DBaaS cluster

#### export_schema_as_string()

Returns a string that can be executed as a query in order to recreate
the entire schema.  The string is formatted to be human readable.

#### get_replicas(keyspace, key)

Returns a list of [`Host`](https://python-driver.docs.scylladb.com/stable/api/cassandra/pool.md#cassandra.pool.Host) instances that are replicas for a given
partition key.

#### add_or_return_host(host)

Returns a tuple (host, new), where `host` is a Host
instance, and `new` is a bool indicating whether
the host was newly added.

#### get_host(endpoint_or_address, port=None)

Find a host in the metadata for a specific endpoint. If a string inet address and port are passed,
iterate all hosts to match the [`broadcast_rpc_address`](https://python-driver.docs.scylladb.com/stable/api/cassandra/pool.md#cassandra.pool.Host.broadcast_rpc_address) and
[`broadcast_rpc_port`](https://python-driver.docs.scylladb.com/stable/api/cassandra/pool.md#cassandra.pool.Host.broadcast_rpc_port) attributes.

#### get_host_by_host_id(host_id)

Same as get_host() but use host_id for lookup.

#### all_hosts()

Returns a list of all known [`Host`](https://python-driver.docs.scylladb.com/stable/api/cassandra/pool.md#cassandra.pool.Host) instances in the cluster.

## Schemas

### *class* cassandra.metadata.KeyspaceMetadata

A representation of the schema for a single keyspace.

#### virtual *= False*

A boolean indicating if this is a virtual keyspace or not. Always `False`
for clusters running Cassandra pre-4.0 and DSE pre-6.7 versions.

#### Versionadded
Added in version 3.15.

#### name *= None*

The string name of the keyspace.

#### durable_writes *= True*

A boolean indicating whether durable writes are enabled for this keyspace
or not.

#### replication_strategy *= None*

A [`ReplicationStrategy`](#cassandra.metadata.ReplicationStrategy) subclass object.

#### tables *= None*

A map from table names to instances of [`TableMetadata`](#cassandra.metadata.TableMetadata).

#### indexes *= None*

A dict mapping index names to [`IndexMetadata`](#cassandra.metadata.IndexMetadata) instances.

#### user_types *= None*

A map from user-defined type names to instances of [`UserType`](#cassandra.metadata.UserType).

#### Versionadded
Added in version 2.1.0.

#### functions *= None*

A map from user-defined function signatures to instances of [`Function`](#cassandra.metadata.Function).

#### Versionadded
Added in version 2.6.0.

#### aggregates *= None*

A map from user-defined aggregate signatures to instances of [`Aggregate`](#cassandra.metadata.Aggregate).

#### Versionadded
Added in version 2.6.0.

#### views *= None*

A dict mapping view names to [`MaterializedViewMetadata`](#cassandra.metadata.MaterializedViewMetadata) instances.

#### graph_engine *= None*

A string indicating whether a graph engine is enabled for this keyspace (Core/Classic).

#### export_as_string()

Returns a CQL query string that can be used to recreate the entire keyspace,
including user-defined types and tables.

#### as_cql_query()

Returns a CQL query string that can be used to recreate just this keyspace,
not including user-defined types and tables.

### *class* cassandra.metadata.UserType

A user defined type, as created by `CREATE TYPE` statements.

User-defined types were introduced in Cassandra 2.1.

#### Versionadded
Added in version 2.1.0.

#### keyspace *= None*

The string name of the keyspace in which this type is defined.

#### name *= None*

The name of this type.

#### field_names *= None*

An ordered list of the names for each field in this user-defined type.

#### field_types *= None*

An ordered list of the types for each field in this user-defined type.

#### as_cql_query(formatted=False)

Returns a CQL query that can be used to recreate this type.
If formatted is set to `True`, extra whitespace will
be added to make the query more readable.

### *class* cassandra.metadata.Function

A user defined function, as created by `CREATE FUNCTION` statements.

User-defined functions were introduced in Cassandra 2.2

#### Versionadded
Added in version 2.6.0.

#### keyspace *= None*

The string name of the keyspace in which this function is defined

#### name *= None*

The name of this function

#### argument_types *= None*

An ordered list of the types for each argument to the function

#### argument_names *= None*

An ordered list of the names of each argument to the function

#### return_type *= None*

Return type of the function

#### language *= None*

Language of the function body

#### body *= None*

Function body string

#### called_on_null_input *= None*

Flag indicating whether this function should be called for rows with null values
(convenience function to avoid handling nulls explicitly if the result will just be null)

#### deterministic *= None*

Flag indicating if this function is guaranteed to produce the same result
for a particular input. This is available only for DSE >=6.0.

#### monotonic *= None*

Flag indicating if this function is guaranteed to increase or decrease
monotonically on any of its arguments. This is available only for DSE >=6.0.

#### monotonic_on *= None*

A list containing the argument or arguments over which this function is
monotonic. This is available only for DSE >=6.0.

#### as_cql_query(formatted=False)

Returns a CQL query that can be used to recreate this function.
If formatted is set to `True`, extra whitespace will
be added to make the query more readable.

### *class* cassandra.metadata.Aggregate

A user defined aggregate function, as created by `CREATE AGGREGATE` statements.

Aggregate functions were introduced in Cassandra 2.2

#### Versionadded
Added in version 2.6.0.

#### keyspace *= None*

The string name of the keyspace in which this aggregate is defined

#### name *= None*

The name of this aggregate

#### argument_types *= None*

An ordered list of the types for each argument to the aggregate

#### state_func *= None*

Name of a state function

#### state_type *= None*

Type of the aggregate state

#### final_func *= None*

Name of a final function

#### initial_condition *= None*

Initial condition of the aggregate

#### return_type *= None*

Return type of the aggregate

#### deterministic *= None*

Flag indicating if this function is guaranteed to produce the same result
for a particular input and state. This is available only with DSE >=6.0.

#### as_cql_query(formatted=False)

Returns a CQL query that can be used to recreate this aggregate.
If formatted is set to `True`, extra whitespace will
be added to make the query more readable.

### *class* cassandra.metadata.TableMetadata

A representation of the schema for a single table.

#### *property* primary_key

A list of [`ColumnMetadata`](#cassandra.metadata.ColumnMetadata) representing the components of
the primary key for this table.

#### *property* is_cql_compatible

A boolean indicating if this table can be represented as CQL in export

#### extensions *= None*

Metadata describing configuration for table extensions

#### keyspace_name *= None*

String name of this Table’s keyspace

#### name *= None*

The string name of the table.

#### partition_key *= None*

A list of [`ColumnMetadata`](#cassandra.metadata.ColumnMetadata) instances representing the columns in
the partition key for this table.  This will always hold at least one
column.

#### clustering_key *= None*

A list of [`ColumnMetadata`](#cassandra.metadata.ColumnMetadata) instances representing the columns
in the clustering key for this table.  These are all of the
[`primary_key`](#cassandra.metadata.TableMetadata.primary_key) columns that are not in the [`partition_key`](#cassandra.metadata.TableMetadata.partition_key).

Note that a table may have no clustering keys, in which case this will
be an empty list.

#### columns *= None*

A dict mapping column names to [`ColumnMetadata`](#cassandra.metadata.ColumnMetadata) instances.

#### indexes *= None*

A dict mapping index names to [`IndexMetadata`](#cassandra.metadata.IndexMetadata) instances.

#### options *= None*

A dict mapping table option names to their specific settings for this
table.

#### triggers *= None*

A dict mapping trigger names to `TriggerMetadata` instances.

#### views *= None*

A dict mapping view names to [`MaterializedViewMetadata`](#cassandra.metadata.MaterializedViewMetadata) instances.

#### virtual *= False*

A boolean indicating if this is a virtual table or not. Always `False`
for clusters running Cassandra pre-4.0 and DSE pre-6.7 versions.

#### Versionadded
Added in version 3.15.

#### export_as_string()

Returns a string of CQL queries that can be used to recreate this table
along with all indexes on it.  The returned string is formatted to
be human readable.

#### as_cql_query(formatted=False)

Returns a CQL query that can be used to recreate this table (index
creations are not included).  If formatted is set to `True`,
extra whitespace will be added to make the query human readable.

### *class* cassandra.metadata.TableMetadataV3

For C\* 3.0+. option_maps take a superset of map names, so if  nothing
changes structurally, new option maps can just be appended to the list.

#### *property* is_cql_compatible

A boolean indicating if this table can be represented as CQL in export

### *class* cassandra.metadata.TableMetadataDSE68

#### vertex *= None*

A [`VertexMetadata`](#cassandra.metadata.VertexMetadata) instance, if graph enabled

#### edge *= None*

A [`EdgeMetadata`](#cassandra.metadata.EdgeMetadata) instance, if graph enabled

#### as_cql_query(formatted=False)

Returns a CQL query that can be used to recreate this table (index
creations are not included).  If formatted is set to `True`,
extra whitespace will be added to make the query human readable.

### *class* cassandra.metadata.ColumnMetadata

A representation of a single column in a table.

#### table *= None*

The [`TableMetadata`](#cassandra.metadata.TableMetadata) this column belongs to.

#### name *= None*

The string name of this column.

#### cql_type *= None*

The CQL type for the column.

#### is_static *= False*

If this column is static (available in Cassandra 2.1+), this will
be `True`, otherwise `False`.

#### is_reversed *= False*

If this column is reversed (DESC) as in clustering order

### *class* cassandra.metadata.IndexMetadata

A representation of a secondary index on a column.

#### keyspace_name *= None*

A string name of the keyspace.

#### table_name *= None*

A string name of the table this index is on.

#### name *= None*

A string name for the index.

#### kind *= None*

A string representing the kind of index (COMPOSITE, CUSTOM,…).

#### index_options *= {}*

A dict of index options.

#### as_cql_query()

Returns a CQL query that can be used to recreate this index.

#### export_as_string()

Returns a CQL query string that can be used to recreate this index.

### *class* cassandra.metadata.MaterializedViewMetadata

A representation of a materialized view on a table

#### extensions *= None*

Metadata describing configuration for table extensions

#### keyspace_name *= None*

A string name of the keyspace of this view.

#### name *= None*

A string name of the view.

#### base_table_name *= None*

A string name of the base table for this view.

#### partition_key *= None*

A list of [`ColumnMetadata`](#cassandra.metadata.ColumnMetadata) instances representing the columns in
the partition key for this view.  This will always hold at least one
column.

#### clustering_key *= None*

A list of [`ColumnMetadata`](#cassandra.metadata.ColumnMetadata) instances representing the columns
in the clustering key for this view.

Note that a table may have no clustering keys, in which case this will
be an empty list.

#### columns *= None*

A dict mapping column names to [`ColumnMetadata`](#cassandra.metadata.ColumnMetadata) instances.

#### include_all_columns *= None*

A flag indicating whether the view was created AS SELECT \*

#### where_clause *= None*

String WHERE clause for the view select statement. From server metadata

#### options *= None*

A dict mapping table option names to their specific settings for this
view.

#### as_cql_query(formatted=False)

Returns a CQL query that can be used to recreate this function.
If formatted is set to `True`, extra whitespace will
be added to make the query more readable.

### *class* cassandra.metadata.VertexMetadata

A representation of a vertex on a table

#### keyspace_name *= None*

A string name of the keyspace.

#### table_name *= None*

A string name of the table this vertex is on.

#### label_name *= None*

A string name of the label of this vertex.

### *class* cassandra.metadata.EdgeMetadata

A representation of an edge on a table

#### keyspace_name *= None*

A string name of the keyspace

#### table_name *= None*

A string name of the table this edge is on

#### label_name *= None*

A string name of the label of this edge

#### from_table *= None*

A string name of the from table of this edge (incoming vertex)

#### from_label *= None*

A string name of the from table label of this edge (incoming vertex)

#### from_partition_key_columns *= None*

The columns that match the partition key of the incoming vertex table.

#### from_clustering_columns *= None*

The columns that match the clustering columns of the incoming vertex table.

#### to_table *= None*

A string name of the to table of this edge (outgoing vertex)

#### to_label *= None*

A string name of the to table label of this edge (outgoing vertex)

#### to_partition_key_columns *= None*

The columns that match the partition key of the outgoing vertex table.

#### to_clustering_columns *= None*

The columns that match the clustering columns of the outgoing vertex table.

## Tokens and Ring Topology

### *class* cassandra.metadata.TokenMap

Information about the layout of the ring.

#### token_class *= None*

A subclass of [`Token`](#cassandra.metadata.Token), depending on what partitioner the cluster uses.

#### ring *= None*

An ordered list of [`Token`](#cassandra.metadata.Token) instances in the ring.

#### token_to_host_owner *= None*

A map of [`Token`](#cassandra.metadata.Token) objects to the [`Host`](https://python-driver.docs.scylladb.com/stable/api/cassandra/pool.md#cassandra.pool.Host) that owns that token.

#### tokens_to_hosts_by_ks *= None*

A map of keyspace names to a nested map of [`Token`](#cassandra.metadata.Token) objects to
sets of [`Host`](https://python-driver.docs.scylladb.com/stable/api/cassandra/pool.md#cassandra.pool.Host) objects.

#### get_replicas(keyspace, token)

Get  a set of [`Host`](https://python-driver.docs.scylladb.com/stable/api/cassandra/pool.md#cassandra.pool.Host) instances representing all of the
replica nodes for a given [`Token`](#cassandra.metadata.Token).

### *class* cassandra.metadata.Token

Abstract class representing a token.

### *class* cassandra.metadata.Murmur3Token(token)

A token for `Murmur3Partitioner`.

token is an int or string representing the token.

### *class* cassandra.metadata.MD5Token(token)

A token for `RandomPartitioner`.

### *class* cassandra.metadata.BytesToken(token)

A token for `ByteOrderedPartitioner`.

#### *classmethod* from_string(token_string)

token_string should be the string representation from the server.

### cassandra.metadata.ReplicationStrategy

alias of `_ReplicationStrategy`

### *class* cassandra.metadata.ReplicationFactor(all_replicas, transient_replicas=None)

Represent the replication factor of a keyspace.

#### all_replicas *= None*

The number of total replicas.

#### transient_replicas *= None*

The number of transient replicas.

Only set if the keyspace has transient replication enabled.

#### full_replicas *= None*

The number of replicas that own a full copy of the data. This is the same
than all_replicas when transient replication is not enabled.

### *class* cassandra.metadata.SimpleStrategy(options_map)

#### *property* replication_factor

The replication factor for this keyspace.

For backward compatibility, this returns the
[`cassandra.metadata.ReplicationFactor.full_replicas`](#cassandra.metadata.ReplicationFactor.full_replicas) value of
[`cassandra.metadata.SimpleStrategy.replication_factor_info`](#cassandra.metadata.SimpleStrategy.replication_factor_info).

#### replication_factor_info *= None*

A [`cassandra.metadata.ReplicationFactor`](#cassandra.metadata.ReplicationFactor) instance.

#### export_for_schema()

Returns a string version of these replication options which are
suitable for use in a CREATE KEYSPACE statement.

### *class* cassandra.metadata.NetworkTopologyStrategy(dc_replication_factors)

#### dc_replication_factors_info *= None*

A map of datacenter names to the [`cassandra.metadata.ReplicationFactor`](#cassandra.metadata.ReplicationFactor) instance for that DC.

#### dc_replication_factors *= None*

A map of datacenter names to the replication factor for that DC.

For backward compatibility, this maps to the [`cassandra.metadata.ReplicationFactor.full_replicas`](#cassandra.metadata.ReplicationFactor.full_replicas)
value of the [`cassandra.metadata.NetworkTopologyStrategy.dc_replication_factors_info`](#cassandra.metadata.NetworkTopologyStrategy.dc_replication_factors_info) dict.

#### export_for_schema()

Returns a string version of these replication options which are
suitable for use in a CREATE KEYSPACE statement.

### *class* cassandra.metadata.LocalStrategy(options_map)

#### export_for_schema()

Returns a string version of these replication options which are
suitable for use in a CREATE KEYSPACE statement.

### cassandra.metadata.group_keys_by_replica(session, keyspace, table, keys)

Returns a `dict` with the keys grouped per host. This can be
used to more accurately group by IN clause or to batch the keys per host.

If a valid replica is not found for a particular key it will be grouped under
`NO_VALID_REPLICA`

Example usage:

```default
>>> result = group_keys_by_replica(
...     session, "system", "peers",
...     (("127.0.0.1", ), ("127.0.0.2", )))
```
