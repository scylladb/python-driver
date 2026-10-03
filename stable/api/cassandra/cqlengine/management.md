# cassandra.cqlengine.management

Schema management for cqlengine

<a id="module-cassandra.cqlengine.management"></a>

A collection of functions for managing keyspace and table schema.

### cassandra.cqlengine.management.create_keyspace_simple(name, replication_factor, durable_writes=True, connections=None)

Creates a keyspace with NetworkTopologyStrategy for replica placement

If the keyspace already exists, it will not be modified.

**This function should be used with caution, especially in production environments.
Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

*There are plans to guard schema-modifying functions with an environment-driven conditional.*

* **Parameters:**
  * **name** (*str*) – name of keyspace to create
  * **replication_factor** (*int*) – keyspace replication factor, used with `NetworkTopologyStrategy`
  * **durable_writes** (*bool*) – Write log is bypassed if set to False
  * **connections** (*list*) – List of connection names

### cassandra.cqlengine.management.create_keyspace_network_topology(name, dc_replication_map, durable_writes=True, connections=None)

Creates a keyspace with NetworkTopologyStrategy for replica placement

If the keyspace already exists, it will not be modified.

**This function should be used with caution, especially in production environments.
Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

*There are plans to guard schema-modifying functions with an environment-driven conditional.*

* **Parameters:**
  * **name** (*str*) – name of keyspace to create
  * **dc_replication_map** (*dict*) – map of dc_names: replication_factor
  * **durable_writes** (*bool*) – Write log is bypassed if set to False
  * **connections** (*list*) – List of connection names

### cassandra.cqlengine.management.drop_keyspace(name, connections=None)

Drops a keyspace, if it exists.

*There are plans to guard schema-modifying functions with an environment-driven conditional.*

**This function should be used with caution, especially in production environments.
Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

* **Parameters:**
  * **name** (*str*) – name of keyspace to drop
  * **connections** (*list*) – List of connection names

### cassandra.cqlengine.management.sync_table(model, keyspaces=None, connections=None)

Inspects the model and creates / updates the corresponding table and columns.

If keyspaces is specified, the table will be synched for all specified keyspaces.
Note that the Model._\_keyspace_\_ is ignored in that case.

If connections is specified, the table will be synched for all specified connections. Note that the Model._\_connection_\_ is ignored in that case.
If not specified, it will try to get the connection from the Model.

Any User Defined Types used in the table are implicitly synchronized.

This function can only add fields that are not part of the primary key.

Note that the attributes removed from the model are not deleted on the database.
They become effectively ignored by (will not show up on) the model.

**This function should be used with caution, especially in production environments.
Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

*There are plans to guard schema-modifying functions with an environment-driven conditional.*

### cassandra.cqlengine.management.sync_type(ks_name, type_model, connection=None)

Inspects the type_model and creates / updates the corresponding type.

Note that the attributes removed from the type_model are not deleted on the database (this operation is not supported).
They become effectively ignored by (will not show up on) the type_model.

**This function should be used with caution, especially in production environments.
Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

*There are plans to guard schema-modifying functions with an environment-driven conditional.*

### cassandra.cqlengine.management.drop_table(model, keyspaces=None, connections=None)

Drops the table indicated by the model, if it exists.

If keyspaces is specified, the table will be dropped for all specified keyspaces. Note that the Model._\_keyspace_\_ is ignored in that case.

If connections is specified, the table will be synched for all specified connections. Note that the Model._\_connection_\_ is ignored in that case.
If not specified, it will try to get the connection from the Model.

**This function should be used with caution, especially in production environments.
Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

*There are plans to guard schema-modifying functions with an environment-driven conditional.*
