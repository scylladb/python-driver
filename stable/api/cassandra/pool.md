# cassandra.pool

Hosts and Connection Pools

<a id="module-cassandra.pool"></a>

Connection pooling and host management.

### *class* cassandra.pool.Host

Represents a single Cassandra node.

#### broadcast_address *= None*

broadcast address configured for the node, *if available*:

‘system.local.broadcast_address’ or ‘system.peers.peer’ (Cassandra 2-3)
‘system.local.broadcast_address’ or ‘system.peers_v2.peer’ (Cassandra 4)

This is not present in the `system.local` table for older versions of Cassandra. It
is also not queried if [`token_metadata_enabled`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.Cluster.token_metadata_enabled) is `False`.

#### broadcast_port *= None*

broadcast port configured for the node, *if available*:

‘system.local.broadcast_port’ or ‘system.peers_v2.peer_port’ (Cassandra 4)

It is also not queried if [`token_metadata_enabled`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.Cluster.token_metadata_enabled) is `False`.

#### broadcast_rpc_address *= None*

The broadcast rpc address of the node:

‘system.local.rpc_address’ or ‘system.peers.rpc_address’ (Cassandra 3)
‘system.local.rpc_address’ or ‘system.peers.native_transport_address (DSE  6+)’
‘system.local.rpc_address’ or ‘system.peers_v2.native_address (Cassandra 4)’

#### broadcast_rpc_port *= None*

The broadcast rpc port of the node, *if available*:

‘system.local.rpc_port’ or ‘system.peers.native_transport_port’ (DSE 6+)
‘system.local.rpc_port’ or ‘system.peers_v2.native_port’ (Cassandra 4)

#### listen_address *= None*

listen address configured for the node, *if available*:

‘system.local.listen_address’

This is only available in the `system.local` table for newer versions of Cassandra. It is also not
queried if [`token_metadata_enabled`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.Cluster.token_metadata_enabled) is `False`. Usually the same as `broadcast_address`
unless configured differently in cassandra.yaml.

#### listen_port *= None*

listen port configured for the node, *if available*:

‘system.local.listen_port’

This is only available in the `system.local` table for newer versions of Cassandra. It is also not
queried if [`token_metadata_enabled`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.Cluster.token_metadata_enabled) is `False`.

#### is_up *= None*

`True` if the node is considered up, `False` if it is
considered down, and `None` if it is not known if the node is
up or down.

#### release_version *= None*

release_version as queried from the control connection system tables

#### dse_version *= None*

dse_version as queried from the control connection system tables. Only populated when connecting to
DSE with this property available. Not queried if [`token_metadata_enabled`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.Cluster.token_metadata_enabled) is `False`.

#### dse_workload *= None*

DSE workload queried from the control connection system tables. Only populated when connecting to
DSE with this property available. Not queried if [`token_metadata_enabled`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.Cluster.token_metadata_enabled) is `False`.
This is a legacy attribute that does not portray multiple workloads in a uniform fashion.
See also [`dse_workloads`](#cassandra.pool.Host.dse_workloads).

#### dse_workloads *= None*

DSE workloads set, queried from the control connection system tables. Only populated when connecting to
DSE with this property available (added in DSE 5.1).
Not queried if [`token_metadata_enabled`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.Cluster.token_metadata_enabled) is `False`.

#### endpoint *= None*

The [`EndPoint`](https://python-driver.docs.scylladb.com/stable/api/cassandra/connection.md#cassandra.connection.EndPoint) to connect to the node.

#### conviction_policy *= None*

A [`ConvictionPolicy`](https://python-driver.docs.scylladb.com/stable/api/cassandra/policies.md#cassandra.policies.ConvictionPolicy) instance for determining when this node should
be marked up or down.

#### host_id *= None*

The unique identifier of the cassandra node

#### *property* address

The IP address of the endpoint. This is the RPC address the driver uses when connecting to the node.

#### *property* datacenter

The datacenter the node is in.

#### *property* rack

The rack the node is in.

### *exception* cassandra.pool.NoConnectionsAvailable

All existing connections to a given host are busy, or there are
no open connections.
