<a id="connectivity"></a>

# Connectivity

The driver starts with one or more contact points, discovers the cluster
topology, and opens connections to the nodes it needs. The application network
must provide a usable path to every discovered node that the load-balancing
configuration may use, either directly or through Client Routes. See
[Getting Started](https://python-driver.docs.scylladb.com/master/getting-started.md) for basic contact-point and session setup.

Authentication and TLS configuration are covered in [Security](https://python-driver.docs.scylladb.com/master/security.md). For
ScyllaDB Cloud, see [ScyllaDB Cloud](https://python-driver.docs.scylladb.com/master/scylla-cloud.md).

Choose the connection model that matches the network:

| Model               | Use when                                                                                                                                        | Driver configuration                                                                                                                              |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| Direct connectivity | The application can route to every node address advertised by the<br/>cluster, including through VPC peering or another routed private network. | Regular [`Cluster`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster) contact points.            |
| Client Routes       | Nodes are reached through per-node proxy endpoints and their advertised<br/>addresses are not directly reachable.                               | [`ClientRoutesConfig`](https://python-driver.docs.scylladb.com/master/api/cassandra/client-routes.md#cassandra.client_routes.ClientRoutesConfig). |

## Contents

[Direct Connectivity (VPC Peering)](https://python-driver.docs.scylladb.com/master/connectivity/direct-connectivity.md)
: Connect directly to every cluster node through VPC peering or another
  routed network.

[Client Routes (Private Networking)](https://python-driver.docs.scylladb.com/master/connectivity/client-routes.md)
: Connect through AWS PrivateLink, GCP Private Service Connect, or a similar
  per-node proxy setup.
