.. _connectivity:

Connectivity
============

The driver starts with one or more contact points, discovers the cluster
topology, and opens connections to the nodes it needs. The application network
must provide a usable path to every discovered node that the load-balancing
configuration may use, either directly or through Client Routes. See
:doc:`getting-started` for basic contact-point and session setup.

Authentication and TLS configuration are covered in :doc:`security`. For a
ScyllaDB Cloud connection bundle, see :doc:`scylla-cloud`.

Choose the connection model that matches the network:

.. list-table::
   :header-rows: 1
   :widths: 25 45 30

   * - Model
     - Use when
     - Driver configuration
   * - Direct connectivity
     - The application can route to every node address advertised by the
       cluster, including through VPC peering or another routed private network.
     - Regular :class:`~cassandra.cluster.Cluster` contact points.
   * - Client Routes
     - Nodes are reached through per-node proxy endpoints and their advertised
       addresses are not directly reachable.
     - :class:`~cassandra.client_routes.ClientRoutesConfig`.

Contents
--------

:doc:`connectivity/direct-connectivity`
    Connect directly to every cluster node through VPC peering or another
    routed network.

:doc:`connectivity/client-routes`
    Connect through AWS PrivateLink, GCP Private Service Connect, or a similar
    per-node proxy setup.

.. toctree::
   :hidden:
   :maxdepth: 1

   connectivity/direct-connectivity
   connectivity/client-routes
