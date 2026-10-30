.. _connectivity:

Connectivity
============

The driver starts with one or more contact points, discovers the cluster
topology, and opens connections to the nodes it needs. The application network
must provide a usable path to every discovered node that the load-balancing
configuration may use, either directly or through Client Routes. See
:doc:`getting-started` for basic contact-point and session setup.

Authentication and TLS configuration are covered in :doc:`security`. For
ScyllaDB Cloud, see :doc:`scylla-cloud`.

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

Control-connection fallback
---------------------------

:class:`~cassandra.cluster.ControlConnectionQueryFallback` offers ``Fallback``,
which uses the shared control connection when no node pool is usable, and
``SkipPoolCreation``, which always uses it for application queries. Both modes
bind fallback queries to one keyspace at a time.

An explicit ``USE`` statement or :meth:`~cassandra.cluster.Session.set_keyspace`
can change that binding only when its Session is the sole fallback user. Another
Session using the control connection causes the change to fail. Queries already
in flight from the same Session may execute under either keyspace; wait for
those queries to finish before sending ``USE`` when their keyspace matters.
Another ``USE`` fails while an explicit keyspace change is pending. If the
driver is still selecting the Session's initial keyspace, an explicit ``USE``
waits for that setup to finish.
An uncertain ``USE`` result, such as a client timeout, closes the shared
control connection so a later query cannot inherit an unknown keyspace state.

Pooled keyspace changes
-----------------------

With node pools, the driver applies a successful ``USE`` to the Session's
other connections. If any connection rejects that change, the calling ``USE``
fails. The driver does not automatically retry the rejected change or replace
that connection.

The failure does not roll back changes that already succeeded:
``Session.keyspace`` and some connections may already use the new keyspace.
Connections still using the old keyspace are not used for ordinary queries.
After correcting the cause of the rejection, issue ``USE`` again or create a
new Session before sending unqualified queries.

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
