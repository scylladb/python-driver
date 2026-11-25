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
``SkipPoolCreation``, which uses it for application queries without an explicit
host. Requests with an explicit host do not use fallback. Every Session belonging
to the same Cluster shares that connection on the fallback path.

Before a fallback request, the driver selects the Session's configured keyspace
if it differs from the connection's recorded keyspace. A Session without a
configured keyspace uses the connection's existing state. The driver does not
parse application CQL. A successful ``USE`` response updates the originating
Session's default and propagates the change to its node pools.

For predictable keyspace selection, finish outstanding application requests
before executing ``USE`` or calling
:meth:`~cassandra.cluster.Session.set_keyspace`, and wait for the change to
succeed before submitting more work. Include further page fetches when
coordinating outstanding work.

The driver permits overlapping keyspace changes and queries, including requests
from Sessions with different defaults. Their effective keyspace and ordering are
unspecified: a change on the shared connection can affect another Session's
request. The driver does not reject or serialize these requests to enforce
keyspace isolation. Fully qualified table names avoid dependence on the
connection's default for those references.

A failed or timed-out ``USE`` may leave connections on different keyspaces.
Further requests remain allowed, and the caller decides whether to continue,
successfully re-establish a default, or recreate the affected Session or Cluster.
Creating a Session without a default does not reset the Cluster's existing
control connection to no keyspace. Normal transport errors, capacity limits,
shutdown and request deadlines still apply.

After reconnecting, a Session whose default was dropped can fail preliminary
keyspace setup even for ``session.execute("USE live_ks")``. Recover with
``session.set_keyspace("live_ks")``; it selects the requested default without
first restoring the unavailable one.

Pooled keyspace changes
-----------------------

With node pools, the driver applies a successful ``USE`` to the Session's
other connections. If any connection rejects that change, the calling ``USE``
fails. The driver does not automatically retry the rejected change or replace
that connection.

The failure does not roll back changes that already succeeded:
``Session.keyspace`` and some connections may already use the new keyspace.
Further queries remain allowed, including through connections still using the
old keyspace. Until a keyspace change completes successfully, the keyspace used
by an unqualified query is unspecified. After correcting the cause of the
rejection, the caller can issue ``USE`` again or create a new Session. The driver
still synchronizes connections it opens in the background with the current
Session default before making them available.

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
