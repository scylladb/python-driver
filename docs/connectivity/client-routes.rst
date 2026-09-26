.. _client-routes:

Client Routes (Private Networking)
==================================

Private-network services such as AWS PrivateLink and GCP Private Service
Connect can expose ScyllaDB nodes through proxy endpoints. The addresses that
the nodes advertise to each other are not necessarily reachable from the
application network. Client Routes lets the driver map each discovered node to
its reachable proxy address and port.

How Client Routes works
-----------------------

Each private connection is selected by a connection ID. A managed platform
may supply this value; for a self-managed connection, choose a stable ID. The
same value must be present in both the driver's
:class:`~cassandra.client_routes.ClientRouteProxy` and the corresponding
``system.client_routes`` rows. For every cluster node, the table associates
that connection ID and the node's host ID with a proxy address and ports.

When Client Routes is configured, the driver:

#. Loads matching rows from ``system.client_routes`` after establishing the
   control connection and whenever that connection is re-established.
#. Resolves discovered nodes through those rows before opening connections.
#. Subscribes to ``CLIENT_ROUTES_CHANGE`` events and updates affected routes
   when the table changes.

Client Routes requires scylla-driver 3.29.9 or later and ScyllaDB 2026.1 or
later. All cluster nodes must support the ScyllaDB Client Routes feature.

Basic usage
-----------

Obtain or choose the connection ID, and obtain the discovery endpoint from the
provider. The following GCP Private Service Connect example uses the endpoint
only to make the initial connection; the driver then uses the per-node ports
from ``system.client_routes``.

.. code-block:: python

    from cassandra.client_routes import ClientRouteProxy, ClientRoutesConfig
    from cassandra.cluster import Cluster

    client_routes = ClientRoutesConfig(
        proxies=[ClientRouteProxy("connection-id-from-provider")]
    )

    cluster = Cluster(
        contact_points=["endpoint.cluster-1.scylladb.com"],
        port=9000,
        client_routes_config=client_routes,
    )
    session = cluster.connect()

The contact-point port is the discovery port supplied for the private
connection. It does not replace the node-specific ``port`` or ``tls_port``
values stored in the route table.

Configure authentication as described in :doc:`../security`. A
:class:`~cassandra.auth.PlainTextAuthProvider` sends credentials without
encryption unless TLS is enabled; see the TLS configuration below before using
it across an untrusted network.

Multiple connection IDs
-----------------------

Pass every connection ID that the application can use, for example one for
each availability zone:

.. code-block:: python

    from cassandra.client_routes import ClientRouteProxy, ClientRoutesConfig

    client_routes = ClientRoutesConfig(
        proxies=[
            ClientRouteProxy("connection-id-zone-a"),
            ClientRouteProxy("connection-id-zone-b"),
        ]
    )

The driver filters route-table rows to the configured IDs.

Override proxy addresses
------------------------

Set ``connection_addr_override`` when every route for a connection ID must use
a different DNS name or IP address. If explicit contact points are omitted,
the override is also used for the initial connection:

.. code-block:: python

    from cassandra.client_routes import ClientRouteProxy, ClientRoutesConfig
    from cassandra.cluster import Cluster

    proxy = ClientRouteProxy(
        connection_id="connection-id-from-provider",
        connection_addr_override="private-endpoint.example.com",
    )
    client_routes = ClientRoutesConfig(proxies=[proxy])

    cluster = Cluster(
        port=9000,
        client_routes_config=client_routes,
    )
    session = cluster.connect()

The override changes the address, not the node-specific route-table port. If
``contact_points`` is set explicitly, those addresses remain the initial
contact points while the override still applies to matching discovered-node
routes.

Provision routes on self-managed clusters
-----------------------------------------

ScyllaDB Cloud or the network provider normally provisions these mappings. Do
not replace provider-managed routes unless its instructions require it.

Operators of self-managed clusters can upsert mappings through the ScyllaDB
admin REST API. Each entry maps one ``connection_id`` and node ``host_id`` to
the proxy address and that node's ports. Run ``nodetool status`` to list each
node's host ID, then match each ID to the proxy listener that forwards to that
same node. For example:

.. code-block:: bash

    curl --fail-with-body --request POST \
      --header 'Content-Type: application/json' \
      --header 'Accept: application/json' \
      --data '[
        {
          "connection_id": "psc-connection-1",
          "host_id": "00000000-0000-0000-0000-000000000001",
          "address": "endpoint.cluster-1.scylladb.com",
          "port": 9001,
          "tls_port": 9101
        },
        {
          "connection_id": "psc-connection-1",
          "host_id": "00000000-0000-0000-0000-000000000002",
          "address": "endpoint.cluster-1.scylladb.com",
          "port": 9002,
          "tls_port": 9102
        },
        {
          "connection_id": "psc-connection-1",
          "host_id": "00000000-0000-0000-0000-000000000003",
          "address": "endpoint.cluster-1.scylladb.com",
          "port": 9003,
          "tls_port": 9103
        }
      ]' \
      http://127.0.0.1:10000/v2/client-routes

Port 10000 is ScyllaDB's default admin API port, not a driver or proxy port.
Run this command from an authorized administration network and replace the
address, host IDs, and ports with values for the deployment. A non-TLS driver
uses ``port``; a TLS-enabled driver uses ``tls_port``.

Verify the stored mappings before starting a private-only client:

.. code-block:: bash

    curl --fail --header 'Accept: application/json' \
      http://127.0.0.1:10000/v2/client-routes

``system.client_routes`` has a node-local view, and applying a cluster-wide
update can lag briefly. Repeat the GET request against the admin API address of
every node until every response contains the configured connection ID and the
correct host ID and proxy port for every node. Starting with an incomplete map
can make the driver fall back to node addresses that the private application
network cannot reach.

TLS
---

Client Routes supports TLS and selects ``tls_port`` from each matching route.
Certificate-chain validation can remain enabled, but hostname verification is
not currently compatible with proxy addresses. Set
``SSLContext.check_hostname`` to ``False``; :class:`~cassandra.cluster.Cluster`
rejects a Client Routes configuration when it is ``True``.

.. code-block:: python

    import ssl

    from cassandra.client_routes import ClientRouteProxy, ClientRoutesConfig
    from cassandra.cluster import Cluster

    ssl_context = ssl.create_default_context(cafile="/path/to/ca.pem")
    ssl_context.check_hostname = False

    client_routes = ClientRoutesConfig(
        proxies=[ClientRouteProxy("connection-id-from-provider")]
    )
    cluster = Cluster(
        contact_points=["private-endpoint.example.com"],
        port=9100,
        ssl_context=ssl_context,
        client_routes_config=client_routes,
    )

Disabling hostname verification prevents checking that the proxy hostname
matches the node certificate. ``ssl.create_default_context`` still requires a
certificate signed by a trusted CA in this example.

Shard awareness
---------------

Client Routes disables advanced shard awareness by default because proxy
infrastructure commonly does not preserve the client source port used to
select a shard. Basic token-aware and shard-aware request routing remains
enabled.

Enable the advanced mode only when the proxy path preserves the required
source-port behavior:

.. code-block:: python

    client_routes = ClientRoutesConfig(
        proxies=[ClientRouteProxy("connection-id-from-provider")],
        advanced_shard_awareness=True,
    )

Fallback and compatibility
--------------------------

If a discovered node has no matching route, the driver falls back to that
node's advertised address and port. Mixed direct and proxied nodes therefore
work when the application network can reach every fallback address. In a
private-only deployment, provide a route for every node to prevent connection
failures.

Client Routes owns endpoint resolution for discovered nodes. A
``client_routes_config`` cannot be combined with a custom ``endpoint_factory``,
and an ``address_translator`` is not applied to Client Routes endpoints. Use
``connection_addr_override`` when a configured connection ID needs a different
address.

See :doc:`/api/cassandra/client-routes` for the configuration API.
