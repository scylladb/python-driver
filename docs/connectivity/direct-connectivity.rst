.. _direct-connectivity:

Direct Connectivity (VPC Peering)
=================================

Use direct connectivity when the application can open a TCP connection to the
native transport address advertised by every ScyllaDB node that its
load-balancing policy may use. The route can be local, public, or private.
Common private-network examples include applications in the same VPC, VPC
peering, AWS Transit Gateway, and routed VPN connections.

Direct connectivity uses the regular :class:`~cassandra.cluster.Cluster`
configuration. It does not require
:class:`~cassandra.client_routes.ClientRoutesConfig`: contact points bootstrap
topology discovery, then the driver connects to the discovered node addresses.

Prepare the network
-------------------

Before starting the driver:

#. Establish routes in both directions between the application network and the
   cluster network. VPC-peered networks must use non-overlapping CIDR ranges.
#. Allow the cluster's CQL native transport and shard-aware transport ports
   through route tables, security groups, network ACLs, and host firewalls for
   every node. The defaults are ``9042`` and ``19042`` without TLS, or ``9142``
   and ``19142`` with TLS, but use the ports configured by the cluster.
#. Make every eligible node's advertised native transport address reachable
   from the application. Reaching only the initial contact points is
   insufficient. This normally means every node in the local datacenter, plus
   any remote nodes enabled for failover.
#. Ensure contact-point hostnames resolve from the application network, if DNS
   names are used.
#. When available, choose at least two contact points in the local datacenter so
   discovery can survive one unavailable seed.

For ScyllaDB Cloud, create the private network connection outside the driver,
then use the contact points and port shown on the cluster's **Connect** tab.
Follow the current platform procedure:

* `ScyllaDB Cloud network access options <https://cloud.docs.scylladb.com/stable/cluster-connections/connectivity-options.html>`_
* `AWS VPC peering <https://cloud.docs.scylladb.com/stable/cluster-connections/aws-vpc-peering.html>`_
* `GCP VPC peering <https://cloud.docs.scylladb.com/stable/cluster-connections/gcp-vpc-peering.html>`_
* `AWS Transit Gateway VPC attachment <https://cloud.docs.scylladb.com/stable/cluster-connections/aws-tgw-vpc-attachment.html>`_
* `ScyllaDB Cloud availability checks <https://cloud.docs.scylladb.com/stable/cluster-connections/checking-cluster-availability.html>`_

ScyllaDB Cloud VPC peering must be enabled when the cluster is created. The AWS
and GCP guides describe the required peering acceptance, routes, CIDRs, and
allowed address ranges. VPC peering is not transitive; use Transit Gateway or
another routed design when applications in multiple networks need access.
ScyllaDB Cloud's AWS Transit Gateway attachment requires the Premium plan and a
cluster created with the **VPC Peering / Transit Gateway** network option
enabled; follow the linked TGW guide for the remaining prerequisites.
ScyllaDB Cloud recommends public direct access only for development and
evaluation; production deployments should use VPC peering or Transit Gateway.
Public Cloud connections require TLS and an appropriate IP allowlist.

The driver uses the shard-aware port advertised by ScyllaDB to target a shard
directly. If that port cannot be routed, either open it or disable only this
advanced connection path with
``shard_aware_options={"disable_shardaware_port": True}``. Token-aware and
basic shard-aware routing continue over the regular CQL port. See the
`ScyllaDB networking port table <https://docs.scylladb.com/manual/stable/operating-scylla/admin.html#networking>`_.

Check reachability
------------------

Test from the same network namespace, VM, container, or pod that will run the
application. Check every node address and the configured CQL port, not only a
load balancer or one seed:

.. code-block:: bash

    nc -vz 10.20.0.11 9042
    nc -vz 10.20.0.12 9042
    nc -vz 10.20.0.13 9042
    nc -vz 10.20.0.11 19042
    nc -vz 10.20.0.12 19042
    nc -vz 10.20.0.13 19042

A successful TCP check proves network reachability, not authentication or TLS
configuration. Configure those separately as described in :doc:`../security`.
Unlike a Client Routes proxy connection, a direct connection can keep TLS
hostname verification enabled when node certificates match their advertised
addresses.

Configure the driver
--------------------

Pass a small set of stable node addresses as contact points. Contact points are
used for initial discovery; they do not restrict which nodes the driver uses
after connecting.

Specify the local datacenter explicitly for predictable routing, especially in
a multi-datacenter cluster:

.. code-block:: python

    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

    profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(
            DCAwareRoundRobinPolicy(local_dc="AWS_US_EAST_1")
        )
    )

    cluster = Cluster(
        contact_points=["10.20.0.11", "10.20.0.12", "10.20.0.13"],
        port=9042,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
    )
    session = cluster.connect()

Use the datacenter name reported by the cluster, not a cloud region name unless
they are identical. Keep contact points in that local datacenter. By default,
the datacenter-aware policy does not open pools to remote-datacenter nodes.

Verify discovery
----------------

After connecting, inspect the driver's topology view:

.. code-block:: python

    for host in cluster.metadata.all_hosts():
        advertised_port = host.broadcast_rpc_port or cluster.port
        print(
            host.host_id,
            host.endpoint,
            f"{host.broadcast_rpc_address}:{advertised_port}",
            host.datacenter,
            host.is_up,
        )

``host.endpoint`` is the connection target selected by the driver, while
``broadcast_rpc_address`` and ``broadcast_rpc_port`` are the values advertised
by the server. Every node the application should use must have a reachable
endpoint. A common failure pattern is that an initial contact point works but
the remaining nodes are down or absent from usable pools; this usually means
their advertised addresses are missing routes or blocked by network policy.

When direct addresses do not work
---------------------------------

Fix routing or the nodes' advertised native transport addresses when possible.
For a stable one-to-one mapping between advertised and reachable addresses, a
custom :class:`~cassandra.policies.AddressTranslator` can translate discovered
node addresses. It does not translate ports or the initial contact points, and
different nodes must not translate to the same endpoint.

If nodes are reachable only through per-node proxy endpoints whose mappings can
change, use :doc:`client-routes` instead.
