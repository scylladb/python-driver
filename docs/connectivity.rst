.. _connectivity:

Connectivity
============

The driver starts with one or more contact points, discovers the cluster
topology, and opens connections to the nodes it needs. See
:doc:`getting-started` for basic contact-point and session setup.

Authentication and TLS configuration are covered in :doc:`security`. For a
ScyllaDB Cloud connection bundle, see :doc:`scylla-cloud`.

Some private-network deployments expose cluster nodes through proxy endpoints
instead of their advertised addresses. Use Client Routes for these deployments.

Contents
--------

:doc:`connectivity/client-routes`
    Connect through AWS PrivateLink, GCP Private Service Connect, or a similar
    per-node proxy setup.

.. toctree::
   :hidden:
   :maxdepth: 1

   connectivity/client-routes
