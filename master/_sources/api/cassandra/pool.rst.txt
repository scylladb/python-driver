cassandra.pool
==============

Hosts and Connection Pools

.. automodule:: cassandra.pool

``Host.host_id`` and ``Host.endpoint`` are read-only. A node endpoint move
publishes a distinct ``Host`` with the same UUID and retires the previous
object. Retained references keep their original endpoint; consult cluster
metadata for the current ``Host``. Equality and hashing use the UUID, so old
and replacement objects compare equal even though they are distinct objects.
Host health, conviction policy, and reconnection state remain mutable.

.. autoclass:: Host ()
    :members:
    :exclude-members: set_location_info, get_and_set_reconnection_handler

.. autoexception:: NoConnectionsAvailable
    :members:
