# cassandra.io.asyncioreactor

`asyncio` Event Loop

<a id="module-cassandra.io.asyncioreactor"></a>

### *class* cassandra.io.asyncioreactor.AsyncioConnection(\*args, \*\*kwargs)

An implementation of `Connection` that uses the `asyncio`
module in the Python standard library for its event loop.

Supports SSL connections via asyncio’s native TLS transport, which
avoids the incompatibility between `ssl.SSLSocket` and asyncio’s
low-level socket methods (`sock_sendall`, `sock_recv`).

TLS session resumption ([`Cluster.ssl_session_cache`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.Cluster.ssl_session_cache)) is not
available on this reactor: the handshake happens inside
`loop.create_connection(..., ssl=...)`, which offers no point at which
a cached session could be restored.

#### *classmethod* initialize_reactor()

Called once by Cluster.connect().  This should be used by implementations
to set up any resources that will be shared across connections.
