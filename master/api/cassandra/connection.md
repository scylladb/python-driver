# cassandra.connection

Low Level Connection Info

<a id="module-cassandra.connection"></a>

### *exception* cassandra.connection.ConnectionException

An unrecoverable error was hit when attempting to use a connection,
or the connection was already closed or defunct.

### *exception* cassandra.connection.ConnectionShutdown

Raised when a connection has been marked as defunct or has been closed.

### *exception* cassandra.connection.ConnectionBusy

An attempt was made to send a message through a `Connection` that
was already at the max number of in-flight operations.

### *exception* cassandra.connection.ProtocolError

Communication did not match the protocol that this driver expects.

### *class* cassandra.connection.EndPoint

Represents the information to connect to a cassandra node.

#### *property* address

The IP address of the node. This is the RPC address the driver uses when connecting to the node

#### *property* port

The port of the node.

#### *property* ssl_options

SSL options specific to this endpoint.

#### *property* socket_family

The socket family of the endpoint.

#### resolve()

Resolve the endpoint to an address/port. This is called
only on socket connection.

### *class* cassandra.connection.EndPointFactory

#### configure(cluster)

This is called by the cluster during its initialization.

#### create(row)

Create an EndPoint from a system.peers row.

### *class* cassandra.connection.SniEndPoint(proxy_address, server_name, port=9042)

SNI Proxy EndPoint implementation.

### *class* cassandra.connection.SniEndPointFactory(proxy_address, port, node_domain=None)

### *class* cassandra.connection.UnixSocketEndPoint(unix_socket_path)

Unix Socket EndPoint implementation.
