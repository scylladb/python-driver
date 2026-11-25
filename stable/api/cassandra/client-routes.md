# cassandra.client_routes

Client Routes configuration for private-network proxy endpoints.

<a id="module-cassandra.client_routes"></a>

### *class* cassandra.client_routes.ClientRouteProxy(connection_id: str, connection_addr_override: str | None = None)

Configuration for one private-network proxy connection.

* **Parameters:**
  * **connection_id** – String identifying the connection (required).
  * **connection_addr_override** – Optional address to use instead of the
    addresses stored in `system.client_routes` for this connection ID.
    When explicit contact points are omitted, this address is also used for
    the initial connection.

### *class* cassandra.client_routes.ClientRoutesConfig(proxies: List[[ClientRouteProxy](#cassandra.client_routes.ClientRouteProxy)] | Tuple[[ClientRouteProxy](#cassandra.client_routes.ClientRouteProxy), ...], advanced_shard_awareness: bool = False)

Configuration for client routes (Private Link support).

* **Parameters:**
  * **proxies** – Non-empty list or tuple of [`ClientRouteProxy`](#cassandra.client_routes.ClientRouteProxy)
    objects (required).
  * **advanced_shard_awareness** – Whether to enable advanced shard awareness
    through the proxy (default: `False`).
