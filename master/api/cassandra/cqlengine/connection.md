# cassandra.cqlengine.connection

Connection management for cqlengine

<a id="module-cassandra.cqlengine.connection"></a>

### cassandra.cqlengine.connection.default()

Configures the default connection to localhost, using the driver defaults
(except for row_factory)

### cassandra.cqlengine.connection.set_session(s)

Configures the default connection with a preexisting [`cassandra.cluster.Session`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session)

Note: the mapper presently requires a Session [`cassandra.cluster.Session.row_factory`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.row_factory) set to `dict_factory`.
This may be relaxed in the future

### cassandra.cqlengine.connection.setup(hosts, default_keyspace, consistency=None, lazy_connect=False, retry_connect=False, \*\*kwargs)

Setup the driver connection used by the mapper

* **Parameters:**
  * **hosts** (*list*) – list of hosts, (`contact_points` for [`cassandra.cluster.Cluster`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster))
  * **default_keyspace** (*str*) – The default keyspace to use
  * **consistency** (*int*) – The global default [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) - default is the same as [`Session.default_consistency_level`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.default_consistency_level)
  * **lazy_connect** (*bool*) – True if should not connect until first use
  * **retry_connect** (*bool*) – True if we should retry to connect even if there was a connection failure initially
  * **kwargs** – Pass-through keyword arguments for [`cassandra.cluster.Cluster`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster)

### cassandra.cqlengine.connection.register_connection(name, hosts=None, consistency=None, lazy_connect=False, retry_connect=False, cluster_options=None, default=False, session=None)

Add a connection to the connection registry. `hosts` and `session` are
mutually exclusive, and `consistency`, `lazy_connect`,
`retry_connect`, and `cluster_options` only work with `hosts`. Using
`hosts` will create a new [`cassandra.cluster.Cluster`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster) and
[`cassandra.cluster.Session`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session).

* **Parameters:**
  * **hosts** (*list*) – list of hosts, (`contact_points` for [`cassandra.cluster.Cluster`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster)).
  * **consistency** (*int*) – The default [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) for the
    registered connection’s new session. Default is the same as
    [`Session.default_consistency_level`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session.default_consistency_level). For use with `hosts` only;
    will fail when used with `session`.
  * **lazy_connect** (*bool*) – True if should not connect until first use. For
    use with `hosts` only; will fail when used with `session`.
  * **retry_connect** (*bool*) – True if we should retry to connect even if there
    was a connection failure initially. For use with `hosts` only; will
    fail when used with `session`.
  * **cluster_options** (*dict*) – A dict of options to be used as keyword
    arguments to [`cassandra.cluster.Cluster`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster). For use with `hosts`
    only; will fail when used with `session`.
  * **default** (*bool*) – If True, set the new connection as the cqlengine
    default
  * **session** ([*Session*](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session)) – A [`cassandra.cluster.Session`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Session) to be used in
    the created connection.

### cassandra.cqlengine.connection.unregister_connection(name)

### cassandra.cqlengine.connection.set_default_connection(name)
