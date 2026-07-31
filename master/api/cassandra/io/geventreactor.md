# cassandra.io.geventreactor

`gevent`-compatible Event Loop

<a id="module-cassandra.io.geventreactor"></a>

### *class* cassandra.io.geventreactor.GeventConnection(\*args, \*\*kwargs)

An implementation of `Connection` that utilizes `gevent`.

This implementation assumes all gevent monkey patching is active. It is not tested with partial patching.

#### *classmethod* initialize_reactor()

Called once by Cluster.connect().  This should be used by implementations
to set up any resources that will be shared across connections.
