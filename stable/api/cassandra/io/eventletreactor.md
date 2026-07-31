# cassandra.io.eventletreactor

`eventlet`-compatible Connection

<a id="module-cassandra.io.eventletreactor"></a>

### *class* cassandra.io.eventletreactor.EventletConnection(\*args, \*\*kwargs)

An implementation of `Connection` that utilizes `eventlet`.

This implementation assumes all eventlet monkey patching is active. It is not tested with partial patching.

#### *classmethod* initialize_reactor()

Called once by Cluster.connect().  This should be used by implementations
to set up any resources that will be shared across connections.

#### *classmethod* service_timeouts()

cls._timeout_watcher runs in this loop forever.
It is usually waiting for the next timeout on the cls._new_timer Event.
When new timers are added, that event is set so that the watcher can
wake up and possibly set an earlier timeout.
