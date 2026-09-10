# cassandra.io.asyncorereactor

`asyncore` Event Loop

<a id="module-cassandra.io.asyncorereactor"></a>

### *class* cassandra.io.asyncorereactor.AsyncoreConnection(\*args: Any, \*\*kwargs: Any)

An implementation of `Connection` that uses the `asyncore`
module in the Python standard library for its event loop.

#### *classmethod* initialize_reactor()

Called once by Cluster.connect().  This should be used by implementations
to set up any resources that will be shared across connections.

#### *classmethod* handle_fork()

Called after a forking.  This should cleanup any remaining reactor state
from the parent process.
