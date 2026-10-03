# Performance Notes

The Python driver for Cassandra offers several methods for executing queries.
You can synchronously block for queries to complete using
[`Session.execute()`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.Session.execute), you can obtain asynchronous request futures through
[`Session.execute_async()`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.Session.execute_async), and you can attach a callback to the future
with [`ResponseFuture.add_callback()`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.ResponseFuture.add_callback).

Examples of multiple request patterns can be found in the benchmark scripts included in the driver project.

The choice of execution pattern will depend on the application context. For applications dealing with multiple
requests in a given context, the recommended pattern is to use concurrent asynchronous
requests with callbacks. For many use cases, you don’t need to implement this pattern yourself.
[`cassandra.concurrent.execute_concurrent()`](https://python-driver.docs.scylladb.com/stable/api/cassandra/concurrent.md#cassandra.concurrent.execute_concurrent) and [`cassandra.concurrent.execute_concurrent_with_args()`](https://python-driver.docs.scylladb.com/stable/api/cassandra/concurrent.md#cassandra.concurrent.execute_concurrent_with_args)
provide this pattern with a synchronous API and tunable concurrency.

Due to the GIL and limited concurrency, the driver can become CPU-bound pretty quickly. The sections below
discuss further runtime and design considerations for mitigating this limitation.

## PyPy

[PyPy](http://pypy.org) is an alternative Python runtime which uses a JIT compiler to
reduce CPU consumption. This leads to a huge improvement in the driver performance,
more than doubling throughput for many workloads.

## Cython Extensions

[Cython](http://cython.org/) is an optimizing compiler and language that can be used to compile the core files and
optional extensions for the driver. Cython is not a strict dependency, but the extensions will be built by default.

See [Installation](https://python-driver.docs.scylladb.com/stable/installation.md) for details on controlling this build.

## multiprocessing

All of the patterns discussed above may be used over multiple processes using the
[multiprocessing](http://docs.python.org/2/library/multiprocessing.html)
module.  Multiple processes will scale better than multiple threads, so if high throughput is your goal,
consider this option.

Be sure to **never share any** [`Cluster`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.Cluster), [`Session`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.Session),
**or** [`ResponseFuture`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.ResponseFuture) **objects across multiple processes**. These
objects should all be created after forking the process, not before.

For further discussion and simple examples using the driver with `multiprocessing`,
see [this blog post](http://www.datastax.com/dev/blog/datastax-python-driver-multiprocessing-example-for-improved-bulk-data-throughput).
