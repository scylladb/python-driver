Performance Notes
=================
The Python driver for Cassandra offers several methods for executing queries.
You can synchronously block for queries to complete using
:meth:`.Session.execute()`, you can obtain asynchronous request futures through
:meth:`.Session.execute_async()`, and you can attach a callback to the future
with :meth:`.ResponseFuture.add_callback()`.

Examples of multiple request patterns can be found in the benchmark scripts included in the driver project.

The choice of execution pattern will depend on the application context. For applications dealing with multiple
requests in a given context, the recommended pattern is to use concurrent asynchronous
requests with callbacks. For many use cases, you don't need to implement this pattern yourself.
:meth:`cassandra.concurrent.execute_concurrent` and :meth:`cassandra.concurrent.execute_concurrent_with_args`
provide this pattern with a synchronous API and tunable concurrency.

Due to the GIL and limited concurrency, the driver can become CPU-bound pretty quickly. The sections below
discuss further runtime and design considerations for mitigating this limitation.

PyPy
----
`PyPy <http://pypy.org>`_ is an alternative Python runtime which uses a JIT compiler to
reduce CPU consumption. This leads to a huge improvement in the driver performance,
more than doubling throughput for many workloads.

Cython Extensions
-----------------
`Cython <http://cython.org/>`_ is an optimizing compiler and language that can be used to compile the core files and
optional extensions for the driver. Cython is not a strict dependency, but the extensions will be built by default.

See :doc:`installation` for details on controlling this build.

multiprocessing
---------------
All of the patterns discussed above may be used over multiple processes using the
`multiprocessing <http://docs.python.org/2/library/multiprocessing.html>`_
module.  Multiple processes will scale better than multiple threads, so if high throughput is your goal,
consider this option.

Be sure to **never share any** :class:`~.Cluster`, :class:`~.Session`,
**or** :class:`~.ResponseFuture` **objects across multiple processes**. These
objects should all be created after forking the process, not before.

For further discussion and simple examples using the driver with ``multiprocessing``,
see `this blog post <http://www.datastax.com/dev/blog/datastax-python-driver-multiprocessing-example-for-improved-bulk-data-throughput>`_.

NumPy-accelerated Vector Serialization
--------------------------------------
When inserting high-dimensional vectors (e.g. ML embeddings), the default
element-by-element serialization in
:class:`~cassandra.cqltypes.VectorType` can become a bottleneck.  If
`NumPy <https://numpy.org>`_ is installed, the driver provides three
progressively faster paths:

**Single-row ndarray fast path** – pass a 1-D ``numpy.ndarray`` directly
as the bound value for a ``vector<float, N>`` column.  The driver
byte-swaps the array to big-endian in a single C-level operation and
calls ``tobytes()``, replacing *N* individual ``struct.pack`` calls::

    import numpy as np
    embedding = np.array([0.1, 0.2, ..., 0.768], dtype=np.float32)
    session.execute(insert_stmt, [key, embedding])

**Bulk serialization** – for batch inserts, convert an entire 2-D array
(one row per vector) into a list of ``bytes`` objects with a single
byte-swap::

    from cassandra.cqltypes import VectorType

    # Build the parameterized type (usually done once)
    ctype = VectorType.apply_parameters(
        [lookup_casstype('org.apache.cassandra.db.marshal.FloatType'), 768],
        names=None,
    )

    vectors_2d = np.array(all_embeddings, dtype=np.float32)   # (N, 768)
    blobs = ctype.serialize_numpy_bulk(vectors_2d)             # list[bytes]

    for key, blob in zip(keys, blobs):
        session.execute(insert_stmt, [key, blob])

Each ``bytes`` object in the returned list is accepted directly by the
driver's bytes-passthrough path, so ``BoundStatement.bind()`` performs no
further conversion.

**Supported subtypes** – the fast paths are available for ``float``
(``>f4``), ``double`` (``>f8``), ``int`` (``>i4``), and ``bigint``
(``>i8``).  Variable-length subtypes (``smallint``, ``tinyint``, text
types, etc.) fall back to the original element-by-element serialization
automatically.

**Benchmarks** – on 768-dimension ``float32`` vectors (100-row batches),
the bulk path is ~146× faster than the baseline, and the bytes
passthrough path is ~298× faster.  See
``benchmarks/bench_vector_numpy_serialize.py`` for reproducible numbers.
