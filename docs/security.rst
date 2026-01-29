.. _security:

Security
========
The two main security components you will use with the
Python driver are Authentication and SSL.

Authentication
--------------
Versions 2.0 and higher of the driver support a SASL-based
authentication mechanism when :attr:`~.Cluster.protocol_version`
is set to 2 or higher.  To use this authentication, set
:attr:`~.Cluster.auth_provider` to an instance of a subclass
of :class:`~cassandra.auth.AuthProvider`.  When working
with Cassandra's ``PasswordAuthenticator``, you can use
the :class:`~cassandra.auth.PlainTextAuthProvider` class.

For example, suppose Cassandra is setup with its default
'cassandra' user with a password of 'cassandra':

.. code-block:: python

    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(auth_provider=auth_provider, protocol_version=2)



Custom Authenticators
^^^^^^^^^^^^^^^^^^^^^
If you're using something other than Cassandra's ``PasswordAuthenticator``,
:class:`~.SaslAuthProvider` is provided for generic SASL authentication mechanisms,
utilizing the ``pure-sasl`` package.
If these do not suit your needs, you may need to create your own subclasses of
:class:`~.AuthProvider` and :class:`~.Authenticator`.  You can use the Sasl classes
as example implementations.

SSL
---
SSL should be used when client encryption is enabled in Cassandra.

To give you as much control as possible over your SSL configuration, our SSL
API takes a user-created `SSLContext` instance from the Python standard library.
These docs will include some examples for how to achieve common configurations,
but the `ssl.SSLContext <https://docs.python.org/3/library/ssl.html#ssl.SSLContext>`_ documentation
gives a more complete description of what is possible.

To enable SSL with version 3.17.0 and higher, you will need to set :attr:`.Cluster.ssl_context` to a
``ssl.SSLContext`` instance to enable SSL. Optionally, you can also set :attr:`.Cluster.ssl_options`
to a dict of options. These will be passed as kwargs to ``ssl.SSLContext.wrap_socket()``
when new sockets are created.

If you create your SSLContext using `ssl.create_default_context <https://docs.python.org/3/library/ssl.html#ssl.create_default_context>`_,
be aware that SSLContext.check_hostname is set to True by default, so the hostname validation will be done
by Python and not the driver. For this reason, we need to set the server_hostname at best effort, which is the
resolved ip address. If this validation needs to be done against the FQDN, consider enabling it using the ssl_options
as described in the following examples or implement your own :class:`~.connection.EndPoint` and
:class:`~.connection.EndPointFactory`.


The following examples assume you have generated your Scylla certificate and
keystore files with these instructions:

* `Scylla TLS/SSL Guide <https://opensource.docs.scylladb.com/stable/operating-scylla/security/client-node-encryption.html>`_

SSL with Twisted or Eventlet
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Twisted and Eventlet both use an alternative SSL implementation called pyOpenSSL, so if your `Cluster`'s connection class is
:class:`~cassandra.io.twistedreactor.TwistedConnection` or :class:`~cassandra.io.eventletreactor.EventletConnection`, you must pass a
`pyOpenSSL context <https://www.pyopenssl.org/en/stable/api/ssl.html#context-objects>`_ instead.
An example is provided in these docs, and more details can be found in the
`documentation <https://www.pyopenssl.org/en/stable/api/ssl.html#context-objects>`_.
pyOpenSSL is not installed by the driver and must be installed separately.

SSL Configuration Examples
^^^^^^^^^^^^^^^^^^^^^^^^^^
Here, we'll describe the server and driver configuration necessary to set up SSL to meet various goals, such as the client verifying the server and the server verifying the client. We'll also include Python code demonstrating how to use servers and drivers configured in these ways.

.. _ssl-no-identify-verification:

No identity verification
++++++++++++++++++++++++

No identity verification at all. Note that this is not recommended for for production deployments.

The Cassandra configuration::

    client_encryption_options:
      enabled: true
      keystore: /path/to/127.0.0.1.keystore
      keystore_password: myStorePass
      require_client_auth: false

The driver configuration:

.. code-block:: python

    from cassandra.cluster import Cluster, Session
    from ssl import SSLContext, PROTOCOL_TLS

    ssl_context = SSLContext(PROTOCOL_TLS)

    cluster = Cluster(['127.0.0.1'], ssl_context=ssl_context)
    session = cluster.connect()

.. _ssl-client-verifies-server:

Client verifies server
++++++++++++++++++++++

Ensure the python driver verifies the identity of the server.

The Cassandra configuration::

    client_encryption_options:
      enabled: true
      keystore: /path/to/127.0.0.1.keystore
      keystore_password: myStorePass
      require_client_auth: false

For the driver configuration, it's very important to set `ssl_context.verify_mode`
to `CERT_REQUIRED`. Otherwise, the loaded verify certificate will have no effect:

.. code-block:: python

    from cassandra.cluster import Cluster, Session
    from ssl import SSLContext, PROTOCOL_TLS, CERT_REQUIRED

    ssl_context = SSLContext(PROTOCOL_TLS)
    ssl_context.load_verify_locations('/path/to/rootca.crt')
    ssl_context.verify_mode = CERT_REQUIRED

    cluster = Cluster(['127.0.0.1'], ssl_context=ssl_context)
    session = cluster.connect()

Additionally, you can also force the driver to verify the `hostname` of the server by passing additional options to `ssl_context.wrap_socket` via the `ssl_options` kwarg:

.. code-block:: python

    from cassandra.cluster import Cluster, Session
    from ssl import SSLContext, PROTOCOL_TLS, CERT_REQUIRED

    ssl_context = SSLContext(PROTOCOL_TLS)
    ssl_context.load_verify_locations('/path/to/rootca.crt')
    ssl_context.verify_mode = CERT_REQUIRED
    ssl_context.check_hostname = True
    ssl_options = {'server_hostname': '127.0.0.1'}

    cluster = Cluster(['127.0.0.1'], ssl_context=ssl_context, ssl_options=ssl_options)
    session = cluster.connect()

.. _ssl-server-verifies-client:

Server verifies client
++++++++++++++++++++++

If Cassandra is configured to verify clients (``require_client_auth``), you need to generate
SSL key and certificate files.

The cassandra configuration::

    client_encryption_options:
      enabled: true
      keystore: /path/to/127.0.0.1.keystore
      keystore_password: myStorePass
      require_client_auth: true
      truststore: /path/to/truststore.jks
      truststore_password: myStorePass

The Python ``ssl`` APIs require the certificate in PEM format. First, create a certificate
conf file:

.. code-block:: bash

    cat > gen_client_cert.conf <<EOF
    [ req ]
    distinguished_name = req_distinguished_name
    prompt = no
    output_password = ${ROOT_CERT_PASS}
    default_bits = 2048

    [ req_distinguished_name ]
    C = ${CERT_COUNTRY}
    O = ${CERT_ORG_NAME}
    OU = ${CERT_OU}
    CN = client
    EOF

Make sure you replaced the variables with the same values you used for the initial
root CA certificate. Then, generate the key:

.. code-block:: bash

    openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -config gen_client_cert.conf

And generate the client signed certificate:

.. code-block:: bash

    openssl x509 -req -CA ${ROOT_CA_BASE_NAME}.crt -CAkey ${ROOT_CA_BASE_NAME}.key -passin pass:${ROOT_CERT_PASS} \
        -in client.csr -out client.crt_signed -days ${CERT_VALIDITY} -CAcreateserial

Finally, you can use that configuration with the following driver code:

.. code-block:: python

    from cassandra.cluster import Cluster, Session
    from ssl import SSLContext, PROTOCOL_TLS

    ssl_context = SSLContext(PROTOCOL_TLS)
    ssl_context.load_cert_chain(
        certfile='/path/to/client.crt_signed',
        keyfile='/path/to/client.key')

    cluster = Cluster(['127.0.0.1'], ssl_context=ssl_context)
    session = cluster.connect()

.. _ssl-server-client-verification:

Server verifies client and client verifies server
+++++++++++++++++++++++++++++++++++++++++++++++++

See the previous section for examples of Cassandra configuration and preparing
the client certificates.

The following driver code specifies that the connection should use two-way verification:

.. code-block:: python

    from cassandra.cluster import Cluster, Session
    from ssl import SSLContext, PROTOCOL_TLS, CERT_REQUIRED

    ssl_context = SSLContext(PROTOCOL_TLS)
    ssl_context.load_verify_locations('/path/to/rootca.crt')
    ssl_context.verify_mode = CERT_REQUIRED
    ssl_context.load_cert_chain(
        certfile='/path/to/client.crt_signed',
        keyfile='/path/to/client.key')

    cluster = Cluster(['127.0.0.1'], ssl_context=ssl_context)
    session = cluster.connect()


The driver uses ``SSLContext`` directly to give you many other options in configuring SSL. Consider reading the `Python SSL documentation <https://docs.python.org/library/ssl.html#ssl.SSLContext>`__
for more details about ``SSLContext`` configuration.

**Server verifies client and client verifies server using Twisted and pyOpenSSL**

.. code-block:: python

    from OpenSSL import SSL, crypto
    from cassandra.cluster import Cluster
    from cassandra.io.twistedreactor import TwistedConnection

    ssl_context = SSL.Context(SSL.TLSv1_2_METHOD)
    ssl_context.set_verify(SSL.VERIFY_PEER, callback=lambda _1, _2, _3, _4, ok: ok)
    ssl_context.use_certificate_file('/path/to/client.crt_signed')
    ssl_context.use_privatekey_file('/path/to/client.key')
    ssl_context.load_verify_locations('/path/to/rootca.crt')

    cluster = Cluster(
        contact_points=['127.0.0.1'],
        connection_class=TwistedConnection,
        ssl_context=ssl_context,
        ssl_options={'check_hostname': True}
    )
    session = cluster.connect()


Connecting using Eventlet would look similar except instead of importing and using ``TwistedConnection``, you would
import and use ``EventletConnection``, including the appropriate monkey-patching.

Versions 3.16.0 and lower
^^^^^^^^^^^^^^^^^^^^^^^^^

To enable SSL you will need to set :attr:`.Cluster.ssl_options` to a
dict of options.  These will be passed as kwargs to ``ssl.wrap_socket()``
when new sockets are created. Note that this use of ssl_options will be
deprecated in the next major release.

By default, a ``ca_certs`` value should be supplied (the value should be
a string pointing to the location of the CA certs file), and you probably
want to specify ``ssl_version`` as ``ssl.PROTOCOL_TLS`` to match
Cassandra's default protocol.

For example:

.. code-block:: python

    from cassandra.cluster import Cluster
    from ssl import PROTOCOL_TLS, CERT_REQUIRED

    ssl_opts = {
        'ca_certs': '/path/to/my/ca.certs',
        'ssl_version': PROTOCOL_TLS,
        'cert_reqs': CERT_REQUIRED  # Certificates are required and validated
    }
    cluster = Cluster(ssl_options=ssl_opts)

This is only an example to show how to pass the ssl parameters. Consider reading
the `python ssl documentation <https://docs.python.org/3/library/ssl.html#ssl.wrap_socket>`__ for
your configuration.

SSL with Twisted
++++++++++++++++

In case the twisted event loop is used pyOpenSSL must be installed or an exception will be risen. Also
to set the ``ssl_version`` and ``cert_reqs`` in ``ssl_opts`` the appropriate constants from pyOpenSSL are expected.

TLS Session Resumption
----------------------

.. versionadded:: 3.30.0

The driver automatically caches TLS sessions to enable session resumption for faster reconnections.
When a TLS connection is established, the session is cached and can be reused for subsequent
connections to the same endpoint, reducing handshake latency and CPU usage.

**TLS Version Support**: Session resumption works with both TLS 1.2 and TLS 1.3. TLS 1.2 uses
Session IDs and optionally Session Tickets (RFC 5077), while TLS 1.3 uses Session Tickets (RFC 8446)
as the primary mechanism. Python's ``ssl.SSLSession`` API handles both versions transparently.

Session caching is **enabled by default** when SSL/TLS is configured and applies to the following
connection classes:

* :class:`~cassandra.io.asyncorereactor.AsyncoreConnection` (default)
* :class:`~cassandra.io.libevreactor.LibevConnection`
* :class:`~cassandra.io.asyncioreactor.AsyncioConnection`
* :class:`~cassandra.io.geventreactor.GeventConnection` (when not using SSL)

.. note::
    Session caching is not currently supported for PyOpenSSL-based reactors
    (:class:`~cassandra.io.twistedreactor.TwistedConnection`,
    :class:`~cassandra.io.eventletreactor.EventletConnection`) but may be added in a future release.

Configuration
^^^^^^^^^^^^^

TLS session caching is controlled by three cluster-level parameters:

* :attr:`~.Cluster.tls_session_cache_enabled` - Enable or disable session caching (default: ``True``)
* :attr:`~.Cluster.tls_session_cache_size` - Maximum number of sessions to cache (default: ``100``)
* :attr:`~.Cluster.tls_session_cache_ttl` - Time-to-live for cached sessions in seconds (default: ``3600``)

Example with default settings (session caching enabled):

.. code-block:: python

    from cassandra.cluster import Cluster
    import ssl

    ssl_context = ssl.create_default_context(cafile='/path/to/ca.crt')
    cluster = Cluster(
        contact_points=['127.0.0.1'],
        ssl_context=ssl_context
    )
    session = cluster.connect()

Example with custom cache settings:

.. code-block:: python

    from cassandra.cluster import Cluster
    import ssl

    ssl_context = ssl.create_default_context(cafile='/path/to/ca.crt')
    cluster = Cluster(
        contact_points=['127.0.0.1'],
        ssl_context=ssl_context,
        tls_session_cache_size=200,  # Cache up to 200 sessions
        tls_session_cache_ttl=7200   # Sessions expire after 2 hours
    )
    session = cluster.connect()

Example with session caching disabled:

.. code-block:: python

    from cassandra.cluster import Cluster
    import ssl

    ssl_context = ssl.create_default_context(cafile='/path/to/ca.crt')
    cluster = Cluster(
        contact_points=['127.0.0.1'],
        ssl_context=ssl_context,
        tls_session_cache_enabled=False
    )
    session = cluster.connect()

How It Works
^^^^^^^^^^^^

When session caching is enabled:

1. The first connection to an endpoint establishes a new TLS session and caches it
2. Subsequent connections to the same endpoint reuse the cached session
3. Sessions are cached per endpoint (host:port combination)
4. Sessions expire after the configured TTL
5. When the cache reaches max size, the least recently used session is evicted

Performance Benefits
^^^^^^^^^^^^^^^^^^^^

TLS session resumption is a standard TLS feature that provides performance benefits:

* **Faster reconnection times** - Reduced handshake latency by reusing cached sessions
* **Lower CPU usage** - Fewer cryptographic operations during reconnection
* **Better overall throughput** - Especially beneficial for workloads with frequent reconnections

The actual performance improvement depends on various factors including network latency,
server configuration, and workload characteristics.

Security Considerations
^^^^^^^^^^^^^^^^^^^^^^^

* Sessions are stored in memory only and never persisted to disk
* Sessions are cached per cluster and not shared across different cluster instances
* Sessions for one endpoint are never used for a different endpoint
* Hostname verification still occurs on each connection, even when reusing sessions
* Sessions automatically expire after the configured TTL
