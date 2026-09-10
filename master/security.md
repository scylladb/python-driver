<a id="security"></a>

# Security

The two main security components you will use with the
Python driver are Authentication and SSL.

## Authentication

Versions 2.0 and higher of the driver support a SASL-based
authentication mechanism when [`protocol_version`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster.protocol_version)
is set to 2 or higher.  To use this authentication, set
[`auth_provider`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster.auth_provider) to an instance of a subclass
of [`AuthProvider`](https://python-driver.docs.scylladb.com/master/api/cassandra/auth.md#cassandra.auth.AuthProvider).  When working
with Cassandra’s `PasswordAuthenticator`, you can use
the [`PlainTextAuthProvider`](https://python-driver.docs.scylladb.com/master/api/cassandra/auth.md#cassandra.auth.PlainTextAuthProvider) class.

For example, suppose Cassandra is setup with its default
‘cassandra’ user with a password of ‘cassandra’:

```python
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(auth_provider=auth_provider, protocol_version=2)
```

### Custom Authenticators

If you’re using something other than Cassandra’s `PasswordAuthenticator`,
[`SaslAuthProvider`](https://python-driver.docs.scylladb.com/master/api/cassandra/auth.md#cassandra.auth.SaslAuthProvider) is provided for generic SASL authentication mechanisms,
utilizing the `pure-sasl` package.
If these do not suit your needs, you may need to create your own subclasses of
[`AuthProvider`](https://python-driver.docs.scylladb.com/master/api/cassandra/auth.md#cassandra.auth.AuthProvider) and [`Authenticator`](https://python-driver.docs.scylladb.com/master/api/cassandra/auth.md#cassandra.auth.Authenticator).  You can use the Sasl classes
as example implementations.

## SSL

SSL should be used when client encryption is enabled in Cassandra.

To give you as much control as possible over your SSL configuration, our SSL
API takes a user-created SSLContext instance from the Python standard library.
These docs will include some examples for how to achieve common configurations,
but the [ssl.SSLContext](https://docs.python.org/3/library/ssl.html#ssl.SSLContext) documentation
gives a more complete description of what is possible.

To enable SSL with version 3.17.0 and higher, you will need to set [`Cluster.ssl_context`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster.ssl_context) to a
`ssl.SSLContext` instance to enable SSL. Optionally, you can also set [`Cluster.ssl_options`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster.ssl_options)
to a dict of options. These will be passed as kwargs to `ssl.SSLContext.wrap_socket()`
when new sockets are created.

If you create your SSLContext using [ssl.create_default_context](https://docs.python.org/3/library/ssl.html#ssl.create_default_context),
be aware that SSLContext.check_hostname is set to True by default, so the hostname validation will be done
by Python and not the driver. For this reason, we need to set the server_hostname at best effort, which is the
resolved ip address. If this validation needs to be done against the FQDN, consider enabling it using the ssl_options
as described in the following examples or implement your own [`EndPoint`](https://python-driver.docs.scylladb.com/master/api/cassandra/connection.md#cassandra.connection.EndPoint) and
[`EndPointFactory`](https://python-driver.docs.scylladb.com/master/api/cassandra/connection.md#cassandra.connection.EndPointFactory).

The following examples assume you have generated your Scylla certificate and
keystore files with these instructions:

* [Scylla TLS/SSL Guide](https://opensource.docs.scylladb.com/stable/operating-scylla/security/client-node-encryption.html)

### SSL Configuration Examples

Here, we’ll describe the server and driver configuration necessary to set up SSL to meet various goals, such as the client verifying the server and the server verifying the client. We’ll also include Python code demonstrating how to use servers and drivers configured in these ways.

<a id="ssl-no-identify-verification"></a>

#### No identity verification

No identity verification at all. Note that this is not recommended for for production deployments.

The Cassandra configuration:

```default
client_encryption_options:
  enabled: true
  keystore: /path/to/127.0.0.1.keystore
  keystore_password: myStorePass
  require_client_auth: false
```

The driver configuration:

```python
from cassandra.cluster import Cluster, Session
from ssl import SSLContext, PROTOCOL_TLS

ssl_context = SSLContext(PROTOCOL_TLS)

cluster = Cluster(['127.0.0.1'], ssl_context=ssl_context)
session = cluster.connect()
```

<a id="ssl-client-verifies-server"></a>

#### Client verifies server

Ensure the python driver verifies the identity of the server.

The Cassandra configuration:

```default
client_encryption_options:
  enabled: true
  keystore: /path/to/127.0.0.1.keystore
  keystore_password: myStorePass
  require_client_auth: false
```

For the driver configuration, it’s very important to set ssl_context.verify_mode
to CERT_REQUIRED. Otherwise, the loaded verify certificate will have no effect:

```python
from cassandra.cluster import Cluster, Session
from ssl import SSLContext, PROTOCOL_TLS, CERT_REQUIRED

ssl_context = SSLContext(PROTOCOL_TLS)
ssl_context.load_verify_locations('/path/to/rootca.crt')
ssl_context.verify_mode = CERT_REQUIRED

cluster = Cluster(['127.0.0.1'], ssl_context=ssl_context)
session = cluster.connect()
```

Additionally, you can also force the driver to verify the hostname of the server by passing additional options to ssl_context.wrap_socket via the ssl_options kwarg:

```python
from cassandra.cluster import Cluster, Session
from ssl import SSLContext, PROTOCOL_TLS, CERT_REQUIRED

ssl_context = SSLContext(PROTOCOL_TLS)
ssl_context.load_verify_locations('/path/to/rootca.crt')
ssl_context.verify_mode = CERT_REQUIRED
ssl_context.check_hostname = True
ssl_options = {'server_hostname': '127.0.0.1'}

cluster = Cluster(['127.0.0.1'], ssl_context=ssl_context, ssl_options=ssl_options)
session = cluster.connect()
```

<a id="ssl-server-verifies-client"></a>

#### Server verifies client

If Cassandra is configured to verify clients (`require_client_auth`), you need to generate
SSL key and certificate files.

The cassandra configuration:

```default
client_encryption_options:
  enabled: true
  keystore: /path/to/127.0.0.1.keystore
  keystore_password: myStorePass
  require_client_auth: true
  truststore: /path/to/truststore.jks
  truststore_password: myStorePass
```

The Python `ssl` APIs require the certificate in PEM format. First, create a certificate
conf file:

```bash
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
```

Make sure you replaced the variables with the same values you used for the initial
root CA certificate. Then, generate the key:

```bash
openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -config gen_client_cert.conf
```

And generate the client signed certificate:

```bash
openssl x509 -req -CA ${ROOT_CA_BASE_NAME}.crt -CAkey ${ROOT_CA_BASE_NAME}.key -passin pass:${ROOT_CERT_PASS} \
    -in client.csr -out client.crt_signed -days ${CERT_VALIDITY} -CAcreateserial
```

Finally, you can use that configuration with the following driver code:

```python
from cassandra.cluster import Cluster, Session
from ssl import SSLContext, PROTOCOL_TLS

ssl_context = SSLContext(PROTOCOL_TLS)
ssl_context.load_cert_chain(
    certfile='/path/to/client.crt_signed',
    keyfile='/path/to/client.key')

cluster = Cluster(['127.0.0.1'], ssl_context=ssl_context)
session = cluster.connect()
```

<a id="ssl-server-client-verification"></a>

#### Server verifies client and client verifies server

See the previous section for examples of Cassandra configuration and preparing
the client certificates.

The following driver code specifies that the connection should use two-way verification:

```python
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
```

The driver uses `SSLContext` directly to give you many other options in configuring SSL. Consider reading the [Python SSL documentation](https://docs.python.org/library/ssl.html#ssl.SSLContext)
for more details about `SSLContext` configuration.

### Versions 3.16.0 and lower

To enable SSL you will need to set [`Cluster.ssl_options`](https://python-driver.docs.scylladb.com/master/api/cassandra/cluster.md#cassandra.cluster.Cluster.ssl_options) to a
dict of options.  These will be passed as kwargs to `ssl.wrap_socket()`
when new sockets are created. Note that this use of ssl_options will be
deprecated in the next major release.

By default, a `ca_certs` value should be supplied (the value should be
a string pointing to the location of the CA certs file), and you probably
want to specify `ssl_version` as `ssl.PROTOCOL_TLS` to match
Cassandra’s default protocol.

For example:

```python
from cassandra.cluster import Cluster
from ssl import PROTOCOL_TLS, CERT_REQUIRED

ssl_opts = {
    'ca_certs': '/path/to/my/ca.certs',
    'ssl_version': PROTOCOL_TLS,
    'cert_reqs': CERT_REQUIRED  # Certificates are required and validated
}
cluster = Cluster(ssl_options=ssl_opts)
```

This is only an example to show how to pass the ssl parameters. Consider reading
the [python ssl documentation](https://docs.python.org/3/library/ssl.html#ssl.wrap_socket) for
your configuration.
