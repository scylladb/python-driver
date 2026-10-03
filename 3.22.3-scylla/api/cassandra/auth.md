# `cassandra.auth` - Authentication

### *class* cassandra.auth.AuthProvider

An abstract class that defines the interface that will be used for
creating [`Authenticator`](#cassandra.auth.Authenticator) instances when opening new
connections to Cassandra.

#### Versionadded
Added in version 2.0.0.

#### new_authenticator(host)

Implementations of this class should return a new instance
of [`Authenticator`](#cassandra.auth.Authenticator) or one of its subclasses.

### *class* cassandra.auth.Authenticator

An abstract class that handles SASL authentication with Cassandra servers.

Each time a new connection is created and the server requires authentication,
a new instance of this class will be created by the corresponding
[`AuthProvider`](#cassandra.auth.AuthProvider) to handler that authentication. The lifecycle of the
new [`Authenticator`](#cassandra.auth.Authenticator) will the be:

1) The [`initial_response()`](#cassandra.auth.Authenticator.initial_response) method will be called. The return
value will be sent to the server to initiate the handshake.

2) The server will respond to each client response by either issuing a
challenge or indicating that the authentication is complete (successful or not).
If a new challenge is issued, [`evaluate_challenge()`](#cassandra.auth.Authenticator.evaluate_challenge)
will be called to produce a response that will be sent to the
server. This challenge/response negotiation will continue until the server
responds that authentication is successful (or an [`AuthenticationFailed`](../cassandra.md#cassandra.AuthenticationFailed)
is raised).

3) When the server indicates that authentication is successful,
[`on_authentication_success()`](#cassandra.auth.Authenticator.on_authentication_success) will be called a token string that
that the server may optionally have sent.

The exact nature of the negotiation between the client and server is specific
to the authentication mechanism configured server-side.

#### Versionadded
Added in version 2.0.0.

#### server_authenticator_class *= None*

Set during the connection AUTHENTICATE phase

#### initial_response()

Returns an message to send to the server to initiate the SASL handshake.
`None` may be returned to send an empty message.

#### evaluate_challenge(challenge)

Called when the server sends a challenge message.  Generally, this method
should return `None` when authentication is complete from a
client perspective.  Otherwise, a string should be returned.

#### on_authentication_success(token)

Called when the server indicates that authentication was successful.
Depending on the authentication mechanism, token may be `None`
or a string.

### *class* cassandra.auth.PlainTextAuthProvider(username, password)

An [`AuthProvider`](#cassandra.auth.AuthProvider) that works with Cassandra’s PasswordAuthenticator.

Example usage:

```default
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(
        username='cassandra', password='cassandra')
cluster = Cluster(auth_provider=auth_provider)
```

#### Versionadded
Added in version 2.0.0.

#### new_authenticator(host)

Implementations of this class should return a new instance
of [`Authenticator`](#cassandra.auth.Authenticator) or one of its subclasses.

### *class* cassandra.auth.PlainTextAuthenticator(username, password)

#### evaluate_challenge(challenge)

Called when the server sends a challenge message.  Generally, this method
should return `None` when authentication is complete from a
client perspective.  Otherwise, a string should be returned.

### *class* cassandra.auth.SaslAuthProvider(\*\*sasl_kwargs)

An [`AuthProvider`](#cassandra.auth.AuthProvider) supporting general SASL auth mechanisms

Suitable for GSSAPI or other SASL mechanisms

Example usage:

```default
from cassandra.cluster import Cluster
from cassandra.auth import SaslAuthProvider

sasl_kwargs = {'service': 'something',
               'mechanism': 'GSSAPI',
               'qops': 'auth'.split(',')}
auth_provider = SaslAuthProvider(**sasl_kwargs)
cluster = Cluster(auth_provider=auth_provider)
```

#### Versionadded
Added in version 2.1.4.

#### new_authenticator(host)

Implementations of this class should return a new instance
of [`Authenticator`](#cassandra.auth.Authenticator) or one of its subclasses.

### *class* cassandra.auth.SaslAuthenticator(host, service, mechanism='GSSAPI', \*\*sasl_kwargs)

A pass-through [`Authenticator`](#cassandra.auth.Authenticator) using the third party package
‘pure-sasl’ for authentication

#### Versionadded
Added in version 2.1.4.

#### initial_response()

Returns an message to send to the server to initiate the SASL handshake.
`None` may be returned to send an empty message.

#### evaluate_challenge(challenge)

Called when the server sends a challenge message.  Generally, this method
should return `None` when authentication is complete from a
client perspective.  Otherwise, a string should be returned.
