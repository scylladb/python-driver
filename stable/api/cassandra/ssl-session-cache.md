# cassandra.ssl_session_cache

### *class* cassandra.ssl_session_cache.SSLSessionCache(max_size=1024)

A thread-safe, bounded cache of TLS sessions, keyed by TLS peer identity.

TLS clients can skip the expensive part of a handshake by replaying a
session established earlier with the same peer (RFC 5077 session tickets
for TLS 1.2, RFC 8446 pre-shared keys for TLS 1.3).  OpenSSL never does
this on its own – the client has to hold on to the session and offer it
on the next connection – so the driver keeps one of these caches per
[`Cluster`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.Cluster) and reuses sessions across every connection it opens,
most importantly the burst of per-shard connections opened to a node at
once.

A cached session is not consumed by being used: the same session can be
replayed by any number of concurrent connections, and each successful
handshake stores back whatever the peer handed over – a fresh session
where one was issued, otherwise the same one again, which keeps the
deadline it already had rather than starting a new one.  An entry whose
lifetime has run out is never handed out again, and is dropped when it
is looked up or when room is needed; entries otherwise go only by being
replaced or, once the cache is full, by having been used least
recently.  A session the server declines for any other reason simply
results in a full handshake, which is what would have happened anyway.

Instances are safe to use from multiple threads, and may be shared
between clusters – which is what makes sessions outlive the cluster that
established them, so that a cluster replacing an earlier one resumes
instead of handshaking in full.  A cache the driver created for a cluster
lives and dies with it; one supplied to [`Cluster`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cluster.md#cassandra.cluster.Cluster) belongs to
whoever supplied it, and the driver removes an entry from it only to
replace it, because its lifetime ran out, or to make room.  Note that an
entry keeps the `SSLContext` its session was established with alive –
CPython’s `SSLSession` holds a reference to it – so a long-lived cache
holds the contexts of at most [`max_size`](#cassandra.ssl_session_cache.SSLSessionCache.max_size) peers.  [`clear()`](#cassandra.ssl_session_cache.SSLSessionCache.clear) drops
everything, for a caller that wants them gone sooner.

* **Parameters:**
  **max_size** – maximum number of peers to keep sessions for.  When
  exceeded, the least recently used entry is evicted.

#### *property* max_size

The maximum number of peers this cache keeps sessions for.

#### get(key)

Return the cached session for *key*, or `None` if there is none
or its lifetime has run out.  A session that is still live stays in the
cache; an expired one is dropped.

#### set(key, session, lifetime=None, offered=None)

Store *session* as the session to offer for *key*, replacing any
previous one.  A `None` session is ignored.

A session the peer handed back unchanged keeps the deadline the entry
already had, rather than starting a new one: its lifetime runs from
when the peer issued it and not from when it was last replayed, so
re-stamping a full lifetime on every reuse would let one ticket be
offered for as long as connections keep being opened.  Resuming below
TLS 1.3 is exactly that case – an abbreviated handshake hands back the
session that was offered, same id and same ticket – while TLS 1.3
normally issues a fresh one, which starts its own lifetime.  Comparing
the two here is what makes the rule hold: what is cached, what the
caller offered and what is replacing them are all read under the one
lock that also stores the result, so a connection storing concurrently
cannot land in between.

* **Parameters:**
  * **lifetime** – how much longer, in seconds, the session may be
    offered.  Once it has passed, the entry is dropped rather than
    returned.  `None` means no limit, which callers should
    reserve for sessions that carry no lifetime of their own.
  * **offered** – the session the caller offered on the handshake it is
    storing the result of, if any.  Storing that same session back when
    the entry no longer holds it is not a new session arriving, and is
    skipped: see below.

#### discard(key, session=None)

Drop the session cached for *key*, if any.

Give *session* to drop it only while that is still the cached one.  A
caller acting on a session it read earlier needs this: by the time it
decides to drop it, another connection may have stored a session the
peer issued in its place, and that one is not the caller’s to remove.

The comparison is by identity, which [`set()`](#cassandra.ssl_session_cache.SSLSessionCache.set) is what makes
dependable: a store of the session already cached keeps the object
that is there, so an entry changes identity only when it changes
credential.

#### clear()

Drop all cached sessions.
