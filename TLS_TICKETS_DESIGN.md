# TLS Tickets Support Design Document

## Overview

This document describes the design and implementation of TLS session ticket support in the Scylla Python driver. TLS session tickets allow for quick TLS renegotiation by resuming previous TLS sessions, reducing the overhead of full handshakes when reconnecting to servers.

## Background

### What are TLS Session Tickets?

TLS session tickets (RFC 5077 and RFC 8446 for TLS 1.3) allow clients to cache session state and reuse it for subsequent connections. This provides:

- **Faster reconnections**: Reduced handshake latency by resuming previous sessions
- **Less CPU usage**: Fewer cryptographic operations during reconnection
- **Better performance**: Especially important for connection pools that frequently reconnect

### Python SSL Support

Python's `ssl` module provides built-in support for TLS session resumption:

- `SSLContext.num_tickets`: Controls the number of TLS 1.3 session tickets (default: 2)
- `SSLSocket.session`: Returns the current session as an `SSLSession` object
- `SSLSocket.session_reused`: Boolean indicating if the session was reused
- `SSLContext.wrap_socket(..., session=...)`: Allows passing a session to reuse

## Current State

The driver currently:
1. Uses `SSLContext` for TLS connections
2. Creates new TLS sessions for each connection
3. Does NOT cache or reuse TLS sessions
4. Does NOT track session statistics

## Design

### Goals

1. **Enable TLS tickets by default** when SSL/TLS is enabled
2. **Implement client-side session cache** to store and reuse sessions
3. **Minimal API changes** - work transparently with existing SSL configuration
4. **Thread-safe** session cache for concurrent connections
5. **Per-endpoint session tracking** to reuse sessions for the same server

### Components

#### 1. TLS Session Cache

A thread-safe cache that stores TLS sessions per endpoint (host:port).

```python
class TLSSessionCache:
    """Thread-safe cache for TLS sessions."""
    
    def __init__(self, max_size=100, ttl=3600):
        """
        Args:
            max_size: Maximum number of sessions to cache
            ttl: Time-to-live for cached sessions in seconds
        """
        self._sessions = {}  # {endpoint_key: (session, timestamp)}
        self._lock = threading.RLock()
        self._max_size = max_size
        self._ttl = ttl
    
    def get_session(self, endpoint_key):
        """Get cached session for endpoint, if valid."""
        pass
    
    def set_session(self, endpoint_key, session):
        """Store session for endpoint."""
        pass
    
    def clear_expired(self):
        """Remove expired sessions."""
        pass
```

#### 2. Integration Points

##### Cluster Level
- Add `tls_session_cache` attribute to `Cluster` class
- Initialize cache when `ssl_context` or `ssl_options` is provided
- Share cache across all connections in the cluster

##### Connection Level
- Modify `Connection._wrap_socket_from_context()` to:
  1. Check cache for existing session for the endpoint
  2. Pass cached session to `wrap_socket()` if available
  3. Store new session after successful connection
- Track session reuse statistics

### Implementation Details

#### Session Cache Key

Use a tuple of `(host, port)` as the cache key to uniquely identify endpoints.

#### Session Expiration

- Default TTL: 1 hour (3600 seconds)
- Sessions older than TTL are not reused
- Periodic cleanup of expired sessions

#### Cache Size Management

- Default max size: 100 sessions
- When cache is full, remove oldest sessions (LRU policy)

#### Statistics Tracking

Add connection-level attributes:
- `session_reused`: Boolean indicating if current connection reused a session
- Existing `SSLContext.session_stats()` can be queried for overall statistics

### Configuration

#### Cluster Configuration

Users can configure TLS session caching via new parameters:

```python
cluster = Cluster(
    contact_points=['127.0.0.1'],
    ssl_context=ssl_context,
    tls_session_cache_enabled=True,  # Default: True
    tls_session_cache_size=100,       # Default: 100
    tls_session_cache_ttl=3600        # Default: 3600 seconds
)
```

For backward compatibility, TLS session caching is **enabled by default** when SSL is configured.

#### Disabling Session Cache

Users can disable session caching by setting:
```python
cluster = Cluster(
    ...,
    tls_session_cache_enabled=False
)
```

## Implementation Plan

### Phase 1: Core Implementation

1. **Create TLSSessionCache class** in `cassandra/connection.py`
   - Thread-safe dictionary-based cache
   - TTL and max_size management
   - LRU eviction policy

2. **Modify Cluster class** in `cassandra/cluster.py`
   - Add configuration parameters
   - Initialize session cache when SSL is enabled
   - Pass cache to connections

3. **Modify Connection class** in `cassandra/connection.py`
   - Accept session cache in constructor
   - Implement session retrieval and storage
   - Update `_wrap_socket_from_context()` to use cached sessions

### Phase 2: Testing

1. **Unit tests**
   - Test TLSSessionCache operations
   - Test cache expiration and eviction
   - Test thread safety

2. **Integration tests**
   - Test session reuse across connections
   - Test with real SSL/TLS connections
   - Verify performance improvements

### Phase 3: Documentation

1. Update API documentation
2. Add usage examples
3. Document configuration options

## Testing Strategy

### Unit Tests

1. **TLSSessionCache Tests**
   - Test get/set operations
   - Test TTL expiration
   - Test max size and LRU eviction
   - Test thread safety

2. **Connection Tests**
   - Mock SSLContext and SSLSocket
   - Verify session is retrieved from cache
   - Verify session is stored after connection
   - Test with cache disabled

### Integration Tests

1. **SSL Connection Tests** (extend existing `tests/integration/long/test_ssl.py`)
   - Connect to SSL-enabled cluster
   - Verify first connection creates new session
   - Verify second connection reuses session
   - Check `session_reused` attribute
   - Verify session stats from `SSLContext.session_stats()`

2. **Performance Tests**
   - Measure connection time with/without session reuse
   - Verify reduced handshake latency

### Test with Different SSL Configurations

- Test with `ssl_context` directly provided
- Test with `ssl_options` (legacy mode)
- Test with cloud config
- Test with twisted/eventlet reactors

## Security Considerations

1. **Session Security**: TLS sessions contain sensitive cryptographic material
   - Sessions are stored in memory only (not persisted)
   - Sessions expire after TTL
   - Sessions are not shared across different clusters

2. **Host Validation**: Sessions are cached per endpoint
   - Sessions for host A are not used for host B
   - Hostname verification still occurs on each connection

3. **Backward Compatibility**: 
   - Feature is enabled by default but transparent
   - No breaking changes to existing API
   - Can be disabled if needed

## Performance Impact

### Expected Benefits

- **Reduced connection time**: 20-50% faster reconnections
- **Lower CPU usage**: Fewer cryptographic operations
- **Better throughput**: Especially for workloads with frequent reconnections

### Overhead

- **Memory**: Minimal (~1KB per cached session)
- **Cache management**: O(1) operations with occasional O(n) cleanup

## Alternatives Considered

### 1. Global Session Cache

**Rejected**: Would share sessions across different clusters, which could be confusing and less secure.

### 2. No TTL/Expiration

**Rejected**: Sessions could become stale or accumulate indefinitely.

### 3. Disable by Default

**Rejected**: Session resumption is a standard TLS feature and should be enabled by default for better performance.

## Future Enhancements

1. **Configurable eviction policies**: LRU, LFU, FIFO
2. **Session statistics**: Track cache hit/miss rates
3. **Metrics integration**: Export session reuse metrics
4. **Session serialization**: Persist sessions across driver restarts (optional)

## References

- [RFC 5077 - TLS Session Resumption without Server-Side State](https://tools.ietf.org/html/rfc5077)
- [RFC 8446 - The Transport Layer Security (TLS) Protocol Version 1.3](https://tools.ietf.org/html/rfc8446)
- [Python ssl module documentation](https://docs.python.org/3/library/ssl.html)
