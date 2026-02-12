# SSL Connection Error Handling Tests

This directory contains comprehensive tests for SSL connection error handling, specifically addressing the "Bad file descriptor" errors that occur when nodes reboot while using client encryption.

## Background

### Issue
When using client encryption and nodes reboot, the driver reported errors like:
```
cassandra.connection.ConnectionShutdown: [Errno 9] Bad file descriptor
```

The issue occurred because:
1. A connection is forcefully closed (e.g., due to node reboot)
2. Parallel operations attempt to read/write to the closed socket
3. This results in "Bad file descriptor" errors
4. The original cause of the connection closure could be lost

See: https://github.com/scylladb/python-driver/issues/614

### Solution
The driver already has proper error handling via the `last_error` mechanism in the `Connection` class. When a connection becomes defunct, the error that caused it is stored in `last_error` and included in subsequent `ConnectionShutdown` exception messages.

This test suite verifies that this mechanism works correctly across all SSL error scenarios.

## Test Files

### Unit Tests (`test_ssl_connection_errors.py`)
14 unit tests that mock SSL connection failures:

1. **Basic SSL Socket Errors**
   - `test_ssl_socket_bad_file_descriptor_on_send`: Simulates EBADF during send
   - `test_ssl_socket_bad_file_descriptor_on_recv`: Simulates EBADF during recv
   - `test_ssl_socket_broken_pipe_error`: Simulates EPIPE (broken pipe)
   - `test_ssl_socket_connection_aborted_error`: Simulates ECONNABORTED
   - `test_ssl_socket_errno_enotconn`: Simulates ENOTCONN

2. **SSL-Specific Errors**
   - `test_ssl_connection_error_during_handshake`: SSL handshake failures
   - `test_ssl_unwrap_error_on_close`: SSL unwrap errors during close

3. **Race Condition Tests**
   - `test_concurrent_operations_on_closed_ssl_socket`: Multiple threads using closed socket
   - `test_parallel_send_on_defuncting_connection`: Thread trying to send while connection defuncts
   - `test_node_reboot_scenario`: Complete node reboot simulation

4. **Error Preservation Tests**
   - `test_multiple_error_scenarios_last_error_preserved`: Verifies first error is preserved
   - `test_wait_for_responses_includes_ssl_error`: Error included in wait_for_responses
   - `test_ssl_error_on_closed_connection_send_msg`: Error included in send_msg

### Integration Tests (`test_ssl_connection_failures.py`)
8 integration tests with actual connection implementations:

1. **AsyncoreConnection Tests** (5 tests)
   - Socket closed forcefully during send
   - Connection reset during recv
   - SSL handshake failure
   - Broken pipe on SSL socket
   - Concurrent operations on closing connection

2. **AsyncioConnection Tests** (2 tests)
   - Socket error preservation
   - Connection reset handling

3. **Error Message Quality Tests** (2 tests)
   - Error message includes root cause
   - Multiple errors preserve first error

## Running the Tests

### Unit Tests Only
```bash
pytest tests/unit/test_ssl_connection_errors.py -v
```

### Integration Tests (requires running cluster)
```bash
pytest tests/integration/standard/test_ssl_connection_failures.py -v
```

### All Connection Tests
```bash
pytest tests/unit/test_connection.py tests/unit/test_ssl_connection_errors.py -v
```

## Test Coverage

The tests verify:
- ✅ Original error is captured in `last_error` field
- ✅ `ConnectionShutdown` exceptions include root cause
- ✅ Concurrent operations see original error, not just "Bad file descriptor"
- ✅ First error is preserved when multiple errors occur
- ✅ Mechanism works across different error types (EBADF, EPIPE, ECONNRESET, etc.)
- ✅ Both AsyncoreConnection and AsyncioConnection handle errors correctly

## Code Changes

**No driver code changes were needed**. The existing error handling mechanism was already correct. These tests document and verify the expected behavior.

## Key Implementation Details

When a connection error occurs:

1. The error is caught in the reactor's `handle_error()`, `handle_write()`, or `handle_read()` method
2. `defunct(exc)` is called with the exception
3. `defunct()` stores the exception in `self.last_error`
4. Subsequent operations check `is_defunct` and raise `ConnectionShutdown` with the original error

Example from `connection.py`:
```python
def send_msg(self, msg, request_id, cb, ...):
    if self.is_defunct:
        msg = "Connection to %s is defunct" % self.endpoint
        if self.last_error:
            msg += ": %s" % (self.last_error,)  # Include original error
        raise ConnectionShutdown(msg)
```

## Contributing

When adding new connection error handling code:
1. Add corresponding tests to `test_ssl_connection_errors.py`
2. Ensure `last_error` is set when connections become defunct
3. Verify error messages include the root cause
4. Test concurrent operation scenarios

## Related Issues
- https://github.com/scylladb/python-driver/issues/614
