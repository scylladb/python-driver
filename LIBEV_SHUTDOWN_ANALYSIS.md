# LibevWrapper Shutdown Crash Analysis

## Executive Summary

The `libevreactor.py` module uses `atexit.register(partial(_cleanup, _global_loop))` to clean up the event loop during Python shutdown. However, this is registered when `_global_loop = None`, causing the cleanup to receive `None` instead of the actual loop instance. This prevents proper shutdown and can lead to crashes when libev callbacks execute during Python interpreter shutdown.

## Root Cause Analysis

### The Bug

In `cassandra/io/libevreactor.py` lines 230-231:

```python
_global_loop = None
atexit.register(partial(_cleanup, _global_loop))
```

The problem:
1. `_global_loop` is `None` at module import time
2. `partial(_cleanup, _global_loop)` captures `None` as the first argument
3. Later, `LibevConnection.initialize_reactor()` sets `_global_loop` to a `LibevLoop` instance
4. During shutdown, atexit calls `_cleanup(None)` instead of `_cleanup(<actual_loop>)`
5. The `_cleanup` function checks `if loop:` and returns immediately without doing anything

### Why This Causes Crashes

When cleanup doesn't run:

1. **Event loop thread keeps running**: The loop thread (`_run_loop`) continues executing
2. **Watchers remain active**: IO, Timer, and Prepare watchers are not stopped
3. **Python objects may be deallocated**: During shutdown, Python starts tearing down modules
4. **Callbacks can fire after Python teardown**: The C callbacks in `libevwrapper.c` can be triggered:
   - `io_callback()` - for socket I/O events
   - `timer_callback()` - for timer events  
   - `prepare_callback()` - before each event loop iteration

5. **Crash scenarios**:
   - Callbacks call `PyGILState_Ensure()` when Python may be finalizing
   - Callbacks call `PyObject_CallFunction()` on potentially deallocated objects
   - Callbacks access `self->callback` which may point to freed memory
   - `PyErr_WriteUnraisable()` may fail if the error handling system is torn down

## Additional Crash Scenarios

### 1. Race Condition: Thread Join Timeout

The cleanup code has a 1-second timeout for joining the event loop thread:

```python
def _cleanup(self):
    # ...
    with self._lock_thread:
        self._thread.join(timeout=1.0)
    
    if self._thread.is_alive():
        log.warning("Event loop thread could not be joined...")
```

**Crash scenario**: If the thread doesn't join in time, it continues running while Python tears down, accessing deallocated objects.

### 2. GIL State Issues During Finalization

All C callbacks use `PyGILState_Ensure()` / `PyGILState_Release()`:

```c
static void io_callback(struct ev_loop *loop, ev_io *watcher, int revents) {
    libevwrapper_IO *self = watcher->data;
    PyObject *result;
    PyGILState_STATE gstate = PyGILState_Ensure();  // May fail during shutdown
    // ...
    PyGILState_Release(gstate);
}
```

**Crash scenario**: During interpreter shutdown, the GIL state management may be invalid or cause deadlocks.

### 3. Object Lifecycle Issues

Watchers hold references to Python callbacks:

```c
typedef struct libevwrapper_IO {
    PyObject_HEAD
    struct ev_io io;
    struct libevwrapper_Loop *loop;
    PyObject *callback;  // This may be deallocated during shutdown
} libevwrapper_IO;
```

**Crash scenario**: 
- Python starts deallocating objects during shutdown
- The event loop is still running and fires a callback
- The callback tries to call `self->callback` which points to freed memory
- Segmentation fault

### 4. Connection Cleanup Not Triggered

Without proper cleanup, connections are not closed:

```python
def _cleanup(self):
    # This never runs if loop is None!
    for conn in self._live_conns | self._new_conns | self._closed_conns:
        conn.close()
        for watcher in (conn._write_watcher, conn._read_watcher):
            if watcher:
                watcher.stop()
```

**Crash scenario**: Active connections with pending I/O can trigger callbacks after Python shutdown.

### 5. Module Deallocation Order

Python doesn't guarantee module deallocation order during shutdown. The libev loop might try to access:
- The `logging` module (for `log.debug()`, `log.warning()`)
- The `os` module (for PID checks)
- The `threading` module (for locks and threads)
- The `time` module (for timers)

**Crash scenario**: If these modules are deallocated before libev callbacks finish, accessing them causes crashes.

### 6. Fork Handling Issues

The code has fork detection:

```python
if _global_loop._pid != os.getpid():
    log.debug("Detected fork, clearing and reinitializing reactor state")
    cls.handle_fork()
    _global_loop = LibevLoop()
```

**Crash scenario**: In a forked child process, if atexit cleanup runs, it might try to clean up the parent's loop state, causing issues.

## Why This Affects Scylla/Cassandra Tests Specifically

From the issue comments:

> "in our tests that it happens, the Cassandra/scylla server is still up, when we shutdown the interpreter"
> "we have in flight request"

**Test scenario that triggers the bug**:

1. Test creates cluster connection
2. Test sends queries (creates active watchers and callbacks)
3. Test completes but server is still responding
4. Test framework exits Python interpreter
5. Active connections have pending I/O events
6. Atexit runs but does nothing (receives None)
7. Event loop keeps running, server sends response
8. IO callback fires during Python shutdown → CRASH

## Impact Analysis

### Frequency
- **Intermittent**: Only crashes when timing is "just right"
- **More likely when**: 
  - Many active connections
  - High query rate
  - Fast test execution (less time for graceful shutdown)
  - Server has pending responses at exit time

### Severity
- **High**: Causes Python interpreter crashes
- **Hard to debug**: Stack traces may be incomplete or corrupted
- **No workaround**: Clearing all atexit hooks breaks other functionality

## Proposed Solutions

### Solution 1: Fix atexit Registration (Minimal Change - RECOMMENDED)

Replace the problematic line with a wrapper function:

```python
def _atexit_cleanup():
    """Cleanup function called by atexit that uses the current _global_loop value."""
    global _global_loop
    if _global_loop is not None:
        _cleanup(_global_loop)

_global_loop = None
atexit.register(_atexit_cleanup)  # Looks up current value at shutdown
```

**Pros**:
- Minimal code change (6-7 lines)
- Fixes the immediate bug
- No API changes
- No C extension changes needed

**Cons**:
- Still relies on atexit (but that's the requirement)

### Solution 2: Add Loop Stop Method (from issue description)

Implement the C code suggested in the issue to add a `loop.stop()` method that can break the event loop from any thread:

```c
typedef struct libevwrapper_Loop {
    PyObject_HEAD
    struct ev_loop *loop;
    ev_async async_watcher;  // New field
} libevwrapper_Loop;

static void async_stop_cb(EV_P_ ev_async *w, int revents) {
    ev_break(EV_A_ EVBREAK_ALL);
}

static PyObject *
Loop_stop(libevwrapper_Loop *self, PyObject *args) {
    ev_async_send(self->loop, &self->async_watcher);
    Py_RETURN_NONE;
}
```

Then in Python:

```python
def _atexit_cleanup():
    global _global_loop
    if _global_loop is not None:
        if _global_loop._loop:
            _global_loop._loop.stop()  # Break the event loop
        _cleanup(_global_loop)
```

**Pros**:
- Provides explicit loop stopping mechanism
- Thread-safe (async is designed for cross-thread communication)
- More robust cleanup

**Cons**:
- Requires C extension changes
- More complex change
- Needs thorough testing

### Solution 3: Callback Safety Guards

Add safety checks in C callbacks to detect shutdown:

```c
static void io_callback(struct ev_loop *loop, ev_io *watcher, int revents) {
    libevwrapper_IO *self = watcher->data;
    PyObject *result;
    
    // Check if Python is finalizing
    if (Py_IsInitialized() == 0) {
        return;  // Don't execute callbacks during shutdown
    }
    
    PyGILState_STATE gstate = PyGILState_Ensure();
    // ... rest of callback
}
```

**Pros**:
- Prevents crashes from callbacks during shutdown
- Defense in depth

**Cons**:
- Doesn't fix the root cause
- `Py_IsInitialized()` may not catch all shutdown states
- Still need to fix the atexit issue

### Solution 4: Weakref-based Cleanup (like twistedreactor)

Similar to `twistedreactor.py`, use weak references:

```python
def _cleanup(loop_ref):
    loop = loop_ref() if callable(loop_ref) else loop_ref
    if loop:
        loop._cleanup()

_global_loop = None

def _register_cleanup():
    global _global_loop
    if _global_loop is not None:
        import weakref
        atexit.register(partial(_cleanup, weakref.ref(_global_loop)))
```

**Pros**:
- Better memory management
- Follows existing pattern in codebase

**Cons**:
- More complex
- Requires calling _register_cleanup() at the right time

## Recommendation

**Implement Solution 1 first** (fix atexit) as it:
- Addresses the immediate bug
- Requires minimal changes
- Can be quickly tested and deployed
- Doesn't change any APIs

**Then consider Solution 2** (add loop.stop()) as an enhancement for more robust shutdown.

**Optionally add Solution 3** (callback guards) for defense in depth.

## Testing Strategy

1. **Unit tests**: Verify atexit callback captures correct loop instance
2. **Subprocess tests**: Verify cleanup runs correctly at process exit
3. **Integration tests**: Test with active connections and pending I/O
4. **Stress tests**: Many connections, rapid creation/destruction
5. **Fork tests**: Verify behavior in forked processes

## References

- GitHub Issue: scylladb/scylla-cluster-tests#11713
- GitHub Issue: scylladb/scylladb#17564
- Existing test: `test_watchers_are_finished` in `test_libevreactor.py`
- Related code: `twistedreactor.py` (uses weakref approach)
