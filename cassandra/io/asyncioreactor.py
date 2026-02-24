import errno
import threading

from cassandra.connection import Connection, ConnectionShutdown
import sys
import asyncio
import logging
import os
import socket
import ssl
from threading import Lock, Thread, get_ident


log = logging.getLogger(__name__)

# Errno values that indicate the remote peer has disconnected.
_PEER_DISCONNECT_ERRNOS = frozenset((
    errno.ENOTCONN, errno.ESHUTDOWN,
    errno.ECONNRESET, errno.ECONNABORTED,
    errno.EBADF,
))

# Windows winerror codes for the same conditions:
# 10053 = WSAECONNABORTED, 10054 = WSAECONNRESET
_PEER_DISCONNECT_WINERRORS = frozenset((10053, 10054))


def _is_peer_disconnect(err):
    """Return True if *err* indicates the remote peer closed the connection."""
    return (isinstance(err, ConnectionError)
            or getattr(err, 'winerror', None) in _PEER_DISCONNECT_WINERRORS
            or getattr(err, 'errno', None) in _PEER_DISCONNECT_ERRNOS)


# This module uses ``yield from`` and ``@asyncio.coroutine`` over ``await`` and
# ``async def`` for pre-Python-3.5 compatibility, so keep in mind that the
# managed coroutines are generator-based, not native coroutines. See PEP 492:
# https://www.python.org/dev/peps/pep-0492/#coroutine-objects


try:
    asyncio.run_coroutine_threadsafe
except AttributeError:
    raise ImportError(
        'Cannot use asyncioreactor without access to '
        'asyncio.run_coroutine_threadsafe (added in 3.4.6 and 3.5.1)'
    )


class AsyncioTimer(object):
    """
    An ``asyncioreactor``-specific Timer. Similar to :class:`.connection.Timer,
    but with a slightly different API due to limitations in the underlying
    ``call_later`` interface. Not meant to be used with a
    :class:`.connection.TimerManager`.
    """

    @property
    def end(self):
        raise NotImplementedError('{} is not compatible with TimerManager and '
                                  'does not implement .end()')

    def __init__(self, timeout, callback, loop):
        delayed = self._call_delayed_coro(timeout=timeout,
                                          callback=callback)
        self._handle = asyncio.run_coroutine_threadsafe(delayed, loop=loop)

    @staticmethod
    async def _call_delayed_coro(timeout, callback):
        await asyncio.sleep(timeout)
        return callback()

    def __lt__(self, other):
        try:
            return self._handle < other._handle
        except AttributeError:
            raise NotImplemented

    def cancel(self):
        self._handle.cancel()

    def finish(self):
        # connection.Timer method not implemented here because we can't inspect
        # the Handle returned from call_later
        raise NotImplementedError('{} is not compatible with TimerManager and '
                                  'does not implement .finish()')


class AsyncioConnection(Connection):
    """
    An experimental implementation of :class:`.Connection` that uses the
    ``asyncio`` module in the Python standard library for its event loop.

    Note that it requires ``asyncio`` features that were only introduced in the
    3.4 line in 3.4.6, and in the 3.5 line in 3.5.1.
    """

    _loop = None
    _pid = os.getpid()

    _lock = Lock()
    _loop_thread = None

    _write_queue = None
    _write_queue_lock = None

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)
        self._background_tasks = set()

        self._connect_socket()
        self._socket.setblocking(0)
        loop_args = dict()
        if sys.version_info[0] == 3 and sys.version_info[1] < 10:
            loop_args['loop'] = self._loop
        self._write_queue = asyncio.Queue(**loop_args)
        self._write_queue_lock = asyncio.Lock(**loop_args)

        # see initialize_reactor -- loop is running in a separate thread, so we
        # have to use a threadsafe call
        self._read_watcher = asyncio.run_coroutine_threadsafe(
            self.handle_read(), loop=self._loop
        )
        self._write_watcher = asyncio.run_coroutine_threadsafe(
            self.handle_write(), loop=self._loop
        )
        self._send_options_message()



    @classmethod
    def initialize_reactor(cls):
        with cls._lock:
            if cls._pid != os.getpid():
                # This means that class was passed to another process,
                # e.g. using multiprocessing.
                # In such case the class instance will be different and passing
                # tasks to loop thread won't work.
                # To fix we need to re-initialize the class
                cls._loop = None
                cls._loop_thread = None
                cls._pid = os.getpid()
            if cls._loop is None:
                assert cls._loop_thread is None
                cls._loop = asyncio.new_event_loop()
                # daemonize so the loop will be shut down on interpreter
                # shutdown
                cls._loop_thread = Thread(target=cls._loop.run_forever,
                                          daemon=True, name="asyncio_thread")
                cls._loop_thread.start()

    @classmethod
    def create_timer(cls, timeout, callback):
        return AsyncioTimer(timeout, callback, loop=cls._loop)

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        # Schedule async cleanup (cancel watchers, error pending requests)
        asyncio.run_coroutine_threadsafe(
            self._close(), loop=self._loop
        )

    async def _close(self):
        log.debug("Closing connection (%s) to %s" % (id(self), self.endpoint))
        if self._write_watcher:
            self._write_watcher.cancel()
        if self._read_watcher:
            self._read_watcher.cancel()
        if self._socket:
            fd = self._socket.fileno()
            if fd >= 0:
                try:
                    self._loop.remove_writer(fd)
                except NotImplementedError:
                    # NotImplementedError: remove_reader/remove_writer are not
                    #   supported on Windows ProactorEventLoop (default since
                    #   Python 3.10). ProactorEventLoop uses completion-based
                    #   IOCP, which has no concept of "watching a fd for
                    #   readiness" to remove.
                    pass
                except Exception:
                    # It is not critical if it fails, driver can keep working,
                    # but it should not be happening, so logged as error
                    log.error("Unexpected error removing writer for %s",
                              self.endpoint, exc_info=True)
                try:
                    self._loop.remove_reader(fd)
                except NotImplementedError:
                    # NotImplementedError: remove_reader/remove_writer are not
                    #   supported on Windows ProactorEventLoop (default since
                    #   Python 3.10). ProactorEventLoop uses completion-based
                    #   IOCP, which has no concept of "watching a fd for
                    #   readiness" to remove.
                    pass
                except Exception:
                    # It is not critical if it fails, driver can keep working,
                    # but it should not be happening, so logged as error
                    log.error("Unexpected error removing reader for %s",
                              self.endpoint, exc_info=True)

                try:
                    self._socket.close()
                except OSError:
                    # Ignore if socket is already closed
                    pass
                except Exception:
                    log.debug("Unexpected error closing socket to %s",
                              self.endpoint, exc_info=True)
            log.debug("Closed socket to %s" % (self.endpoint,))

        if not self.is_defunct:
            msg = "Connection to %s was closed" % self.endpoint
            if self.last_error:
                msg += ": %s" % (self.last_error,)
            self.error_all_requests(ConnectionShutdown(msg))
            # don't leave in-progress operations hanging
            self.connected_event.set()

    def push(self, data):
        if self.is_closed or self.is_defunct:
            raise ConnectionShutdown(
                "Connection to %s is already closed" % self.endpoint)
        buff_size = self.out_buffer_size
        if len(data) > buff_size:
            chunks = []
            for i in range(0, len(data), buff_size):
                chunks.append(data[i:i + buff_size])
        else:
            chunks = [data]

        if self._loop_thread != threading.current_thread():
            asyncio.run_coroutine_threadsafe(
                self._push_msg(chunks),
                loop=self._loop
            )
        else:
            # avoid races/hangs by just scheduling this, not using threadsafe
            task = self._loop.create_task(self._push_msg(chunks))

            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

    async def _push_msg(self, chunks):
        # This lock ensures all chunks of a message are sequential in the Queue
        async with self._write_queue_lock:
            for chunk in chunks:
                self._write_queue.put_nowait(chunk)


    async def handle_write(self):
        exc = None
        try:
            while True:
                next_msg = await self._write_queue.get()
                if next_msg:
                    await self._loop.sock_sendall(self._socket, next_msg)
        except asyncio.CancelledError:
            pass
        except Exception as err:
            if _is_peer_disconnect(err):
                log.debug("Connection %s closed by peer during write: %s",
                          self, err)
            else:
                exc = err
                log.debug("Exception in send for %s: %s", self, err)
        finally:
            self.defunct(exc or ConnectionShutdown(
                "Connection to %s was closed" % self.endpoint))

    async def handle_read(self):
        exc = None
        try:
            while True:
                try:
                    buf = await self._loop.sock_recv(self._socket, self.in_buffer_size)
                    self._iobuf.write(buf)
                # sock_recv expects EWOULDBLOCK if socket provides no data, but
                # nonblocking ssl sockets raise these instead, so we handle them
                # ourselves by yielding to the event loop, where the socket will
                # get the reading/writing it "wants" before retrying
                except (ssl.SSLWantWriteError, ssl.SSLWantReadError):
                    # Apparently the preferred way to yield to the event loop from within
                    # a native coroutine based on https://github.com/python/asyncio/issues/284
                    await asyncio.sleep(0)
                    continue

                if buf and self._iobuf.tell():
                    self.process_io_buffer()
                else:
                    log.debug("Connection %s closed by server", self)
                    exc = ConnectionShutdown(
                        "Connection to %s was closed by server" % self.endpoint)
                    return
        except asyncio.CancelledError:
            # Task cancellation is treated as a normal connection shutdown;
            # cleanup and marking the connection as defunct are handled in finally.
            pass
        except Exception as err:
            if _is_peer_disconnect(err):
                log.debug("Connection %s closed by peer during read: %s",
                          self, err)
            else:
                exc = err
                log.debug("Exception during socket recv for %s: %s",
                          self, err)
        finally:
            self.defunct(exc or ConnectionShutdown(
                "Connection to %s was closed" % self.endpoint))
