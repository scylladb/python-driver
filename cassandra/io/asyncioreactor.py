import threading

from cassandra.connection import Connection, ConnectionShutdown
import sys
import asyncio
import logging
import os
import socket
import ssl
from threading import Lock, Thread


log = logging.getLogger(__name__)


# This module uses ``yield from`` and ``@asyncio.coroutine`` over ``await`` and
# ``async def`` for pre-Python-3.5 compatibility, so keep in mind that the
# managed coroutines are generator-based, not native coroutines. See PEP 492:
# https://www.python.org/dev/peps/pep-0492/#coroutine-objects


try:
    asyncio.run_coroutine_threadsafe
except AttributeError:
    raise ImportError(
        "Cannot use asyncioreactor without access to "
        "asyncio.run_coroutine_threadsafe (added in 3.4.6 and 3.5.1)"
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
        raise NotImplementedError(
            "{} is not compatible with TimerManager and does not implement .end()"
        )

    def __init__(self, timeout, callback, loop):
        delayed = self._call_delayed_coro(timeout=timeout, callback=callback)
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
        raise NotImplementedError(
            "{} is not compatible with TimerManager and does not implement .finish()"
        )


class _AsyncioProtocol(asyncio.Protocol):
    """
    Protocol adapter for asyncio SSL connections. Bridges asyncio's
    transport/protocol API back to AsyncioConnection's buffer processing.
    """

    def __init__(self, connection, loop_args=None):
        self._connection = connection
        self.transport = None
        self.write_ready = asyncio.Event(**(loop_args or {}))
        self.write_ready.set()

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        conn = self._connection
        conn._iobuf.write(data)
        if conn._iobuf.tell():
            conn.process_io_buffer()

    def pause_writing(self):
        self.write_ready.clear()

    def resume_writing(self):
        self.write_ready.set()

    def connection_lost(self, exc):
        # Unblock any paused writer so shutdown does not hang
        self.write_ready.set()
        conn = self._connection
        if exc:
            log.debug("Connection %s lost: %s", conn, exc)
            conn.defunct(exc)
        else:
            log.debug("Connection %s closed by server", conn)
            conn.close()

    def eof_received(self):
        return False


class AsyncioConnection(Connection):
    """
    An implementation of :class:`.Connection` that uses the ``asyncio``
    module in the Python standard library for its event loop.

    Supports SSL connections via asyncio's native TLS transport, which
    avoids the incompatibility between ``ssl.SSLSocket`` and asyncio's
    low-level socket methods (``sock_sendall``, ``sock_recv``).

    Note: TLS session resumption (:attr:`.Cluster.ssl_session_cache`) is not
    supported on this reactor.  TLS is established through
    ``loop.create_connection(..., ssl=...)``, which exposes no hook to restore a
    cached session before the handshake, so sessions are neither restored nor
    stored here.
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
        self._transport = None
        self._using_ssl = bool(self.ssl_context)

        self._connect_socket()
        self._socket.setblocking(0)
        loop_args = dict()
        if sys.version_info[0] == 3 and sys.version_info[1] < 10:
            loop_args["loop"] = self._loop
        self._protocol = _AsyncioProtocol(self, loop_args) if self._using_ssl else None
        self._ssl_ready = asyncio.Event(**loop_args) if self._using_ssl else None
        self._write_queue = asyncio.Queue(**loop_args)
        self._write_queue_lock = asyncio.Lock(**loop_args)

        # see initialize_reactor -- loop is running in a separate thread, so we
        # have to use a threadsafe call
        if self._using_ssl:
            # For SSL: set up asyncio transport/protocol, then start writer
            self._read_watcher = asyncio.run_coroutine_threadsafe(
                self._setup_ssl_and_run(), loop=self._loop
            )
        else:
            # For non-SSL: use low-level sock_sendall/sock_recv as before
            self._read_watcher = asyncio.run_coroutine_threadsafe(
                self.handle_read(), loop=self._loop
            )
        self._write_watcher = asyncio.run_coroutine_threadsafe(
            self.handle_write(), loop=self._loop
        )
        self._send_options_message()

    def _connect_socket(self):
        """
        Override base class to skip SSL wrapping of the socket.
        For SSL connections, the plain TCP socket is connected here, and TLS
        is set up later via asyncio's native SSL transport in _setup_ssl_and_run().
        """
        sockerr = None
        addresses = self._get_socket_addresses()
        for af, socktype, proto, _, sockaddr in addresses:
            try:
                self._socket = self._socket_impl.socket(af, socktype, proto)
                # Do NOT wrap with ssl_context here -- asyncio will handle TLS
                self._socket.settimeout(self.connect_timeout)
                self._initiate_connection(sockaddr)
                self._socket.settimeout(None)

                local_addr = self._socket.getsockname()
                log.debug("Connection %s: '%s' -> '%s'", id(self), local_addr, sockaddr)
                sockerr = None
                break
            except socket.error as err:
                if self._socket:
                    self._socket.close()
                    self._socket = None
                sockerr = err

        if sockerr:
            raise socket.error(
                sockerr.errno,
                "Tried connecting to %s. Last error: %s"
                % ([a[4] for a in addresses], sockerr.strerror or sockerr),
            )

        if self.sockopts:
            for args in self.sockopts:
                self._socket.setsockopt(*args)

    async def _setup_ssl_and_run(self):
        """
        Upgrade the plain TCP connection to TLS using asyncio's native SSL
        transport, then continuously read data via the protocol callbacks.
        """
        try:
            ssl_context = self.ssl_context
            server_hostname = None
            if self.ssl_options:
                server_hostname = self.ssl_options.get("server_hostname", None)
            if server_hostname is None:
                # asyncio's create_connection requires server_hostname when
                # ssl= is set. Use endpoint address for SNI/verification when
                # check_hostname is enabled; otherwise pass "" to suppress SNI.
                server_hostname = (
                    self.endpoint.address if ssl_context.check_hostname else ""
                )

            transport, protocol = await self._loop.create_connection(
                lambda: self._protocol,
                sock=self._socket,
                ssl=ssl_context,
                server_hostname=server_hostname,
            )
            self._transport = transport

            if self._check_hostname:
                self._validate_hostname()

            self._ssl_ready.set()
        except Exception as exc:
            log.debug("SSL setup failed for %s: %s", self, exc)
            self.defunct(exc)
            # Unblock handle_write so it can observe the defunct state and exit
            self._ssl_ready.set()
            return

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
                cls._loop_thread = Thread(
                    target=cls._loop.run_forever, daemon=True, name="asyncio_thread"
                )
                cls._loop_thread.start()

    @classmethod
    def create_timer(cls, timeout, callback):
        return AsyncioTimer(timeout, callback, loop=cls._loop)

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        # close from the loop thread to avoid races when removing file
        # descriptors
        asyncio.run_coroutine_threadsafe(self._close(), loop=self._loop)

    async def _close(self):
        log.debug("Closing connection (%s) to %s" % (id(self), self.endpoint))
        if self._write_watcher:
            self._write_watcher.cancel()
        if self._read_watcher:
            self._read_watcher.cancel()
        if self._transport:
            self._transport.close()
            self._transport = None
        elif self._socket:
            self._loop.remove_writer(self._socket.fileno())
            self._loop.remove_reader(self._socket.fileno())
            self._socket.close()

        log.debug("Closed socket to %s" % (self.endpoint,))

        if not self.is_defunct:
            msg = "Connection to %s was closed" % self.endpoint
            if self.last_error:
                msg += ": %s" % (self.last_error,)
            self.error_all_requests(ConnectionShutdown(msg))
            # don't leave in-progress operations hanging
            self.connected_event.set()

    def push(self, data):
        buff_size = self.out_buffer_size
        if len(data) > buff_size:
            chunks = []
            for i in range(0, len(data), buff_size):
                chunks.append(data[i : i + buff_size])
        else:
            chunks = [data]

        if self._loop_thread != threading.current_thread():
            asyncio.run_coroutine_threadsafe(self._push_msg(chunks), loop=self._loop)
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
        # For SSL connections, wait until the TLS handshake completes
        if self._ssl_ready:
            await self._ssl_ready.wait()
            if self.is_defunct:
                return
        while True:
            try:
                next_msg = await self._write_queue.get()
                if next_msg:
                    if self._transport:
                        # SSL: use asyncio transport (handles TLS transparently)
                        await self._protocol.write_ready.wait()
                        if self.is_closed or self.is_defunct or not self._transport:
                            return
                        self._transport.write(next_msg)
                    else:
                        # Non-SSL: use low-level socket API
                        await self._loop.sock_sendall(self._socket, next_msg)
            except socket.error as err:
                log.debug("Exception in send for %s: %s", self, err)
                self.defunct(err)
                return
            except asyncio.CancelledError:
                return

    async def handle_read(self):
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
            except socket.error as err:
                log.debug("Exception during socket recv for %s: %s", self, err)
                self.defunct(err)
                return  # leave the read loop
            except asyncio.CancelledError:
                return

            if buf and self._iobuf.tell():
                self.process_io_buffer()
            else:
                log.debug("Connection %s closed by server", self)
                self.close()
                return
