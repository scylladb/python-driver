AsyncioConnection, ASYNCIO_AVAILABLE = None, False
try:
    from cassandra.io.asyncioreactor import AsyncioConnection
    ASYNCIO_AVAILABLE = True
except (ImportError, SyntaxError, AttributeError):
    AsyncioConnection = None
    ASYNCIO_AVAILABLE = False

from tests import is_monkey_patched, connection_class
from tests.unit.io.utils import TimerCallback, TimerTestMixin

from cassandra.connection import DefaultEndPoint
from unittest.mock import patch, MagicMock
import selectors
import unittest
import time

skip_me = (is_monkey_patched() or
           (not ASYNCIO_AVAILABLE) or
           (connection_class is not AsyncioConnection))


@unittest.skipIf(is_monkey_patched(), 'runtime is monkey patched for another reactor')
@unittest.skipIf(connection_class is not AsyncioConnection,
                 'not running asyncio tests; current connection_class is {}'.format(connection_class))
@unittest.skipUnless(ASYNCIO_AVAILABLE, "asyncio is not available for this runtime")
class AsyncioSSLContextTest(unittest.TestCase):

    def setUp(self):
        if skip_me:
            return
        self.old_loop = AsyncioConnection._loop
        AsyncioConnection._loop = object()
        self.addCleanup(setattr, AsyncioConnection, '_loop', self.old_loop)

    def test_empty_ssl_options_use_tls_transport_path(self):
        socket_mock = MagicMock()
        scheduled_tasks = []

        def set_socket(conn):
            conn._socket = socket_mock

        def schedule_task(task, loop):
            scheduled_tasks.append((task, loop))
            close = getattr(task, 'close', None)
            if close:
                close()
            return MagicMock()

        with patch.object(AsyncioConnection, '_connect_socket', autospec=True,
                          side_effect=set_socket) as connect_socket:
            with patch.object(AsyncioConnection, '_setup_ssl_and_run') as setup_ssl_and_run:
                with patch.object(AsyncioConnection, 'handle_read') as handle_read:
                    with patch.object(AsyncioConnection, 'handle_write') as handle_write:
                        with patch('cassandra.io.asyncioreactor.asyncio.run_coroutine_threadsafe',
                                   side_effect=schedule_task) as run_coro:
                            with patch.object(AsyncioConnection, '_send_options_message'):
                                conn = AsyncioConnection(DefaultEndPoint('1.2.3.4'), ssl_options={})

        assert conn._using_ssl
        assert conn._protocol is not None
        assert conn._ssl_ready is not None
        connect_socket.assert_called_once_with(conn)
        socket_mock.setblocking.assert_called_once_with(0)
        setup_ssl_and_run.assert_called_once_with()
        handle_read.assert_not_called()
        handle_write.assert_called_once_with()
        assert run_coro.call_count == 2
        assert scheduled_tasks[0][1] is AsyncioConnection._loop
        assert scheduled_tasks[1][1] is AsyncioConnection._loop


@unittest.skipIf(is_monkey_patched(), 'runtime is monkey patched for another reactor')
@unittest.skipIf(connection_class is not AsyncioConnection,
                 'not running asyncio tests; current connection_class is {}'.format(connection_class))
@unittest.skipUnless(ASYNCIO_AVAILABLE, "asyncio is not available for this runtime")
class AsyncioTimerTests(TimerTestMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if skip_me:
            return
        cls.connection_class = AsyncioConnection
        AsyncioConnection.initialize_reactor()

    @classmethod
    def tearDownClass(cls):
        if skip_me:
            return
        if ASYNCIO_AVAILABLE and AsyncioConnection._loop:
            AsyncioConnection._loop.stop()

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        raise RuntimeError('no TimerManager for AsyncioConnection')

    def setUp(self):
        if skip_me:
            return
        socket_patcher = patch('socket.socket')
        self.addCleanup(socket_patcher.stop)
        socket_patcher.start()

        old_selector = AsyncioConnection._loop._selector
        AsyncioConnection._loop._selector = MagicMock(spec=selectors.BaseSelector)

        def reset_selector():
            AsyncioConnection._loop._selector = old_selector

        self.addCleanup(reset_selector)

        super(AsyncioTimerTests, self).setUp()

    def test_timer_cancellation(self):
        # Various lists for tracking callback stage
        timeout = .1
        callback = TimerCallback(timeout)
        timer = self.create_timer(timeout, callback.invoke)
        timer.cancel()
        # Release context allow for timer thread to run.
        time.sleep(.2)
        # Assert that the cancellation was honored
        assert not callback.was_invoked()
