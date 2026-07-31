# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Connection pooling and host management.
"""
from collections import deque
from concurrent.futures import Future
from contextlib import ExitStack
from functools import total_ordering
import inspect
import logging
import time
import random
import copy
from threading import Lock, RLock, Condition
import weakref
try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet  # NOQA

from cassandra import AuthenticationFailed, RequestValidationException
from cassandra.connection import (ConnectionException, EndPoint, DefaultEndPoint,
                                  _ConnectionClosedDuringStartup,
                                  _set_keyspace_blocking,
                                  _startup_close_error)
from cassandra.policies import HostDistance
from cassandra.protocol import (
    RequestValidationException as ProtocolRequestValidationException)

log = logging.getLogger(__name__)

_REQUEST_VALIDATION_EXCEPTIONS = (
    RequestValidationException,
    ProtocolRequestValidationException)


def _signal_connection_failure(
        cluster, host, connection_exc, is_host_addition,
        expect_host_to_be_down=False, force=False):
    """Invoke the failure hook without breaking pre-``force`` overrides."""
    method = cluster.signal_connection_failure
    try:
        parameters = tuple(inspect.signature(method).parameters.values())
    except (TypeError, ValueError):
        # This is the historical four-positional-argument API. In particular,
        # do not guess that an uninspectable override accepts ``force``.
        return method(
            host,
            connection_exc,
            is_host_addition,
            expect_host_to_be_down)

    positional_parameters = tuple(
        parameter for parameter in parameters
        if parameter.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD))
    has_varargs = any(
        parameter.kind == inspect.Parameter.VAR_POSITIONAL
        for parameter in parameters)
    has_varkw = any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters)
    parameters_by_name = {
        parameter.name: parameter for parameter in parameters}

    args = [host, connection_exc]
    kwargs = {}
    addition_parameter = parameters_by_name.get('is_host_addition')
    if (
            addition_parameter is not None and
            addition_parameter.kind == inspect.Parameter.KEYWORD_ONLY):
        kwargs['is_host_addition'] = is_host_addition
    elif len(positional_parameters) >= 3 or has_varargs:
        args.append(is_host_addition)
    elif has_varkw:
        kwargs['is_host_addition'] = is_host_addition
    else:
        # Preserve the historical positional call and let an actually
        # incompatible override raise a useful TypeError.
        args.append(is_host_addition)

    expect_parameter = parameters_by_name.get('expect_host_to_be_down')
    if (
            expect_parameter is not None and
            expect_parameter.kind in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY)):
        kwargs['expect_host_to_be_down'] = expect_host_to_be_down
    elif len(positional_parameters) >= 4:
        # Some historical overrides renamed this argument. Pass it by
        # position so those overrides continue to work.
        args.append(expect_host_to_be_down)
    elif has_varkw:
        kwargs['expect_host_to_be_down'] = expect_host_to_be_down
    elif has_varargs:
        args.append(expect_host_to_be_down)

    force_parameter = parameters_by_name.get('force')
    if (
            force_parameter is not None and
            force_parameter.kind == inspect.Parameter.POSITIONAL_ONLY):
        # A positional-only force parameter necessarily follows the
        # historical expectation slot.
        if len(args) == 3 and 'expect_host_to_be_down' in kwargs:
            args.append(expect_host_to_be_down)
            kwargs.pop('expect_host_to_be_down', None)
        args.append(force)
    elif (
            has_varkw or
            (
                force_parameter is not None and
                force_parameter.kind in (
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.KEYWORD_ONLY))):
        kwargs['force'] = force

    return method(*args, **kwargs)


class NoConnectionsAvailable(Exception):
    """
    All existing connections to a given host are busy, or there are
    no open connections.
    """
    pass


@total_ordering
class Host(object):
    """
    Represents a single Cassandra node.
    """

    endpoint = None
    """
    The :class:`~.connection.EndPoint` to connect to the node.
    """

    broadcast_address = None
    """
    broadcast address configured for the node, *if available*:

    'system.local.broadcast_address' or 'system.peers.peer' (Cassandra 2-3)
    'system.local.broadcast_address' or 'system.peers_v2.peer' (Cassandra 4)

    This is not present in the ``system.local`` table for older versions of Cassandra. It
    is also not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    broadcast_port = None
    """
    broadcast port configured for the node, *if available*:

    'system.local.broadcast_port' or 'system.peers_v2.peer_port' (Cassandra 4)

    It is also not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    broadcast_rpc_address = None
    """
    The broadcast rpc address of the node:

    'system.local.rpc_address' or 'system.peers.rpc_address' (Cassandra 3)
    'system.local.rpc_address' or 'system.peers.native_transport_address (DSE  6+)'
    'system.local.rpc_address' or 'system.peers_v2.native_address (Cassandra 4)'
    """

    broadcast_rpc_port = None
    """
    The broadcast rpc port of the node, *if available*:
    
    'system.local.rpc_port' or 'system.peers.native_transport_port' (DSE 6+)
    'system.local.rpc_port' or 'system.peers_v2.native_port' (Cassandra 4)
    """

    listen_address = None
    """
    listen address configured for the node, *if available*:

    'system.local.listen_address'

    This is only available in the ``system.local`` table for newer versions of Cassandra. It is also not
    queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``. Usually the same as ``broadcast_address``
    unless configured differently in cassandra.yaml.
    """

    listen_port = None
    """
    listen port configured for the node, *if available*:

    'system.local.listen_port'

    This is only available in the ``system.local`` table for newer versions of Cassandra. It is also not
    queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    conviction_policy = None
    """
    A :class:`~.ConvictionPolicy` instance for determining when this node should
    be marked up or down.
    """

    is_up = None
    """
    :const:`True` if the node is considered up, :const:`False` if it is
    considered down, and :const:`None` if it is not known if the node is
    up or down.
    """

    release_version = None
    """
    release_version as queried from the control connection system tables
    """

    host_id = None
    """
    The unique identifier of the cassandra node
    """

    dse_version = None
    """
    dse_version as queried from the control connection system tables. Only populated when connecting to
    DSE with this property available. Not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    dse_workload = None
    """
    DSE workload queried from the control connection system tables. Only populated when connecting to
    DSE with this property available. Not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    This is a legacy attribute that does not portray multiple workloads in a uniform fashion.
    See also :attr:`~.Host.dse_workloads`.
    """

    dse_workloads = None
    """
    DSE workloads set, queried from the control connection system tables. Only populated when connecting to
    DSE with this property available (added in DSE 5.1).
    Not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    _datacenter = None
    _rack = None
    _reconnection_handler = None
    lock = None

    _currently_handling_node_up = False
    _currently_handling_node_add = False
    _recovery_epoch = 0
    _is_removed = False
    _transition_lock = None
    _transition_queue = None
    _transition_running = False
    _transition_notification_queue = None
    _transition_notification_running = False
    _transition_event_sequence = 0
    _latest_endpoint_change_sequence = 0
    _latest_preemptive_down_sequence = 0
    _pending_down_notification_epoch = None

    sharding_info = None

    def __init__(self, endpoint, conviction_policy_factory, datacenter=None, rack=None, host_id=None):
        if endpoint is None:
            raise ValueError("endpoint may not be None")
        if conviction_policy_factory is None:
            raise ValueError("conviction_policy_factory may not be None")

        self.endpoint = endpoint if isinstance(endpoint, EndPoint) else DefaultEndPoint(endpoint)
        self.conviction_policy = conviction_policy_factory(self)
        if not host_id:
            raise ValueError("host_id may not be None")
        self.host_id = host_id
        self.set_location_info(datacenter, rack)
        self.lock = RLock()
        # Cluster topology side effects are drained serially without holding
        # this lock while callbacks execute. Reentrant/cross-host callbacks
        # enqueue and return instead of nesting host locks.
        self._transition_lock = RLock()
        self._transition_queue = deque()
        self._transition_running = False
        self._transition_owner = None
        self._transition_notification_queue = deque()
        self._transition_notification_running = False
        self._transition_event_sequence = 0
        self._latest_endpoint_change_sequence = 0
        self._latest_preemptive_down_sequence = 0
        self._pending_down_notification_epoch = None
        self._recovery_epoch = 0
        self._currently_handling_node_up = False
        self._currently_handling_node_add = False
        self._is_removed = False

    @property
    def address(self):
        """
        The IP address of the endpoint. This is the RPC address the driver uses when connecting to the node.
        """
        # backward compatibility
        return self.endpoint.address

    @property
    def datacenter(self):
        """ The datacenter the node is in.  """
        return self._datacenter

    @property
    def rack(self):
        """ The rack the node is in.  """
        return self._rack

    def set_location_info(self, datacenter, rack):
        """
        Sets the datacenter and rack for this node. Intended for internal
        use (by the control connection, which periodically checks the
        ring topology) only.
        """
        self._datacenter = datacenter
        self._rack = rack

    def set_up(self):
        if not self.is_up:
            log.debug("Host %s is now marked up", self.endpoint)
        self.conviction_policy.reset()
        self.is_up = True

    def set_down(self):
        self.is_up = False

    def signal_connection_failure(self, connection_exc):
        return self.conviction_policy.add_failure(connection_exc)

    def is_currently_reconnecting(self):
        return self._reconnection_handler is not None

    def get_and_set_reconnection_handler(self, new_handler):
        """
        Atomically replaces the reconnection handler for this
        host.  Intended for internal use only.
        """
        with self.lock:
            old = self._reconnection_handler
            self._reconnection_handler = new_handler
            return old

    def clear_reconnection_handler(self, expected_handler):
        """Clear a reconnector only if it still owns the host slot."""
        with self.lock:
            if self._reconnection_handler is expected_handler:
                self._reconnection_handler = None
                return expected_handler
            return None

    def __eq__(self, other):
        if isinstance(other, Host):
            return self.endpoint == other.endpoint
        else:  # TODO Backward compatibility, remove next major
            return self.endpoint.address == other

    def __hash__(self):
        return hash(self.endpoint)

    def __lt__(self, other):
        return self.endpoint < other.endpoint

    def __str__(self):
        return str(self.endpoint)

    def __repr__(self):
        dc = (" %s" % (self._datacenter,)) if self._datacenter else ""
        return "<%s: %s%s>" % (self.__class__.__name__, self.endpoint, dc)


class _ReconnectionHandler(object):
    """
    Abstract class for attempting reconnections with a given
    schedule and scheduler.
    """

    _cancelled = False

    def __init__(self, scheduler, schedule, callback, *callback_args, **callback_kwargs):
        self.scheduler = scheduler
        self.schedule = schedule
        self.callback = callback
        self.callback_args = callback_args
        self.callback_kwargs = callback_kwargs

    def start(self):
        if self._cancelled:
            log.debug("Reconnection handler was cancelled before starting")
            return

        first_delay = next(self.schedule)
        self.scheduler.schedule(first_delay, self.run)

    def run(self):
        if self._cancelled:
            return

        conn = None
        connection_transferred = False
        try:
            conn = self.try_reconnect()
        except Exception as exc:
            try:
                next_delay = next(self.schedule)
            except StopIteration:
                # the schedule has been exhausted
                next_delay = None

            # call on_exception for logging purposes even if next_delay is None
            if self.on_exception(exc, next_delay):
                if next_delay is None:
                    log.warning(
                        "Will not continue to retry reconnection attempts "
                        "due to an exhausted retry schedule")
                else:
                    self.scheduler.schedule(next_delay, self.run)
        else:
            if not self._cancelled:
                connection_transferred = self.on_reconnection(conn) is True
                self.callback(*(self.callback_args), **(self.callback_kwargs))
        finally:
            if conn and not connection_transferred:
                conn.close()

    def cancel(self):
        self._cancelled = True

    def try_reconnect(self):
        """
        Subclasses must implement this method.  It should attempt to
        open a new Connection and return it; if a failure occurs, an
        Exception should be raised.
        """
        raise NotImplementedError()

    def on_reconnection(self, connection):
        """
        Called when a new Connection is successfully opened.  Nothing is
        done by default.
        """
        pass

    def on_exception(self, exc, next_delay):
        """
        Called when an Exception is raised when trying to connect.
        `exc` is the Exception that was raised and `next_delay` is the
        number of seconds (as a float) that the handler will wait before
        attempting to connect again.

        Subclasses should return :const:`False` if no more attempts to
        connection should be made, :const:`True` otherwise.  The default
        behavior is to always retry unless the error is an
        :exc:`.AuthenticationFailed` instance.
        """
        if isinstance(exc, AuthenticationFailed):
            return False
        else:
            return True


class _HostReconnectionHandler(_ReconnectionHandler):

    def __init__(self, host, connection_factory, is_host_addition, on_add, on_up, *args, **kwargs):
        _ReconnectionHandler.__init__(self, *args, **kwargs)
        self.is_host_addition = is_host_addition
        self.on_add = on_add
        self.on_up = on_up
        self.host = host
        self.connection_factory = connection_factory

    def try_reconnect(self):
        connection = self.connection_factory()
        if connection.is_closed:
            error = _startup_close_error(connection, self.host.endpoint)
            connection.close()
            raise error
        return connection

    def on_reconnection(self, connection):
        log.info("Successful reconnection to %s, marking node up if it isn't already", self.host)
        if self.is_host_addition:
            self.on_add(self.host)
        else:
            self.on_up(self.host)

    def on_exception(self, exc, next_delay):
        if isinstance(exc, _ConnectionClosedDuringStartup):
            log.info(
                "Connection to %s closed cleanly during startup; leaving the "
                "host down until an UP event or verified control reconnect",
                self.host)
            return False
        elif isinstance(exc, AuthenticationFailed):
            return False
        else:
            log.warning("Error attempting to reconnect to %s, scheduling retry in %s seconds: %s",
                        self.host, next_delay, exc)
            log.debug("Reconnection error details", exc_info=True)
            return True


class HostConnection(object):
    """
    When using v3 of the native protocol, this is useddue to the increased in-flight capacity
    of individual connections.
    """

    host = None
    host_distance = None
    is_shutdown = False
    shutdown_on_error = False

    _session = None
    _lock = None
    _keyspace = None

    # If the number of excess connections exceeds the number of shards times
    # the number below, all excess connections will be closed.
    max_excess_connections_per_shard_multiplier = 3

    tablets_routing_v1 = False

    @staticmethod
    def _session_keyspace_snapshot(session):
        """
        Return the Session keyspace together with its change generation.

        Real Sessions publish both values under their lock. Lightweight
        Session stand-ins used by applications and tests may not expose the
        generation, in which case comparing the keyspace itself still
        provides the historical best-effort behavior.
        """
        lock = getattr(session, '_lock', None)
        generation = getattr(session, '_keyspace_generation', None)
        if (
                isinstance(generation, int) and
                hasattr(lock, '__enter__')):
            with lock:
                return (
                    session.keyspace,
                    getattr(session, '_keyspace_generation', generation))
        return session.keyspace, None

    def __init__(
            self, host, host_distance, session, pool_generation=None,
            host_recovery_epoch=None):
        self.host = host
        self.host_distance = host_distance
        self._session = weakref.proxy(session)
        # Session supplies the generation for pools built asynchronously.
        # This lets a shard worker hand recovery off even if it fails before
        # the candidate pool is published.
        self._pool_generation = pool_generation
        self._host_recovery_epoch = host_recovery_epoch
        self._lock = Lock()
        # this is used in conjunction with the connection streams. Not using the connection lock because the connection can be replaced in the lifetime of the pool.
        self._stream_available_condition = Condition(Lock())
        self._is_replacing = False
        self._regular_replacement_future = None
        self._connecting = set()
        # Maps a shard id to the unique token for the attempt which currently
        # owns its `_connecting` marker.  `_connecting` is retained for
        # compatibility with code which introspects pool state.
        self._shard_connection_attempts = {}
        self._connections = {}
        self._pending_connections = []
        self._shutdown_owned_pending_ids = set()
        # A pool of additional connections which are not used but affect how Scylla
        # assigns shards to them. Scylla tends to assign the shard which has
        # the lowest number of connections. If connections are not distributed
        # evenly at the moment, we might need to open several dummy connections
        # to other shards before Scylla returns a connection to the shards we are
        # interested in.
        # After we get at least one connection for each shard, we can close
        # the additional connections.
        self._excess_connections = set()
        # Contains connections which shouldn't be used anymore
        # and are waiting until all requests time out or complete
        # so that we can dispose of them.
        self._trash = set()
        self._shard_connections_futures = []
        self.advanced_shardaware_block_until = 0
        self._keyspace_generation = 0
        self._keyspace_update_queue = deque()
        self._keyspace_update_in_progress = False
        self._keyspace_update_runner_active = False
        self._keyspace_update_run_requested = False
        self._keyspace_update_current = None

        if host_distance == HostDistance.IGNORED:
            log.debug("Not opening connection to ignored host %s", self.host)
            return
        elif host_distance == HostDistance.REMOTE and not session.cluster.connect_to_remote_hosts:
            log.debug("Not opening connection to remote host %s", self.host)
            return

        log.debug("Initializing connection for host %s", self.host)
        first_connection = None
        try:
            first_connection = session.cluster.connection_factory(
                self.host.endpoint,
                host_conn=self,
                on_orphaned_stream_released=self.on_orphaned_stream_released)
            if not self._register_pending_connection(first_connection):
                if not self._shutdown_owns_pending_connection(
                        first_connection):
                    first_connection.close()
                raise ConnectionException(
                    "Pool for %s was shutdown during initialization" %
                    (self.host,),
                    self.host)
            if first_connection.is_closed:
                raise _startup_close_error(first_connection, self.host.endpoint)
            log.debug("First connection created to %s for shard_id=%i", self.host, first_connection.features.shard_id)
            self._keyspace, keyspace_generation = \
                self._session_keyspace_snapshot(session)

            while self._keyspace:
                try:
                    _set_keyspace_blocking(
                        first_connection,
                        self._keyspace,
                        session.cluster.connect_timeout)
                    break
                except _REQUEST_VALIDATION_EXCEPTIONS:
                    current_keyspace, current_generation = \
                        self._session_keyspace_snapshot(session)
                    if (
                            current_keyspace == self._keyspace and
                            (
                                keyspace_generation is None or
                                current_generation == keyspace_generation)):
                        raise
                    # The failed USE targeted a keyspace generation that is no
                    # longer current. Reconcile on this still-owned socket
                    # before deciding that pool construction failed.
                    self._keyspace = current_keyspace
                    keyspace_generation = current_generation

            with self._lock:
                if self.is_shutdown:
                    raise ConnectionException(
                        "Pool for %s was shutdown during initialization" %
                        (self.host,),
                        self.host)
                if not self._remove_pending_connection_locked(
                        first_connection):
                    raise ConnectionException(
                        "Initial connection ownership was lost",
                        self.host.endpoint)
                self._connections[
                    first_connection.features.shard_id] = first_connection

            if first_connection.features.sharding_info and not self._session.cluster.shard_aware_options.disable:
                self.host.sharding_info = first_connection.features.sharding_info
                self._open_connections_for_all_shards(first_connection.features.shard_id)
            self.tablets_routing_v1 = first_connection.features.tablets_routing_v1
        except BaseException:
            # A constructor failure leaves no published pool for Session to
            # shut down.  Take ownership here, including any shard attempts
            # which may already have been submitted.
            self.shutdown()
            raise

        log.debug("Finished initializing connection for host %s", self.host)

    def _is_shard_aware(self):
        return bool(
            self.host.sharding_info and
            not self._session.cluster.shard_aware_options.disable)

    def _remove_pending_connection_locked(self, connection):
        for i, pending in enumerate(self._pending_connections):
            if pending is connection:
                del self._pending_connections[i]
                return True
        return False

    def _register_pending_connection(self, connection):
        """Publish a factory-owned socket atomically with shutdown."""
        with self._lock:
            if self.is_shutdown:
                return False
            try:
                if connection._owning_pool is None:
                    connection._owning_pool = self
            except AttributeError:
                # Preserve compatibility with third-party Connection
                # stand-ins which do not expose the optional owner hook.
                pass
            if any(
                    pending is connection
                    for pending in self._pending_connections):
                # Connection.factory keeps a successful candidate registered
                # until its caller adopts it. Recognize that handoff without
                # inserting the same socket twice.
                return True
            self._pending_connections.append(connection)
            return True

    def _unregister_pending_connection(self, connection):
        """Release factory ownership without racing adoption or shutdown."""
        with self._lock:
            return self._remove_pending_connection_locked(connection)

    def _shutdown_owns_pending_connection(self, connection):
        """Return whether shutdown already claimed this pending candidate."""
        with self._lock:
            return id(connection) in self._shutdown_owned_pending_ids

    def _handoff_replacement_failure(self, exc):
        """
        Hand an unusable replacement to host recovery.

        Validate in the cluster-wide Cluster-lock, Host-lock, then Session-lock
        order. The user-extensible conviction policy runs without those locks;
        the pool is revalidated before committing the forced DOWN transition.
        """
        try:
            cluster_lock = self._session.cluster._lock
            host_lock = self.host.lock
            session_lock = self._session._lock
            pools = self._session._pools
        except (AttributeError, ReferenceError):
            cluster_lock = None
            host_lock = None
            session_lock = None
            pools = None
        if not hasattr(cluster_lock, '__enter__'):
            cluster_lock = None
        if not hasattr(host_lock, '__enter__'):
            host_lock = None
        if not hasattr(session_lock, '__enter__'):
            session_lock = None

        # Some unit-test and third-party Session stand-ins do not expose a real
        # pool mapping.  Preserve the historical behavior for those objects.
        has_current_pool_fence = (
            isinstance(pools, dict) and
            session_lock is not None)
        cluster = self._session.cluster
        down_locked = getattr(type(cluster), '_on_down_locked', None)
        uses_default_hooks = getattr(
            cluster, '_uses_default_failure_hooks', None)
        can_use_private_down = bool(
            down_locked is not None and
            callable(uses_default_hooks) and
            uses_default_hooks() is True)

        if not has_current_pool_fence:
            if self.is_shutdown:
                return False
            _signal_connection_failure(
                cluster,
                self.host,
                exc,
                is_host_addition=False,
                expect_host_to_be_down=True,
                force=True)
            return True

        locks = [
            lock for lock in (cluster_lock, host_lock, session_lock)
            if lock is not None and (
                lock is not session_lock or has_current_pool_fence)]

        def is_current_locked():
            if self.is_shutdown:
                return False
            if (
                    self._host_recovery_epoch is not None and
                    getattr(
                        self.host,
                        '_recovery_epoch',
                        self._host_recovery_epoch) !=
                    self._host_recovery_epoch):
                return False
            current_pool = pools.get(self.host)
            if current_pool is None:
                for pool_host, pool in tuple(pools.items()):
                    if pool_host is self.host:
                        current_pool = pool
                        break
            generation_is_current = self._pool_generation is None
            if self._pool_generation is not None:
                get_generation = getattr(
                    self._session, '_get_pool_generation_locked', None)
                if get_generation is not None:
                    generation_is_current = (
                        get_generation(self.host) ==
                        self._pool_generation)
            candidate_is_owned = bool(
                current_pool is self or
                self._pool_generation is not None)
            return not (
                self._session.is_shutdown or
                not generation_is_current or
                not candidate_is_owned)

        with ExitStack() as stack:
            for lock in locks:
                stack.enter_context(lock)
            if not is_current_locked():
                return False

        if not can_use_private_down:
            # Notify legacy/custom public hooks through their compatible
            # signature first. If they cannot express the newer
            # authoritative ``force`` transition, commit the inherited
            # private DOWN path while this pool is still current.
            recovery_epoch = getattr(self.host, '_recovery_epoch', None)
            if not isinstance(recovery_epoch, int):
                recovery_epoch = None
            try:
                _signal_connection_failure(
                    cluster,
                    self.host,
                    exc,
                    is_host_addition=False,
                    expect_host_to_be_down=True,
                    force=True)
            finally:
                if down_locked is not None:
                    with ExitStack() as stack:
                        for lock in locks:
                            stack.enter_context(lock)
                        recovery_was_started = bool(
                            recovery_epoch is not None and
                            getattr(
                                self.host,
                                '_recovery_epoch',
                                recovery_epoch) != recovery_epoch)
                        if (
                                is_current_locked() and
                                not recovery_was_started):
                            down_locked(
                                cluster,
                                self.host,
                                is_host_addition=False,
                                expect_host_to_be_down=True,
                                force=True)
            return True

        # Conviction policies may call back into Session/Cluster code.  This
        # recovery handoff is authoritative, so a broken user policy must not
        # strand an empty pool without a reconnector.
        try:
            self.host.signal_connection_failure(exc)
        except BaseException:
            log.exception(
                "Conviction policy failed while handing replacement failure "
                "for host %s to recovery",
                self.host)

        with ExitStack() as stack:
            for lock in locks:
                stack.enter_context(lock)
            if not is_current_locked():
                return False
            down_locked(
                cluster,
                self.host,
                is_host_addition=False,
                expect_host_to_be_down=True,
                force=True)
            return True

    def _retire_connection_locked(self, connection):
        """
        Stop using a connection and atomically decide who closes it.

        The pool lock must be held by the caller.  Taking the connection lock
        while publishing it to `_trash` closes the race with its last return.
        """
        with connection.lock:
            connection._pool_retired = True
            remaining = self._connection_active_uses_locked(connection)
            if remaining <= 0:
                return True, remaining
            self._trash.add(connection)
            return False, remaining

    @staticmethod
    def _connection_active_uses_locked(connection):
        """
        Count work which still requires an open retired connection.

        Continuous paging keeps its stream after the initial request has been
        returned to the pool, so it is not represented by ``in_flight``.
        """
        pending_requests = max(
            connection.in_flight -
            len(connection.orphaned_request_ids),
            0)
        paging_sessions = len(getattr(
            connection, '_continuous_paging_sessions', ()))
        return pending_requests + paging_sessions

    @staticmethod
    def _connection_needs_replacement(connection):
        return bool(
            connection.is_closed or
            connection.is_defunct or
            getattr(connection, '_pool_retired', False) is True or
            connection.orphaned_threshold_reached)

    def _schedule_connection_to_missing_shard(
            self, shard_id, is_replacement=False, track_future=False):
        """
        Start at most one connection attempt for a shard.

        A replacement request promotes an already-running optimistic attempt,
        so its eventual failure is handed to host recovery.
        """
        with self._lock:
            if self.is_shutdown:
                return None
            if is_replacement:
                current = self._connections.get(shard_id)
                if (
                        current is not None and
                        not self._connection_needs_replacement(current)):
                    return None

            attempt = self._shard_connection_attempts.get(shard_id)
            if attempt is not None:
                if is_replacement:
                    attempt['is_replacement'] = True
                return attempt.get('future')

            token = object()
            attempt = {
                'token': token,
                'is_replacement': is_replacement,
                'future': None,
            }
            self._shard_connection_attempts[shard_id] = attempt
            self._connecting.add(shard_id)

        # Session.submit may be implemented by an application and execute the
        # work synchronously.  Never invoke it while holding the pool lock.
        try:
            future = self._session.submit(
                self._open_connection_to_missing_shard,
                shard_id,
                token)
        except BaseException:
            self._finish_shard_connection_attempt(shard_id, token)
            raise

        with self._lock:
            current_attempt = self._shard_connection_attempts.get(shard_id)
            if (
                    current_attempt is not None and
                    current_attempt['token'] is token):
                if future is None:
                    del self._shard_connection_attempts[shard_id]
                    self._connecting.discard(shard_id)
                    return None
                current_attempt['future'] = future
                if track_future and isinstance(future, Future):
                    self._shard_connections_futures.append(future)

        if isinstance(future, Future):
            def attempt_completed(completed_future):
                if not completed_future.cancelled():
                    return
                should_recover = False
                was_replacement = self._finish_shard_connection_attempt(
                    shard_id, token)
                with self._lock:
                    should_recover = bool(
                        was_replacement and
                        not self.is_shutdown)
                if should_recover:
                    try:
                        self._handoff_replacement_failure(
                            ConnectionException(
                                "Shard replacement task was cancelled",
                                self.host))
                    except BaseException:
                        log.exception(
                            "Failed handing cancelled shard replacement for "
                            "host %s to recovery",
                            self.host)

            future.add_done_callback(attempt_completed)
        return future

    def _finish_shard_connection_attempt(self, shard_id, token):
        with self._lock:
            attempt = self._shard_connection_attempts.get(shard_id)
            if attempt is None or attempt['token'] is not token:
                return False
            is_replacement = attempt['is_replacement']
            del self._shard_connection_attempts[shard_id]
            self._connecting.discard(shard_id)
            return is_replacement

    def _claim_failed_shard_attempt(self, shard_id, token):
        """
        Atomically classify a failed attempt against replacement promotion.

        Replacement attempts remain published until host recovery is
        signaled.  Opportunistic attempts are removed immediately so a later
        request can retry.  This makes promotion and failure linearizable.
        """
        with self._lock:
            attempt = self._shard_connection_attempts.get(shard_id)
            if attempt is None or attempt['token'] is not token:
                return False
            if attempt['is_replacement']:
                return True
            del self._shard_connection_attempts[shard_id]
            self._connecting.discard(shard_id)
            return False

    def _get_connection_for_routing_key(
            self, routing_key=None, keyspace=None, table=None,
            allow_keyspace_mismatch=False):
        if self.is_shutdown:
            raise ConnectionException(
                "Pool for %s is shutdown" % (self.host,), self.host)

        shard_id = None
        if not self._session.cluster.shard_aware_options.disable and self.host.sharding_info and routing_key:
            t = self._session.cluster.metadata.token_map.token_class.from_key(routing_key)
            
            shard_id = None
            if self.tablets_routing_v1 and table is not None:
                if keyspace is None:
                    keyspace = self._keyspace

                tablet = self._session.cluster.metadata._tablets.get_tablet_for_key(keyspace, table, t)

                if tablet is not None:
                    for replica in tablet.replicas:
                        if replica[0] == self.host.host_id:
                            shard_id = replica[1]
                            break

            if shard_id is None:
                shard_id = self.host.sharding_info.shard_id_from_token(t.value)

        with self._lock:
            conn = self._connections.get(shard_id)

        def keyspace_is_usable(connection):
            return bool(
                allow_keyspace_mismatch or
                getattr(
                    connection,
                    '_pool_keyspace_mismatch',
                    False) is not True)

        # Routed queries normally have a healthy connection for their target
        # shard. Keep that path O(1); the borrow-side validation below closes
        # races with replacement or shutdown after this snapshot.
        if (
                shard_id is not None and
                conn and
                not self._connection_needs_replacement(conn) and
                keyspace_is_usable(conn)):
            return conn

        with self._lock:
            connections = list(self._connections.values())

        # A connection can close while idle, so there may be no request
        # callback to return it to the pool and trigger replacement. Retire
        # unhealthy mappings during selection, including unkeyed requests.
        unhealthy_connections = []
        seen = set()
        for connection in connections:
            if (
                    id(connection) not in seen and
                    (connection.is_closed or connection.is_defunct)):
                seen.add(id(connection))
                unhealthy_connections.append(connection)
        for connection in unhealthy_connections:
            self.return_connection(
                connection, stream_was_orphaned=True)

        if unhealthy_connections:
            if self.is_shutdown:
                raise ConnectionException(
                    "Pool for %s is shutdown" % (self.host,),
                    self.host)
            with self._lock:
                conn = self._connections.get(shard_id)
                connections = list(self._connections.values())

        shard_aware = self._is_shard_aware()
        if shard_aware:
            for connection in connections:
                if (
                        not (connection.is_closed or connection.is_defunct) and
                        connection.orphaned_threshold_reached):
                    try:
                        self._schedule_connection_to_missing_shard(
                            connection.features.shard_id,
                            is_replacement=True)
                    except BaseException as exc:
                        self._handoff_replacement_failure(exc)
                        if not isinstance(exc, Exception):
                            raise
        else:
            regular_replacement = next(
                (
                    connection for connection in connections
                    if (
                        not (
                            connection.is_closed or
                            connection.is_defunct) and
                        connection.orphaned_threshold_reached)
                ),
                None)
            if regular_replacement is not None:
                self._submit_regular_replacement(regular_replacement)

        # missing shard aware connection to shard_id, let's schedule an
        # optimistic try to connect to it
        if shard_id is not None:
            needs_replacement = bool(
                conn and self._connection_needs_replacement(conn))
            if conn is None or needs_replacement:
                try:
                    self._schedule_connection_to_missing_shard(
                        shard_id,
                        is_replacement=needs_replacement)
                except BaseException as exc:
                    if needs_replacement:
                        self._handoff_replacement_failure(exc)
                    else:
                        log.warning(
                            "Unable to open an optional connection to missing "
                            "shard %i on host %s; using another shard",
                            shard_id,
                            self.host,
                            exc_info=True)
                    if not isinstance(exc, Exception):
                        raise
                if needs_replacement:
                    log.debug(
                        "Connection to shard_id=%i needs replacement on host %s (%s/%i)",
                        shard_id,
                        self.host,
                        len(connections),
                        self.host.sharding_info.shards_count
                    )
                else:
                    # rate controlled optimistic attempt to connect to a
                    # missing shard
                    log.debug(
                        "Trying to connect to missing shard_id=%i on host %s (%s/%i)",
                        shard_id,
                        self.host,
                        len(connections),
                        self.host.sharding_info.shards_count
                    )

        # Session.submit is intentionally allowed to execute synchronously,
        # and a fast executor can also install a replacement before scheduling
        # returns. Select from the current mappings rather than the stale
        # pre-scheduling snapshot.
        with self._lock:
            if self.is_shutdown:
                raise ConnectionException(
                    "Pool for %s is shutdown" % (self.host,),
                    self.host)
            conn = self._connections.get(shard_id)
            connections = list(self._connections.values())

        if (
                conn and
                not self._connection_needs_replacement(conn) and
                keyspace_is_usable(conn)):
            return conn
        active_connections = [
            connection for connection in connections
            if (
                not self._connection_needs_replacement(connection) and
                keyspace_is_usable(connection))]
        if active_connections:
            return random.choice(active_connections)
        awaiting_replacement = [
            connection for connection in connections
            if not (
                connection.is_closed or
                connection.is_defunct or
                getattr(connection, '_pool_retired', False) is True or
                not keyspace_is_usable(connection))]
        if awaiting_replacement:
            return random.choice(awaiting_replacement)
        raise NoConnectionsAvailable(
            "No open connections to host %s" % (self.host,))

    def borrow_connection(
            self, timeout, routing_key=None, keyspace=None, table=None,
            allow_keyspace_mismatch=False):
        def select_connection():
            return self._get_connection_for_routing_key(
                routing_key,
                keyspace,
                table,
                allow_keyspace_mismatch=allow_keyspace_mismatch)

        conn = select_connection()
        start = time.time()
        remaining = timeout
        last_retry = False
        while True:
            if (
                    conn.is_closed or
                    conn.is_defunct or
                    getattr(conn, '_pool_retired', False) is True or
                    (
                        not allow_keyspace_mismatch and
                        getattr(
                            conn,
                            '_pool_keyspace_mismatch',
                            False) is True)):
                # The connection might have failed or been superseded in the
                # meantime; select the current mapping and schedule repair.
                conn = select_connection()
            with self._lock:
                if self.is_shutdown:
                    raise ConnectionException(
                        "Pool for %s is shutdown" % (self.host,),
                        self.host)
                current = self._connections.get(
                    conn.features.shard_id)
                with conn.lock:
                    if (
                            current is conn and
                            not (conn.is_closed or conn.is_defunct) and
                            getattr(
                                conn,
                                '_pool_retired',
                                False) is not True and
                            (
                                allow_keyspace_mismatch or
                                getattr(
                                    conn,
                                    '_pool_keyspace_mismatch',
                                    False) is not True) and
                            conn.in_flight < conn.max_request_id):
                        conn.in_flight += 1
                        return conn, conn.get_request_id()
            if current is not conn:
                conn = select_connection()
                continue
            if timeout is not None:
                remaining = timeout - time.time() + start
                if remaining < 0:
                    # When timeout reached we try to get connection last time and break if it fails
                    if last_retry:
                        break
                    last_retry = True
                    continue
            retry_selection = False
            with self._stream_available_condition:
                # Recheck after acquiring the condition so a stream return or
                # shutdown between the first check and wait cannot be lost.
                with self._lock:
                    if self.is_shutdown:
                        raise ConnectionException(
                            "Pool for %s is shutdown" % (self.host,),
                            self.host)
                    current = self._connections.get(
                        conn.features.shard_id)
                    with conn.lock:
                        retry_selection = bool(
                            current is not conn or
                            conn.is_closed or
                            conn.is_defunct or
                            getattr(conn, '_pool_retired', False) is True or
                            (
                                not allow_keyspace_mismatch and
                                getattr(
                                    conn,
                                    '_pool_keyspace_mismatch',
                                    False) is True) or
                            conn.in_flight < conn.max_request_id)
                if not retry_selection:
                    self._stream_available_condition.wait(remaining)
            if retry_selection:
                # Selection may synchronously install a replacement and
                # notify this same non-reentrant Condition.
                conn = select_connection()
                continue

        raise NoConnectionsAvailable("All request IDs are currently in use")

    def return_connection(self, connection, stream_was_orphaned=False):
        if not stream_was_orphaned:
            with connection.lock:
                connection.in_flight -= 1
            with self._stream_available_condition:
                self._stream_available_condition.notify()

        connection_is_bad = False
        connection_error = None
        connection_was_retired = False
        retired_connection_needs_close = False
        should_signal_error = False

        # Snapshot shutdown and connection health atomically in the pool ->
        # connection lock order used by retirement. This prevents a deliberate
        # shutdown close from being mistaken for a host failure.
        with self._lock:
            if self.is_shutdown:
                return
            with connection.lock:
                connection_is_bad = bool(
                    connection.is_defunct or connection.is_closed)
                connection_error = connection.last_error
                connection_was_retired = (
                    getattr(connection, '_pool_retired', False) is True)
                retired_connection_needs_close = bool(
                    connection_was_retired and
                    not connection.is_closed and
                    self._connection_active_uses_locked(connection) <= 0)
                should_signal_error = bool(
                    connection_is_bad and
                    not connection_was_retired and
                    not connection.signaled_error)
                if should_signal_error:
                    connection.signaled_error = True

        if connection_is_bad and connection_was_retired:
            # A replacement deliberately closed this connection after its
            # last request decremented in_flight but before this health
            # classification.  It is not evidence that the host failed.
            with self._lock:
                self._trash.discard(connection)
            if retired_connection_needs_close:
                connection.close()
            return

        if connection_is_bad:
            is_down = False
            if should_signal_error:
                log.debug("Defunct or closed connection (%s) returned to pool, potentially "
                          "marking host %s as down", id(connection), self.host)
                try:
                    is_down = self.host.signal_connection_failure(
                        connection_error)
                except BaseException:
                    # Conviction policies are user-extensible. If one fails,
                    # release this connection's claim so a later return can
                    # retry instead of permanently suppressing host recovery.
                    with self._lock:
                        if not self.is_shutdown:
                            with connection.lock:
                                connection.signaled_error = False
                    raise

            if self.shutdown_on_error and not is_down:
                is_down = True

            if is_down:
                # Conviction callbacks are user-extensible and run without
                # pool locks. A healthy replacement may have been installed
                # while the callback was blocked; atomically fence adoption
                # and revalidate before committing pool shutdown.
                if not self._shutdown_if_connection_failure_current(
                        connection):
                    return
                self._session.cluster.on_down(self.host, is_host_addition=False)
                return

            connection.close()
            shard_id = connection.features.shard_id
            schedule_shard_replacement = False
            schedule_regular_replacement = False
            with self._lock:
                self._trash.discard(connection)
                current = self._connections.get(shard_id)
                if current is connection:
                    del self._connections[shard_id]
                    current = None

                if self.is_shutdown:
                    return

                # A returned retired connection must not evict or replace the
                # healthy connection which superseded it.
                if current is None:
                    if self._is_shard_aware():
                        schedule_shard_replacement = True
                    elif not self._is_replacing:
                        self._is_replacing = True
                        schedule_regular_replacement = True

            if schedule_shard_replacement:
                try:
                    self._schedule_connection_to_missing_shard(
                        shard_id,
                        is_replacement=True)
                except BaseException as exc:
                    self._handoff_replacement_failure(exc)
                    if not isinstance(exc, Exception):
                        raise
            elif schedule_regular_replacement:
                self._submit_regular_replacement(connection, claimed=True)
            return

        self.on_connection_released(connection)

    def on_connection_released(self, connection):
        """
        Close a retired connection after its final active use is released.

        Some internal users, notably successful heartbeats and continuous
        paging, release connection state without going through
        :meth:`return_connection`; they notify the pool through this hook.
        """
        close_connection = False
        with self._lock:
            if connection in self._trash:
                with connection.lock:
                    no_active_uses = (
                        self._connection_active_uses_locked(connection) <= 0)
                if no_active_uses:
                    self._trash.remove(connection)
                    close_connection = True

        if close_connection:
            log.debug(
                "Closing trashed connection (%s) to %s",
                id(connection),
                self.host)
            connection.close()

    def on_orphaned_stream_released(self):
        """
        Called when a response for an orphaned stream (timed out on the client
        side) was received.
        """
        # The callback predates passing the releasing Connection. Scan only
        # retired sockets; unlike the routed query path this is not per-query.
        with self._lock:
            retired_connections = list(self._trash)
        for connection in retired_connections:
            self.on_connection_released(connection)
        with self._stream_available_condition:
            self._stream_available_condition.notify()

    def _submit_regular_replacement(self, connection, claimed=False):
        """
        Submit one replacement for a non-shard-aware connection.

        ``claimed`` means the caller already set ``_is_replacing`` while
        removing a failed mapping.
        """
        if not claimed:
            shard_id = connection.features.shard_id
            with self._lock:
                if self.is_shutdown or self._is_replacing:
                    return None
                current = self._connections.get(shard_id)
                if current is not connection:
                    return None
                self._is_replacing = True

        try:
            future = self._session.submit(self._replace, connection)
        except Exception as exc:
            with self._lock:
                self._is_replacing = False
            self._handoff_replacement_failure(exc)
            return None
        except BaseException as exc:
            with self._lock:
                self._is_replacing = False
            try:
                self._handoff_replacement_failure(exc)
            except BaseException:
                log.exception(
                    "Additional failure handing replacement submission "
                    "cancellation to host recovery for %s",
                    self.host)
            raise

        if future is None:
            with self._lock:
                self._is_replacing = False
            return None

        if isinstance(future, Future):
            with self._lock:
                if self._is_replacing:
                    self._regular_replacement_future = future

            def replacement_completed(completed_future):
                cancelled_replacement = False
                with self._lock:
                    if self._regular_replacement_future is not \
                            completed_future:
                        return
                    self._regular_replacement_future = None
                    if (
                            completed_future.cancelled() and
                            self._is_replacing and
                            not self.is_shutdown):
                        self._is_replacing = False
                        cancelled_replacement = True

                if cancelled_replacement:
                    try:
                        self._handoff_replacement_failure(
                            ConnectionException(
                                "Regular replacement task was cancelled",
                                self.host))
                    except BaseException:
                        log.exception(
                            "Failed handing cancelled replacement for host "
                            "%s to recovery",
                            self.host)

            future.add_done_callback(replacement_completed)
        return future

    def _replace(self, connection):
        replacement = None
        replacement_is_pending = False
        replacement_was_adopted = False
        retired_connection = None
        close_retired_connection = False
        abandon_replacement = False
        shard_id = connection.features.shard_id

        try:
            with self._lock:
                if self.is_shutdown:
                    return

                log.debug("Replacing connection (%s) to %s", id(connection), self.host)
                current = self._connections.get(shard_id)
                if current is not None and current is not connection and not (
                        current.is_closed or
                        current.is_defunct or
                        current.orphaned_threshold_reached):
                    # This work item is stale; preserve the newer healthy
                    # mapping instead of deleting it by shard id.
                    self._is_replacing = False
                    return
                shard_aware = self._is_shard_aware()
                if shard_aware and current is not None:
                    del self._connections[shard_id]
                    retired_connection = current
                    (
                        close_retired_connection,
                        _
                    ) = self._retire_connection_locked(current)
                if shard_aware:
                    # `_is_replacing` is retained only for non-shard pools.
                    self._is_replacing = False

            if close_retired_connection:
                retired_connection.close()
            retired_connection = None
            close_retired_connection = False

            if shard_aware:
                self._schedule_connection_to_missing_shard(
                    shard_id,
                    is_replacement=True)
                return

            replacement = self._session.cluster.connection_factory(
                self.host.endpoint,
                host_conn=self,
                on_orphaned_stream_released=self.on_orphaned_stream_released)
            if not self._register_pending_connection(replacement):
                if not self._shutdown_owns_pending_connection(replacement):
                    replacement.close()
                return
            replacement_is_pending = True
            if replacement.is_closed:
                raise _startup_close_error(replacement, self.host.endpoint)

            while True:
                with self._lock:
                    if self.is_shutdown:
                        # shutdown() drained the pending list and owns the
                        # replacement.
                        return
                    keyspace = self._keyspace
                    keyspace_generation = self._keyspace_generation

                if keyspace:
                    try:
                        _set_keyspace_blocking(
                            replacement,
                            keyspace,
                            self._session.cluster.connect_timeout)
                    except _REQUEST_VALIDATION_EXCEPTIONS:
                        # A dropped/invalid Session keyspace does not make the
                        # host unavailable. Keep ownership so a later USE can
                        # repair the Session, but quarantine this socket from
                        # ordinary requests until it selects a keyspace.
                        replacement._pool_keyspace_mismatch = True
                        log.warning(
                            "Unable to set keyspace %s on replacement "
                            "connection to %s; quarantining the open "
                            "connection until a successful USE",
                            keyspace,
                            self.host.endpoint)
                    else:
                        replacement._pool_keyspace_mismatch = False
                if replacement.is_closed:
                    raise _startup_close_error(
                        replacement,
                        self.host.endpoint)

                with self._lock:
                    if self.is_shutdown:
                        # shutdown() drained the pending list and owns the
                        # replacement.
                        return
                    if keyspace_generation != self._keyspace_generation:
                        continue
                    current = self._connections.get(shard_id)
                    abandon_replacement = bool(
                        current is not None and
                        current is not connection and
                        not self._connection_needs_replacement(current))
                    if not self._remove_pending_connection_locked(replacement):
                        raise ConnectionException(
                            "Replacement connection ownership was lost",
                            self.host.endpoint)
                    replacement_is_pending = False

                    if abandon_replacement:
                        # Another worker installed a healthy connection while
                        # this factory/setup was running. Release the pending
                        # candidate without disturbing the newer mapping.
                        self._is_replacing = False
                    else:
                        if (
                                current is not None and
                                current is not replacement):
                            retired_connection = current
                            (
                                close_retired_connection,
                                _
                            ) = self._retire_connection_locked(current)
                        if (
                                replacement.features.shard_id != shard_id and
                                shard_id in self._connections and
                                self._connections.get(shard_id) is current):
                            del self._connections[shard_id]
                        self._connections[
                            replacement.features.shard_id] = replacement
                        replacement_was_adopted = True
                        self._is_replacing = False

                if abandon_replacement:
                    replacement.close()
                    return
                break

            if close_retired_connection:
                retired_connection.close()

            with self._stream_available_condition:
                self._stream_available_condition.notify_all()
        except BaseException as exc:
            shutdown_owns_replacement = False
            with self._lock:
                if replacement_is_pending:
                    if not self._remove_pending_connection_locked(replacement):
                        # shutdown() already drained and owns closing it.
                        shutdown_owns_replacement = self.is_shutdown
                is_shutdown = self.is_shutdown

            if (
                    replacement is not None and
                    not replacement_was_adopted and
                    not shutdown_owns_replacement):
                replacement.close()

            if not isinstance(exc, Exception):
                if not is_shutdown:
                    try:
                        self._handoff_replacement_failure(exc)
                    except BaseException:
                        log.exception(
                            "Additional failure handing replacement "
                            "cancellation to host recovery for %s",
                            self.host)
                with self._lock:
                    self._is_replacing = False
                raise

            if is_shutdown:
                return

            # A replacement failure cannot leave this pool serviceable.  Hand
            # it to the host reconnector, which supplies the retry backoff.
            try:
                self._handoff_replacement_failure(exc)
            except BaseException:
                with self._lock:
                    self._is_replacing = False
                raise
            if isinstance(exc, _ConnectionClosedDuringStartup):
                log.info(
                    "Replacement connection to %s closed cleanly during "
                    "startup; handing recovery to the host reconnector",
                    self.host.endpoint)
            else:
                log.warning(
                    "Failed reconnecting %s; handing recovery to the host "
                    "reconnector",
                    self.host.endpoint,
                    exc_info=True)

    def _begin_shutdown_locked(self):
        if self.is_shutdown:
            return None

        futures_to_cancel = list(self._shard_connections_futures)
        if isinstance(self._regular_replacement_future, Future):
            futures_to_cancel.append(self._regular_replacement_future)
        connections_to_close = list(self._connections.values())
        connections_to_close.extend(self._pending_connections)
        self._shutdown_owned_pending_ids.update(
            id(connection)
            for connection in self._pending_connections)
        connections_to_close.extend(self._excess_connections)
        connections_to_close.extend(self._trash)
        current_keyspace_update = self._keyspace_update_current
        claimed_keyspace_update = False
        if current_keyspace_update is not None:
            shutdown_error = ConnectionException(
                "Pool for %s is shutdown" % (self.host,),
                self.host)
            claimed_keyspace_update = self._claim_keyspace_update(
                current_keyspace_update, shutdown_error)

        # Publish shutdown only after the active keyspace generation has been
        # atomically classified. A response which already owned the update
        # lock wins before shutdown; no response can win in a post-commit gap.
        self.is_shutdown = True
        self._connections.clear()
        self._pending_connections.clear()
        self._excess_connections.clear()
        self._trash.clear()
        self._shard_connections_futures = []
        self._regular_replacement_future = None
        self._shard_connection_attempts.clear()
        self._connecting.clear()
        self._is_replacing = False
        return (
            futures_to_cancel,
            connections_to_close,
            current_keyspace_update,
            claimed_keyspace_update)

    def _finish_shutdown(self, shutdown_state):
        (
            futures_to_cancel,
            connections_to_close,
            current_keyspace_update,
            claimed_keyspace_update
        ) = shutdown_state

        with self._stream_available_condition:
            self._stream_available_condition.notify_all()

        # connection.close can call pool.return_connection, which will
        #  obtain self._lock via self._stream_available_condition.
        # So, it never should be called within self._lock context
        seen = set()
        for connection in connections_to_close:
            if id(connection) in seen:
                continue
            seen.add(id(connection))
            log.debug("Closing connection (%s) to %s", id(connection), self.host)
            connection.close()

        # Future cancellation may synchronously invoke callbacks which reenter
        # or block. All sockets are already closed and waiters notified before
        # any such callback can delay shutdown.
        for future in futures_to_cancel:
            future.cancel()

        if claimed_keyspace_update:
            self._deliver_keyspace_update(current_keyspace_update)
        elif current_keyspace_update is None:
            # shutdown may win before the runner publishes its current
            # generation.  Requesting a run makes every queued callback
            # complete in FIFO order with the shutdown error.
            self._request_next_keyspace_update()

    def _shutdown_if_connection_failure_current(self, connection):
        with self._lock:
            if self.is_shutdown:
                return False
            current = self._connections.get(
                connection.features.shard_id)
            with connection.lock:
                retired = (
                    getattr(connection, '_pool_retired', False) is True)
            if self._is_shard_aware():
                replacement_candidates = (current,)
            else:
                # A regular Scylla connection can land on a different random
                # shard id even though this pool is operating in non-shard
                # mode. Any healthy mapped socket supersedes the failed one.
                replacement_candidates = tuple(
                    self._connections.values())
            healthy_replacement = any(
                candidate is not None and
                candidate is not connection and
                not self._connection_needs_replacement(candidate)
                for candidate in replacement_candidates)
            if retired or healthy_replacement:
                return False
            shutdown_state = self._begin_shutdown_locked()

        log.debug(
            "Shutting down connections to %s after connection failure",
            self.host)
        self._finish_shutdown(shutdown_state)
        return True

    def shutdown(self):
        log.debug("Shutting down connections to %s", self.host)
        with self._lock:
            shutdown_state = self._begin_shutdown_locked()
        if shutdown_state is None:
            return
        self._finish_shutdown(shutdown_state)

    def _close_excess_connections(self):
        with self._lock:
            if not self._excess_connections:
                return
            conns = self._excess_connections.copy()
            self._excess_connections.clear()

        for c in conns:
            log.debug("Closing excess connection (%s) to %s", id(c), self.host)
            c.close()

    def disable_advanced_shard_aware(self, secs):
        log.warning("disabling advanced_shard_aware for %i seconds, could be that this client is behind NAT?", secs)
        self.advanced_shardaware_block_until = max(time.time() + secs, self.advanced_shardaware_block_until)

    def _get_shard_aware_endpoint(self):
        """
        Return an endpoint for the advertised shard-aware port, if usable.

        Plaintext clusters use shard_aware_port. SSL-enabled clusters use only
        shard_aware_port_ssl; if it is absent, return None so the pool opens a
        regular SSL connection instead of falling back to the plaintext port.
        Explicit ssl_options={}, like ssl_context, marks the cluster SSL-enabled.
        """
        if (self.advanced_shardaware_block_until and self.advanced_shardaware_block_until > time.time()) or \
           self._session.cluster.shard_aware_options.disable_shardaware_port:
            return None

        cluster = self._session.cluster
        ssl_enabled = cluster.ssl_context is not None or cluster.ssl_options is not None

        endpoint = None
        if ssl_enabled and self.host.sharding_info.shard_aware_port_ssl:
            endpoint = copy.copy(self.host.endpoint)
            endpoint._port = self.host.sharding_info.shard_aware_port_ssl
        elif not ssl_enabled and self.host.sharding_info.shard_aware_port:
            endpoint = copy.copy(self.host.endpoint)
            endpoint._port = self.host.sharding_info.shard_aware_port

        return endpoint

    def _open_connection_to_missing_shard(self, shard_id, attempt_token=None):
        # Direct callers from older integrations did not pass a token.  Give
        # them ownership only when there is no real attempt already running.
        if attempt_token is None:
            with self._lock:
                attempt = self._shard_connection_attempts.get(shard_id)
                if attempt is None:
                    attempt_token = object()
                    self._shard_connection_attempts[shard_id] = {
                        'token': attempt_token,
                        'is_replacement': False,
                        'future': None,
                    }
                    self._connecting.add(shard_id)
                else:
                    # A direct/stale invocation must not clear the live
                    # attempt's marker when it finishes.
                    attempt_token = object()

        try:
            result = self._open_connection_to_missing_shard_impl(shard_id)
        except Exception as exc:
            clean_startup_close = isinstance(
                exc,
                _ConnectionClosedDuringStartup)
            if clean_startup_close:
                # A clean close during startup is authoritative maintenance
                # evidence, even if this began as an optimistic attempt. Keep
                # the marker published until the handoff completes.
                is_replacement = True
            else:
                is_replacement = self._claim_failed_shard_attempt(
                    shard_id,
                    attempt_token)
            try:
                if is_replacement:
                    self._handoff_replacement_failure(exc)
                    log.warning(
                        "Failed replacing connection to shard %i on host %s; "
                        "handing recovery to the host reconnector",
                        shard_id,
                        self.host,
                        exc_info=(
                            type(exc),
                            exc,
                            exc.__traceback__))
                    return None

                raise
            finally:
                self._finish_shard_connection_attempt(
                    shard_id,
                    attempt_token)
        except BaseException as exc:
            is_replacement = self._claim_failed_shard_attempt(
                shard_id,
                attempt_token)
            try:
                if is_replacement:
                    try:
                        self._handoff_replacement_failure(exc)
                    except BaseException:
                        log.exception(
                            "Additional failure handing interrupted shard "
                            "replacement to host recovery for %s",
                            self.host)
            finally:
                self._finish_shard_connection_attempt(
                    shard_id,
                    attempt_token)
            raise
        else:
            self._finish_shard_connection_attempt(shard_id, attempt_token)
            return result

    def _open_connection_to_missing_shard_impl(self, shard_id):
        """
        Creates a new connection, checks its shard_id and populates our shard
        aware connections if the current shard_id is missing a connection.

        The `shard_id` parameter is only here to control parallelism on
        attempts to connect. This means that if this attempt finds another
        missing shard_id, we will keep it anyway.

        NOTE: This is an optimistic implementation since we cannot control
        which shard we want to connect to from the client side and depend on
        the round-robin of the system.clients shard_id attribution.

        If we get a duplicate connection to some shard, we put it into the
        excess connection pool. The more connections a particular shard has,
        the smaller the chance that further connections will be assigned
        to that shard.
        """
        with self._lock:
            if self.is_shutdown:
                return

        shard_aware_endpoint = self._get_shard_aware_endpoint()
        log.debug("shard_aware_endpoint=%r", shard_aware_endpoint)
        conn = None
        pending = False
        adopted = False
        shutdown_owns_connection = False
        try:
            if shard_aware_endpoint:
                conn = self._session.cluster.connection_factory(
                    shard_aware_endpoint,
                    host_conn=self,
                    on_orphaned_stream_released=self.on_orphaned_stream_released,
                    shard_id=shard_id,
                    total_shards=self.host.sharding_info.shards_count)
            else:
                conn = self._session.cluster.connection_factory(
                    self.host.endpoint,
                    host_conn=self,
                    on_orphaned_stream_released=self.on_orphaned_stream_released)

            if not self._register_pending_connection(conn):
                if not self._shutdown_owns_pending_connection(conn):
                    conn.close()
                return
            pending = True
            if conn.is_closed:
                raise _startup_close_error(conn, self.host.endpoint)

            if shard_aware_endpoint:
                conn.original_endpoint = self.host.endpoint

            actual_shard_id = conn.features.shard_id
            log.debug(
                "Received a connection %s for shard_id=%i on host %s",
                id(conn),
                actual_shard_id if actual_shard_id is not None else -1,
                self.host)

            if shard_aware_endpoint and shard_id != actual_shard_id:
                # The connection did not land on the requested shard, which
                # commonly indicates NAT in front of the shard-aware port.
                self.disable_advanced_shard_aware(10 * 60)

            while True:
                with self._lock:
                    if self.is_shutdown:
                        shutdown_owns_connection = not (
                            self._remove_pending_connection_locked(conn))
                        pending = False
                        close_connection = not shutdown_owns_connection
                        keyspace = None
                        keyspace_generation = None
                    else:
                        close_connection = False
                        keyspace = self._keyspace
                        keyspace_generation = self._keyspace_generation

                if close_connection:
                    conn.close()
                    return
                if shutdown_owns_connection:
                    return

                if keyspace:
                    try:
                        _set_keyspace_blocking(
                            conn,
                            keyspace,
                            self._session.cluster.connect_timeout)
                    except _REQUEST_VALIDATION_EXCEPTIONS:
                        # An invalid/dropped Session keyspace is not a host
                        # failure. Keep ownership so a later USE can repair
                        # the Session, but quarantine this socket from
                        # ordinary requests until it selects a keyspace.
                        conn._pool_keyspace_mismatch = True
                        log.warning(
                            "Unable to set keyspace %s on connection to shard "
                            "%i of %s; quarantining it until a successful USE",
                            keyspace,
                            actual_shard_id,
                            self.host)
                    else:
                        conn._pool_keyspace_mismatch = False
                if conn.is_closed:
                    raise _startup_close_error(conn, self.host.endpoint)

                old_connection = None
                close_old_connection = False
                old_remaining = 0
                excess_to_close = []
                close_connection = False
                mapped_connection_adopted = False
                with self._lock:
                    if self.is_shutdown:
                        shutdown_owns_connection = not (
                            self._remove_pending_connection_locked(conn))
                        pending = False
                        close_connection = not shutdown_owns_connection
                    elif keyspace_generation != self._keyspace_generation:
                        continue
                    elif not self._remove_pending_connection_locked(conn):
                        raise ConnectionException(
                            "Missing-shard connection ownership was lost",
                            self.host.endpoint)
                    else:
                        pending = False
                        old_connection = self._connections.get(
                            actual_shard_id)
                        if (
                                old_connection is None or
                                self._connection_needs_replacement(
                                    old_connection)):
                            self._connections[actual_shard_id] = conn
                            adopted = True
                            mapped_connection_adopted = True
                            if old_connection is not None:
                                (
                                    close_old_connection,
                                    old_remaining
                                ) = self._retire_connection_locked(
                                    old_connection)

                            if self.num_missing_or_needing_replacement == 0:
                                excess_to_close = list(
                                    self._excess_connections)
                                self._excess_connections.clear()
                        elif (
                                len(self._connections) ==
                                self.host.sharding_info.shards_count and
                                self.num_missing_or_needing_replacement == 0):
                            close_connection = True
                        else:
                            if (
                                    len(self._excess_connections) >=
                                    self._excess_connection_limit):
                                excess_to_close = list(
                                    self._excess_connections)
                                self._excess_connections.clear()
                            self._excess_connections.add(conn)
                            adopted = True

                if shutdown_owns_connection:
                    return
                if close_connection:
                    conn.close()
                if close_old_connection:
                    log.debug(
                        "Immediately closing retired connection (%s) for "
                        "shard %i on host %s",
                        id(old_connection),
                        actual_shard_id,
                        self.host)
                    old_connection.close()
                elif old_connection is not None:
                    log.debug(
                        "Moved connection (%s) for shard %i to trash on host "
                        "%s, %i requests remaining",
                        id(old_connection),
                        actual_shard_id,
                        self.host,
                        old_remaining)
                for excess_connection in excess_to_close:
                    excess_connection.close()

                if mapped_connection_adopted:
                    with self._stream_available_condition:
                        self._stream_available_condition.notify_all()
                    log.debug(
                        "Connected to %s/%i shards on host %s (%i missing or "
                        "needs replacement)",
                        len(self._connections),
                        self.host.sharding_info.shards_count,
                        self.host,
                        self.num_missing_or_needing_replacement)
                return
        except BaseException:
            if conn is not None and not adopted:
                with self._lock:
                    if pending:
                        if not self._remove_pending_connection_locked(conn):
                            shutdown_owns_connection = self.is_shutdown
                        pending = False
                if not shutdown_owns_connection:
                    conn.close()
            raise

    def _open_connections_for_all_shards(self, skip_shard_id=None):
        """
        Loop over all the shards and try to open a connection to each one.
        """
        with self._lock:
            if self.is_shutdown:
                return
            shards_count = self.host.sharding_info.shards_count

        for shard_id in range(shards_count):
            if skip_shard_id is not None and skip_shard_id == shard_id:
                continue
            self._schedule_connection_to_missing_shard(
                shard_id,
                track_future=True)

        trash_conns = None
        with self._lock:
            if self._trash:
                trash_conns = self._trash
                self._trash = set()

        if trash_conns is not None:
            for conn in trash_conns:
                conn.close()

    def _set_keyspace_for_all_conns(self, keyspace, callback):
        """
        Asynchronously sets the keyspace for all connections.  When all
        connections have been set, `callback` will be called with two
        arguments: this pool, and a list of any errors that occurred.
        """
        with self._lock:
            self._keyspace = keyspace
            self._keyspace_generation += 1
            self._keyspace_update_queue.append((keyspace, callback))
            if self._keyspace_update_in_progress:
                return
            self._keyspace_update_in_progress = True

        self._request_next_keyspace_update()

    def _request_next_keyspace_update(self):
        with self._lock:
            self._keyspace_update_run_requested = True
            if self._keyspace_update_runner_active:
                return
            self._keyspace_update_runner_active = True

        while True:
            with self._lock:
                if not self._keyspace_update_run_requested:
                    self._keyspace_update_runner_active = False
                    return
                self._keyspace_update_run_requested = False
            try:
                self._run_next_keyspace_update()
            except BaseException:
                with self._lock:
                    self._keyspace_update_runner_active = False
                    rerun = self._keyspace_update_run_requested
                if rerun:
                    # A synchronous completion callback can enqueue/request the
                    # next generation and then raise a cancellation-style
                    # BaseException. Preserve propagation to its caller, but
                    # first transfer runner ownership so queued USE operations
                    # cannot remain permanently unresolved.
                    try:
                        self._request_next_keyspace_update()
                    except BaseException:
                        log.exception(
                            "Additional failure while resuming queued "
                            "keyspace updates for host %s",
                            self.host)
                raise

    def _run_next_keyspace_update(self):
        with self._lock:
            if not self._keyspace_update_queue:
                self._keyspace_update_in_progress = False
                return
            keyspace, callback = self._keyspace_update_queue.popleft()
            connections = list(self._connections.values())
            shutdown_error = ConnectionException(
                "Pool for %s is shutdown" % (self.host,),
                self.host) if self.is_shutdown else None

            update = {
                'callback': callback,
                'errors': (
                    [shutdown_error]
                    if shutdown_error is not None else []),
                'finished': False,
                'lock': Lock(),
                'remaining': set(connections),
            }
            self._keyspace_update_current = update

        if not update['remaining']:
            self._finish_keyspace_update(update)
            return

        def connection_finished_setting_keyspace(conn, error):
            with update['lock']:
                if conn not in update['remaining']:
                    return
                update['remaining'].remove(conn)
                conn._pool_keyspace_mismatch = error is not None
                update_already_finished = update['finished']
                if error and not update_already_finished:
                    update['errors'].append(error)
                claimed = bool(
                    not update_already_finished and
                    not update['remaining'])
                if claimed:
                    # The last response owns completion before releasing this
                    # lock. In particular, shutdown cannot overtake a response
                    # here while return_connection balances in_flight.
                    update['finished'] = True

            try:
                self.return_connection(conn)
            except BaseException:
                log.exception(
                    "Error returning connection after setting keyspace on "
                    "host %s",
                    self.host)
            finally:
                if claimed:
                    self._deliver_keyspace_update(update)

        for conn in connections:
            with update['lock']:
                if update['finished']:
                    break
            try:
                conn.set_keyspace_async(
                    keyspace,
                    connection_finished_setting_keyspace)
            except BaseException as exc:
                # Connection.set_keyspace_async maintains the in-flight
                # invariant before issuing work, even for synchronous send
                # failures, so finish it through the normal callback path.
                connection_finished_setting_keyspace(conn, exc)

    def _finish_keyspace_update(self, update, error=None):
        """
        Complete one serialized keyspace generation exactly once.

        ``shutdown`` uses this path to force the active generation to finish
        before queued generations, even on reactors whose connection close
        callbacks run later.
        """
        if not self._claim_keyspace_update(update, error):
            return False
        self._deliver_keyspace_update(update)
        return True

    @staticmethod
    def _claim_keyspace_update(update, error=None):
        with update['lock']:
            if update['finished']:
                return False
            update['finished'] = True
            if error is not None:
                update['errors'].append(error)
            return True

    def _deliver_keyspace_update(self, update):
        try:
            update['callback'](self, update['errors'])
        except Exception:
            log.exception(
                "Error completing keyspace update on host %s",
                self.host)
        finally:
            # Keep the completed generation published until its user callback
            # returns.  Otherwise a concurrent shutdown can advance the queue
            # and invoke the next callback out of FIFO order.
            with self._lock:
                if self._keyspace_update_current is update:
                    self._keyspace_update_current = None
            self._request_next_keyspace_update()

    def get_connections(self):
        with self._lock:
            connections = self._connections
            return list(connections.values()) if connections else []

    def get_state(self):
        in_flights = [c.in_flight for c in list(self._connections.values())]
        orphan_requests = [c.orphaned_request_ids for c in list(self._connections.values())]
        return {'shutdown': self.is_shutdown, 'open_count': self.open_count, \
                'in_flights': in_flights, 'orphan_requests': orphan_requests}

    @property
    def num_missing_or_needing_replacement(self):
        return self.host.sharding_info.shards_count \
            - sum(
                1 for c in list(self._connections.values())
                if not self._connection_needs_replacement(c))

    @property
    def open_count(self):
        return sum([
            1
            if (
                c and
                not (c.is_closed or c.is_defunct))
            else 0
            for c in list(self._connections.values())])

    @property
    def _excess_connection_limit(self):
        return self.host.sharding_info.shards_count * self.max_excess_connections_per_shard_multiplier
