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

"""Internal host lifecycle coordination; not part of the driver public API.

Classes use ordinary names within this private module. Import the module
explicitly for internal use; no names are exported by wildcard imports.
"""

from collections import deque
from concurrent.futures import Future
from contextlib import contextmanager, nullcontext
from functools import partial, wraps
import logging
from threading import local, RLock
import weakref
from weakref import WeakValueDictionary

from cassandra import DriverException

__all__ = ()

# Preserve the existing logging category for cluster lifecycle events.
log = logging.getLogger("cassandra.cluster")


class EndpointMoveRejected(ValueError):
    """A batch could not publish because its metadata snapshot went stale."""


class PoolFailureReservation:
    """Fence an admitted pool failure without locking across policy callbacks."""

    def __init__(self, session, host, generation, expected_pool, intent, endpoint):
        self.session = session
        self.host = host
        self.generation = generation
        self.expected_pool = expected_pool
        self.intent = intent
        self.endpoint = endpoint

    def owns_locked(self):
        return (self.session._pool_intents_locked().get(self.host) is self.intent and
                self.session._owns_pool_generation_locked(
                    self.host, self.generation, self.expected_pool, self.endpoint))

    def is_current(self):
        with self.session._lock:
            with self.host.lock:
                return self.owns_locked()


class LifecycleCoordinator:
    """Own host admission, readiness, retirement and notification ordering.

    Metadata owns indexes and token maps; Cluster and Session perform the
    policy, connection and pool operations. This coordinator publishes their
    transitions under a short lock and delivers FIFO notifications unlocked.
    A callback may synchronously publish another topology. Its notifications
    join the queue instead of recursively overtaking the current lifecycle.
    """

    def __init__(self, cluster=None):
        self._cluster = weakref.proxy(cluster) if cluster is not None else None
        self._lock = RLock()
        self._local = local()
        self._events = deque()
        self._draining = False
        self._requested = 0
        self._published = 0
        self._force_revisions = deque()
        self._closed = False
        self._retired = WeakValueDictionary()

    @property
    def closed(self):
        # Readable under host.lock without acquiring the publication lock.
        return self._closed or (self._cluster is not None and
                                self._cluster.is_shutdown)

    @property
    def delivering(self):
        return getattr(self._local, 'delivering', False)

    def close(self):
        """Reject new work and abandon queued work without joining callbacks."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._events.clear()
            self._force_revisions.clear()
            hosts = list(self._retired.values())
            if self._cluster is not None:
                hosts.extend(self._cluster.metadata.all_hosts())
            self._retired.clear()
            reconnectors = []
            for host in hosts:
                with host.lock:
                    self.cancel_preparation(host)
                    host._pending_location = None
                    host._pending_location_token = None
                    host._lifecycle_pending = False
                    host._pending_host_addition = False
                    host._pending_host_addition_notification = False
                    host._pending_host_addition_callback = None
                    host._currently_handling_node_up = False
                    host._currently_handling_node_down = False
                    host._down_event_generation += 1
                    handler = host.get_and_set_reconnection_handler(None)
                    if handler is not None:
                        reconnectors.append(handler)
        for handler in reconnectors:
            try:
                handler.cancel()
            except Exception:
                log.exception("Error cancelling host reconnector during shutdown")

    def begin_preparation(self, host):
        with host.lock:
            if (not self.is_current(host) or
                    getattr(host, '_lifecycle_preparation', None) is not None):
                return None
            token = object()
            host._lifecycle_preparation = token
            return token

    def finish_preparation(self, host, token):
        with host.lock:
            if (not self.is_current(host) or
                    getattr(host, '_lifecycle_preparation', None) is not token):
                return False
            host._lifecycle_preparation = None
            return True

    @staticmethod
    def cancel_preparation(host):
        # Called under host.lock. Release UP before DOWN chooses its recovery
        # path; otherwise a restart could mistake abandoned work for an owner.
        if getattr(host, '_lifecycle_preparation', None) is not None:
            host._lifecycle_preparation = None
            host._currently_handling_node_up = False
            host._lifecycle_pending = False

    def submit_readiness(self, host, callback):
        # Capture the generation at FIFO admission, before a newer DOWN can
        # publish state while this notification waits behind another callback.
        with self.transaction():
            with host.lock:
                generation = getattr(host, '_readiness_generation', 0)
            self.submit(callback, host, generation)

    def readiness_is_current(self, host, generation=None):
        return (self.is_current(host) and
                (generation is None or generation ==
                 getattr(host, '_readiness_generation', 0)))

    def begin_up(self, host, generation=None):
        with host.lock:
            if (not self.readiness_is_current(host, generation) or host.is_up or
                    host._currently_handling_node_up or
                    getattr(host, '_lifecycle_preparation', None) is not None):
                return False
            host._currently_handling_node_up = True
            host._addition_token = None
            # UP owns recovery now; old scheduled ADD work must not replay
            # callbacks while UP is pending or after it completes.
            host._endpoint_add_retry_pending = False
            return True

    @staticmethod
    def finish_up(host, generation=None):
        with host.lock:
            if (generation is None or generation ==
                    getattr(host, '_readiness_generation', 0)):
                host._currently_handling_node_up = False

    def observe_health(self, host):
        """Capture ownership before inspecting policies or session pools."""
        with host.lock:
            if self.closed or host._is_removed:
                return None
            return host._health_generation

    def begin_down(self, host, health_generation, connected=False,
                   expect_down=False, pool_failure=None):
        """Return (generation, retry_only), or None if DOWN is discounted."""
        failure_lock = (pool_failure.session._lock
                        if pool_failure is not None else nullcontext())
        with failure_lock:
            with host.lock:
                if (self.closed or host._is_removed or connected or
                        health_generation != host._health_generation or
                        (pool_failure is not None and not pool_failure.owns_locked())):
                    return None
                host._health_generation += 1
                self.cancel_preparation(host)
                host._readiness_generation = (
                    getattr(host, '_readiness_generation', 0) + 1)
                host._currently_handling_node_up = False
                was_up = host.is_up
                host.set_down()
                if (not was_up and (host.is_currently_reconnecting() or
                                    host._currently_handling_node_down)):
                    return None
                host._currently_handling_node_down = True
                host._down_event_generation += 1
                return host._down_event_generation, not was_up and not expect_down

    def dispatch_down(self, host, health_generation, is_host_addition,
                      connected, expect_host_to_be_down, on_add_reconnection,
                      pool_failure=None):
        # Publish DOWN state and FIFO admission together. Do not drain on
        # this caller; the executor (or another drainer) owns delivery.
        with self.transaction(drain=False):
            admission = self.begin_down(
                host, health_generation, connected=connected,
                expect_down=expect_host_to_be_down,
                pool_failure=pool_failure)
            if admission is None:
                return False
            down_event_generation, restart_reconnector = admission

            if restart_reconnector:
                if on_add_reconnection is None:
                    future = self._cluster._restart_reconnector(
                        host, is_host_addition, down_event_generation)
                else:
                    future = self._cluster._restart_reconnector(
                        host, is_host_addition, down_event_generation,
                        on_add_reconnection=on_add_reconnection)
                if future is None:
                    self.finish_down(host, down_event_generation)
                # Restarting the host reconnector does not dispatch the DOWN
                # callbacks that reconnect the control connection.
                return False

            log.warning("Host %s has been marked down", host)

            if on_add_reconnection is None:
                future = self._cluster.on_down_potentially_blocking(
                    host, is_host_addition, down_event_generation)
            else:
                future = self._cluster.on_down_potentially_blocking(
                    host, is_host_addition, down_event_generation,
                    on_add_reconnection=on_add_reconnection)
            if future is None:
                self.finish_down(host, down_event_generation)
            return future is not None

    @staticmethod
    def finish_down(host, generation):
        with host.lock:
            if generation == host._down_event_generation:
                host._currently_handling_node_down = False

    def down_is_current(self, host, generation=None):
        return (not self.closed and not host._is_removed and
                (generation is None or generation == host._down_event_generation))

    @staticmethod
    def supersede_down(host):
        """Invalidate queued recovery after terminal addition failure."""
        with host.lock:
            host._down_event_generation += 1
            host._currently_handling_node_down = False
            return host.get_and_set_reconnection_handler(None)

    @contextmanager
    def transaction(self, drain=True):
        try:
            with self._lock:
                self._local.depth = getattr(self._local, 'depth', 0) + 1
                try:
                    yield
                finally:
                    self._local.depth -= 1
        finally:
            if drain and not getattr(self._local, 'depth', 0):
                self.drain()

    def reserve_refresh(self, force_token_rebuild=False):
        with self._lock:
            self._requested += 1
            if force_token_rebuild:
                self._force_revisions.append(self._requested)
            return self._requested

    def force_rebuild_required(self, revision):
        """Keep force intent until this or a newer snapshot rebuilds tokens."""
        return bool(self._force_revisions and self._force_revisions[0] <= revision)

    def complete_token_rebuild(self, revision):
        while self._force_revisions and self._force_revisions[0] <= revision:
            self._force_revisions.popleft()

    def accept_refresh(self, revision):
        # Called inside a publication transaction.
        if self.closed or revision < self._published:
            return False
        self._published = revision
        return True

    def submit(self, callback, *args, **kwargs):
        """Queue a notification; delivery never has a synchronous result."""
        with self._lock:
            if self.closed:
                return
            self._events.append(partial(callback, *args, **kwargs))
        if not getattr(self._local, 'depth', 0):
            self.drain()

    def submit_async(self, executor, callback, *args, **kwargs):
        """Queue before executor handoff so later notifications cannot overtake."""
        event = partial(callback, *args, **kwargs)
        with self._lock:
            if self.closed:
                return None
            self._events.append(event)
        try:
            return executor.submit(self.drain)
        except Exception:
            log.exception("Failed to schedule host lifecycle notifications")
            # If no drainer has claimed this event, failed handoff cancels
            # admission. Otherwise a concurrent submit already delivered it.
            with self._lock:
                try:
                    self._events.remove(event)
                except ValueError:
                    if self.closed:
                        return None
                else:
                    return None
            completed = Future()
            completed.set_result(None)
            return completed

    def is_current(self, host):
        """Check that this exact, nonterminal Host owns its metadata entry."""
        return (not self.closed and not host._is_removed and
                self._cluster.metadata.get_host_by_host_id(host.host_id) is host)

    def dispatch_current(self, host, callback, *args, generation=None,
                         readiness_generation=None, **kwargs):
        """Admit one callback; retirement may publish after admission."""
        with host.lock:
            # DOWN work already owns a generation. A transient metadata
            # reindex must not prevent its recovery notifications.
            admitted = (self.readiness_is_current(host, readiness_generation)
                        if generation is None else
                        self.down_is_current(host, generation))
            if not admitted:
                return False
        callback(host, *args, **kwargs)
        return True

    def submit_current(self, host, callback, *args, **kwargs):
        """Check identity when queued work starts, not when it is submitted."""
        def deliver():
            if self.is_current(host):
                callback(*args, **kwargs)
        self.submit(deliver)

    @staticmethod
    def reserve_addition(host, pending=True):
        """Reserve a candidate before publication and pool reconciliation."""
        with host.lock:
            host._lifecycle_pending = pending

    def begin_addition(self, host, on_reconnection=None):
        with host.lock:
            if (not self.is_current(host) or
                    host._currently_handling_node_up or
                    getattr(host, '_lifecycle_preparation', None) is not None):
                return False
            host._lifecycle_pending = True
            self.remember_addition(host, on_reconnection)
        return True

    def claim_addition(self, host):
        # Readiness generation rejects newer DOWN, while this token also
        # fences failure cleanup superseded by another ADD or UP owner.
        with host.lock:
            token = object()
            host._addition_token = token
            return token

    def addition_is_current(self, host, token):
        return (self.is_current(host) and
                getattr(host, '_addition_token', None) is token)

    @staticmethod
    def remember_addition(host, on_reconnection=None):
        """Retain addition intent across retries; callers hold host.lock."""
        host._pending_host_addition = True
        host._pending_host_addition_notification = True
        if on_reconnection is not None:
            host._pending_host_addition_callback = on_reconnection

    def release_addition(self, host, addition_token=None):
        """Let session reconciliation resume after an ordinary ADD failure."""
        with host.lock:
            if (self.is_current(host) and
                    (addition_token is None or
                     self.addition_is_current(host, addition_token))):
                host._lifecycle_pending = False

    def abandon_addition(self, host, generation, addition_token):
        """Release callback recovery ownership, retaining only discovery notice.

        Future explicit ADD/UP can finish discovery. Ordinary pool reconciliation
        is no longer reserved, and DOWN recovery must not replay failed ADD
        policy/control callbacks automatically.
        """
        with host.lock:
            if (self.readiness_is_current(host, generation) and
                    self.addition_is_current(host, addition_token)):
                host._addition_token = None
                host._lifecycle_pending = False
                host._pending_host_addition = False
                host._pending_host_addition_callback = None
                host._endpoint_add_retry_pending = False
                reconnector = self.supersede_down(host)
            else:
                return
        if reconnector:
            reconnector.cancel()

    def claim_addition_notification(self, host, generation):
        with host.lock:
            if (not self.readiness_is_current(host, generation) or
                    not host._pending_host_addition_notification):
                return False
            host._pending_host_addition_notification = False
            return True

    def mark_ready(self, host, set_up=True, readiness_generation=None,
                   addition_token=None, advance_health=True):
        if (not self.readiness_is_current(host, readiness_generation) or
                (addition_token is not None and
                 not self.addition_is_current(host, addition_token))):
            return False
        if set_up:
            host.conviction_policy.reset()
        with host.lock:
            if (not self.readiness_is_current(host, readiness_generation) or
                    (addition_token is not None and
                     not self.addition_is_current(host, addition_token))):
                return False
            if set_up:
                host.is_up = True
                if advance_health:
                    host._health_generation += 1
            host._addition_token = None
            host._endpoint_add_retry_pending = False
            host._lifecycle_pending = False
            host._pending_host_addition = False
            host._pending_host_addition_callback = None
        return True

    def _retire(self, host):
        # A Host object is never re-added after lifecycle removal; a later
        # discovery creates a new object, even when it carries the same ID.
        # Pool/reconnection publication checks this terminal state under the
        # same state lock. Notifications run later, without holding it.
        with host.lock:
            if host._is_removed:
                return False
            self.cancel_preparation(host)
            host._is_removed = True
            self._retired[id(host)] = host
            host.set_down()
            host._pending_host_addition = False
            host._pending_host_addition_notification = False
            host._pending_host_addition_callback = None
            host._lifecycle_pending = False
            host._pending_location = None
            host._pending_location_token = None
            host._currently_handling_node_up = False
            host._currently_handling_node_down = False
            host._down_event_generation += 1
            return True

    def retire(self, host, trigger_reconciliation=True, refresh_nodes=True):
        with self.transaction():
            if not self._retire(host):
                return
            self.submit(self._cluster._notify_host_removed, host,
                        trigger_reconciliation, True, refresh_nodes)

    def remove(self, host, trigger_reconciliation=True, refresh_nodes=True):
        with self.transaction():
            if (host is None or not self.is_current(host) or
                    not self._cluster.metadata.remove_host(host)):
                return
            self._retire(host)
            log.info("Cassandra host %s removed", host)
            self.submit(self._cluster._notify_host_removed, host,
                        trigger_reconciliation, True, refresh_nodes)

    def publish_host(self, candidate, signal, refresh_nodes,
                     reconcile_pools_on_failure, replace_existing_id):
        """Publish discovery and queue displaced cleanup before new ADD."""
        with self.transaction():
            if self.closed:
                raise DriverException("Cluster is already shut down")
            self.reserve_addition(candidate, pending=signal)
            host, new, displaced = self._cluster.metadata._add_or_replace_host(
                candidate, allow_replace=signal)
            if not new and host.endpoint != candidate.endpoint:
                if not signal:
                    raise ValueError("Endpoint replacement requires host addition signaling")
                if not replace_existing_id:
                    raise ValueError("Endpoint replacement is disabled for this host addition")
                return self._cluster.replace_host_endpoint(
                    host, candidate.endpoint, candidate.datacenter, candidate.rack), False
            self._notify_addition(host, new, displaced, signal, refresh_nodes,
                                  reconcile_pools_on_failure)
            return host, new

    def _notify_addition(self, host, new, displaced, signal, refresh_nodes,
                        reconcile_pools_on_failure):
        """Order a displaced lifecycle's cleanup before its successor's ADD."""
        cluster = self._cluster
        if displaced is not None:
            log.info("Cassandra host %s removed", displaced)
            try:
                cluster.on_remove(displaced, trigger_reconciliation=False,
                                  suppress_callback_errors=True)
            except Exception:
                log.exception("Error notifying removal of replaced host %s",
                              displaced)
            reconcile_pools_on_failure = True
        if new and signal:
            self.reserve_addition(host)
            log.info("New Cassandra host %r discovered", host)
            cluster.on_add(host, refresh_nodes,
                           reconcile_pools_on_failure=reconcile_pools_on_failure)

    def publish_replacements(self, replacements, reconnect=None):
        """Publish a batch and schedule retirement before replacement ADD."""
        with self.transaction():
            if self.closed:
                raise DriverException("Cluster is already shut down")
            for _, replacement in replacements:
                self.reserve_addition(replacement)
            try:
                self._cluster.metadata.replace_host_endpoints(replacements)
            except ValueError as exc:
                raise EndpointMoveRejected(str(exc)) from exc
            self._notify_replacements(replacements, reconnect)

    def _notify_replacements(self, replacements, reconnect):
        cluster = self._cluster
        for host, replacement in replacements:
            log.info("Cassandra host %s moved from %s to %s", host.host_id,
                     host.endpoint, replacement.endpoint)
            cluster.on_remove(host, trigger_reconciliation=False,
                              suppress_callback_errors=True)
        try:
            for _, replacement in replacements:
                try:
                    cluster.on_add(replacement, refresh_nodes=False,
                                   reconcile_pools_on_failure=True)
                except Exception:
                    log.exception("Error notifying addition of moved host %s",
                                  replacement)
                    with replacement.lock:
                        replacement._endpoint_add_retry_pending = True
                    cluster._schedule_failed_endpoint_add_retry(replacement)
        finally:
            if reconnect is not None:
                self.submit(reconnect)

    def update_location(self, host, datacenter, rack):
        with self.transaction():
            if self.closed:
                return False
            return self._queue_location(host, datacenter, rack)

    def _queue_location(self, host, datacenter, rack):
        desired = (datacenter, rack)
        pending = getattr(host, '_pending_location', None)
        if (pending if pending is not None else
                (host.datacenter, host.rack)) == desired:
            return False
        token = object()
        host._pending_location = desired
        host._pending_location_token = token
        self.submit(
            self._notify_location_changed, host, datacenter, rack, token)
        return True

    def _notify_location_changed(self, host, datacenter, rack, token):
        try:
            if not self.is_current(host):
                return
            # Policies use the old DC/rack to remove membership. Publish the
            # new location only after that callback, then invalidate replica
            # caches by replacing their owner object, not clearing it in place.
            self._cluster.profile_manager.on_down(host)
            with self.transaction():
                if not self.is_current(host):
                    return
                changed = self._cluster.metadata._update_host_location(
                    host, datacenter, rack)
            if changed:
                self.dispatch_current(host, self._cluster.profile_manager.on_up)
        finally:
            with self.transaction():
                if getattr(host, '_pending_location_token', None) is token:
                    host._pending_location = None
                    host._pending_location_token = None

    def drain(self):
        with self._lock:
            if self._draining or self.closed:
                return
            self._draining = True
        try:
            while True:
                with self._lock:
                    if self.closed or not self._events:
                        self._events.clear()
                        self._draining = False
                        return
                    callback = self._events.popleft()
                try:
                    self._local.delivering = True
                    callback()
                except Exception:
                    log.exception("Error delivering host lifecycle notification")
                finally:
                    self._local.delivering = False
        except BaseException:
            with self._lock:
                self._draining = False
            raise


def event(method):
    @wraps(method)
    def dispatch(self, *args, **kwargs):
        return self._lifecycle.submit(method, self, *args, **kwargs)
    dispatch._lifecycle_method = method
    return dispatch


def transaction(method):
    @wraps(method)
    def publish(self, *args, **kwargs):
        with self._lifecycle.transaction():
            return method(self, *args, **kwargs)
    return publish
