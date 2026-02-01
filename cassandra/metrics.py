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
Driver metrics collection module.

This module provides metrics collection functionality without external dependencies.
It was originally based on the `scales` library but now uses a self-contained
implementation.
"""

from itertools import chain
import logging
import math
import random
import threading

log = logging.getLogger(__name__)


# Global stats registry
_stats_registry = {}
_registry_lock = threading.Lock()


def getStats():
    """
    Returns a copy of all registered stats.
    """
    with _registry_lock:
        return {name: stats._get_stats_dict() for name, stats in _stats_registry.items()}


class IntStat:
    """
    A thread-safe integer counter statistic.
    """
    __slots__ = ('name', '_value', '_lock')

    def __init__(self, name):
        self.name = name
        self._value = 0
        self._lock = threading.Lock()

    def __iadd__(self, other):
        with self._lock:
            self._value += other
        return self

    def __int__(self):
        return self._value

    def __eq__(self, other):
        if isinstance(other, IntStat):
            return self._value == other._value
        return self._value == other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if isinstance(other, IntStat):
            return self._value < other._value
        return self._value < other

    def __le__(self, other):
        if isinstance(other, IntStat):
            return self._value <= other._value
        return self._value <= other

    def __gt__(self, other):
        if isinstance(other, IntStat):
            return self._value > other._value
        return self._value > other

    def __ge__(self, other):
        if isinstance(other, IntStat):
            return self._value >= other._value
        return self._value >= other

    def __hash__(self):
        return hash(self._value)

    def __repr__(self):
        return f"IntStat({self.name}={self._value})"

    @property
    def value(self):
        return self._value


class Stat:
    """
    A gauge statistic that evaluates a callable on access.
    """
    __slots__ = ('name', '_func')

    def __init__(self, name, func):
        self.name = name
        self._func = func

    @property
    def value(self):
        return self._func()

    def __repr__(self):
        return f"Stat({self.name}={self.value})"


class PmfStat:
    """
    A probability mass function statistic that tracks timing/size distributions.

    Computes count, min, max, mean, stddev, median and various percentiles.
    Uses reservoir sampling to limit memory usage for large sample counts.
    """
    __slots__ = ('name', '_values', '_lock', '_count', '_min', '_max', '_sum', '_sum_sq')

    # Maximum number of values to retain for percentile calculations
    _MAX_SAMPLES = 10000

    def __init__(self, name):
        self.name = name
        self._values = []
        self._lock = threading.Lock()
        self._count = 0
        self._min = float('inf')
        self._max = float('-inf')
        self._sum = 0.0
        self._sum_sq = 0.0

    def addValue(self, value):
        """Record a new value."""
        with self._lock:
            self._count += 1
            self._sum += value
            self._sum_sq += value * value

            if value < self._min:
                self._min = value
            if value > self._max:
                self._max = value

            # Reservoir sampling for percentiles
            if len(self._values) < self._MAX_SAMPLES:
                self._values.append(value)
            else:
                # Replace random element with decreasing probability
                idx = random.randint(0, self._count - 1)
                if idx < self._MAX_SAMPLES:
                    self._values[idx] = value

    def _percentile(self, sorted_values, p):
        """Calculate the p-th percentile from sorted values."""
        if not sorted_values:
            return 0.0
        k = (len(sorted_values) - 1) * p / 100.0
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_values[int(k)]
        return sorted_values[int(f)] * (c - k) + sorted_values[int(c)] * (k - f)

    def _get_stats(self):
        """Calculate all statistics."""
        with self._lock:
            count = self._count
            if count == 0:
                return {
                    'count': 0,
                    'min': 0.0,
                    'max': 0.0,
                    'mean': 0.0,
                    'stddev': 0.0,
                    'median': 0.0,
                    '75percentile': 0.0,
                    '95percentile': 0.0,
                    '98percentile': 0.0,
                    '99percentile': 0.0,
                    '999percentile': 0.0,
                }

            mean = self._sum / count

            # Calculate stddev using Welford's algorithm values
            variance = (self._sum_sq / count) - (mean * mean)
            stddev = math.sqrt(max(0, variance))  # max to handle floating point errors

            sorted_values = sorted(self._values)

            return {
                'count': count,
                'min': self._min,
                'max': self._max,
                'mean': mean,
                'stddev': stddev,
                'median': self._percentile(sorted_values, 50),
                '75percentile': self._percentile(sorted_values, 75),
                '95percentile': self._percentile(sorted_values, 95),
                '98percentile': self._percentile(sorted_values, 98),
                '99percentile': self._percentile(sorted_values, 99),
                '999percentile': self._percentile(sorted_values, 99.9),
            }

    def __getitem__(self, key):
        return self._get_stats()[key]

    def __iter__(self):
        return iter(self._get_stats())

    def keys(self):
        return self._get_stats().keys()

    def items(self):
        return self._get_stats().items()

    def values(self):
        return self._get_stats().values()

    def __repr__(self):
        return f"PmfStat({self.name}, count={self._count})"


class StatsCollection:
    """
    A named collection of statistics.
    """
    __slots__ = ('_name', '_stats', '_int_stats', '_pmf_stats', '_gauge_stats')

    def __init__(self, name, *stats):
        self._name = name
        self._stats = {}
        self._int_stats = {}
        self._pmf_stats = {}
        self._gauge_stats = {}

        for stat in stats:
            self._stats[stat.name] = stat
            if isinstance(stat, IntStat):
                self._int_stats[stat.name] = stat
            elif isinstance(stat, PmfStat):
                self._pmf_stats[stat.name] = stat
            elif isinstance(stat, Stat):
                self._gauge_stats[stat.name] = stat

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)
        try:
            stats = object.__getattribute__(self, '_stats')
            if name in stats:
                return stats[name]
        except AttributeError:
            pass
        raise AttributeError(f"No stat named '{name}'")

    def __setattr__(self, name, value):
        if name.startswith('_'):
            object.__setattr__(self, name, value)
            return
        # Allow rebinding stats (e.g., for augmented assignment like stats.errors += 1)
        try:
            stats = object.__getattribute__(self, '_stats')
            if name in stats:
                # For augmented assignment, value should be the same IntStat/PmfStat object
                # Just verify and allow the rebind
                return
        except AttributeError:
            pass
        raise AttributeError(f"Cannot set attribute '{name}' on StatsCollection")

    def _get_stats_dict(self):
        """Return dictionary representation of all stats."""
        result = {}
        for name, stat in self._int_stats.items():
            result[name] = stat.value
        for name, stat in self._pmf_stats.items():
            result[name] = stat._get_stats()
        for name, stat in self._gauge_stats.items():
            result[name] = stat.value
        return result


def collection(name, *stats):
    """
    Create a named collection of statistics and register it globally.
    """
    coll = StatsCollection(name, *stats)
    with _registry_lock:
        _stats_registry[name] = coll
    return coll


def init(obj, path):
    """
    Initialize class-level stats on an instance and register in the global registry.

    This allows class-level PmfStat/IntStat descriptors to be used per-instance.
    """
    # Get class-level stats and create instance copies
    cls = obj.__class__
    instance_stats = {}

    for attr_name in dir(cls):
        attr = getattr(cls, attr_name, None)
        if isinstance(attr, (PmfStat, IntStat)):
            # Create a new instance of the stat for this object
            if isinstance(attr, PmfStat):
                new_stat = PmfStat(attr.name)
            else:
                new_stat = IntStat(attr.name)
            instance_stats[attr_name] = new_stat
            # Set on instance to shadow class attribute
            object.__setattr__(obj, attr_name, new_stat)

    # Register under the given path (remove leading /)
    reg_name = path.lstrip('/')
    if instance_stats:
        stats_coll = StatsCollection(reg_name, *instance_stats.values())
        with _registry_lock:
            _stats_registry[reg_name] = stats_coll


class Metrics(object):
    """
    A collection of timers and counters for various performance metrics.

    Timer metrics are represented as floating point seconds.
    """

    request_timer = None
    """
    A :class:`~cassandra.metrics.PmfStat` timer for requests. This is a dict-like
    object with the following keys:

    * count - number of requests that have been timed
    * min - min latency
    * max - max latency
    * mean - mean latency
    * stddev - standard deviation for latencies
    * median - median latency
    * 75percentile - 75th percentile latencies
    * 95percentile - 95th percentile latencies
    * 98percentile - 98th percentile latencies
    * 99percentile - 99th percentile latencies
    * 999percentile - 99.9th percentile latencies
    """

    connection_errors = None
    """
    A :class:`~cassandra.metrics.IntStat` count of the number of times that a
    request to a Cassandra node has failed due to a connection problem.
    """

    write_timeouts = None
    """
    A :class:`~cassandra.metrics.IntStat` count of write requests that resulted
    in a timeout.
    """

    read_timeouts = None
    """
    A :class:`~cassandra.metrics.IntStat` count of read requests that resulted
    in a timeout.
    """

    unavailables = None
    """
    A :class:`~cassandra.metrics.IntStat` count of write or read requests that
    failed due to an insufficient number of replicas being alive to meet
    the requested :class:`.ConsistencyLevel`.
    """

    other_errors = None
    """
    A :class:`~cassandra.metrics.IntStat` count of all other request failures,
    including failures caused by invalid requests, bootstrapping nodes,
    overloaded nodes, etc.
    """

    retries = None
    """
    A :class:`~cassandra.metrics.IntStat` count of the number of times a
    request was retried based on the :class:`.RetryPolicy` decision.
    """

    ignores = None
    """
    A :class:`~cassandra.metrics.IntStat` count of the number of times a
    failed request was ignored based on the :class:`.RetryPolicy` decision.
    """

    known_hosts = None
    """
    A :class:`~cassandra.metrics.IntStat` count of the number of nodes in
    the cluster that the driver is aware of, regardless of whether any
    connections are opened to those nodes.
    """

    connected_to = None
    """
    A :class:`~cassandra.metrics.IntStat` count of the number of nodes that
    the driver currently has at least one connection open to.
    """

    open_connections = None
    """
    A :class:`~cassandra.metrics.IntStat` count of the number connections
    the driver currently has open.
    """

    _stats_counter = 0

    def __init__(self, cluster_proxy):
        log.debug("Starting metric capture")

        self.stats_name = 'cassandra-{0}'.format(str(self._stats_counter))
        Metrics._stats_counter += 1
        self.stats = collection(self.stats_name,
            PmfStat('request_timer'),
            IntStat('connection_errors'),
            IntStat('write_timeouts'),
            IntStat('read_timeouts'),
            IntStat('unavailables'),
            IntStat('other_errors'),
            IntStat('retries'),
            IntStat('ignores'),

            # gauges
            Stat('known_hosts',
                lambda: len(cluster_proxy.metadata.all_hosts())),
            Stat('connected_to',
                lambda: len(set(chain.from_iterable(list(s._pools.keys()) for s in cluster_proxy.sessions)))),
            Stat('open_connections',
                lambda: sum(sum(p.open_count for p in list(s._pools.values())) for s in cluster_proxy.sessions)))

        # TODO, to be removed in 4.0
        # /cassandra contains the metrics of the first cluster registered
        with _registry_lock:
            if 'cassandra' not in _stats_registry:
                _stats_registry['cassandra'] = _stats_registry[self.stats_name]

        self.request_timer = self.stats.request_timer
        self.connection_errors = self.stats.connection_errors
        self.write_timeouts = self.stats.write_timeouts
        self.read_timeouts = self.stats.read_timeouts
        self.unavailables = self.stats.unavailables
        self.other_errors = self.stats.other_errors
        self.retries = self.stats.retries
        self.ignores = self.stats.ignores
        self.known_hosts = self.stats.known_hosts
        self.connected_to = self.stats.connected_to
        self.open_connections = self.stats.open_connections

    def on_connection_error(self):
        self.stats.connection_errors += 1

    def on_write_timeout(self):
        self.stats.write_timeouts += 1

    def on_read_timeout(self):
        self.stats.read_timeouts += 1

    def on_unavailable(self):
        self.stats.unavailables += 1

    def on_other_error(self):
        self.stats.other_errors += 1

    def on_ignore(self):
        self.stats.ignores += 1

    def on_retry(self):
        self.stats.retries += 1

    def get_stats(self):
        """
        Returns the metrics for the registered cluster instance.
        """
        return getStats()[self.stats_name]

    def set_stats_name(self, stats_name):
        """
        Set the metrics stats name.
        The stats_name is a string used to access the metrics through getStats(): getStats()[<stats_name>]
        Default is 'cassandra-<num>'.
        """

        if self.stats_name == stats_name:
            return

        with _registry_lock:
            if stats_name in _stats_registry:
                raise ValueError('"{0}" already exists in stats.'.format(stats_name))

            stats = _stats_registry[self.stats_name]
            del _stats_registry[self.stats_name]
            self.stats_name = stats_name
            _stats_registry[self.stats_name] = stats

    def shutdown(self):
        """
        Remove this metrics instance from the global registry.
        Called when the cluster is shutdown to prevent stale references.
        """
        with _registry_lock:
            if self.stats_name in _stats_registry:
                del _stats_registry[self.stats_name]
            # Also clean up the legacy 'cassandra' entry if it points to our stats
            if _stats_registry.get('cassandra') is self.stats:
                del _stats_registry['cassandra']
