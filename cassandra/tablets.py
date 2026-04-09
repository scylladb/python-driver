from bisect import bisect_left
from operator import attrgetter
from random import getrandbits
from threading import Lock
from typing import Optional
from uuid import UUID

# C-accelerated attrgetter avoids per-call lambda allocation overhead
_get_first_token = attrgetter("first_token")
_get_last_token = attrgetter("last_token")


def choose_tablet_version_block(tablet_version: int) -> int:
    """
    Encode a tablet_version_block byte from a cached tablet_version.
    Picks a block index at random across calls.
    Returns an int in [0, 255].

    The byte layout: the high nibble is the block index, the low nibble is the value
    of that block. Blocks are indexed from the least significant bits to the most
    significant ones, so block `idx` occupies bits [idx*4, idx*4 + 4).
    """
    # Pick the block index in [0, 15]; getrandbits(4) is a fast C call with no
    # application-level shared state.
    idx = getrandbits(4)
    # Extract the 4-bit nibble at block index `idx` (0 = least significant).
    shift = idx * 4
    nibble = (tablet_version >> shift) & 0xF
    return (idx << 4) | nibble


def random_tablet_version_block() -> int:
    """
    Generate a random tablet_version_block byte for cold start.
    """
    return getrandbits(8)


class Tablet(object):
    """
    Represents a single ScyllaDB tablet.
    It stores information about each replica, its host and shard,
    and the token interval in the format (first_token, last_token].
    """
# uint64 hash; None means unknown -- a cold start, or a tablet learned over
    # TABLETS_ROUTING_V1, which does not report a version.
    __slots__ = ('first_token', 'last_token', 'replicas', 'tablet_version', '_replica_dict')

    def __init__(self, first_token=0, last_token=0, replicas=None, tablet_version=None):
        self.first_token = first_token
        self.last_token = last_token
        # Materialize once: `replicas` may be a one-shot iterator, and both
        # the tuple and the lookup dict must come from the same iteration.
        self.replicas = tuple(replicas) if replicas is not None else None
        self._replica_dict = {r[0]: r[1] for r in self.replicas} if self.replicas else {}
        self.tablet_version = tablet_version

    def __str__(self):
        return "<Tablet: first_token=%s last_token=%s replicas=%s tablet_version=%s>" \
               % (self.first_token, self.last_token, self.replicas, self.tablet_version)
    __repr__ = __str__

    @staticmethod
    def _is_valid_tablet(replicas):
        return replicas is not None and len(replicas) != 0

    @staticmethod
    def from_row(first_token, last_token, replicas, tablet_version=None):
        if Tablet._is_valid_tablet(replicas):
            if tablet_version is not None:
                # tablet_version is an unsigned 64-bit value, but it is
                # deserialized from the wire as a signed LongType; normalize it
                # back to unsigned so it matches the server's representation.
                tablet_version &= 0xFFFFFFFFFFFFFFFF
            tablet = Tablet(first_token, last_token, replicas, tablet_version)
            return tablet
        return None

    @property
    def leader(self) -> Optional[UUID]:
        """
        The ``host_id`` of this tablet's Raft leader, or ``None`` if there is
        none to report.

        A strongly-consistent tablet has one distinguished replica, the leader,
        that coordinates its writes and its linearizable reads. The server does
        not name it in a separate field: ``TABLETS_ROUTING_V2`` orders the
        replica set so that the leader comes first, which is why this is simply
        ``replicas[0]``.

        That ordering only carries meaning for a tablet of a strongly-consistent
        keyspace that was learned over V2. An eventually-consistent tablet has no
        leader at all, and a tablet learned over ``TABLETS_ROUTING_V1`` -- which
        reports no ``tablet_version``, so ``tablet_version`` is ``None`` -- has no
        leader ordering either. Callers must establish both of those before
        treating the result as a leader; this property only answers "which
        replica is first, if any".

        Returns ``None`` for a tablet with no replicas rather than raising, so
        callers do not have to guard the lookup themselves.
        """
        if not self.replicas:
            return None
        return self.replicas[0][0]

    def replica_contains_host_id(self, uuid: UUID) -> bool:
        return uuid in self._replica_dict

    def get_replica_shard_id(self, uuid: UUID) -> Optional[int]:
        return self._replica_dict.get(uuid)


class Tablets(object):
    _lock = None
    _tablets = {}

    def __init__(self, tablets):
        self._tablets = tablets
        self._lock = Lock()

    def table_has_tablets(self, keyspace, table) -> bool:
        return bool(self._tablets.get((keyspace, table), []))

    def get_tablet_for_key(self, keyspace, table, t):
        tablet = self._tablets.get((keyspace, table), [])
        if not tablet:
            return None

        id = bisect_left(tablet, t.value, key=_get_last_token)
        if id < len(tablet) and t.value > tablet[id].first_token:
            return tablet[id]
        return None

    def drop_tablets(self, keyspace: str, table: Optional[str] = None):
        with self._lock:
            if table is not None:
                self._tablets.pop((keyspace, table), None)
                return

            to_be_deleted = []
            for key in self._tablets.keys():
                if key[0] == keyspace:
                    to_be_deleted.append(key)

            for key in to_be_deleted:
                del self._tablets[key]

    def drop_tablets_by_host_id(self, host_id: Optional[UUID]):
        if host_id is None:
            return
        with self._lock:
            for key, tablets in self._tablets.items():
                to_be_deleted = []
                for tablet_id, tablet in enumerate(tablets):
                    if tablet.replica_contains_host_id(host_id):
                        to_be_deleted.append(tablet_id)

                for tablet_id in reversed(to_be_deleted):
                    tablets.pop(tablet_id)

    def add_tablet(self, keyspace, table, tablet):
        with self._lock:
            tablets_for_table = self._tablets.setdefault((keyspace, table), [])

            # find first overlapping range
            start = bisect_left(tablets_for_table, tablet.first_token, key=_get_first_token)
            if start > 0 and tablets_for_table[start - 1].last_token > tablet.first_token:
                start = start - 1

            # find last overlapping range
            end = bisect_left(tablets_for_table, tablet.last_token, key=_get_last_token)
            if end < len(tablets_for_table) and tablets_for_table[end].first_token >= tablet.last_token:
                end = end - 1

            if start <= end:
                del tablets_for_table[start:end + 1]

            tablets_for_table.insert(start, tablet)

