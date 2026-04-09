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
    __slots__ = ('first_token', 'last_token', 'replicas', 'tablet_version', '_replica_dict')

    def __init__(self, first_token=0, last_token=0, replicas=None, tablet_version=None):
        self.first_token = first_token
        self.last_token = last_token
        # Materialize once: `replicas` may be a one-shot iterator, and both
        # the tuple and the lookup dict must come from the same iteration.
        self.replicas = tuple(replicas) if replicas is not None else None
        # Keyed by host_id.int: UUID.__hash__ is pure Python, int hashing is C (~2x faster).
        self._replica_dict = {r[0].int: r[1] for r in self.replicas} if self.replicas else {}
        # uint64 hash; None = unknown (cold start, or learned over TABLETS_ROUTING_V1).
        self.tablet_version = tablet_version

    def __str__(self):
        return "<Tablet: first_token=%s last_token=%s replicas=%s tablet_version=%s>" \
               % (self.first_token, self.last_token, self.replicas, self.tablet_version)
    __repr__ = __str__

    @staticmethod
    def from_row(first_token, last_token, replicas, tablet_version=None):
        if tablet_version is not None:
            # tablet_version is an unsigned 64-bit value, but it is
            # deserialized from the wire as a signed LongType; normalize it
            # back to unsigned so it matches the server's representation.
            tablet_version &= 0xFFFFFFFFFFFFFFFF
        # __init__ materializes replicas, so empty generators are caught too.
        tablet = Tablet(first_token, last_token, replicas, tablet_version)
        return tablet if tablet.replicas else None

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

    def replica_contains_host_id(self, uuid: Optional[UUID]) -> bool:
        # A host whose id is not yet known (discovery/metadata transitions) is
        # not a replica; treat it as a non-match rather than raising.
        if uuid is None:
            return False
        return uuid.int in self._replica_dict

    def get_replica_shard_id(self, uuid: Optional[UUID]) -> Optional[int]:
        if uuid is None:
            return None
        return self._replica_dict.get(uuid.int)


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
            for key, tablets in list(self._tablets.items()):
                kept = [tablet for tablet in tablets if not tablet.replica_contains_host_id(host_id)]
                if len(kept) != len(tablets):
                    self._tablets[key] = kept

    def add_tablet(self, keyspace, table, tablet):
        with self._lock:
            # Copy-on-write: lock-free readers in get_tablet_for_key may hold the old list.
            tablets_for_table = list(self._tablets.get((keyspace, table), ()))

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
            self._tablets[(keyspace, table)] = tablets_for_table

