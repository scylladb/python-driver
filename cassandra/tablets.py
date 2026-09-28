from bisect import bisect_left, bisect_right
from random import getrandbits
from threading import Lock
from typing import Optional
from uuid import UUID


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
    def __init__(self, tablets):
        # Instance-only: mutable class-level dicts would be shared across instances.
        self._lock = Lock()
        self._tablets = tablets
        # Parallel (keyspace, table) -> list[int] so bisect runs without a key= callback.
        self._last_tokens = {
            key: [t.last_token for t in tlist]
            for key, tlist in tablets.items()
        }

    def table_has_tablets(self, keyspace, table) -> bool:
        return bool(self._tablets.get((keyspace, table), []))

    def get_tablet_for_key(self, keyspace, table, t):
        # Lock-free hot path: writers may be mid-update, so verify the pick covers the token.
        key = (keyspace, table)
        last_tokens = self._last_tokens.get(key)
        if not last_tokens:
            return None

        token_value = t.value
        try:
            tablet = self._tablets[key][bisect_left(last_tokens, token_value)]
        except (KeyError, IndexError):
            return None
        if tablet.first_token < token_value <= tablet.last_token:
            return tablet
        return None

    def drop_tablets(self, keyspace: str, table: Optional[str] = None):
        with self._lock:
            if table is not None:
                key = (keyspace, table)
                self._tablets.pop(key, None)
                self._last_tokens.pop(key, None)
                return

            to_be_deleted = []
            for key in self._tablets.keys():
                if key[0] == keyspace:
                    to_be_deleted.append(key)

            for key in to_be_deleted:
                del self._tablets[key]
                self._last_tokens.pop(key, None)

    def drop_tablets_by_host_id(self, host_id: Optional[UUID]):
        if host_id is None:
            return
        with self._lock:
            emptied = []
            for key, tablets in self._tablets.items():
                # Filter in one pass instead of popping one-by-one (O(n) vs O(k*n))
                kept = [t for t in tablets if not t.replica_contains_host_id(host_id)]
                if len(kept) == len(tablets):
                    continue  # nothing to drop
                if kept:
                    self._tablets[key] = kept
                    self._last_tokens[key] = [t.last_token for t in kept]
                else:
                    emptied.append(key)
            # A table left with no tablets must not keep an empty entry in
            # either map; drop both keys entirely instead.
            for key in emptied:
                del self._tablets[key]
                self._last_tokens.pop(key, None)

    def add_tablet(self, keyspace, table, tablet):
        with self._lock:
            key = (keyspace, table)
            tablets_for_table = self._tablets.setdefault(key, [])
            last_tokens = self._last_tokens.setdefault(key, [])

            # find first overlapping range
            start = bisect_right(last_tokens, tablet.first_token)

            # find last overlapping range
            end = bisect_left(last_tokens, tablet.last_token)
            if end < len(last_tokens) and tablets_for_table[end].first_token >= tablet.last_token:
                end = end - 1

            # Slice assignment: no memmove when one tablet replaces one, and inserts when start > end.
            tablets_for_table[start:end + 1] = (tablet,)
            last_tokens[start:end + 1] = (tablet.last_token,)

