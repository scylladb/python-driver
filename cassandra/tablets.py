from bisect import bisect_left
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
    def from_row(first_token, last_token, replicas, tablet_version=None):
        # Materialize once: `replicas` may be a one-shot iterator (e.g. a
        # generator), and a plain `if not replicas` truthiness check would
        # always be False for such an object even when it yields nothing,
        # since iterators have no __len__/__bool__ and are always truthy.
        replicas_tuple = tuple(replicas) if replicas is not None else ()
        if not replicas_tuple:
            return None
        if tablet_version is not None:
            # tablet_version is an unsigned 64-bit value, but it is
            # deserialized from the wire as a signed LongType; normalize it
            # back to unsigned so it matches the server's representation.
            tablet_version &= 0xFFFFFFFFFFFFFFFF
        return Tablet(first_token, last_token, replicas_tuple, tablet_version)

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
    def __init__(self, tablets):
        # NOTE: these are intentionally instance attributes only (not class
        # attributes) to avoid mutable class-level dicts being shared across
        # instances, e.g. if a future alternative constructor were to bypass
        # __init__.
        self._lock = Lock()
        self._tablets = tablets
        # Build parallel token index lists from any pre-populated data
        # (keyspace, table) -> list[int] for both _first_tokens/_last_tokens
        self._first_tokens = {
            key: [t.first_token for t in tlist]
            for key, tlist in tablets.items()
        }
        self._last_tokens = {
            key: [t.last_token for t in tlist]
            for key, tlist in tablets.items()
        }

    def table_has_tablets(self, keyspace, table) -> bool:
        return bool(self._tablets.get((keyspace, table), []))

    def get_tablet_for_key(self, keyspace, table, t):
        key = (keyspace, table)
        last_tokens = self._last_tokens.get(key)
        if not last_tokens:
            return None

        token_value = t.value
        id = bisect_left(last_tokens, token_value)
        if id < len(last_tokens) and token_value > self._first_tokens[key][id]:
            return self._tablets[key][id]
        return None

    def drop_tablets(self, keyspace: str, table: Optional[str] = None):
        with self._lock:
            if table is not None:
                key = (keyspace, table)
                self._tablets.pop(key, None)
                self._first_tokens.pop(key, None)
                self._last_tokens.pop(key, None)
                return

            to_be_deleted = []
            for key in self._tablets.keys():
                if key[0] == keyspace:
                    to_be_deleted.append(key)

            for key in to_be_deleted:
                del self._tablets[key]
                self._first_tokens.pop(key, None)
                self._last_tokens.pop(key, None)

    def drop_tablets_by_host_id(self, host_id: Optional[UUID]):
        if host_id is None:
            return
        with self._lock:
            for key, tablets in self._tablets.items():
                # Filter in one pass instead of popping one-by-one (O(n) vs O(k*n))
                keep = [i for i, t in enumerate(tablets)
                        if not t.replica_contains_host_id(host_id)]
                if len(keep) == len(tablets):
                    continue  # nothing to drop
                self._tablets[key] = [tablets[i] for i in keep]
                first = self._first_tokens[key]
                last = self._last_tokens[key]
                self._first_tokens[key] = [first[i] for i in keep]
                self._last_tokens[key] = [last[i] for i in keep]

    def add_tablet(self, keyspace, table, tablet):
        with self._lock:
            key = (keyspace, table)
            tablets_for_table = self._tablets.setdefault(key, [])
            first_tokens = self._first_tokens.setdefault(key, [])
            last_tokens = self._last_tokens.setdefault(key, [])

            # find first overlapping range
            start = bisect_left(first_tokens, tablet.first_token)
            if start > 0 and last_tokens[start - 1] > tablet.first_token:
                start = start - 1

            # find last overlapping range
            end = bisect_left(last_tokens, tablet.last_token)
            if end < len(last_tokens) and first_tokens[end] >= tablet.last_token:
                end = end - 1

            if start <= end:
                del tablets_for_table[start:end + 1]
                del first_tokens[start:end + 1]
                del last_tokens[start:end + 1]

            tablets_for_table.insert(start, tablet)
            first_tokens.insert(start, tablet.first_token)
            last_tokens.insert(start, tablet.last_token)

