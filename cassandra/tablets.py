from bisect import bisect_left
from threading import Lock
from typing import Optional
from uuid import UUID


class Tablet(object):
    """
    Represents a single ScyllaDB tablet.
    It stores information about each replica, its host and shard,
    and the token interval in the format (first_token, last_token].
    """
    __slots__ = ('first_token', 'last_token', 'replicas', '_replica_dict')

    def __init__(self, first_token=0, last_token=0, replicas=None):
        self.first_token = first_token
        self.last_token = last_token
        if replicas is not None:
            replicas_tuple = tuple(replicas)
            self.replicas = replicas_tuple
            self._replica_dict = {r[0]: r[1] for r in replicas_tuple}
        else:
            self.replicas = None
            self._replica_dict = {}

    def __str__(self):
        return "<Tablet: first_token=%s last_token=%s replicas=%s>" \
               % (self.first_token, self.last_token, self.replicas)
    __repr__ = __str__

    @staticmethod
    def from_row(first_token, last_token, replicas):
        if not replicas:
            return None
        return Tablet(first_token, last_token, replicas)

    def replica_contains_host_id(self, uuid: UUID) -> bool:
        return uuid in self._replica_dict


class Tablets(object):
    _lock = None
    _tablets = {}       # (keyspace, table) -> list[Tablet]
    _first_tokens = {}  # (keyspace, table) -> list[int]
    _last_tokens = {}   # (keyspace, table) -> list[int]

    def __init__(self, tablets):
        self._tablets = tablets
        # Build parallel token index lists from any pre-populated data
        self._first_tokens = {
            key: [t.first_token for t in tlist]
            for key, tlist in tablets.items()
        }
        self._last_tokens = {
            key: [t.last_token for t in tlist]
            for key, tlist in tablets.items()
        }
        self._lock = Lock()

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

