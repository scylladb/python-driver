# Copyright 2026 ScyllaDB, Inc.
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

import time
import unittest
from concurrent.futures import ThreadPoolExecutor

import pytest

from cassandra.ssl_session_cache import SSLSessionCache


class _Session(object):
    """
    Stands in for ssl.SSLSession, which only a real handshake can produce.
    Only the session id matters here: it is how the cache tells a session the
    peer reissued from the one it handed back unchanged.
    """

    def __init__(self, id=b'\x01' * 32):
        self.id = id


class SSLSessionCacheTest(unittest.TestCase):

    def test_get_missing_key_returns_none(self):
        assert SSLSessionCache().get(('10.0.0.1', 9042)) is None

    def test_set_then_get(self):
        cache = SSLSessionCache()
        session = object()
        cache.set(('10.0.0.1', 9042), session)

        assert cache.get(('10.0.0.1', 9042)) is session
        assert cache.get(('10.0.0.2', 9042)) is None

    def test_get_does_not_consume_the_session(self):
        # Sessions are replayable: a burst of per-shard connections to one
        # node must all be able to offer the same cached session.
        cache = SSLSessionCache()
        session = object()
        cache.set(('10.0.0.1', 9042), session)

        assert [cache.get(('10.0.0.1', 9042)) for _ in range(10)] == [session] * 10
        assert len(cache) == 1

    def test_set_replaces_the_previous_session(self):
        cache = SSLSessionCache()
        older, newer = object(), object()
        cache.set(('10.0.0.1', 9042), older)
        cache.set(('10.0.0.1', 9042), newer)

        assert cache.get(('10.0.0.1', 9042)) is newer
        assert len(cache) == 1

    def test_none_session_is_ignored(self):
        cache = SSLSessionCache()
        session = object()
        cache.set(('10.0.0.1', 9042), session)
        cache.set(('10.0.0.1', 9042), None)

        assert cache.get(('10.0.0.1', 9042)) is session
        assert len(cache) == 1

    def test_evicts_least_recently_used_key(self):
        cache = SSLSessionCache(max_size=2)
        first, second, third = object(), object(), object()
        cache.set('first', first)
        cache.set('second', second)

        # Touching 'first' makes 'second' the least recently used.
        assert cache.get('first') is first
        cache.set('third', third)

        assert len(cache) == 2
        assert cache.get('second') is None
        assert cache.get('first') is first
        assert cache.get('third') is third

    def test_set_refreshes_recency(self):
        cache = SSLSessionCache(max_size=2)
        cache.set('first', object())
        cache.set('second', object())
        cache.set('first', object())
        cache.set('third', object())

        assert cache.get('second') is None
        assert cache.get('first') is not None

    def test_expired_entry_is_not_returned_and_is_dropped(self):
        cache = SSLSessionCache()
        cache.set('key', object(), lifetime=-1)

        assert cache.get('key') is None
        assert len(cache) == 0

    def test_live_entry_is_returned(self):
        cache = SSLSessionCache()
        session = object()
        cache.set('key', session, lifetime=3600)

        assert cache.get('key') is session

    def test_the_same_session_again_keeps_the_deadline_it_had(self):
        # What a resumed handshake below TLS 1.3 stores back: the session that
        # was offered, whose lifetime runs from when the peer issued it and not
        # from when it was last replayed.
        cache = SSLSessionCache()
        cache.set('key', _Session(), lifetime=0.05)
        cache.set('key', _Session(), lifetime=3600)

        time.sleep(0.06)

        assert cache.get('key') is None

    def test_a_reissued_session_starts_its_own_deadline(self):
        # What TLS 1.3 normally stores back: a fresh ticket, which is entitled
        # to the lifetime the peer announced with it.
        cache = SSLSessionCache()
        cache.set('key', _Session(b'first'), lifetime=-1)
        reissued = _Session(b'second')
        cache.set('key', reissued, lifetime=3600)

        assert cache.get('key') is reissued

    def test_the_offered_session_does_not_revive_an_entry_that_was_dropped(self):
        # The deadline passed between the offer and the store, and a lookup
        # dropped the entry.  Storing the offered session back would put a full
        # fresh lifetime on a session the peer issued long enough ago to have
        # expired.
        cache = SSLSessionCache()
        session = _Session()
        cache.set('key', session, lifetime=-1)
        assert cache.get('key') is None

        cache.set('key', session, lifetime=3600, offered=session)

        assert cache.get('key') is None
        assert len(cache) == 0

    def test_the_offered_session_does_not_displace_a_siblings(self):
        # Connections to one node are opened together: another may have stored
        # a session the peer reissued to it between this one's offer and its
        # store.  That entry is fresher than what this caller has to say.
        cache = SSLSessionCache()
        offered, reissued = _Session(b'offered'), _Session(b'reissued')
        cache.set('key', offered, lifetime=3600)
        cache.set('key', reissued, lifetime=3600)
        deadline = cache._sessions['key'][1]

        cache.set('key', offered, lifetime=3600, offered=offered)

        assert cache.get('key') is reissued
        assert cache._sessions['key'][1] == deadline

    def test_a_reissued_session_still_replaces_a_siblings(self):
        # The other side of it: what the peer issued to this connection is new,
        # and is entitled to the lifetime announced with it.
        cache = SSLSessionCache()
        offered, theirs = _Session(b'offered'), _Session(b'theirs')
        cache.set('key', theirs, lifetime=3600)
        mine = _Session(b'mine')

        cache.set('key', mine, lifetime=3600, offered=offered)

        assert cache.get('key') is mine

    def test_tickets_that_carry_no_session_id_are_not_told_apart(self):
        # RFC 5077 3.4 lets a server issue a ticket and send an empty session
        # id with it, and SSLSession exposes no ticket to compare instead, so
        # two of them are indistinguishable.  Reading them as the same session
        # keeps the deadline where it is; reading them as different would
        # re-stamp a full lifetime on what may be the ticket already held.
        cache = SSLSessionCache()
        first, second = _Session(b''), _Session(b'')
        cache.set('key', first, lifetime=0.05)
        cache.set('key', second, lifetime=3600)

        # One session as far as this can tell, so the entry keeps both the
        # deadline it had and the object holding it.
        assert cache.get('key') is first
        time.sleep(0.06)
        assert cache.get('key') is None

    def test_an_empty_id_costs_a_store_where_the_entry_has_gone(self):
        # The other half of that reading, and the reason it is the safe one:
        # the offered ticket and the one that came back cannot be told apart,
        # so this store is skipped rather than reviving a deadline.  The next
        # connection offers nothing and caches whatever it is given.
        cache = SSLSessionCache()
        offered = _Session(b'')
        cache.set('key', offered, lifetime=-1)
        assert cache.get('key') is None

        cache.set('key', _Session(b''), lifetime=3600, offered=offered)
        assert len(cache) == 0

        fresh = _Session(b'')
        cache.set('key', fresh, lifetime=3600)
        assert cache.get('key') is fresh

    def test_an_object_carrying_no_id_is_taken_to_be_new(self):
        # Distinct from an empty id: nothing the driver caches is in this
        # position, since every SSLSession has the attribute.
        cache = SSLSessionCache()
        cache.set('key', object(), lifetime=-1)
        session = object()
        cache.set('key', session, lifetime=3600)

        assert cache.get('key') is session

    def test_a_lifetime_replaces_the_previous_one(self):
        cache = SSLSessionCache()
        cache.set('key', object(), lifetime=-1)
        session = object()
        cache.set('key', session, lifetime=3600)

        assert cache.get('key') is session

    def test_a_dead_entry_is_evicted_before_a_live_one(self):
        # Whose lifetime has run out and which was used least recently are
        # independent once peers announce different lifetimes.
        cache = SSLSessionCache(max_size=3)
        cache.set('live-1', 'A', lifetime=3600)
        cache.set('live-2', 'B', lifetime=3600)
        cache.set('expired', 'C', lifetime=-1)

        cache.set('fourth', 'D', lifetime=3600)

        assert cache.get('live-1') == 'A'
        assert cache.get('live-2') == 'B'
        assert cache.get('fourth') == 'D'
        assert len(cache) == 3

    def test_the_lru_still_goes_when_nothing_has_expired(self):
        cache = SSLSessionCache(max_size=2)
        cache.set('first', 'A', lifetime=3600)
        cache.set('second', 'B', lifetime=3600)

        cache.set('third', 'C', lifetime=3600)

        assert cache.get('first') is None
        assert cache.get('second') == 'B'
        assert cache.get('third') == 'C'

    def test_a_dead_entry_lingers_until_it_is_looked_up_or_room_is_needed(self):
        # Documented rather than swept eagerly: nothing walks the cache on a
        # timer, so an entry nobody asks for and nobody needs room for stays.
        cache = SSLSessionCache(max_size=8)
        cache.set('expired', 'C', lifetime=-1)

        assert len(cache) == 1
        assert cache.get('expired') is None
        assert len(cache) == 0

    def test_discard(self):
        cache = SSLSessionCache()
        cache.set('key', object())
        cache.discard('key')

        assert cache.get('key') is None
        assert len(cache) == 0
        cache.discard('key')  # discarding what is not there is fine

    def test_storing_the_same_session_keeps_the_object_that_holds_it(self):
        # SSLSocket.session builds a new wrapper on every access, so what a
        # resumed connection stores back is another handle on the credential
        # already cached.  Keeping the one that is there is what lets the
        # connection that offered it retract it: identity is how discard()
        # tells its own session from one the peer issued in its place.
        cache = SSLSessionCache()
        offered = _Session()
        cache.set('key', offered, lifetime=3600)
        cache.set('key', _Session(), lifetime=3600)     # a sibling resumes

        assert cache.get('key') is offered

        cache.discard('key', offered)
        assert cache.get('key') is None

    def test_a_session_the_peer_issued_in_its_place_is_not_retractable(self):
        # The other side of it: that one never failed anything, and every later
        # connection would pay a full handshake for dropping it.
        cache = SSLSessionCache()
        offered = _Session(b'offered')
        cache.set('key', offered, lifetime=3600)
        reissued = _Session(b'reissued')
        cache.set('key', reissued, lifetime=3600)

        cache.discard('key', offered)

        assert cache.get('key') is reissued

    def test_discard_of_a_named_session_spares_a_newer_one(self):
        # A connection acting on a session it read earlier must not remove the
        # fresh one another connection stored under the same key meanwhile.
        cache = SSLSessionCache()
        older, newer = object(), object()
        cache.set('key', older)
        cache.set('key', newer)

        cache.discard('key', older)

        assert cache.get('key') is newer

    def test_discard_of_a_named_session_removes_it_when_still_current(self):
        cache = SSLSessionCache()
        session = object()
        cache.set('key', session)

        cache.discard('key', session)

        assert cache.get('key') is None

    def test_clear(self):
        cache = SSLSessionCache()
        cache.set('key', object())
        cache.clear()

        assert len(cache) == 0
        assert cache.get('key') is None

    def test_rejects_invalid_max_size(self):
        # A float would pass a plain `< 1` check and then never bound the cache
        # (nan and inf compare False against every limit), and True is an int
        # that passes it and would cap the cache at a single entry.
        for max_size in (0, -1, float('nan'), float('inf'), 2.5, '8', None,
                         True, False):
            with pytest.raises(ValueError):
                SSLSessionCache(max_size=max_size)

    def test_repr(self):
        cache = SSLSessionCache(max_size=7)
        cache.set('key', object())

        assert repr(cache) == '<SSLSessionCache max_size=7 size=1>'

    def test_concurrent_access_keeps_the_cache_bounded(self):
        cache = SSLSessionCache(max_size=8)

        def hammer(worker):
            for i in range(500):
                key = (worker + i) % 32
                cache.set(key, object())
                cache.get(key)
                assert len(cache) <= 8

        # result() re-raises whatever a worker hit, with its own traceback.
        with ThreadPoolExecutor(max_workers=8) as pool:
            for future in [pool.submit(hammer, worker) for worker in range(8)]:
                future.result()

        assert len(cache) <= 8
