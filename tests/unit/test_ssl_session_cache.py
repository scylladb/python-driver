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

import unittest
from concurrent.futures import ThreadPoolExecutor

import pytest

from cassandra.connection import SSLSessionCache


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

    def test_releasing_the_last_owner_drops_that_contexts_sessions(self):
        # A cache may be shared between clusters, so a departing one must take
        # only its own context's entries with it.
        cache = SSLSessionCache()
        one, other = object(), object()
        cache.acquire_context(one)
        cache.acquire_context(other)
        cache.set((one, ('10.0.0.1', 9042), None), object())
        cache.set((one, ('10.0.0.2', 9042), None), object())
        theirs = object()
        cache.set((other, ('10.0.0.1', 9042), None), theirs)

        cache.release_context(one)

        assert len(cache) == 1
        assert cache.get((other, ('10.0.0.1', 9042), None)) is theirs

    def test_sessions_survive_while_another_owner_holds_the_context(self):
        # Clusters sharing a cache to share sessions share the context those
        # sessions belong to, so one shutting down must not take them.
        cache = SSLSessionCache()
        context = object()
        cache.acquire_context(context)
        cache.acquire_context(context)
        session = object()
        cache.set((context, ('10.0.0.1', 9042), None), session)

        cache.release_context(context)
        assert cache.get((context, ('10.0.0.1', 9042), None)) is session

        cache.release_context(context)
        assert len(cache) == 0

    def test_release_leaves_keys_it_could_not_have_created(self):
        # A cache is not restricted to the driver's own keys -- the
        # concurrency test below uses plain ints -- so anything that is not a
        # tuple naming a context cannot belong to one.
        cache = SSLSessionCache()
        context = object()
        cache.acquire_context(context)
        for key in (7, 'a string', (), (object(), 'other context')):
            cache.set(key, 'not this context')
        cache.set((context, ('10.0.0.1', 9042), None), object())

        cache.release_context(context)

        assert len(cache) == 4
        assert cache.get(7) == 'not this context'

    def test_releasing_an_unregistered_context_does_nothing(self):
        cache = SSLSessionCache()
        session = object()
        cache.set((object(), ('10.0.0.1', 9042), None), session)

        cache.release_context(object())

        assert len(cache) == 1

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
