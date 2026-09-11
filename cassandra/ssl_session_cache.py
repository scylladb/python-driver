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
"""
Storage for TLS sessions, so that connections can resume one instead of
performing a full handshake.

The policy around this -- what may be offered to whom, for how long, and what
a handshake hands back -- lives with the connections in
:mod:`cassandra.connection`; what is here is only the keeping of them.
"""

import time
from collections import OrderedDict
from threading import Lock


class SSLSessionCache(object):
    """
    A thread-safe, bounded cache of TLS sessions, keyed by TLS peer identity.

    TLS clients can skip the expensive part of a handshake by replaying a
    session established earlier with the same peer (RFC 5077 session tickets
    for TLS 1.2, RFC 8446 pre-shared keys for TLS 1.3).  OpenSSL never does
    this on its own -- the client has to hold on to the session and offer it
    on the next connection -- so the driver keeps one of these caches per
    :class:`~.Cluster` and reuses sessions across every connection it opens,
    most importantly the burst of per-shard connections opened to a node at
    once.

    A cached session is not consumed by being used: the same session can be
    replayed by any number of concurrent connections, and each successful
    handshake stores back whatever the peer handed over -- a fresh session
    where one was issued, otherwise the same one again, which keeps the
    deadline it already had rather than starting a new one.  An entry whose lifetime has run out is never handed out again, and
    is dropped when it is looked up or when room is needed; entries otherwise
    go only by being replaced or, once the cache is full, by having been used
    least recently.  A session the server declines for any other reason simply
    results in a full handshake, which is what would have happened anyway.

    Instances are safe to use from multiple threads, and may be shared
    between clusters -- which is what makes sessions outlive the cluster that
    established them, so that a cluster replacing an earlier one resumes
    instead of handshaking in full.  A cache the driver created for a cluster
    lives and dies with it; one supplied to :class:`~.Cluster` belongs to
    whoever supplied it, and the driver removes an entry from it only to
    replace it, because its lifetime ran out, or to make room.  Note that an
    entry keeps the ``SSLContext`` its session was established with alive --
    CPython's ``SSLSession`` holds a reference to it -- so a long-lived cache
    holds the contexts of at most :attr:`max_size` peers.  :meth:`clear` drops
    everything, for a caller that wants them gone sooner.
    """

    def __init__(self, max_size=1024):
        """
        :param max_size: maximum number of peers to keep sessions for.  When
            exceeded, the least recently used entry is evicted.
        """
        # Anything but a positive integer is rejected outright rather than
        # compared against: a float such as nan or inf would pass a `< 1` check
        # and then leave the cache growing without bound, while True is an int
        # that passes it and would quietly cap the cache at one entry.
        if (not isinstance(max_size, int) or isinstance(max_size, bool)
                or max_size < 1):
            raise ValueError(
                "max_size must be a positive integer, got %r" % (max_size,))
        self._max_size = max_size
        self._sessions = OrderedDict()
        self._lock = Lock()

    @property
    def max_size(self):
        """The maximum number of peers this cache keeps sessions for."""
        return self._max_size

    def get(self, key):
        """
        Return the cached session for *key*, or :const:`None` if there is none
        or its lifetime has run out.  A session that is still live stays in the
        cache; an expired one is dropped.
        """
        with self._lock:
            entry = self._sessions.get(key)
            if entry is None:
                return None
            session, expires_at = entry
            if expires_at is not None and time.monotonic() >= expires_at:
                del self._sessions[key]
                return None
            self._sessions.move_to_end(key)
            return session

    def set(self, key, session, lifetime=None, offered=None):
        """
        Store *session* as the session to offer for *key*, replacing any
        previous one.  A :const:`None` session is ignored.

        A session the peer handed back unchanged keeps the deadline the entry
        already had, rather than starting a new one: its lifetime runs from
        when the peer issued it and not from when it was last replayed, so
        re-stamping a full lifetime on every reuse would let one ticket be
        offered for as long as connections keep being opened.  Resuming below
        TLS 1.3 is exactly that case -- an abbreviated handshake hands back the
        session that was offered, same id and same ticket -- while TLS 1.3
        normally issues a fresh one, which starts its own lifetime.  Comparing
        the two here is what makes the rule hold: what is cached, what the
        caller offered and what is replacing them are all read under the one
        lock that also stores the result, so a connection storing concurrently
        cannot land in between.

        :param lifetime: how much longer, in seconds, the session may be
            offered.  Once it has passed, the entry is dropped rather than
            returned.  :const:`None` means no limit, which callers should
            reserve for sessions that carry no lifetime of their own.
        :param offered: the session the caller offered on the handshake it is
            storing the result of, if any.  Storing that same session back when
            the entry no longer holds it is not a new session arriving, and is
            skipped: see below.
        """
        if session is None:
            return
        expires_at = None if lifetime is None else time.monotonic() + lifetime
        with self._lock:
            previous = self._sessions.get(key)
            if previous is not None and self._is_same_session(previous[0], session):
                # The entry keeps both its deadline and the object holding it.
                # ``SSLSocket.session`` builds a new wrapper on every access, so
                # what arrives here for a session already cached is another
                # handle on the same credential; swapping one for the other
                # changes nothing except the identity that :meth:`discard`
                # compares against, and a connection that offered the entry
                # would then be unable to retract what it offered.
                session, expires_at = previous
            elif offered is not None and self._is_same_session(offered, session):
                # The peer handed this caller back the very session it offered,
                # but the entry no longer holds it: another connection opened
                # alongside stored a session the peer reissued to it, or the
                # deadline passed and a lookup dropped the entry.  Either way
                # this caller has nothing to add -- and storing it would put a
                # deadline running from now on a session the peer issued at
                # some earlier point, which is the one thing the rule above
                # exists to prevent.
                return
            self._sessions[key] = (session, expires_at)
            self._sessions.move_to_end(key)
            if len(self._sessions) > self._max_size:
                # Whose lifetime has run out and which was used least recently
                # are independent once peers announce different lifetimes, so
                # evicting purely by recency can drop a live entry and keep a
                # dead one.  Take the dead ones first.
                self._drop_expired_unlocked()
            while len(self._sessions) > self._max_size:
                self._sessions.popitem(last=False)

    @staticmethod
    def _is_same_session(cached, session):
        """
        Whether *session* is the one already cached, so that the entry's
        deadline is not a new store's to move.

        ``SSLSocket.session`` builds a new object on each access, so identity
        cannot answer this on its own; a session id can, and is what tells a
        ticket the peer reissued from the one it handed back -- including the
        TLS 1.3 server that resumes without issuing one.

        An id of no length is a case of its own.  RFC 5077 section 3.4 lets a
        server issue a ticket and send an empty session id with it, and
        ``SSLSession`` exposes no ticket to compare instead, so two such
        sessions cannot be told apart at all.  They are reported as the same
        one, which is the conservative reading: the deadline then stays where
        it is, where calling them different would re-stamp a full lifetime on
        what may well be the ticket already held -- the one thing this
        comparison exists to prevent.  What that costs is resumption, never
        correctness: a reissued ticket inherits its predecessor's deadline, and
        one arriving where the entry has since gone is not stored at all, which
        the next connection puts right by offering nothing and storing afresh.
        An object carrying no id at all is not a session this can recognise,
        and is taken to be new.
        """
        if cached is session:
            return True
        cached_id = getattr(cached, 'id', None)
        return cached_id is not None and cached_id == getattr(session, 'id', None)

    def _drop_expired_unlocked(self):
        now = time.monotonic()
        for key in [key for key, (_, expires_at) in self._sessions.items()
                    if expires_at is not None and now >= expires_at]:
            del self._sessions[key]

    def discard(self, key, session=None):
        """
        Drop the session cached for *key*, if any.

        Give *session* to drop it only while that is still the cached one.  A
        caller acting on a session it read earlier needs this: by the time it
        decides to drop it, another connection may have stored a session the
        peer issued in its place, and that one is not the caller's to remove.

        The comparison is by identity, which :meth:`set` is what makes
        dependable: a store of the session already cached keeps the object
        that is there, so an entry changes identity only when it changes
        credential.
        """
        with self._lock:
            entry = self._sessions.get(key)
            if entry is None:
                return
            if session is not None and entry[0] is not session:
                return
            del self._sessions[key]

    def clear(self):
        """Drop all cached sessions."""
        with self._lock:
            self._sessions.clear()

    def __len__(self):
        with self._lock:
            return len(self._sessions)

    def __repr__(self):
        return "<%s max_size=%d size=%d>" % (
            self.__class__.__name__, self._max_size, len(self))
