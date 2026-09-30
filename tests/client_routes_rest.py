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
Client-routes REST helper shared by the integration tests and its unit tests.

Deliberately free of CCM/cluster imports and of any CASSANDRA_VERSION
requirement, so it can be imported on every platform (including the Windows
wheel builds) without starting or configuring a cluster.
"""

import json as _json
import logging
import urllib.error
import urllib.request
# Bind our own name for time.sleep, distinct from the process-wide time
# module: unrelated code (e.g. background reactor threads) also calls
# time.sleep, and tests mock this symbol to assert on the retry/backoff
# behavior without capturing those unrelated calls.
from time import sleep as _sleep

log = logging.getLogger(__name__)

REST_TIMEOUT = 30  # bounds the connect and each blocking socket read
RETRY_DELAY = 1
MAX_ATTEMPTS = 5
# Cap how much of an error body we read: with the per-read socket timeout this
# stops a trickling body from stalling the retry loop, and bounds log size.
MAX_ERROR_BODY_BYTES = 4096
MAX_LOG_BODY_CHARS = 512


def _sanitize_log_text(text, limit=MAX_LOG_BODY_CHARS):
    """Bound and escape a server-supplied body so it cannot forge CI log lines."""
    if len(text) > limit:
        text = text[:limit] + "...<truncated>"
    return text.encode("unicode_escape").decode("ascii")


def post_client_routes(contact_point, routes):
    """
    Post client routes to Scylla's REST API.

    :param contact_point: IP/hostname of a Scylla node (e.g. "127.0.0.1")
    :param routes: List of route dicts with keys: connection_id, host_id, address, port
                   and optionally tls_port
    """
    payload = []
    for route in routes:
        entry = {
            "connection_id": str(route["connection_id"]),
            "host_id": str(route["host_id"]),
            "address": route["address"],
            "port": route["port"],
        }
        if route.get("tls_port") is not None:
            entry["tls_port"] = route["tls_port"]
        payload.append(entry)

    url = "http://%s:10000/v2/client-routes" % contact_point
    log.info("Posting %d routes to %s", len(payload), url)
    data = _json.dumps(payload).encode("utf-8")

    # Right after a decommission the REST API can briefly answer 5xx. Retry
    # those (logged); 4xx and connection errors always raise.
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(req, timeout=REST_TIMEOUT) as response:
                log.info("Routes posted successfully (status %d)", response.status)
                return
        except urllib.error.HTTPError as e:
            try:
                body = e.read(MAX_ERROR_BODY_BYTES).decode("utf-8", "replace")
            except Exception:
                body = "<unreadable body>"
            finally:
                e.close()
            body = _sanitize_log_text(body)
            if 500 <= e.code < 600 and attempt < MAX_ATTEMPTS:
                log.warning(
                    "POST %s -> HTTP %d (attempt %d/%d), retrying: %s",
                    url, e.code, attempt, MAX_ATTEMPTS, body)
                _sleep(RETRY_DELAY)
                continue
            log.error("POST %s -> HTTP %d: %s", url, e.code, body)
            raise
