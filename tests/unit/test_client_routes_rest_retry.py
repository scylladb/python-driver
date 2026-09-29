# Copyright ScyllaDB, Inc.
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
"""Cluster-free tests for the 5xx retry in the client-routes REST helper."""

import importlib
import io
import os
import unittest
import urllib.error
import uuid
from unittest import mock

# tests.integration reads CASSANDRA_VERSION at import time and fails without
# it. Scope the fallback to the import so it does not leak into later tests.
with mock.patch.dict(os.environ, {"CASSANDRA_VERSION": "4.0.0"}):
    tcr = importlib.import_module("tests.integration.standard.test_client_routes")

ROUTES = [{"connection_id": uuid.uuid4(), "host_id": uuid.uuid4(),
           "address": "127.0.0.1", "port": 9042}]


def _http_error(code, body=b"boom"):
    return urllib.error.HTTPError("http://x", code, "err", {}, io.BytesIO(body))


def _ok():
    resp = mock.MagicMock()
    resp.__enter__.return_value = resp
    resp.status = 200
    return resp


class PostClientRoutesRetryTest(unittest.TestCase):

    def _run(self, side_effect, **kw):
        with mock.patch("urllib.request.urlopen", side_effect=side_effect) as up, \
                mock.patch("time.sleep") as sleep:
            try:
                tcr.post_client_routes("127.0.0.1", ROUTES, **kw)
            except Exception as e:
                return up, sleep, e
        return up, sleep, None

    def test_retries_5xx_then_succeeds(self):
        up, sleep, err = self._run([_http_error(500), _http_error(503), _ok()],
                                   )
        self.assertIsNone(err)
        self.assertEqual(up.call_count, 3)
        # a delay between attempts, none after success
        self.assertEqual(sleep.call_args_list, [mock.call(tcr.RETRY_DELAY)] * 2)
        self.assertGreater(tcr.RETRY_DELAY, 0)

    def test_exhaustion_raises_without_trailing_sleep(self):
        up, sleep, err = self._run([_http_error(500) for _ in range(tcr.MAX_ATTEMPTS)],
                                   )
        self.assertIsInstance(err, urllib.error.HTTPError)
        self.assertEqual(up.call_count, tcr.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, tcr.MAX_ATTEMPTS - 1)

    def test_non_retryable_fail_immediately(self):
        for exc in (_http_error(400), _http_error(404), _http_error(600),
                    urllib.error.URLError("refused")):
            up, sleep, err = self._run([exc])
            self.assertIsNotNone(err)
            self.assertEqual(up.call_count, 1)
            sleep.assert_not_called()

    def test_unreadable_body_keeps_status_and_retries(self):
        def bad():
            e = _http_error(500)
            e.read = mock.Mock(side_effect=OSError("truncated"))
            return e

        up, _, err = self._run([bad(), _ok()])
        self.assertIsNone(err)
        self.assertEqual(up.call_count, 2)

        with self.assertLogs(tcr.log, "ERROR") as cm:
            _, _, err = self._run([bad() for _ in range(tcr.MAX_ATTEMPTS)])
        self.assertEqual(err.code, 500)
        self.assertIn("<unreadable body>", "\n".join(cm.output))

    def test_timeout_passed_on_every_attempt(self):
        up, _, err = self._run([_http_error(500), _http_error(503), _ok()])
        self.assertIsNone(err)
        self.assertEqual(up.call_count, 3)
        for call in up.call_args_list:
            self.assertEqual(call.kwargs["timeout"], tcr.REST_TIMEOUT)

    def test_retryable_and_terminal_responses_closed(self):
        retryable = _http_error(500)
        retryable.close = mock.Mock(wraps=retryable.close)
        self._run([retryable, _ok()])
        retryable.close.assert_called_once()

        exhausted = [_http_error(500) for _ in range(tcr.MAX_ATTEMPTS)]
        for e in exhausted:
            e.close = mock.Mock(wraps=e.close)
        self._run(exhausted)
        for e in exhausted:
            e.close.assert_called_once()

        fatal = _http_error(404)
        fatal.close = mock.Mock(wraps=fatal.close)
        self._run([fatal])
        fatal.close.assert_called_once()

    def test_error_body_is_logged(self):
        with self.assertLogs(tcr.log, "WARNING") as cm:
            self._run([_http_error(500, b"settling"), _ok()])
        self.assertIn("settling", "\n".join(cm.output))
        with self.assertLogs(tcr.log, "ERROR") as cm:
            self._run([_http_error(404, b"nope")])
        self.assertIn("nope", "\n".join(cm.output))
