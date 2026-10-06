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

import io
import unittest
import urllib.error
import uuid
from unittest import mock

import tests.client_routes_rest as tcr

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

    def _run(self, side_effect, retries=None, **kw):
        if retries is not None:
            kw["retries"] = retries
        with mock.patch("urllib.request.urlopen", side_effect=side_effect) as up, \
                mock.patch("tests.client_routes_rest._sleep") as sleep:
            try:
                tcr.post_client_routes("127.0.0.1", ROUTES, **kw)
            except Exception as e:
                return up, sleep, e
        return up, sleep, None

    def test_retry_contract_values(self):
        # Pinned literals: a change to the retry/timeout contract must be
        # intentional and visible in review, not silently absorbed.
        self.assertEqual(tcr.MAX_ATTEMPTS, 5)
        self.assertEqual(tcr.RETRY_DELAY, 1)
        self.assertEqual(tcr.REST_TIMEOUT, 30)
        self.assertEqual(tcr.MAX_ERROR_BODY_BYTES, 4096)
        self.assertEqual(tcr.ERROR_BODY_DEADLINE, 5)

    def test_retries_5xx_then_succeeds(self):
        up, sleep, err = self._run([_http_error(500), _http_error(503), _ok()],
                                   retries=tcr.MAX_ATTEMPTS)
        self.assertIsNone(err)
        self.assertEqual(up.call_count, 3)
        # a delay between attempts, none after success
        self.assertEqual(sleep.call_args_list, [mock.call(1)] * 2)

    def test_exhaustion_raises_without_trailing_sleep(self):
        up, sleep, err = self._run([_http_error(500) for _ in range(5)],
                                   retries=tcr.MAX_ATTEMPTS)
        self.assertIsInstance(err, urllib.error.HTTPError)
        self.assertEqual(up.call_count, 5)
        self.assertEqual(sleep.call_count, 4)

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

        up, _, err = self._run([bad(), _ok()], retries=tcr.MAX_ATTEMPTS)
        self.assertIsNone(err)
        self.assertEqual(up.call_count, 2)

        with self.assertLogs(tcr.log, "ERROR") as cm:
            _, _, err = self._run([bad() for _ in range(5)], retries=tcr.MAX_ATTEMPTS)
        self.assertEqual(err.code, 500)
        self.assertIn("<unreadable body>", "\n".join(cm.output))

    def test_error_body_read_is_capped(self):
        err = _http_error(500)
        err.read = mock.Mock(wraps=err.read)
        self._run([err, _ok()], retries=tcr.MAX_ATTEMPTS)
        # _read_error_body issues at least one read bounded by the byte cap
        first = err.read.call_args_list[0]
        self.assertEqual(first.args[0], tcr.MAX_ERROR_BODY_BYTES)

    def test_timeout_passed_on_every_attempt(self):
        up, _, err = self._run([_http_error(500), _http_error(503), _ok()],
                               retries=tcr.MAX_ATTEMPTS)
        self.assertIsNone(err)
        self.assertEqual(up.call_count, 3)
        for call in up.call_args_list:
            self.assertEqual(call.kwargs["timeout"], 30)

    def test_retryable_and_terminal_responses_closed(self):
        retryable = _http_error(500)
        retryable.close = mock.Mock(wraps=retryable.close)
        ok = _ok()
        self._run([retryable, ok], retries=tcr.MAX_ATTEMPTS)
        retryable.close.assert_called_once()
        # the successful response is closed by its context manager
        ok.__exit__.assert_called_once()

        exhausted = [_http_error(500) for _ in range(5)]
        for e in exhausted:
            e.close = mock.Mock(wraps=e.close)
        self._run(exhausted, retries=tcr.MAX_ATTEMPTS)
        for e in exhausted:
            e.close.assert_called_once()

        fatal = _http_error(404)
        fatal.close = mock.Mock(wraps=fatal.close)
        self._run([fatal], retries=tcr.MAX_ATTEMPTS)
        fatal.close.assert_called_once()

    def test_error_body_is_logged(self):
        with self.assertLogs(tcr.log, "WARNING") as cm:
            self._run([_http_error(500, b"settling"), _ok()], retries=tcr.MAX_ATTEMPTS)
        self.assertIn("settling", "\n".join(cm.output))
        with self.assertLogs(tcr.log, "ERROR") as cm:
            self._run([_http_error(404, b"nope")])
        self.assertIn("nope", "\n".join(cm.output))

    def test_error_body_is_sanitized_in_logs(self):
        # A server-supplied body must not be able to inject newlines or
        # terminal control sequences into CI logs.
        with self.assertLogs(tcr.log, "ERROR") as cm:
            self._run([_http_error(404, b"boom\nFORGED\x1b[31m")])
        out = "\n".join(cm.output)
        self.assertIn(r"boom\nFORGED\x1b[31m", out)
        self.assertNotIn("boom\nFORGED", out)

    def test_default_does_not_retry(self):
        # Only the post-decommission path has evidence of a transient 5xx, so
        # the default must surface a server failure immediately instead of
        # masking an unrelated regression behind a retry.
        up, sleep, err = self._run([_http_error(500), _ok()])
        self.assertIsInstance(err, urllib.error.HTTPError)
        self.assertEqual(err.code, 500)
        self.assertEqual(up.call_count, 1)
        sleep.assert_not_called()

    def test_retries_param_is_respected(self):
        up, sleep, err = self._run([_http_error(500), _http_error(503), _ok()],
                                   retries=3)
        self.assertIsNone(err)
        self.assertEqual(up.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(1)] * 2)

    def test_error_body_deadline_bounds_elapsed_time(self):
        # A peer that trickles bytes must not stall the loop past the deadline:
        # the second read should never happen once the clock passes it.
        err = _http_error(500)
        calls = {"n": 0}

        def slow_read(n):
            calls["n"] += 1
            if calls["n"] >= 2:
                raise AssertionError("read past deadline")
            return b"partial"

        err.read = mock.Mock(side_effect=slow_read)
        # Clock: first check ok, second check already past the deadline.
        clock = iter([0.0, 0.0, 100.0])
        with mock.patch("tests.client_routes_rest._monotonic",
                        side_effect=lambda: next(clock)):
            with self.assertLogs(tcr.log, "ERROR") as cm:
                self._run([err])
        self.assertIn("deadline exceeded", "\n".join(cm.output))

    def test_escaping_cap_is_applied_after_escaping(self):
        # Control chars expand when escaped; the logged body must still respect
        # the documented character cap.
        with self.assertLogs(tcr.log, "ERROR") as cm:
            self._run([_http_error(404, b"\x01" * 512)])
        msg = "\n".join(cm.output)
        logged = msg.split(": ", 1)[1]
        self.assertLessEqual(len(logged), tcr.MAX_LOG_BODY_CHARS + len("...<truncated>"))
