# Copyright DataStax, Inc.
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
from unittest.mock import patch

from tests.util import wait_until


class WaitUntilTests(unittest.TestCase):

    def test_succeeds_immediately(self):
        wait_until(lambda: True, delay=0, max_attempts=3)

    def test_succeeds_on_final_poll(self):
        """
        The condition becoming true on the very last poll (after the last sleep,
        with the attempt counter at max_attempts) must count as success, not a
        timeout - see https://github.com/scylladb/python-driver/pull/1021.
        """
        calls = []

        def condition():
            calls.append(None)
            return len(calls) > 3

        with patch('tests.util.time.sleep'):
            wait_until(condition, delay=0, max_attempts=3)

        self.assertEqual(len(calls), 4)

    def test_raises_after_exhausting_attempts(self):
        with patch('tests.util.time.sleep'):
            with self.assertRaises(Exception):
                wait_until(lambda: False, delay=0, max_attempts=3)
