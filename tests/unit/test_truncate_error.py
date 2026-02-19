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
from cassandra.protocol import TruncateError


class TruncateErrorTest(unittest.TestCase):
    """
    Test TruncateError exception behavior
    """

    def test_truncate_error_message_includes_server_message(self):
        """
        Verify that TruncateError includes the server-provided error message
        in addition to the generic summary.
        
        This addresses the issue where the error message was thought to be ignored.
        The server message should always be included in the error string representation.
        """
        server_message = "unconfigured table test_table"
        error = TruncateError(code=0x1003, message=server_message, info=None)
        error_str = str(error)
        
        # Verify both the summary and server message are in the error string
        self.assertIn("Error during truncate", error_str, 
                     "Generic summary should be in error string")
        self.assertIn(server_message, error_str,
                     "Server-provided message should be in error string")
        
    def test_truncate_error_code(self):
        """
        Verify that TruncateError has the correct error code (0x1003)
        """
        error = TruncateError(code=0x1003, message="test", info=None)
        self.assertEqual(error.code, 0x1003)
        self.assertEqual(error.error_code, 0x1003)

    def test_truncate_error_summary(self):
        """
        Verify that TruncateError has the correct summary message
        """
        error = TruncateError(code=0x1003, message="test", info=None)
        self.assertEqual(error.summary, "Error during truncate")


if __name__ == '__main__':
    unittest.main()
