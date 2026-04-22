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

"""
Test to demonstrate the libevwrapper atexit cleanup issue.

This test demonstrates the problem where the atexit callback is registered
with _global_loop=None at import time, causing it to receive None during
shutdown instead of the actual loop instance.
"""

import unittest
import sys
import subprocess
import tempfile
import os
from pathlib import Path

from cassandra import DependencyException

try:
    from cassandra.io.libevreactor import LibevConnection
except (ImportError, DependencyException):
    LibevConnection = None

from tests import is_monkey_patched


class LibevAtexitCleanupTest(unittest.TestCase):
    """
    Test case to demonstrate the atexit cleanup bug in libevreactor.
    
    The bug: atexit.register(partial(_cleanup, _global_loop)) is called when
    _global_loop is None, so the cleanup function receives None at shutdown
    instead of the actual LibevLoop instance that was created later.
    """

    def setUp(self):
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test libev with monkey patching")
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')

    def test_atexit_callback_uses_current_global_loop(self):
        """
        Test that verifies the atexit callback fix.
        
        The fix uses a wrapper function _atexit_cleanup() that looks up the
        current value of _global_loop at shutdown time, instead of capturing
        it at import time with partial().
        
        @since 3.29
        @jira_ticket PYTHON-XXX
        @expected_result The atexit handler calls cleanup with the actual loop
        
        @test_category connection
        """
        from cassandra.io import libevreactor
        
        # Verify the fix: _atexit_cleanup should exist as a module-level function
        self.assertTrue(hasattr(libevreactor, '_atexit_cleanup'),
                       "Module should have _atexit_cleanup function")

        # Verify it's not a partial (the old buggy implementation)
        from functools import partial
        self.assertNotIsInstance(libevreactor._atexit_cleanup, partial,
                                "The _atexit_cleanup should NOT be a partial function")

        # Verify it's actually a function
        self.assertTrue(callable(libevreactor._atexit_cleanup),
                       "_atexit_cleanup should be callable")

        # Initialize the reactor
        LibevConnection.initialize_reactor()

        # At this point, libevreactor._global_loop is not None
        self.assertIsNotNone(libevreactor._global_loop,
                           "Global loop should be initialized")

        # The fix: _atexit_cleanup is a function that will look up
        # _global_loop when it's called, not a partial with captured args
        self.assertEqual(libevreactor._atexit_cleanup.__name__, '_atexit_cleanup',
                        "The function should have the correct name")

    def test_shutdown_cleanup_works_with_fix(self):
        """
        Test that verifies the atexit cleanup fix works in a subprocess.
        
        This test creates a minimal script that:
        1. Imports the driver
        2. Initializes the reactor (creates the global loop)
        3. Verifies the _atexit_cleanup function is available
        4. Exits without explicit cleanup
        
        With the fix, atexit should properly clean up the loop using the
        wrapper function that looks up _global_loop at shutdown time.
        
        @since 3.29
        @jira_ticket PYTHON-XXX
        @expected_result The subprocess shows the fix is working
        
        @test_category connection
        """
        # Create a test script that verifies the fix
        test_script = '''
import sys
import os

# Add the driver path
sys.path.insert(0, {driver_path!r})

# Import and setup
from cassandra.io import libevreactor
from cassandra.io.libevreactor import LibevConnection
import atexit

# Initialize the reactor (creates the global loop)
LibevConnection.initialize_reactor()

print("Global loop initialized:", libevreactor._global_loop is not None)

# Verify the fix is in place: _atexit_cleanup should be a module-level function
if hasattr(libevreactor, '_atexit_cleanup'):
    print("FIXED: Module has _atexit_cleanup function")
    print("This function will look up _global_loop at shutdown time")
    # Verify it's not using partial with None
    import inspect
    source = inspect.getsource(libevreactor._atexit_cleanup)
    if "global _global_loop" in source and "_global_loop is not None" in source:
        print("Verified: _atexit_cleanup uses current _global_loop value")
else:
    print("BUG: No _atexit_cleanup function found")

# Exit without explicit cleanup - atexit should handle it properly with the fix!
print("Exiting with proper cleanup...")
'''
        
        driver_path = str(Path(__file__).parent.parent.parent.parent)
        script_content = test_script.format(driver_path=driver_path)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(script_content)
            script_path = f.name
        
        try:
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            output = result.stdout
            print("\n=== Subprocess Output ===")
            print(output)
            print("=== End Output ===\n")
            
            # Verify the output shows the fix is working
            self.assertIn("Global loop initialized: True", output)
            self.assertIn("FIXED: Module has _atexit_cleanup function", output)
            self.assertIn("Verified: _atexit_cleanup uses current _global_loop value", output)
            self.assertNotIn("BUG", output.replace("BUG STILL PRESENT", "").replace("DEBUG", ""))  # Allow "BUG" only in success message

        finally:
            os.unlink(script_path)


class LibevShutdownRaceConditionTest(unittest.TestCase):
    """
    Tests to analyze potential race conditions and crashes during shutdown.
    """

    def setUp(self):
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test libev with monkey patching")
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')

    def test_cleanup_with_fix_properly_shuts_down(self):
        """
        Test to verify the fix properly shuts down the event loop.
        
        With the fix in place, the atexit cleanup will:
        1. Look up the current _global_loop value (not None)
        2. Call _cleanup with the actual loop instance
        3. Properly shut down the loop and its watchers
        
        This prevents the crash scenario where:
        - Various modules are being torn down during Python shutdown
        - The libev event loop is still running
        - Callbacks fire and try to access deallocated Python objects
        
        @since 3.29
        @jira_ticket PYTHON-XXX
        @expected_result Cleanup properly shuts down the loop with the fix
        
        @test_category connection
        """
        from cassandra.io import libevreactor
        from cassandra.io.libevreactor import _cleanup, _atexit_cleanup

        LibevConnection.initialize_reactor()
        
        # Verify the loop exists
        self.assertIsNotNone(libevreactor._global_loop)

        # Before cleanup, the loop should not be shut down
        self.assertFalse(libevreactor._global_loop._shutdown,
                        "Loop should not be shut down initially")
        
        # Simulate what the OLD buggy code would do
        _cleanup(None)  # This does nothing
        self.assertFalse(libevreactor._global_loop._shutdown,
                        "Loop should NOT be shut down when cleanup receives None")
        
        # Now test the FIX: call the wrapper that looks up _global_loop
        _atexit_cleanup()  # This is what atexit will actually call
        
        # With the fix, the loop should be properly shut down
        self.assertTrue(libevreactor._global_loop._shutdown,
                       "Loop should be shut down when _atexit_cleanup is called")


if __name__ == '__main__':
    unittest.main()
