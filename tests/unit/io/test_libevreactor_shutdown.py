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

"""
Test to demonstrate the libevwrapper atexit cleanup issue.

This test demonstrates the problem where the atexit callback is registered
with _global_loop=None at import time, causing it to receive None during
shutdown instead of the actual loop instance.
"""

import unittest
import atexit
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

    def test_atexit_callback_registered_with_none(self):
        """
        Test that demonstrates the atexit callback bug.
        
        The atexit.register(partial(_cleanup, _global_loop)) line is executed
        when _global_loop is None. This means the partial function captures
        None as the argument, and when atexit calls it during shutdown, it
        passes None to _cleanup instead of the actual loop instance.
        
        @since 3.29
        @jira_ticket PYTHON-XXX
        @expected_result The test demonstrates that atexit cleanup is broken
        
        @test_category connection
        """
        from cassandra.io import libevreactor
        from functools import partial
        
        # Check the current atexit handlers
        # Note: atexit._exithandlers is an implementation detail but useful for debugging
        if hasattr(atexit, '_exithandlers'):
            # Find our cleanup handler
            cleanup_handler = None
            for handler in atexit._exithandlers:
                func = handler[0]
                # Check if this is our partial(_cleanup, _global_loop) handler
                if isinstance(func, partial):
                    if func.func.__name__ == '_cleanup':
                        cleanup_handler = func
                        break
            
            if cleanup_handler:
                # The problem: the partial was created with _global_loop=None
                # So even if _global_loop is later set to a LibevLoop instance,
                # the atexit callback will still call _cleanup(None)
                captured_arg = cleanup_handler.args[0] if cleanup_handler.args else None
                
                # This assertion will fail after LibevConnection.initialize_reactor()
                # is called and _global_loop is set to a LibevLoop instance
                LibevConnection.initialize_reactor()
                
                # At this point, libevreactor._global_loop is not None
                self.assertIsNotNone(libevreactor._global_loop,
                                   "Global loop should be initialized")
                
                # But the atexit handler still has None captured!
                self.assertIsNone(captured_arg,
                                "The atexit handler captured None, not the actual loop instance. "
                                "This is the BUG: cleanup will receive None at shutdown!")
                
    def test_shutdown_crash_scenario_subprocess(self):
        """
        Test that simulates a Python shutdown crash scenario in a subprocess.
        
        This test creates a minimal script that:
        1. Imports the driver
        2. Creates a connection (which starts the event loop)
        3. Exits without explicit cleanup
        
        The expected behavior is that atexit should clean up the loop, but
        because of the bug, the cleanup receives None and doesn't actually
        stop the loop or its watchers. This can lead to crashes if callbacks
        fire during shutdown.
        
        @since 3.29
        @jira_ticket PYTHON-XXX
        @expected_result The subprocess demonstrates the cleanup issue
        
        @test_category connection
        """
        # Create a test script that demonstrates the issue
        test_script = '''
import sys
import os

# Add the driver path
sys.path.insert(0, {driver_path!r})

# Import and setup
from cassandra.io.libevreactor import LibevConnection, _global_loop
import atexit

# Initialize the reactor (creates the global loop)
LibevConnection.initialize_reactor()

print("Global loop initialized:", _global_loop is not None)

# Check what atexit will actually call
if hasattr(atexit, '_exithandlers'):
    from functools import partial
    for handler in atexit._exithandlers:
        func = handler[0]
        if isinstance(func, partial) and func.func.__name__ == '_cleanup':
            captured_arg = func.args[0] if func.args else None
            print("Atexit will call _cleanup with:", captured_arg)
            print("But _global_loop is:", _global_loop)
            print("BUG: Cleanup will receive None instead of the loop!")
            break

# Exit without explicit cleanup - atexit should handle it, but won't!
print("Exiting...")
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
            
            # Verify the output shows the bug
            self.assertIn("Global loop initialized: True", output)
            self.assertIn("Atexit will call _cleanup with: None", output)
            self.assertIn("BUG: Cleanup will receive None instead of the loop!", output)
            
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

    def test_callback_during_shutdown_scenario(self):
        """
        Test to document the potential crash scenario.
        
        When Python is shutting down:
        1. Various modules are being torn down
        2. The libev event loop may still be running
        3. If a callback (io_callback, timer_callback, prepare_callback) fires:
           - It calls PyGILState_Ensure()
           - It tries to call Python functions (PyObject_CallFunction)
           - If Python objects have been deallocated, this can crash
        
        The root cause: The atexit cleanup doesn't actually run because it
        receives None instead of the loop instance, so it never:
        - Sets _shutdown flag
        - Stops watchers
        - Joins the event loop thread
        
        @since 3.29
        @jira_ticket PYTHON-XXX
        @expected_result Documents the crash scenario
        
        @test_category connection
        """
        from cassandra.io.libevreactor import _global_loop, _cleanup
        
        # This test documents the issue - we can't easily reproduce a crash
        # in a unit test without actually tearing down Python, but we can
        # verify the conditions that lead to it
        
        LibevConnection.initialize_reactor()
        
        # Verify the loop exists
        self.assertIsNotNone(_global_loop)
        
        # Simulate what atexit would call (with the bug)
        _cleanup(None)  # BUG: receives None instead of _global_loop
        
        # The loop is still running because cleanup did nothing!
        self.assertFalse(_global_loop._shutdown,
                        "Loop should NOT be shut down when cleanup receives None")
        
        # Now call it correctly
        _cleanup(_global_loop)
        
        # Now it should be shut down
        self.assertTrue(_global_loop._shutdown,
                       "Loop should be shut down when cleanup receives the actual loop")


if __name__ == '__main__':
    unittest.main()
