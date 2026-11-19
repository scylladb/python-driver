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

import json
import os
from pathlib import Path
import runpy
import signal
import subprocess
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from setuptools._distutils.errors import (
    CompileError, DistutilsExecError, LinkError)

from tests.unit.io import test_libevwrapper_finalization as native


class LibevBuildTest(unittest.TestCase):

    def test_missing_prerequisites_skip(self):
        for failure in (CompileError('missing header'),
                        DistutilsExecError('missing compiler'),
                        LinkError('missing library')):
            with self.subTest(failure=failure), \
                    tempfile.TemporaryDirectory() as directory:
                compiler = Mock()
                compiler.compile.side_effect = failure
                with patch.object(native, 'new_compiler', return_value=compiler), \
                        patch.object(native, 'customize_compiler'):
                    with self.assertRaises(unittest.SkipTest):
                        native._build_harness(Path(directory))

    def test_broken_harness_fails_after_successful_probe(self):
        with tempfile.TemporaryDirectory() as directory:
            compiler = Mock()
            compiler.compile.side_effect = [
                ['probe.o'], CompileError('broken harness')]
            with patch.object(native, 'new_compiler', return_value=compiler), \
                    patch.object(native, 'customize_compiler'):
                with self.assertRaises(CompileError):
                    native._build_harness(Path(directory))

    def test_compiler_diagnostics_are_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'broken_build.py'
            source.write_text(
                'def _build_harness(directory):\n'
                '    raise RuntimeError("compiler diagnostic")\n')
            with patch.object(native, '__file__', str(source)):
                with self.assertRaisesRegex(RuntimeError, 'compiler diagnostic'):
                    native._compile_harness(Path(directory))

    def test_missing_prerequisites_skip_in_child(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'missing_prerequisites.py'
            source.write_text(
                'import unittest\n'
                'def _build_harness(directory):\n'
                '    raise unittest.SkipTest("missing native prerequisite")\n')
            with patch.object(native, '__file__', str(source)):
                with self.assertRaisesRegex(
                        unittest.SkipTest, 'missing native prerequisite'):
                    native._compile_harness(Path(directory))

    @unittest.skipUnless(os.name == 'posix', 'build process groups require POSIX')
    def test_compiler_timeout_is_a_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'slow_build.py'
            source.write_text(
                'def _build_harness(directory):\n'
                '    import subprocess, sys\n'
                '    subprocess.run([sys.executable, "-c", '
                '"from pathlib import Path; import sys, time; '
                'time.sleep(0.5); Path(sys.argv[1]).touch(); time.sleep(60)", '
                'str(directory / "child_started")])\n')
            # Start the timeout after the grandchild owns the output pipe.
            popen = subprocess.Popen
            started = []

            def start_ready_process(*args, **kwargs):
                process = popen(*args, **kwargs)
                try:
                    deadline = time.monotonic() + 30
                    while not (Path(directory) / 'child_started').is_file():
                        if process.poll() is not None:
                            self.fail('build fixture exited before child startup')
                        if time.monotonic() >= deadline:
                            self.fail('build fixture child did not start')
                        time.sleep(0.01)
                except BaseException:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        # The process group may have exited before cleanup.
                        pass
                    process.communicate()
                    raise
                started.append(time.monotonic())
                return process

            with patch.object(native, '__file__', str(source)), \
                    patch.object(native.subprocess, 'Popen',
                                 side_effect=start_ready_process):
                with self.assertRaises(subprocess.TimeoutExpired):
                    native._compile_harness(Path(directory), timeout=0.2)
                self.assertLess(time.monotonic() - started[0], 10)

    def test_paths_include_overrides_and_conan(self):
        libev_paths = runpy.run_path(str(
            Path(__file__).resolve().parents[2]
            / 'build-release/libev.py'))['libev_paths']
        with tempfile.TemporaryDirectory() as directory:
            conan = Path(directory) / 'build-release/conan/conandeps.env'
            conan.parent.mkdir(parents=True)
            conan.write_text(json.dumps({'include_dirs': ['/conan/include'],
                                         'library_dirs': ['/conan/lib']}))
            with patch.dict(os.environ, {
                    'CASS_DRIVER_LIBEV_INCLUDES':
                        ' , /custom include , , /other/include , ',
                    'CASS_DRIVER_LIBEV_LIBS': ' /custom lib , , /other/lib '}), \
                    patch('sys.platform', 'linux'):
                includes, libraries = libev_paths(directory)
            self.assertEqual(
                includes, ['/custom include', '/other/include', '/conan/include'])
            self.assertEqual(
                libraries, ['/custom lib', '/other/lib', '/conan/lib'])
