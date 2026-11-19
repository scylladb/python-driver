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

import os
from pathlib import Path
import runpy
import signal
import subprocess
import sys
import sysconfig
import tempfile
import unittest

from setuptools._distutils.ccompiler import new_compiler
from setuptools._distutils.errors import CompileError, DistutilsExecError, LinkError
from setuptools._distutils.sysconfig import customize_compiler

from cassandra import DependencyException

try:
    from cassandra.io import libevwrapper
except (ImportError, DependencyException):
    libevwrapper = None


_NATIVE_TEST = r'''
#include "libevwrapper.c"
#include <stdio.h>
#include <stdlib.h>

static int callback_kind;

static void finalized_callback(void) {
    if (Py_IsInitialized()) {
        fputs("interpreter is still initialized\n", stderr);
        _Exit(2);
    }
    if (callback_kind == 0) {
        libevwrapper_IO self = {0};
        ev_io watcher = {0};
        self.callback = Py_None;
        watcher.data = &self;
        io_callback(NULL, &watcher, EV_READ);
    } else if (callback_kind == 1) {
        libevwrapper_Prepare self = {0};
        ev_prepare watcher = {0};
        self.callback = Py_None;
        watcher.data = &self;
        prepare_callback(NULL, &watcher, 0);
    } else {
        libevwrapper_Timer self = {0};
        ev_timer watcher = {0};
        self.callback = Py_None;
        watcher.data = &self;
        timer_callback(NULL, &watcher, 0);
    }
    puts("native callback returned after finalization");
}

static PyObject *register_callback(PyObject *self, PyObject *args) {
    int kind;
    if (!PyArg_ParseTuple(args, "i", &kind)) return NULL;
    callback_kind = kind;
    if (Py_AtExit(finalized_callback) != 0) {
        return PyErr_Format(PyExc_RuntimeError,
                            "Py_AtExit registration failed");
    }
    Py_RETURN_NONE;
}

static PyMethodDef test_methods[] = {
    {"register_callback", register_callback, METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef test_module = {
    PyModuleDef_HEAD_INIT, "_libev_finalization_test", NULL, -1, test_methods
};

PyMODINIT_FUNC PyInit__libev_finalization_test(void) {
    return PyModule_Create(&test_module);
}
'''


def _build_harness(directory):
    driver_root = Path(__file__).resolve().parents[3]
    libev_paths = runpy.run_path(
        str(driver_root / 'build-release/libev.py'))['libev_paths']
    libev_includes, library_dirs = libev_paths(driver_root)
    include_dirs = [sysconfig.get_path('include'),
                    str(driver_root / 'cassandra/io'), *libev_includes]
    compiler = new_compiler()
    customize_compiler(compiler)

    # Probe only prerequisites: errors in the actual harness must still fail.
    probe = directory / 'probe.c'
    probe.write_text('#include <Python.h>\n#include <ev.h>\n'
                     'int probe(void) { return ev_version_major(); }\n')
    try:
        objects = compiler.compile([str(probe)], output_dir=str(directory),
                                   include_dirs=include_dirs)
        compiler.link_shared_object(objects, str(directory / 'probe.so'),
                                    libraries=['ev'], library_dirs=library_dirs)
    except (CompileError, DistutilsExecError, LinkError, OSError) as exc:
        raise unittest.SkipTest(
            'native finalization harness requires a C compiler, Python headers '
            'and libev development files: %s' % exc) from exc

    source = directory / 'finalization.c'
    source.write_text(_NATIVE_TEST)
    output = directory / (
        '_libev_finalization_test' + sysconfig.get_config_var('EXT_SUFFIX'))
    objects = compiler.compile([str(source)], output_dir=str(directory),
                               include_dirs=include_dirs)
    compiler.link_shared_object(objects, str(output), libraries=['ev'],
                                library_dirs=library_dirs)


def _compile_harness(directory, timeout=30):
    # Bound the whole build, including compiler launches on older setuptools.
    script = (
        'import runpy, sys, unittest\n'
        'from pathlib import Path\n'
        'build = runpy.run_path(sys.argv[1])["_build_harness"]\n'
        'try:\n'
        '    build(Path(sys.argv[2]))\n'
        'except unittest.SkipTest as exc:\n'
        '    print(exc, file=sys.stderr)\n'
        '    sys.exit(77)\n'
    )
    command = [sys.executable, '-c', script, str(Path(__file__).resolve()),
               str(directory)]
    with subprocess.Popen(command, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True,
                          start_new_session=True) as process:
        try:
            diagnostics, _ = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            # Compiler children can retain the output pipe after Python exits.
            os.killpg(process.pid, signal.SIGKILL)
            diagnostics, _ = process.communicate()
            raise subprocess.TimeoutExpired(command, timeout,
                                            output=diagnostics)
    if process.returncode == 77:
        raise unittest.SkipTest(diagnostics)
    if process.returncode:
        raise RuntimeError('native finalization harness build failed:\n' + diagnostics)


@unittest.skipUnless(
    sys.implementation.name == 'cpython' and os.name == 'posix',
    'native finalization harness requires POSIX CPython')
@unittest.skipIf(libevwrapper is None, 'libev extension is unavailable')
class LibevNativeFinalizationTest(unittest.TestCase):
    """Invoke the actual native callbacks from a C finalization hook."""

    @classmethod
    def setUpClass(cls):
        directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(directory.cleanup)
        cls.directory = Path(directory.name)
        _compile_harness(cls.directory)

    def _check_callback(self, kind):
        script = (
            'import sys; sys.path.insert(0, sys.argv[1]); '
            'import _libev_finalization_test as native; '
            'native.register_callback(int(sys.argv[2]))'
        )
        result = subprocess.run(
            [sys.executable, '-c', script, str(self.directory), str(kind)],
            capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('native callback returned after finalization',
                      result.stdout)

    def test_io_callback_after_finalization(self):
        self._check_callback(0)

    def test_prepare_callback_after_finalization(self):
        self._check_callback(1)

    def test_timer_callback_after_finalization(self):
        self._check_callback(2)
