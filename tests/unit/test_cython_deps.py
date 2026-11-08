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

import warnings
from unittest.mock import patch

import pytest

import cassandra.cython_deps as cython_deps


def test_have_numpy_is_deprecated_and_reports_numpy_availability():
    for available in (True, False):
        with patch.object(cython_deps, 'numpy_available', return_value=available):
            with pytest.warns(DeprecationWarning, match='numpy_available'):
                assert cython_deps.HAVE_NUMPY is available


def test_have_numpy_warning_points_at_the_caller():
    with patch.object(cython_deps, 'numpy_available', return_value=False), \
            warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        cython_deps.HAVE_NUMPY

    assert caught[0].filename == __file__


def test_unknown_attribute_raises_attribute_error():
    with pytest.raises(AttributeError, match="has no attribute 'NOT_A_NAME'"):
        cython_deps.NOT_A_NAME


def test_dir_lists_have_numpy_and_module_names():
    names = dir(cython_deps)
    assert 'HAVE_NUMPY' in names
    assert 'HAVE_CYTHON' in names
    assert names == sorted(names)
