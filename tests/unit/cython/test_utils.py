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

from tests.unit.cython.utils import cyimport, cythontest
utils_testhelper = cyimport('tests.unit.cython.utils_testhelper')

import unittest


class UtilsTest(unittest.TestCase):
    """Test Cython Utils functions"""

    @cythontest
    def test_datetime_from_timestamp(self):
        utils_testhelper.test_datetime_from_timestamp()
