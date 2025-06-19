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

from cassandra import cluster
from cassandra.cluster import ContinuousPagingOptions
from cassandra.datastax.graph.fluent import DseGraph
from cassandra.graph import VertexProperty

from tests.integration.advanced.graph import (
    GraphUnitTestCase, ClassicGraphSchema, CoreGraphSchema,
    VertexLabel, GraphTestConfiguration
)
from tests.integration.advanced.graph.fluent import (
    BaseExplicitExecutionTest, create_traversal_profiles, check_equality_base)

import unittest


class ContinuousPagingOptionsForTests(ContinuousPagingOptions):
    def __init__(self,
                 page_unit=ContinuousPagingOptions.PagingUnit.ROWS, max_pages=1,  # max_pages=1
                 max_pages_per_second=0, max_queue_size=4):
        super(ContinuousPagingOptionsForTests, self).__init__(page_unit, max_pages, max_pages_per_second,
                                                              max_queue_size)


def reset_paging_options():
    cluster.ContinuousPagingOptions = ContinuousPagingOptions

