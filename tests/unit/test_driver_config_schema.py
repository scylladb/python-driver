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
"""
Tests of the vendored schema and of the helper that validates against it.

These do not exercise :class:`~.DriverConfigReporter`; they establish that the
contract is being enforced at all, so that the tests which do exercise it are
worth something. What the driver actually reports is checked in
``test_driver_config.py``.
"""

import copy
import json
import unittest

import jsonschema
import pytest

from tests.driver_config_schema import SCHEMA_PATH, load_schema, validate_report

MINIMAL_REPORT = {
    'version': 1,
    'connection': {
        'connect': {},
        'requests': {'in-flight': {'max': 1}},
        'pool': {'shard-aware': {'enabled': False}},
        'socket': {'tcp-no-delay': False, 'keep-alive': False, 'reuse-address': False},
        'reconnection': {'policy': None},
    },
    'control-plane': {
        'queries': {'system': {'timeout': {}}},
        'schema': {'agreement': {'timeout-ms': 0}},
    },
    'query': {
        'defaults': {'consistency': 'LOCAL_ONE', 'idempotence': False},
        'retry': {'policy': {'type': 'fallthrough'}},
        'load-balancing': {'policy': {'type': 'custom', 'name': 'X'}},
    },
}
"""
The smallest document the schema accepts: every required group, and in each one
only the required keys. Spelled out rather than generated, so that a change to
what the shared contract demands shows up here as a diff.
"""


class DriverConfigSchemaTest(unittest.TestCase):
    def test_the_vendored_schema_is_the_shared_one(self):
        """
        The schema is vendored from upstream, where it is maintained. Its $id is
        what a consumer keys off, so a copy that lost it is not the contract.
        """
        schema = load_schema()

        assert schema['$id'] == 'https://scylladb.com/schemas/driver-client-options/v1.json'
        assert schema['$schema'] == 'https://json-schema.org/draft/2020-12/schema'

    def test_the_vendored_schema_is_itself_valid(self):
        jsonschema.Draft202012Validator.check_schema(load_schema())

    def test_the_vendored_copy_is_byte_for_byte(self):
        """
        Vendored verbatim, so that drift from upstream is a diff in the resource
        rather than a reinterpretation. Reformatting it would defeat that, so
        this pins the formatting the shared copy has.
        """
        with open(SCHEMA_PATH, encoding='utf8') as f:
            raw = f.read()

        assert raw.startswith('{\n  "$schema"')
        assert raw.endswith('}\n')
        # Reserialising with the shared copy's formatting must be a no-op. The
        # descriptions contain em dashes, which the shared copy leaves as they
        # are rather than escaping.
        assert json.dumps(json.loads(raw), indent=2, ensure_ascii=False) + '\n' == raw

    def test_the_minimal_report_validates(self):
        assert validate_report(MINIMAL_REPORT) == MINIMAL_REPORT

    def test_a_report_is_accepted_as_wire_text(self):
        """
        The helper takes what goes on the wire and comes back out of the clients
        table, not only an already parsed document.
        """
        assert validate_report(json.dumps(MINIMAL_REPORT)) == MINIMAL_REPORT

    def _rejects(self, mutate):
        report = copy.deepcopy(MINIMAL_REPORT)
        mutate(report)
        with pytest.raises(jsonschema.ValidationError):
            validate_report(report)

    def test_unknown_keys_are_rejected(self):
        """
        Every built-in group is additionalProperties: false, so a key this driver
        invents or misspells fails validation instead of being ignored by a
        consumer. This is the property that makes the schema worth validating
        against at all.
        """
        def top_level(report):
            report['made-up'] = 1

        def inside_a_group(report):
            report['connection']['made-up'] = 1

        def inside_a_nested_group(report):
            report['query']['defaults']['made-up'] = 1

        for mutate in (top_level, inside_a_group, inside_a_nested_group):
            self._rejects(mutate)

    def test_missing_required_groups_are_rejected(self):
        for group in ('connection', 'control-plane', 'query'):
            self._rejects(lambda report, group=group: report.pop(group))

    def test_a_foreign_schema_version_is_rejected(self):
        for version in (0, 2, '1'):
            self._rejects(lambda report, version=version: report.__setitem__('version', version))

    def test_out_of_range_numbers_are_rejected(self):
        def zero_in_flight(report):
            # positiveInteger: 0 in-flight requests would describe a connection
            # that cannot carry a request.
            report['connection']['requests']['in-flight']['max'] = 0

        def negative_agreement_timeout(report):
            # nonNegativeInteger: 0 is meaningful here, below that is not.
            report['control-plane']['schema']['agreement']['timeout-ms'] = -1

        for mutate in (zero_in_flight, negative_agreement_timeout):
            self._rejects(mutate)

    def test_unknown_enum_members_are_rejected(self):
        self._rejects(lambda report: report['query']['defaults'].__setitem__('consistency', 'MOSTLY'))

    def test_a_consistency_level_is_not_a_number(self):
        """
        The wire form of a consistency level is an integer and the schema wants
        the name, which is the mistake this driver is closest to making.
        """
        self._rejects(lambda report: report['query']['defaults'].__setitem__('consistency', 4))

    def test_discriminated_unions_reject_foreign_parameters(self):
        def constant_delay_on_an_exponential_policy(report):
            report['connection']['reconnection']['policy'] = {
                'type': 'exponential', 'base-ms': 1, 'max-ms': 2, 'delay-ms': 3}

        def a_custom_policy_without_a_name(report):
            report['query']['load-balancing']['policy'] = {'type': 'custom'}

        for mutate in (constant_delay_on_an_exponential_policy, a_custom_policy_without_a_name):
            self._rejects(mutate)

    def test_backoff_is_rejected_on_a_fallthrough_retry_policy(self):
        """
        A policy that never retries cannot have a delay between retries. The
        schema says so conditionally, which is the one rule a producer is likely
        to break without noticing.
        """
        self._rejects(lambda report: report['query']['retry'].__setitem__(
            'backoff', {'type': 'constant', 'delay-ms': 1}))

    def test_the_orphan_bound_is_optional_but_permitted(self):
        """
        The shared schema leaves connection.requests.orphaned optional, for a
        client with nothing bounding its orphaned requests. This driver has such
        a bound in Connection.orphaned_threshold and reports it; optional is not
        forbidden, so both documents have to validate.
        """
        without = copy.deepcopy(MINIMAL_REPORT)
        assert 'orphaned' not in without['connection']['requests']
        validate_report(without)

        with_bound = copy.deepcopy(MINIMAL_REPORT)
        with_bound['connection']['requests']['orphaned'] = {'max': 0}
        validate_report(with_bound)

        # Present but empty is still a violation: the group exists to carry the
        # bound, so it may be absent but not uninformative.
        self._rejects(lambda report: report['connection']['requests'].__setitem__('orphaned', {}))
