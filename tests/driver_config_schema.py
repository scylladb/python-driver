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
Validation of the ``DRIVER_CONFIG`` report against the schema shared by the
ScyllaDB drivers.

The schema is the cross-driver contract: it is what an operator reading
``system.clients.client_options`` can rely on, whichever driver wrote the row.
Every group it defines is ``additionalProperties: false``, so a key this driver
invents, or misspells, is a validation failure rather than something a consumer
silently ignores.
"""

import json
import os

import jsonschema

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'resources',
                           'driver-config-schema-v1.json')
"""
Vendored copy of the normative schema, byte for byte as it appears upstream,
which is where it is maintained:

  https://github.com/scylladb/gocql/blob/master/docs/driver-config-schema.json

Vendored rather than reformatted, so that a drift from the shared contract shows
up as a diff in this file instead of as a divergence nobody notices.
"""


def load_schema():
    """
    Returns the parsed schema. Not cached: the callers are tests, and a mutable
    document shared between them is a worse trade than re-reading a 30 KiB file.
    """
    with open(SCHEMA_PATH, encoding='utf8') as f:
        return json.load(f)


def _validator():
    """
    A validator built once for the whole test run.

    Built here rather than through :func:`jsonschema.validate`, which reparses
    the file and recompiles the validator on every call -- and this is called
    from every unit case that produces a report as well as from every
    integration one. The validator keeps the schema to itself and never hands it
    back, so sharing one costs none of the isolation :func:`load_schema` exists
    to give a caller that wants the document.

    The dialect comes from the schema's own ``$schema``, so a future revision of
    the shared contract is validated as whatever it declares itself to be.
    """
    schema = load_schema()
    cls = jsonschema.validators.validator_for(schema)
    cls.check_schema(schema)
    return cls(schema)


_VALIDATOR = _validator()


def validate_report(report):
    """
    Validates a configuration report against the schema, raising
    :exc:`jsonschema.ValidationError` if it does not conform.

    `report` is either the JSON text of the ``DRIVER_CONFIG`` option, as it goes
    on the wire and comes back out of the clients table, or an already parsed
    document. Returns the parsed document, so a test can go on to assert
    specific values against the thing that was validated.
    """
    if isinstance(report, (str, bytes)):
        report = json.loads(report)

    _VALIDATOR.validate(report)
    return report
