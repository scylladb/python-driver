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

import pytest

from cassandra.application_info import ApplicationInfo


@pytest.mark.parametrize('argument', ['application_name', 'application_version', 'client_id'])
def test_non_string_value_is_rejected(argument):
    with pytest.raises(TypeError, match='{} must be a string'.format(argument)):
        ApplicationInfo(**{argument: 123})


def test_all_fields_are_added_to_startup_options():
    info = ApplicationInfo(application_name='app', application_version='1.2.3',
                           client_id='client-1')
    options = {'CQL_VERSION': '3.0.0'}

    info.add_startup_options(options)

    assert options == {
        'CQL_VERSION': '3.0.0',
        'APPLICATION_NAME': 'app',
        'APPLICATION_VERSION': '1.2.3',
        'CLIENT_ID': 'client-1',
    }


@pytest.mark.parametrize('argument,option', [
    ('application_name', 'APPLICATION_NAME'),
    ('application_version', 'APPLICATION_VERSION'),
    ('client_id', 'CLIENT_ID'),
])
def test_only_set_fields_are_added_to_startup_options(argument, option):
    options = {}
    ApplicationInfo(**{argument: 'value'}).add_startup_options(options)
    assert options == {option: 'value'}


@pytest.mark.parametrize('value', [None, ''])
def test_empty_values_are_accepted_and_not_sent(value):
    info = ApplicationInfo(application_name=value, application_version=value,
                           client_id=value)
    options = {}

    info.add_startup_options(options)

    assert options == {}
    assert info.application_name == value
