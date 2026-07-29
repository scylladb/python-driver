#!/usr/bin/env python
# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Validate Astra bundle TLS hostname verification on a pyOpenSSL reactor.

Set ``ASTRA_SECURE_CONNECT_BUNDLE``, ``ASTRA_CLIENT_ID``, and
``ASTRA_CLIENT_SECRET``. Run once for each reactor:

    uv run python scripts/validate_astra_tls.py twisted
    uv run python scripts/validate_astra_tls.py eventlet

The script never prints credentials or bundle contents.
"""

import argparse
import os


def _required_environment():
    names = (
        'ASTRA_SECURE_CONNECT_BUNDLE',
        'ASTRA_CLIENT_ID',
        'ASTRA_CLIENT_SECRET',
    )
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        raise SystemExit(
            'Missing required environment variables: %s' %
            ', '.join(missing))
    return {name: os.environ[name] for name in names}


def _connection_class(reactor_name):
    if reactor_name == 'eventlet':
        import eventlet
        eventlet.monkey_patch()
        from cassandra.io.eventletreactor import EventletConnection
        return EventletConnection

    from cassandra.io.twistedreactor import TwistedConnection
    return TwistedConnection


def main():
    parser = argparse.ArgumentParser(
        description='Validate Astra TLS with Twisted or Eventlet')
    parser.add_argument('reactor', choices=('twisted', 'eventlet'))
    args = parser.parse_args()
    environment = _required_environment()
    connection_class = _connection_class(args.reactor)

    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster

    Cluster.connection_class = connection_class
    cluster = Cluster(
        cloud={
            'secure_connect_bundle':
                environment['ASTRA_SECURE_CONNECT_BUNDLE'],
        },
        auth_provider=PlainTextAuthProvider(
            environment['ASTRA_CLIENT_ID'],
            environment['ASTRA_CLIENT_SECRET']),
    )
    try:
        session = cluster.connect(wait_for_all_pools=True)
        row = session.execute(
            "SELECT release_version FROM system.local WHERE key='local'"
        ).one()
        if row is None:
            raise RuntimeError('Astra query returned no system.local row')
        print(
            '%s Astra TLS validation passed (release %s)' %
            (args.reactor, row.release_version))
    finally:
        cluster.shutdown()


if __name__ == '__main__':
    main()
