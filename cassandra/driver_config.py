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
Reporting of the driver's own identity and configuration to the cluster through
the CQL ``STARTUP`` options. ScyllaDB echoes those options into the
``client_options`` column of its clients table, so an operator investigating an
incident can inspect the settings of a client without access to its host.
"""

import json
import logging
import weakref

log = logging.getLogger(__name__)


SESSION_ID_OPTION = 'SESSION_ID'
"""
``STARTUP`` option correlating the connections that belong to the same
:class:`~.Cluster` in the clients table. Every connection reports it, since
correlating them is the whole point of it.

The name follows the convention shared with the other ScyllaDB drivers, where a
"session" is what this driver calls a :class:`~.Cluster`; it is unrelated to
:attr:`.Session.session_id`.
"""

DRIVER_CONFIG_OPTION = 'DRIVER_CONFIG'
"""
``STARTUP`` option holding the JSON description of the effective driver
configuration. The configuration is the same for every connection of a cluster,
so only the control connection reports it, keeping the other ``STARTUP`` frames
small.
"""

DRIVER_CONFIG_SCHEMA_VERSION = 1
"""
Major version of the reported configuration schema. Adding keys to the report is
backwards compatible and does not bump it, only changing or removing the meaning
of an existing key does.
"""

MAX_DRIVER_CONFIG_LENGTH = 32 * 1024
"""
Upper bound for the length, in bytes, of the :const:`DRIVER_CONFIG_OPTION` value.

``STARTUP`` options are serialized by :func:`cassandra.protocol.write_string`,
which prefixes every value with a 16 bit length, so a longer value would fail to
pack and take the handshake down with it. The report is a handful of bytes for
now, but the configuration groups added later describe user supplied values,
such as the settings of custom policies, and can grow arbitrarily large.
Enforcing a limit here keeps "reporting must never prevent a connection from
being established" a property of this module rather than of the user's
configuration.

32 KiB rather than the protocol's own 65535 byte ceiling: real world reports are
expected to stay well under a couple of kilobytes, so this leaves ample headroom
while remaining far short of the point where the value would stop protecting
anything.
"""


class DriverConfigReporter:
    """
    Builds the :const:`DRIVER_CONFIG_OPTION` ``STARTUP`` option describing the
    effective configuration of a :class:`~.Cluster`.

    One instance is created per :class:`~.Cluster` and shared by all of its
    connections, but only the control connection ever asks it for options. Which
    connections report is decided by
    :meth:`cassandra.connection.Connection._handle_options_response`, not here.
    """

    def __init__(self, cluster):
        # Weak, because the cluster owns the reporter and hands it to every
        # connection it opens: a strong reference here would run back through
        # each of them and keep the cluster alive for as long as any connection
        # holds a reporter.
        self._cluster = weakref.ref(cluster)

    def add_startup_options(self, options, is_scylla):
        """
        Adds the configuration report to the ``STARTUP`` options being built.

        `is_scylla` says whether the node this connection is being established
        to is a ScyllaDB one, which decides the keys that describe behaviour the
        driver only has against ScyllaDB.

        Reporting is best effort: this runs while a connection is being
        established, so a report that cannot be built or does not fit is logged
        and left out rather than allowed to fail the connection.

        Everything up to and including the assignment is guarded, not just the
        building of the report: :meth:`_populate_report` is an extension point,
        so a subclass returning something that is not a string has to be as
        harmless as one raising. The assignment comes last, so nothing partial is
        left in ``options`` either.
        """
        try:
            cluster = self._cluster()
            if cluster is None:
                # The application dropped its Cluster while this connection was
                # being established. Nothing is wrong and nothing is worth
                # warning about: the connection is on its way out too.
                log.debug("The cluster is gone, its configuration will not be "
                          "reported on this connection")
                return

            report = self._build_report(cluster, is_scylla)
            length = len(report.encode('utf8'))
            if length > MAX_DRIVER_CONFIG_LENGTH:
                log.warning("The driver configuration report is %d bytes long, which exceeds the "
                            "%d bytes limit, it will not be reported to the cluster",
                            length, MAX_DRIVER_CONFIG_LENGTH)
                return

            options[DRIVER_CONFIG_OPTION] = report
        except Exception:
            log.warning("Unable to build the driver configuration report, "
                        "it will not be reported to the cluster", exc_info=True)

    def _build_report(self, cluster, is_scylla):
        """
        Returns the JSON configuration report of `cluster`.

        It is built for every control connection rather than cached, so that it
        always describes the configuration as it is at that point in time. Some
        of what it describes is only known once a connection has got this far:
        `is_scylla` comes out of the ``SUPPORTED`` response, and a datacenter
        the driver inferred rather than was given is not known until the first
        host comes up.
        """
        report = {'version': DRIVER_CONFIG_SCHEMA_VERSION}
        self._populate_report(report, cluster, is_scylla)
        # Separators without whitespace: the report is a wire value bounded by
        # MAX_DRIVER_CONFIG_LENGTH, not something meant to be read as it is.
        return json.dumps(report, separators=(',', ':'))

    def _populate_report(self, report, cluster, is_scylla):
        """
        Extension point for adding the configuration groups themselves to the
        report. Empty for now.
        """
        pass
