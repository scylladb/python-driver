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

import os
import subprocess
import sys
import unittest
from unittest import mock

from packaging.version import Version

from tests.integration import CASSANDRA_VERSION
from tests.integration.simulacron import utils


class PrimeServerVersionsTests(unittest.TestCase):
    """
    The Simulacron suite is not run by CI, so these exercise the harness
    priming directly without a running Simulacron process.
    """

    def test_primes_cql_and_release_version(self):
        """The restored prime carries the release version and a valid CQL version."""
        client = utils.SimulacronClient()
        with mock.patch.object(client, "submit_request") as submit_request:
            client.prime_server_versions(Version("4.0.5"))

        submit_request.assert_called_once()
        prime = submit_request.call_args[0][0]
        self.assertIsInstance(prime, utils.PrimeQuery)
        self.assertEqual(
            prime.expected_query,
            "SELECT cql_version, release_version FROM system.local WHERE key='local'")
        self.assertEqual(prime.rows,
                         [{"cql_version": utils.CQL_VERSION,
                           "release_version": "4.0.5-SNAPSHOT"}])
        self.assertEqual(prime.column_types,
                         {"cql_version": "ascii", "release_version": "ascii"})


class StartAndPrimeClusterDefaultsTests(unittest.TestCase):

    def _start_and_prime(self, **kwargs):
        """Run start_and_prime with only Simulacron itself mocked out."""
        patcher = mock.patch.object(utils.SimulacronClient, "submit_request")
        submit_request = patcher.start()
        self.addCleanup(patcher.stop)
        with mock.patch.object(utils, "start_simulacron"), \
                mock.patch.object(utils, "prime_cluster") as prime_cluster:
            utils.start_and_prime_cluster_defaults(**kwargs)
        return prime_cluster, submit_request

    def _system_local_prime(self, submit_request):
        """Return the single system.local prime that was submitted."""
        primes = [call.args[0] for call in submit_request.call_args_list]
        matches = [p for p in primes
                   if isinstance(p, utils.PrimeQuery) and "system.local" in p.expected_query]
        self.assertEqual(len(matches), 1)
        return matches[0]

    def test_forwards_version_to_primes(self):
        """A custom version reaches the cluster prime and the system.local prime."""
        version = Version("4.1.0")
        prime_cluster, submit_request = self._start_and_prime(version=version)

        self.assertEqual(prime_cluster.call_args[1]["version"], version)
        self.assertEqual(self._system_local_prime(submit_request).rows,
                         [{"cql_version": utils.CQL_VERSION,
                           "release_version": "4.1.0-SNAPSHOT"}])

    def test_none_version_falls_back_to_default(self):
        """An explicit None falls back to CASSANDRA_VERSION instead of crashing."""
        prime_cluster, submit_request = self._start_and_prime(version=None)

        self.assertEqual(prime_cluster.call_args[1]["version"], CASSANDRA_VERSION)
        self.assertEqual(self._system_local_prime(submit_request).rows,
                         [{"cql_version": utils.CQL_VERSION,
                           "release_version": CASSANDRA_VERSION.base_version + "-SNAPSHOT"}])


class IntegrationImportTests(unittest.TestCase):
    """Run in a subprocess so the result is independent of the installed ccm and env."""

    def _run(self, code, **env):
        e = {k: v for k, v in os.environ.items()
             if k not in ("CASSANDRA_VERSION", "SCYLLA_VERSION",
                          "MAPPED_CASSANDRA_VERSION", "MAPPED_SCYLLA_VERSION")}
        e.update(env)
        # cibuildwheel runs from a temp dir, so the child can't find `tests` on its own.
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        code = "import sys; sys.path.append(%r)\n" % root + code
        return subprocess.run([sys.executable, "-c", code], env=e,
                              capture_output=True, text=True)

    def test_imports_without_ccmlib_or_version(self):
        """The Windows-wheel contract: no ccmlib and no version still imports."""
        r = self._run("import sys; sys.modules['ccmlib'] = None\n"
                      "import tests.integration as i\n"
                      "assert i.CCMClusterFactory is None\n"
                      "assert i.CCMCluster is object\n"
                      "assert str(i.CASSANDRA_VERSION) == '3.11.4'")
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_malformed_version_is_an_error(self):
        """An explicit malformed version must not silently fall back."""
        r = self._run("import tests.integration", CASSANDRA_VERSION="not-a-version")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("InvalidVersion", r.stderr)


if __name__ == "__main__":
    unittest.main()
