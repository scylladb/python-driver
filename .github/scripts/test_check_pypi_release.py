"""Exercise release inventory checks without contacting PyPI."""

import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

import check_pypi_release as release


class InventoryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dist = Path(self.temp.name)
        self.filenames = ['scylla_driver-3.29.13-py3-none-any.whl',
                          'scylla_driver-3.29.13.tar.gz']
        for filename in self.filenames:
            (self.dist / filename).write_bytes(b'identical artifact')
        request_patch = patch.object(release, 'urlopen')
        self.urlopen = request_patch.start()
        self.addCleanup(request_patch.stop)

    def inventory(self, payload):
        response = MagicMock()
        response.status = 200
        response.read.return_value = json.dumps(payload).encode()
        self.urlopen.return_value.__enter__.return_value = response
        return response

    def check(self):
        release.check_release('3.29.13', self.dist)

    def test_missing_version_allows_first_upload(self):
        self.urlopen.side_effect = HTTPError('url', 404, 'Missing', {}, None)
        self.check()

    def test_empty_and_nonoverlapping_inventory_allows_upload(self):
        for filenames in ([], ['scylla_driver-3.29.13-other.whl']):
            with self.subTest(filenames=filenames):
                self.inventory({'urls': [{'filename': name}
                                         for name in filenames]})
                self.check()

    def test_partial_and_complete_overlap_rejects_regardless_of_hash(self):
        identical = hashlib.sha256(b'identical artifact').hexdigest()
        for filenames in (self.filenames[:1], self.filenames):
            for digest in (identical, '0' * 64):
                with self.subTest(filenames=filenames, digest=digest):
                    self.inventory({'urls': [
                        {'filename': name, 'digests': {'sha256': digest}}
                        for name in filenames]})
                    with self.assertRaisesRegex(ValueError, 'already exist'):
                        self.check()

    def test_transport_and_non404_http_errors_propagate(self):
        for error in (URLError('unavailable'), TimeoutError('timeout'),
                      HTTPError('url', 503, 'Unavailable', {}, None)):
            with self.subTest(error=error):
                self.urlopen.side_effect = error
                with self.assertRaises(type(error)):
                    self.check()

    def test_malformed_inventory_rejects(self):
        for payload in (None, [], {}, {'urls': {}}, {'urls': [None]},
                        {'urls': [{}]}, {'urls': [{'filename': None}]},
                        {'urls': [{'filename': ''}]},
                        {'urls': [{'filename': '../artifact.whl'}]}):
            with self.subTest(payload=payload):
                self.inventory(payload)
                with self.assertRaisesRegex(ValueError, 'Malformed PyPI'):
                    self.check()
        response = self.inventory({'urls': []})
        response.read.return_value = b'not JSON'
        with self.assertRaises(json.JSONDecodeError):
            self.check()

    def test_request_is_public_release_specific_and_bounded(self):
        response = self.inventory({'urls': []})
        self.check()
        args, kwargs = self.urlopen.call_args
        self.assertEqual(len(args), 1)
        self.assertEqual(args[0].full_url,
                         'https://pypi.org/pypi/scylla-driver/3.29.13/json')
        self.assertEqual(args[0].header_items(),
                         [('Accept', 'application/json')])
        self.assertEqual(kwargs, {'timeout': 30})
        response.read.assert_called_once_with(
            release.MAX_INVENTORY_BYTES + 1)
        response.read.return_value = b' ' * (release.MAX_INVENTORY_BYTES + 1)
        with self.assertRaisesRegex(ValueError, 'too large'):
            self.check()
        response.status = 204
        with self.assertRaisesRegex(ValueError, 'HTTP status'):
            self.check()

    def test_invalid_version_and_artifact_directory_fail_before_request(self):
        for version in ('v3.29.13', '3.29.13-scylla', '03.29.13',
                        '3.29.13\n'):
            with self.subTest(version=version):
                with self.assertRaises(ValueError):
                    release.check_release(version, self.dist)
        for filename in self.filenames:
            (self.dist / filename).unlink()
        with self.assertRaisesRegex(ValueError, 'nonempty directory'):
            self.check()
        (self.dist / 'nested').mkdir()
        with self.assertRaisesRegex(ValueError, 'distribution files'):
            self.check()
        self.urlopen.assert_not_called()


if __name__ == '__main__':
    unittest.main()
