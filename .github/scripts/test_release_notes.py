"""Offline regression checks for release notes automation."""

import unittest
import urllib.error
from unittest.mock import Mock

from release_notes import create_release


class ReleaseTest(unittest.TestCase):
    reference = {'object': {'type': 'commit', 'sha': 'published-sha'}}

    def test_existing_release_is_preserved(self):
        api = Mock()
        api.request.side_effect = [self.reference, {
            'html_url': 'existing', 'draft': False,
            'published_at': '2026-10-25T15:00:00Z',
        }]
        create_release(api, '3.29.12-scylla', 'published-sha')
        self.assertEqual(api.request.call_count, 2)
        api.request.assert_any_call('GET', 'git/ref/tags/3.29.12-scylla')

    def test_unpublished_existing_release_is_rejected(self):
        for draft, published_at in (
                (True, None), (False, None), (True, 'date')):
            with self.subTest(draft=draft, published_at=published_at):
                api = Mock()
                api.request.side_effect = [self.reference, {
                    'html_url': 'unpublished', 'draft': draft,
                    'published_at': published_at,
                }]
                with self.assertRaisesRegex(ValueError, 'unpublished'):
                    create_release(api, '3.29.12-scylla', 'published-sha')
                self.assertEqual(api.request.call_count, 2)

    def test_new_release_uses_generated_notes(self):
        api = Mock()
        api.request.side_effect = [
            self.reference,
            urllib.error.HTTPError('', 404, 'missing', {}, None),
            [], self.reference, {'html_url': 'created'},
        ]
        create_release(api, '3.29.12-scylla', 'published-sha')
        api.request.assert_called_with('POST', 'releases', {
            'tag_name': '3.29.12-scylla', 'name': 'Release 3.29.12',
            'generate_release_notes': True,
        })

    def test_pending_draft_is_found_after_published_lookup_misses(self):
        api = Mock()
        api.request.side_effect = [
            self.reference,
            urllib.error.HTTPError('', 404, 'missing', {}, None),
            [{'tag_name': '3.29.12-scylla', 'draft': True,
              'published_at': None}],
        ]
        with self.assertRaisesRegex(ValueError, 'unpublished'):
            create_release(api, '3.29.12-scylla', 'published-sha')
        api.request.assert_called_with('GET', 'releases?per_page=100&page=1')
        self.assertEqual(api.request.call_count, 3)

    def test_release_discovery_reads_later_pages(self):
        api = Mock()
        api.request.side_effect = [
            self.reference,
            urllib.error.HTTPError('', 404, 'missing', {}, None),
            [{'tag_name': f'other-{number}'} for number in range(100)],
            [{'tag_name': '3.29.12-scylla', 'draft': False,
              'published_at': 'date', 'html_url': 'existing'}],
        ]
        create_release(api, '3.29.12-scylla', 'published-sha')
        api.request.assert_called_with('GET', 'releases?per_page=100&page=2')
        self.assertEqual(api.request.call_count, 4)

    def test_failed_draft_discovery_prevents_creation(self):
        api = Mock()
        api.request.side_effect = [
            self.reference,
            urllib.error.HTTPError('', 404, 'missing', {}, None),
            urllib.error.HTTPError('', 403, 'denied', {}, None),
        ]
        with self.assertRaises(urllib.error.HTTPError):
            create_release(api, '3.29.12-scylla', 'published-sha')
        api.request.assert_called_with('GET', 'releases?per_page=100&page=1')
        self.assertEqual(api.request.call_count, 3)

    def test_annotated_tag_is_peeled(self):
        api = Mock()
        api.request.side_effect = [
            {'object': {'type': 'tag', 'sha': 'annotation-sha'}},
            self.reference, {
                'html_url': 'existing', 'draft': False,
                'published_at': '2026-10-25T15:00:00Z',
            },
        ]
        create_release(api, '3.29.12-scylla', 'published-sha')
        api.request.assert_any_call('GET', 'git/tags/annotation-sha')

    def test_wrong_or_moving_tag_prevents_release(self):
        api = Mock()
        api.request.return_value = self.reference
        with self.assertRaises(ValueError):
            create_release(api, '3.29.12-scylla', 'other-source-sha')
        self.assertEqual(api.request.call_count, 1)
        api.reset_mock()
        api.request.side_effect = [
            self.reference,
            urllib.error.HTTPError('', 404, 'missing', {}, None),
            [], {'object': {'type': 'commit', 'sha': 'moved-sha'}},
        ]
        with self.assertRaises(ValueError):
            create_release(api, '3.29.12-scylla', 'published-sha')
        self.assertEqual(api.request.call_count, 4)

    def test_tag_and_api_failures_prevent_creation(self):
        api = Mock()
        for tag in (
                'master', '3.30.1', 'v3.29.12-scylla', '3.29.12-scylla-extra'):
            with self.assertRaises(ValueError):
                create_release(api, tag, 'published-sha')
        api.request.assert_not_called()
        api.request.side_effect = urllib.error.HTTPError(
            '', 404, 'missing', {}, None)
        with self.assertRaises(urllib.error.HTTPError):
            create_release(api, '3.29.12-scylla', 'published-sha')
        self.assertEqual(api.request.call_count, 1)
        api.reset_mock()
        api.request.side_effect = [
            self.reference,
            urllib.error.HTTPError('', 403, 'denied', {}, None),
        ]
        with self.assertRaises(urllib.error.HTTPError):
            create_release(api, '3.29.12-scylla', 'published-sha')
        self.assertEqual(api.request.call_count, 2)


if __name__ == '__main__':
    unittest.main()
