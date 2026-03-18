"""Integration test: bookmark is advanced after sync for incremental streams."""
import unittest
from unittest.mock import patch

import tap_taboola as taboola

try:
    from .base import TaboolaBaseTest
except ImportError:
    from base import TaboolaBaseTest


class BookmarkIntegrationTest(TaboolaBaseTest, unittest.TestCase):

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.request")
    def test_sync_advances_bookmark_for_campaign_performance(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """After syncing campaign_performance, the bookmark should advance
        to the max date seen in the records."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)
        state = {}

        taboola.sync_campaign_performance(
            config, state, 'mock-token', config['account_id'])

        # write_state should have been called with the max date
        mock_write_state.assert_called()
        last_state = mock_write_state.call_args[0][0]
        bookmark_date = last_state['bookmarks']['campaign_performance']['date']

        # The max date in mock data is 2025-01-15
        self.assertEqual(bookmark_date, '2025-01-15')

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.request")
    def test_sync_with_existing_bookmark_returns_fewer_records(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """When a bookmark already exists, the API query filters by start_date
        so fewer records should be returned."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        # First sync with no bookmark — should get all 3 records
        state_1 = {}
        taboola.sync_campaign_performance(
            config, state_1, 'mock-token', config['account_id'])
        count_1 = sum(
            1 for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        )

        mock_write_record.reset_mock()

        # Second sync with bookmark at 2024-12-01 — only 2025-01-15 record passes
        state_2 = {
            'bookmarks': {
                'campaign_performance': {'date': '2024-12-01'}
            }
        }
        taboola.sync_campaign_performance(
            config, state_2, 'mock-token', config['account_id'])
        count_2 = sum(
            1 for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        )

        self.assertEqual(count_1, 3)
        self.assertEqual(count_2, 1)
        self.assertGreater(count_1, count_2)

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.request")
    def test_full_table_stream_has_no_bookmark(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """Full table streams (campaigns) should not write bookmark state."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        taboola.sync_campaigns('mock-token', config['account_id'])

        # sync_campaigns does not call write_state
        mock_write_state.assert_not_called()
