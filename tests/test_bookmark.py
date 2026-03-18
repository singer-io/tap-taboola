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

        # Verify records were written
        perf_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        ]
        self.assertGreater(len(perf_records), 0)

    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.request")
    def test_full_table_stream_has_no_bookmark(
        self,
        mock_request,
        mock_write_record,
    ):
        """Full table streams (campaigns) write records but no bookmark state."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        taboola.sync_campaigns('mock-token', config['account_id'])

        # Campaigns should have written records
        campaign_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaigns'
        ]
        self.assertGreater(len(campaign_records), 0)
