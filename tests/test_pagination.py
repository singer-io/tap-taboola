"""Integration test: pagination — Taboola returns all records in a single
response so no multi-page fetching is needed, but we verify the tap handles
the full result set correctly."""
import unittest
from unittest.mock import patch

import tap_taboola as taboola

try:
    from .base import TaboolaBaseTest
except ImportError:
    from base import TaboolaBaseTest


class PaginationIntegrationTest(TaboolaBaseTest, unittest.TestCase):

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_all_campaigns_returned_in_single_response(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """Verify all campaigns are written when the API returns them
        in a single response (no pagination)."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        taboola.sync_campaigns('mock-token', config['account_id'])

        campaign_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaigns'
        ]
        self.assertEqual(len(campaign_records), len(self.MOCK_CAMPAIGNS))

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_all_performance_rows_returned_in_single_response(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """Verify all campaign_performance rows are written when the API
        returns them in a single response (no pagination)."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        taboola.sync_campaign_performance(
            config, {}, 'mock-token', config['account_id'])

        perf_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        ]
        self.assertEqual(len(perf_records), len(self.MOCK_CAMPAIGN_PERFORMANCE))
