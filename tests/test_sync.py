"""Integration test: sync function writes records and updates state
correctly."""
import unittest
from unittest.mock import patch

import tap_taboola as taboola

try:
    from .base import TaboolaBaseTest
except ImportError:
    from base import TaboolaBaseTest


class SyncIntegrationTest(TaboolaBaseTest, unittest.TestCase):

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.request")
    def test_sync_empty_campaigns_no_crash(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """When API returns empty campaigns, sync should complete without
        errors and not write any campaign records."""
        mock_request.side_effect = self._mock_request(campaigns=[])
        config = dict(self.default_config)

        taboola.sync_campaigns('mock-token', config['account_id'])

        campaign_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaigns'
        ]
        self.assertEqual(len(campaign_records), 0)

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.request")
    def test_sync_empty_performance_no_crash(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """When API returns empty performance data, sync should complete
        without errors."""
        mock_request.side_effect = self._mock_request(performance=[])
        config = dict(self.default_config)

        taboola.sync_campaign_performance(
            config, {}, 'mock-token', config['account_id'])

        perf_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        ]
        self.assertEqual(len(perf_records), 0)

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.request")
    def test_write_record_called_for_each_campaign(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """Verify write_record is called once per campaign in the response."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        taboola.sync_campaigns('mock-token', config['account_id'])

        campaign_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaigns'
        ]
        self.assertEqual(len(campaign_records), len(self.MOCK_CAMPAIGNS))

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.request")
    def test_write_record_called_for_each_performance_row(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """Verify write_record is called once per performance row."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        taboola.sync_campaign_performance(
            config, {}, 'mock-token', config['account_id'])

        perf_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        ]
        self.assertEqual(len(perf_records), len(self.MOCK_CAMPAIGN_PERFORMANCE))

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.request")
    def test_campaign_parse_types(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """Verify campaign fields are correctly typed after parsing."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        taboola.sync_campaigns('mock-token', config['account_id'])

        record = mock_write_record.call_args_list[0][0][1]
        self.assertIsInstance(record['id'], int)
        self.assertIsInstance(record['cpc'], float)
        self.assertIsInstance(record['is_active'], bool)
        self.assertIsInstance(record['name'], str)

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.request")
    def test_performance_parse_types(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """Verify campaign_performance fields are correctly typed after parsing."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        taboola.sync_campaign_performance(
            config, {}, 'mock-token', config['account_id'])

        record = mock_write_record.call_args_list[0][0][1]
        self.assertIsInstance(record['campaign_id'], int)
        self.assertIsInstance(record['impressions'], int)
        self.assertIsInstance(record['ctr'], float)
        self.assertIsInstance(record['clicks'], int)
        self.assertIsInstance(record['date'], str)
        # Date should be YYYY-MM-DD format
        self.assertRegex(record['date'], r'^\d{4}-\d{2}-\d{2}$')

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.request")
    def test_campaign_null_end_date_uses_sentinel(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """Campaigns with None end_date should use '9999-12-31' sentinel."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        taboola.sync_campaigns('mock-token', config['account_id'])

        # First campaign has end_date=None
        record = mock_write_record.call_args_list[0][0][1]
        self.assertEqual(record['end_date'], '9999-12-31')


