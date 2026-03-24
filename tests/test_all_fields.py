"""Integration test: sync all streams with mocked API responses
and verify all fields are replicated."""
import unittest
from unittest.mock import patch

import tap_taboola as taboola

try:
    from .base import TaboolaBaseTest
except ImportError:
    from base import TaboolaBaseTest

# Fields emitted by parse_campaign()
CAMPAIGN_FIELDS = {
    'id', 'advertiser_id', 'name', 'tracking_code', 'cpc', 'daily_cap',
    'spending_limit', 'spending_limit_model', 'country_targeting',
    'platform_targeting', 'publisher_targeting', 'start_date', 'end_date',
    'approval_state', 'is_active', 'spent', 'status',
}

# Fields emitted by parse_campaign_performance()
PERFORMANCE_FIELDS = {
    'campaign_id', 'impressions', 'ctr', 'cpc', 'cpa_actions_num', 'cpa',
    'cpm', 'clicks', 'currency', 'cpa_conversion_rate', 'spent', 'date',
    'campaign_name', 'conversions_value',
}


class AllFieldsIntegrationTest(TaboolaBaseTest, unittest.TestCase):

    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_sync_all_streams_writes_records(
        self,
        mock_request,
        mock_write_record,
    ):
        """Sync both streams with mocked API data and verify
        records are written for each stream."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)
        state = {}

        taboola.sync_campaigns('mock-token', config['account_id'])
        taboola.sync_campaign_performance(
            config, state, 'mock-token', config['account_id'])

        written_streams = {
            call_args[0][0] for call_args in mock_write_record.call_args_list
        }

        self.assertIn('campaigns', written_streams)
        self.assertIn('campaign_performance', written_streams)

    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_sync_campaigns_only(
        self,
        mock_request,
        mock_write_record,
    ):
        """Sync only campaigns and verify only campaign records are written."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        taboola.sync_campaigns('mock-token', config['account_id'])

        written_streams = {
            call_args[0][0] for call_args in mock_write_record.call_args_list
        }
        self.assertIn('campaigns', written_streams)
        self.assertNotIn('campaign_performance', written_streams)

    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_all_campaign_fields_replicated(
        self,
        mock_request,
        mock_write_record,
    ):
        """Verify all expected fields are present in written campaign records."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        taboola.sync_campaigns('mock-token', config['account_id'])

        for call_args in mock_write_record.call_args_list:
            if call_args[0][0] == 'campaigns':
                record = call_args[0][1]
                self.assertEqual(set(record.keys()), CAMPAIGN_FIELDS)

    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_all_performance_fields_replicated(
        self,
        mock_request,
        mock_write_record,
    ):
        """Verify all expected fields are present in written performance records."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        taboola.sync_campaign_performance(
            config, {}, 'mock-token', config['account_id'])

        for call_args in mock_write_record.call_args_list:
            if call_args[0][0] == 'campaign_performance':
                record = call_args[0][1]
                # PERFORMANCE_FIELDS represents the minimum expected set of fields;
                # allow additional fields (e.g., schema-required id/created_at).
                self.assertTrue(PERFORMANCE_FIELDS.issubset(set(record.keys())))
