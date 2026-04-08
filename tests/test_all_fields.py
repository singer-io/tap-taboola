"""Integration test: sync all streams with mocked API responses
and verify all fields are replicated."""
import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import tap_taboola as taboola

try:
    from .base import TaboolaBaseTest
except ImportError:
    from base import TaboolaBaseTest

# Fields emitted by parse_campaign()
CAMPAIGN_FIELDS = {
    'id', 'advertiser_id', 'name', 'tracking_code', 'cpc',
    'daily_cap', 'spending_limit', 'spending_limit_model',
    'country_targeting', 'platform_targeting', 'publisher_targeting',
    'start_date', 'end_date', 'approval_state', 'is_active', 'spent',
    'status',
}

# Fields emitted by parse_campaign_performance()
PERFORMANCE_FIELDS = {
    'campaign_id', 'impressions', 'ctr', 'cpc',
    'cpa_actions_num', 'cpa', 'cpm', 'clicks', 'currency',
    'cpa_conversion_rate', 'spent', 'date', 'campaign_name',
    'conversions_value',
}


class AllFieldsIntegrationTest(TaboolaBaseTest, unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.tmpdir, 'config.json')
        with open(self.config_path, 'w') as f:
            json.dump(self.default_config, f)
        self.state_path = os.path.join(self.tmpdir, 'state.json')
        with open(self.state_path, 'w') as f:
            json.dump({}, f)
        self.catalog_path = os.path.join(self.tmpdir, 'catalog.json')
        with open(self.catalog_path, 'w') as f:
            json.dump(self._make_selected_catalog(), f)

    def _make_args(self, config=None, state=None, catalog=None):
        return SimpleNamespace(
            config=config or self.config_path,
            state=state or self.state_path,
            catalog=catalog or self.catalog_path,
        )

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_sync_all_streams_writes_records(
        self,
        mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """Sync both streams via do_sync and verify
        records are written for each stream."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args())

        written_streams = {
            call_args[0][0] for call_args in mock_write_record.call_args_list
        }

        self.assertIn('campaigns', written_streams)
        self.assertIn('campaign_performance', written_streams)

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_sync_campaigns_only(
        self,
        mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """Sync only campaigns via do_sync and verify only campaign records are written."""
        catalog_campaigns = self._make_selected_catalog(stream_names=['campaigns'])
        catalog_path = os.path.join(self.tmpdir, 'catalog_campaigns.json')
        with open(catalog_path, 'w') as f:
            json.dump(catalog_campaigns, f)

        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args(catalog=catalog_path))

        written_streams = {
            call_args[0][0] for call_args in mock_write_record.call_args_list
        }
        self.assertIn('campaigns', written_streams)
        self.assertNotIn('campaign_performance', written_streams)

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_all_campaign_fields_replicated(
        self,
        mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """Verify all expected fields are present in written campaign records."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args())

        for call_args in mock_write_record.call_args_list:
            if call_args[0][0] == 'campaigns':
                record = call_args[0][1]
                self.assertEqual(set(record.keys()), CAMPAIGN_FIELDS)

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_all_performance_fields_replicated(
        self,
        mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """Verify all expected fields are present in written performance records."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args())

        for call_args in mock_write_record.call_args_list:
            if call_args[0][0] == 'campaign_performance':
                record = call_args[0][1]
                # PERFORMANCE_FIELDS represents the minimum expected set of fields;
                # allow additional fields (e.g., schema-required id/created_at).
                self.assertTrue(PERFORMANCE_FIELDS.issubset(set(record.keys())))
