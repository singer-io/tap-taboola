"""Integration test: sync all streams with mocked API responses
and verify all fields are replicated."""
import unittest
from unittest.mock import patch

import tap_taboola as taboola
import tap_taboola.schemas as schemas

try:
    from .base import TaboolaBaseTest
except ImportError:
    from base import TaboolaBaseTest


class AllFieldsIntegrationTest(TaboolaBaseTest, unittest.TestCase):

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.singer.write_schema")
    @patch("tap_taboola.request")
    def test_sync_all_streams_writes_records(
        self,
        mock_request,
        mock_write_schema,
        mock_write_record,
        mock_write_state,
    ):
        """Sync both streams with mocked API data and verify
        records are written for each stream."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)
        state = {}

        # Write schemas (as do_sync does)
        taboola.singer.write_schema(
            'campaigns', schemas.campaign, key_properties=['id'])
        taboola.singer.write_schema(
            'campaign_performance', schemas.campaign_performance,
            key_properties=['campaign_id', 'date'])

        # Sync both streams
        taboola.sync_campaigns('mock-token', config['account_id'])
        taboola.sync_campaign_performance(
            config, state, 'mock-token', config['account_id'])

        # Collect all streams that had records written
        written_streams = {
            call_args[0][0] for call_args in mock_write_record.call_args_list
        }

        self.assertIn('campaigns', written_streams)
        self.assertIn('campaign_performance', written_streams)

        # Verify write_schema was called for both streams
        schema_streams = {
            call_args[0][0] for call_args in mock_write_schema.call_args_list
        }
        self.assertIn('campaigns', schema_streams)
        self.assertIn('campaign_performance', schema_streams)

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.singer.write_schema")
    @patch("tap_taboola.request")
    def test_sync_campaigns_only(
        self,
        mock_request,
        mock_write_schema,
        mock_write_record,
        mock_write_state,
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

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.singer.write_schema")
    @patch("tap_taboola.request")
    def test_all_campaign_fields_replicated(
        self,
        mock_request,
        mock_write_schema,
        mock_write_record,
        mock_write_state,
    ):
        """Verify all schema fields are present in written campaign records."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        taboola.sync_campaigns('mock-token', config['account_id'])

        expected_fields = set(schemas.campaign['properties'].keys())
        for call_args in mock_write_record.call_args_list:
            if call_args[0][0] == 'campaigns':
                record = call_args[0][1]
                self.assertEqual(set(record.keys()), expected_fields)

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.singer.write_schema")
    @patch("tap_taboola.request")
    def test_all_performance_fields_replicated(
        self,
        mock_request,
        mock_write_schema,
        mock_write_record,
        mock_write_state,
    ):
        """Verify all schema fields are present in written performance records."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        taboola.sync_campaign_performance(
            config, {}, 'mock-token', config['account_id'])

        expected_fields = set(schemas.campaign_performance['properties'].keys())
        for call_args in mock_write_record.call_args_list:
            if call_args[0][0] == 'campaign_performance':
                record = call_args[0][1]
                self.assertEqual(set(record.keys()), expected_fields)
