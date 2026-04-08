"""Integration test: campaign_performance records are written for incremental streams."""
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


class CampaignPerformanceIntegrationTest(TaboolaBaseTest, unittest.TestCase):

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
    def test_sync_writes_campaign_performance_records(
        self,
        mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """After syncing campaign_performance via do_sync, records should be written
        and the bookmark (start_date) should be advanced to the max date seen."""
        catalog_perf = self._make_selected_catalog(stream_names=['campaign_performance'])
        catalog_path = os.path.join(self.tmpdir, 'catalog_perf.json')
        with open(catalog_path, 'w') as f:
            json.dump(catalog_perf, f)

        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args(catalog=catalog_path))

        # Verify records were written
        perf_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        ]
        self.assertGreater(len(perf_records), 0)

        # Verify bookmark was advanced
        self.assertTrue(mock_write_state.called,
                        "write_state should be called to advance the bookmark")
        last_state_call = mock_write_state.call_args_list[-1]
        state = last_state_call[0][0]
        self.assertIn('start_date', state,
                       "Bookmark state should contain 'start_date'")
        # The max date in MOCK_CAMPAIGN_PERFORMANCE is '2025-01-15'
        self.assertEqual(state['start_date'], '2025-01-15')

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_full_table_stream_has_no_bookmark(
        self,
        mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """Full table streams (campaigns) write records but no bookmark state."""
        catalog_campaigns = self._make_selected_catalog(stream_names=['campaigns'])
        catalog_path = os.path.join(self.tmpdir, 'catalog_campaigns.json')
        with open(catalog_path, 'w') as f:
            json.dump(catalog_campaigns, f)

        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args(catalog=catalog_path))

        # Campaigns should have written records
        campaign_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaigns'
        ]
        self.assertGreater(len(campaign_records), 0)

        # Full table stream should NOT write any bookmark state
        self.assertFalse(mock_write_state.called,
                         "FULL_TABLE stream 'campaigns' should not write bookmark state")
