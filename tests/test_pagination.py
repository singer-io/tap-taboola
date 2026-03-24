"""Integration test: pagination — Taboola returns all records in a single
response so no multi-page fetching is needed, but we verify the tap handles
the full result set correctly."""
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


class PaginationIntegrationTest(TaboolaBaseTest, unittest.TestCase):

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
    def test_all_campaigns_returned_in_single_response(
        self,
        mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """Verify all campaigns are written when the API returns them
        in a single response (no pagination)."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args())

        campaign_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaigns'
        ]
        self.assertEqual(len(campaign_records), len(self.MOCK_CAMPAIGNS))

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_all_performance_rows_returned_in_single_response(
        self,
        mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """Verify all campaign_performance rows are written when the API
        returns them in a single response (no pagination)."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args())

        perf_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        ]
        self.assertEqual(len(perf_records), len(self.MOCK_CAMPAIGN_PERFORMANCE))
