"""Integration test: start_date controls which campaign_performance
records are returned from the API."""
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


class StartDateIntegrationTest(TaboolaBaseTest, unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.state_path = os.path.join(self.tmpdir, 'state.json')
        with open(self.state_path, 'w') as f:
            json.dump({}, f)
        self.catalog_path = os.path.join(self.tmpdir, 'catalog.json')
        with open(self.catalog_path, 'w') as f:
            json.dump(self._make_selected_catalog(), f)

    def _write_config(self, overrides=None, filename='config.json'):
        config = dict(self.default_config)
        if overrides:
            config.update(overrides)
        path = os.path.join(self.tmpdir, filename)
        with open(path, 'w') as f:
            json.dump(config, f)
        return path

    def _make_args(self, config=None, state=None, catalog=None):
        return SimpleNamespace(
            config=config or self._write_config(),
            state=state or self.state_path,
            catalog=catalog or self.catalog_path,
        )

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_start_date_filters_campaign_performance(
        self,
        mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """campaign_performance records before start_date should not be
        returned by the API (mocked filter)."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        config_path = self._write_config(
            {'start_date': '2024-06-01T00:00:00Z'}, 'config_filtered.json')

        taboola.do_sync(self._make_args(config=config_path))

        # With start_date=2024-06-01, only records with date >= 2024-06-01
        # should be returned: 2024-07-01 and 2025-01-15 (2 of 3)
        written_records = [
            call_args[0][1]
            for call_args in mock_write_record.call_args_list
            if call_args[0][0] == 'campaign_performance'
        ]
        self.assertEqual(len(written_records), 2)
        for record in written_records:
            self.assertGreaterEqual(record['date'], '2024-06-01')

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_different_start_dates_yield_different_record_counts(
        self,
        mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """A later start_date should yield fewer (or equal) records for
        campaign_performance."""
        catalog_perf = self._make_selected_catalog(stream_names=['campaign_performance'])
        catalog_path = os.path.join(self.tmpdir, 'catalog_perf.json')
        with open(catalog_path, 'w') as f:
            json.dump(catalog_perf, f)

        # First sync with early start_date
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        config_early = self._write_config(
            {'start_date': '2023-01-01T00:00:00Z'}, 'config_early.json')

        taboola.do_sync(self._make_args(config=config_early, catalog=catalog_path))
        early_count = sum(
            1 for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        )

        mock_write_record.reset_mock()
        mock_write_state.reset_mock()
        mock_write_schema.reset_mock()

        # Second sync with later start_date
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        config_late = self._write_config(
            {'start_date': '2025-01-01T00:00:00Z'}, 'config_late.json')

        # Write fresh state so previous sync's bookmark doesn't interfere
        state_path = os.path.join(self.tmpdir, 'state_fresh.json')
        with open(state_path, 'w') as f:
            json.dump({}, f)

        taboola.do_sync(self._make_args(config=config_late, state=state_path,
                                        catalog=catalog_path))
        late_count = sum(
            1 for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        )

        self.assertGreater(early_count, late_count)
        self.assertEqual(early_count, 3)  # all records
        self.assertEqual(late_count, 1)   # only 2025-01-15

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_full_table_stream_ignores_start_date(
        self,
        mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """Campaigns is a full-table stream — start_date should not reduce
        the number of records."""
        catalog_campaigns = self._make_selected_catalog(stream_names=['campaigns'])
        catalog_path = os.path.join(self.tmpdir, 'catalog_campaigns.json')
        with open(catalog_path, 'w') as f:
            json.dump(catalog_campaigns, f)

        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        # Even with a very late start_date, campaigns returns all records
        config_path = self._write_config(
            {'start_date': '2025-12-01T00:00:00Z'}, 'config_late.json')

        taboola.do_sync(self._make_args(config=config_path, catalog=catalog_path))

        campaign_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaigns'
        ]
        self.assertEqual(len(campaign_records), len(self.MOCK_CAMPAIGNS))
