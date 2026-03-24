"""Integration test: start_date controls which campaign_performance
records are returned from the API."""
import unittest
from unittest.mock import patch

import tap_taboola as taboola

try:
    from .base import TaboolaBaseTest
except ImportError:
    from base import TaboolaBaseTest


class StartDateIntegrationTest(TaboolaBaseTest, unittest.TestCase):

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_start_date_filters_campaign_performance(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """campaign_performance records before start_date should not be
        returned by the API (mocked filter)."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)
        config['start_date'] = '2024-06-01T00:00:00Z'
        state = {}

        taboola.sync_campaign_performance(
            config, state, 'mock-token', config['account_id'])

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
    @patch("tap_taboola.streams.request")
    def test_different_start_dates_yield_different_record_counts(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """A later start_date should yield fewer (or equal) records for
        campaign_performance."""
        mock_request.side_effect = self._mock_request()

        # First sync with early start_date
        config_early = dict(self.default_config)
        config_early['start_date'] = '2023-01-01T00:00:00Z'

        taboola.sync_campaign_performance(
            config_early, {}, 'mock-token', config_early['account_id'])
        early_count = sum(
            1 for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        )

        mock_write_record.reset_mock()
        mock_write_state.reset_mock()

        # Second sync with later start_date
        config_late = dict(self.default_config)
        config_late['start_date'] = '2025-01-01T00:00:00Z'

        taboola.sync_campaign_performance(
            config_late, {}, 'mock-token', config_late['account_id'])
        late_count = sum(
            1 for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        )

        self.assertGreater(early_count, late_count)
        self.assertEqual(early_count, 3)  # all records
        self.assertEqual(late_count, 1)   # only 2025-01-15

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_full_table_stream_ignores_start_date(
        self,
        mock_request,
        mock_write_record,
        mock_write_state,
    ):
        """Campaigns is a full-table stream — start_date should not reduce
        the number of records."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)

        # Even with a very late start_date, campaigns returns all records
        config['start_date'] = '2025-12-01T00:00:00Z'
        taboola.sync_campaigns('mock-token', config['account_id'])

        campaign_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaigns'
        ]
        self.assertEqual(len(campaign_records), len(self.MOCK_CAMPAIGNS))
