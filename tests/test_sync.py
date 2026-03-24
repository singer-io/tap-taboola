"""Integration test: sync function writes records and updates state
correctly."""
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


class SyncIntegrationTest(TaboolaBaseTest, unittest.TestCase):

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
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

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
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

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
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

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
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

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
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

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
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

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
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


class DoSyncIntegrationTest(TaboolaBaseTest, unittest.TestCase):
    """Test do_sync() end-to-end with mocked config, catalog, and API."""

    def setUp(self):
        """Write temporary config, state, and catalog files for do_sync()."""
        self.tmpdir = tempfile.mkdtemp()

        # Config file
        self.config_path = os.path.join(self.tmpdir, 'config.json')
        with open(self.config_path, 'w') as f:
            json.dump(self.default_config, f)

        # State file (empty)
        self.state_path = os.path.join(self.tmpdir, 'state.json')
        with open(self.state_path, 'w') as f:
            json.dump({}, f)

        # Generate a selected catalog via discovery
        self.catalog_all = self._make_selected_catalog()
        self.catalog_path = os.path.join(self.tmpdir, 'catalog.json')
        with open(self.catalog_path, 'w') as f:
            json.dump(self.catalog_all, f)

    def _make_args(self, config=None, state=None, catalog=None):
        return SimpleNamespace(
            config=config or self.config_path,
            state=state or self.state_path,
            catalog=catalog or self.catalog_path,
        )

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_full_pipeline_emits_schemas_and_records(
        self, mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """do_sync() should emit SCHEMA then RECORDs for each selected stream."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args())

        # Auth was called
        mock_gen_token.assert_called_once()

        # Schemas written for both streams
        schema_streams = [c[0][0] for c in mock_write_schema.call_args_list]
        self.assertIn('campaigns', schema_streams)
        self.assertIn('campaign_performance', schema_streams)

        # Records written for both streams
        record_streams = [c[0][0] for c in mock_write_record.call_args_list]
        self.assertIn('campaigns', record_streams)
        self.assertIn('campaign_performance', record_streams)

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_correct_record_counts(
        self, mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """do_sync() emits the right number of records per stream."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args())

        campaign_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaigns'
        ]
        perf_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        ]
        self.assertEqual(len(campaign_records), len(self.MOCK_CAMPAIGNS))
        self.assertEqual(len(perf_records), len(self.MOCK_CAMPAIGN_PERFORMANCE))

    # ------------------------------------------------------------------
    # Schema emission order
    # ------------------------------------------------------------------

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_schema_emitted_before_records(
        self, mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """For each stream, write_schema must be called before write_record."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        # Track call order
        call_order = []
        mock_write_schema.side_effect = lambda *a, **k: call_order.append(('schema', a[0]))
        mock_write_record.side_effect = lambda *a, **k: call_order.append(('record', a[0]))

        taboola.do_sync(self._make_args())

        # For each stream, first occurrence of 'schema' must precede first 'record'
        for stream_name in ('campaigns', 'campaign_performance'):
            schema_idx = next(
                i for i, (t, s) in enumerate(call_order)
                if t == 'schema' and s == stream_name
            )
            record_idx = next(
                i for i, (t, s) in enumerate(call_order)
                if t == 'record' and s == stream_name
            )
            self.assertLess(
                schema_idx, record_idx,
                f"Schema for {stream_name} must come before its records",
            )

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_schema_includes_key_properties(
        self, mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """write_schema is called with correct key_properties for each stream."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args())

        for schema_call in mock_write_schema.call_args_list:
            stream_name = schema_call[0][0]
            key_props = schema_call[0][2]
            self.assertIsInstance(key_props, list)
            self.assertIn('id', key_props,
                          f"key_properties for {stream_name} should include 'id'")

    # ------------------------------------------------------------------
    # Stream selection
    # ------------------------------------------------------------------

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_only_selected_streams_are_synced(
        self, mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """When only 'campaigns' is selected, campaign_performance is skipped."""
        catalog_campaigns_only = self._make_selected_catalog(
            stream_names=['campaigns'])
        catalog_path = os.path.join(self.tmpdir, 'catalog_campaigns.json')
        with open(catalog_path, 'w') as f:
            json.dump(catalog_campaigns_only, f)

        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args(catalog=catalog_path))

        record_streams = {c[0][0] for c in mock_write_record.call_args_list}
        schema_streams = {c[0][0] for c in mock_write_schema.call_args_list}

        self.assertIn('campaigns', record_streams)
        self.assertNotIn('campaign_performance', record_streams)
        self.assertIn('campaigns', schema_streams)
        self.assertNotIn('campaign_performance', schema_streams)

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_only_performance_selected(
        self, mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """When only 'campaign_performance' is selected, campaigns is skipped."""
        catalog_perf_only = self._make_selected_catalog(
            stream_names=['campaign_performance'])
        catalog_path = os.path.join(self.tmpdir, 'catalog_perf.json')
        with open(catalog_path, 'w') as f:
            json.dump(catalog_perf_only, f)

        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args(catalog=catalog_path))

        record_streams = {c[0][0] for c in mock_write_record.call_args_list}
        self.assertNotIn('campaigns', record_streams)
        self.assertIn('campaign_performance', record_streams)

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_no_streams_selected_writes_nothing(
        self, mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """When no streams are selected, nothing is written."""
        catalog_none = self._make_selected_catalog(stream_names=[])
        catalog_path = os.path.join(self.tmpdir, 'catalog_none.json')
        with open(catalog_path, 'w') as f:
            json.dump(catalog_none, f)

        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args(catalog=catalog_path))

        mock_write_schema.assert_not_called()
        mock_write_record.assert_not_called()
        mock_write_state.assert_not_called()

    # ------------------------------------------------------------------
    # Bookmark / State
    # ------------------------------------------------------------------

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_state_emitted_with_max_date(
        self, mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """After syncing campaign_performance, state should contain the max date."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args())

        mock_write_state.assert_called()
        # The last write_state call should have the max date from mock data
        final_state = mock_write_state.call_args[0][0]
        self.assertIn('start_date', final_state)
        self.assertEqual(final_state['start_date'], '2025-01-15')

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_existing_state_is_used_for_start_date(
        self, mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """An existing state with start_date should filter performance records."""
        # Write state with a date that filters out the first record
        state_path = os.path.join(self.tmpdir, 'state_existing.json')
        with open(state_path, 'w') as f:
            json.dump({'start_date': '2024-01-01'}, f)

        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args(state=state_path))

        perf_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        ]
        # Only records with date >= 2024-01-01 should be returned
        # (2024-07-01 and 2025-01-15, not 2023-07-01)
        self.assertEqual(len(perf_records), 2)

    # ------------------------------------------------------------------
    # Empty API responses
    # ------------------------------------------------------------------

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_empty_api_responses_no_crash(
        self, mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """do_sync() completes without error when API returns empty results."""
        mock_init_request.side_effect = self._mock_request(
            campaigns=[], performance=[])
        mock_stream_request.side_effect = self._mock_request(
            campaigns=[], performance=[])

        taboola.do_sync(self._make_args())

        # Schemas still emitted even with no data
        schema_streams = {c[0][0] for c in mock_write_schema.call_args_list}
        self.assertIn('campaigns', schema_streams)
        self.assertIn('campaign_performance', schema_streams)
        # No records written
        mock_write_record.assert_not_called()
        # No state written (no performance dates to bookmark)
        mock_write_state.assert_not_called()

    # ------------------------------------------------------------------
    # Account verification
    # ------------------------------------------------------------------

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_account_id_mismatch_uses_token_account(
        self, mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """When token account_id differs from config, the token's value is used."""
        mock_init_request.side_effect = self._mock_request(
            account_id='token-account-id')
        mock_stream_request.side_effect = self._mock_request(
            account_id='token-account-id')

        taboola.do_sync(self._make_args())

        # Verify request URLs used the token account, not the config account
        for c in mock_stream_request.call_args_list:
            url = c[0][0]
            if 'campaigns' in url or 'campaign-summary' in url:
                self.assertIn('token-account-id', url)
                self.assertNotIn('test-account-id', url)

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_invalid_catalog_file_raises(self, mock_gen_token):
        """do_sync() raises when catalog file contains invalid JSON."""
        bad_catalog = os.path.join(self.tmpdir, 'bad_catalog.json')
        with open(bad_catalog, 'w') as f:
            f.write('NOT VALID JSON{{{')

        with self.assertRaises(Exception):
            taboola.do_sync(self._make_args(catalog=bad_catalog))

    def test_missing_config_file_raises(self):
        """do_sync() raises when config file doesn't exist."""
        with self.assertRaises(Exception):
            taboola.do_sync(self._make_args(
                config='/tmp/nonexistent_config_12345.json'))

    # ------------------------------------------------------------------
    # Record field correctness
    # ------------------------------------------------------------------

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_campaign_records_have_correct_types(
        self, mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """Campaign records emitted by do_sync() have properly typed fields."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args())

        campaign_records = [
            c[0][1] for c in mock_write_record.call_args_list
            if c[0][0] == 'campaigns'
        ]
        self.assertGreater(len(campaign_records), 0)
        record = campaign_records[0]
        self.assertIsInstance(record['id'], int)
        self.assertIsInstance(record['cpc'], float)
        self.assertIsInstance(record['is_active'], bool)
        self.assertIsInstance(record['name'], str)
        self.assertIsInstance(record['start_date'], str)

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_performance_records_have_correct_types(
        self, mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """Performance records emitted by do_sync() have properly typed fields."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args())

        perf_records = [
            c[0][1] for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        ]
        self.assertGreater(len(perf_records), 0)
        record = perf_records[0]
        self.assertIsInstance(record['campaign_id'], int)
        self.assertIsInstance(record['impressions'], int)
        self.assertIsInstance(record['ctr'], float)
        self.assertIsInstance(record['clicks'], int)
        self.assertIsInstance(record['date'], str)
        self.assertRegex(record['date'], r'^\d{4}-\d{2}-\d{2}$')

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_null_end_date_uses_sentinel_via_do_sync(
        self, mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """Campaigns with end_date=None get '9999-12-31' through do_sync."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args())

        campaign_records = [
            c[0][1] for c in mock_write_record.call_args_list
            if c[0][0] == 'campaigns'
        ]
        # First mock campaign has end_date=None
        self.assertEqual(campaign_records[0]['end_date'], '9999-12-31')


