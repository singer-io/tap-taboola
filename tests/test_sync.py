"""Integration tests for the sync flow.

All HTTP calls are mocked so these tests run without real Taboola credentials.
They exercise the full path from do_sync() / sync_campaigns() /
sync_campaign_performance() through to singer.write_record() and
singer.write_state(), verifying that the records and state emitted are correct.
"""

import io
import json
import tempfile
import os
import unittest
from unittest.mock import MagicMock, patch

import singer

import tap_taboola
from tests.base import TaboolaBaseTest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_response(json_body, status_code=200):
    """Return a MagicMock that behaves like a successful requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body
    resp.raise_for_status.return_value = None
    return resp


def _catalog_for_stream(stream_name, selected=True):
    """Return a minimal Singer catalog dict with *stream_name* selected."""
    from tap_taboola.discover import discover
    import singer.metadata as singer_metadata

    catalog = discover()
    entry = catalog.get_stream(stream_name)
    mdata = singer_metadata.to_map(entry.metadata)
    mdata[()]["selected"] = selected
    entry.metadata = singer_metadata.to_list(mdata)
    return catalog.to_dict()


# ---------------------------------------------------------------------------
# sync_campaigns integration
# ---------------------------------------------------------------------------


class TestSyncCampaignsIntegration(unittest.TestCase, TaboolaBaseTest):
    """Integration tests for the campaigns sync path."""

    def setUp(self):
        self.access_token = "test-access-token"
        self.account_id = "test-account-123"

    @patch("tap_taboola.request")
    def test_sync_campaigns_emits_correct_number_of_records(self, mock_request):
        api_records = [self.make_campaign_api_record(i) for i in range(1, 6)]
        mock_request.return_value = _make_mock_response({"results": api_records})

        written_records = []
        with patch("tap_taboola.singer.write_record",
                   side_effect=lambda *a, **kw: written_records.append(a)):
            tap_taboola.sync_campaigns(self.access_token, self.account_id)

        self.assertEqual(len(written_records), 5)

    @patch("tap_taboola.request")
    def test_sync_campaigns_emits_correctly_parsed_records(self, mock_request):
        api_records = [self.make_campaign_api_record(42)]
        mock_request.return_value = _make_mock_response({"results": api_records})

        written_records = []

        def capture(stream_name, record, **kwargs):
            written_records.append((stream_name, record))

        with patch("tap_taboola.singer.write_record", side_effect=capture):
            tap_taboola.sync_campaigns(self.access_token, self.account_id)

        stream_name, record = written_records[0]
        self.assertEqual(stream_name, "campaigns")
        self.assertEqual(record["id"], 42)
        self.assertIsInstance(record["cpc"], float)
        self.assertIsInstance(record["is_active"], bool)

    @patch("tap_taboola.request")
    def test_sync_campaigns_handles_null_dates(self, mock_request):
        """Campaigns with null start_date / end_date use the sentinel value."""
        raw = self.make_campaign_api_record(1)
        raw["start_date"] = None
        raw["end_date"] = None
        mock_request.return_value = _make_mock_response({"results": [raw]})

        written_records = []

        def capture(stream_name, record, **kwargs):
            written_records.append(record)

        with patch("tap_taboola.singer.write_record", side_effect=capture):
            tap_taboola.sync_campaigns(self.access_token, self.account_id)

        record = written_records[0]
        self.assertEqual(record["start_date"], "9999-12-31")
        self.assertEqual(record["end_date"], "9999-12-31")


# ---------------------------------------------------------------------------
# sync_campaign_performance integration
# ---------------------------------------------------------------------------


class TestSyncCampaignPerformanceIntegration(unittest.TestCase, TaboolaBaseTest):
    """Integration tests for the campaign_performance sync path."""

    def setUp(self):
        self.config = self.make_config()
        self.access_token = "test-access-token"
        self.account_id = "test-account-123"

    @patch("tap_taboola.request")
    def test_emits_correct_number_of_records(self, mock_request):
        api_records = [
            self.make_campaign_performance_api_record(
                i, "2024-01-{:02d} 00:00:00.000000".format(i)
            )
            for i in range(1, 8)
        ]
        mock_request.return_value = _make_mock_response({"results": api_records})

        written_records = []
        with patch("tap_taboola.singer.write_record",
                   side_effect=lambda *a, **kw: written_records.append(a)):
            tap_taboola.sync_campaign_performance(
                self.config, {}, self.access_token, self.account_id
            )

        self.assertEqual(len(written_records), 7)

    @patch("tap_taboola.request")
    def test_state_bookmark_is_written_after_sync(self, mock_request):
        api_records = [
            self.make_campaign_performance_api_record(1, "2024-03-10 00:00:00.000000"),
            self.make_campaign_performance_api_record(2, "2024-03-15 00:00:00.000000"),
        ]
        mock_request.return_value = _make_mock_response({"results": api_records})

        with patch("tap_taboola.singer.write_record"):
            state = tap_taboola.sync_campaign_performance(
                self.config, {}, self.access_token, self.account_id
            )

        bookmark = singer.get_bookmark(state, "campaign_performance", "date")
        self.assertEqual(bookmark, "2024-03-15")

    @patch("tap_taboola.request")
    def test_start_date_from_state_bookmark_is_used_in_request(self, mock_request):
        """The start_date query parameter should come from the state bookmark."""
        mock_request.return_value = _make_mock_response({"results": []})

        existing_state = singer.write_bookmark(
            {}, "campaign_performance", "date", "2024-05-01"
        )

        with patch("tap_taboola.singer.write_record"):
            tap_taboola.sync_campaign_performance(
                self.config, existing_state, self.access_token, self.account_id
            )

        # Verify the URL request was made with the bookmark as start_date
        call_params = mock_request.call_args[0][2]
        self.assertEqual(call_params["start_date"], "2024-05-01")

    @patch("tap_taboola.request")
    def test_start_date_from_config_when_no_bookmark(self, mock_request):
        """When there is no bookmark, start_date falls back to config.start_date."""
        mock_request.return_value = _make_mock_response({"results": []})

        with patch("tap_taboola.singer.write_record"):
            tap_taboola.sync_campaign_performance(
                self.config, {}, self.access_token, self.account_id
            )

        call_params = mock_request.call_args[0][2]
        self.assertEqual(call_params["start_date"], self.config["start_date"])

    @patch("tap_taboola.request")
    def test_performance_record_fields_have_correct_types(self, mock_request):
        raw = self.make_campaign_performance_api_record(5, "2024-06-20 00:00:00.000000")
        mock_request.return_value = _make_mock_response({"results": [raw]})

        captured = []

        def capture(stream_name, record, **kwargs):
            captured.append(record)

        with patch("tap_taboola.singer.write_record", side_effect=capture):
            tap_taboola.sync_campaign_performance(
                self.config, {}, self.access_token, self.account_id
            )

        record = captured[0]
        self.assertIsInstance(record["campaign_id"], int)
        self.assertIsInstance(record["impressions"], int)
        self.assertIsInstance(record["ctr"], float)
        self.assertIsInstance(record["currency"], str)
        self.assertEqual(record["date"], "2024-06-20")


# ---------------------------------------------------------------------------
# do_sync integration
# ---------------------------------------------------------------------------


class TestDoSyncIntegration(unittest.TestCase, TaboolaBaseTest):
    """Integration tests for tap_taboola.do_sync()."""

    def _make_args(self, config_data, catalog_data, state_data=None):
        """Write config / catalog / state to temp files and return a mock args object."""
        config_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        )
        json.dump(config_data, config_file)
        config_file.close()

        catalog_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        )
        json.dump(catalog_data, catalog_file)
        catalog_file.close()

        state_file = None
        if state_data is not None:
            sf = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
            json.dump(state_data, sf)
            sf.close()
            state_file = sf.name

        args = MagicMock()
        args.config = config_file.name
        args.catalog = catalog_file.name
        args.state = state_file
        return args, [config_file.name, catalog_file.name] + (
            [state_file] if state_file else []
        )

    def tearDown(self):
        pass

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_schema")
    @patch("tap_taboola.sync_campaign_performance")
    @patch("tap_taboola.verify_account_access")
    @patch("tap_taboola.generate_token")
    def test_do_sync_calls_sync_campaign_performance_when_selected(
        self,
        mock_gen_token,
        mock_verify,
        mock_sync_perf,
        mock_write_schema,
        mock_write_state,
    ):
        mock_gen_token.return_value = "tok"
        mock_verify.return_value = "test-account-123"
        mock_sync_perf.return_value = {}

        catalog_dict = _catalog_for_stream("campaign_performance", selected=True)
        args, paths = self._make_args(self.make_config(), catalog_dict)

        try:
            tap_taboola.do_sync(args)
        finally:
            for p in paths:
                os.unlink(p)

        mock_sync_perf.assert_called_once()

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_schema")
    @patch("tap_taboola.sync_campaigns")
    @patch("tap_taboola.verify_account_access")
    @patch("tap_taboola.generate_token")
    def test_do_sync_calls_sync_campaigns_when_selected(
        self,
        mock_gen_token,
        mock_verify,
        mock_sync_camp,
        mock_write_schema,
        mock_write_state,
    ):
        mock_gen_token.return_value = "tok"
        mock_verify.return_value = "test-account-123"

        catalog_dict = _catalog_for_stream("campaigns", selected=True)
        args, paths = self._make_args(self.make_config(), catalog_dict)

        try:
            tap_taboola.do_sync(args)
        finally:
            for p in paths:
                os.unlink(p)

        mock_sync_camp.assert_called_once()

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_schema")
    @patch("tap_taboola.sync_campaigns")
    @patch("tap_taboola.verify_account_access")
    @patch("tap_taboola.generate_token")
    def test_do_sync_writes_schema_before_sync(
        self,
        mock_gen_token,
        mock_verify,
        mock_sync_camp,
        mock_write_schema,
        mock_write_state,
    ):
        mock_gen_token.return_value = "tok"
        mock_verify.return_value = "test-account-123"

        catalog_dict = _catalog_for_stream("campaigns", selected=True)
        args, paths = self._make_args(self.make_config(), catalog_dict)

        try:
            tap_taboola.do_sync(args)
        finally:
            for p in paths:
                os.unlink(p)

        mock_write_schema.assert_called_once()
        schema_stream_name = mock_write_schema.call_args[0][0]
        self.assertEqual(schema_stream_name, "campaigns")

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_schema")
    @patch("tap_taboola.sync_campaigns")
    @patch("tap_taboola.sync_campaign_performance")
    @patch("tap_taboola.verify_account_access")
    @patch("tap_taboola.generate_token")
    def test_do_sync_skips_unselected_streams(
        self,
        mock_gen_token,
        mock_verify,
        mock_sync_perf,
        mock_sync_camp,
        mock_write_schema,
        mock_write_state,
    ):
        mock_gen_token.return_value = "tok"
        mock_verify.return_value = "test-account-123"
        mock_sync_perf.return_value = {}

        # Select only campaign_performance; campaigns should be skipped.
        catalog_dict = _catalog_for_stream("campaign_performance", selected=True)
        args, paths = self._make_args(self.make_config(), catalog_dict)

        try:
            tap_taboola.do_sync(args)
        finally:
            for p in paths:
                os.unlink(p)

        mock_sync_camp.assert_not_called()
        mock_sync_perf.assert_called_once()

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_schema")
    @patch("tap_taboola.sync_campaign_performance")
    @patch("tap_taboola.verify_account_access")
    @patch("tap_taboola.generate_token")
    def test_do_sync_passes_initial_state_to_sync_performance(
        self,
        mock_gen_token,
        mock_verify,
        mock_sync_perf,
        mock_write_schema,
        mock_write_state,
    ):
        mock_gen_token.return_value = "tok"
        mock_verify.return_value = "test-account-123"
        mock_sync_perf.return_value = {}

        initial_state = {"bookmarks": {"campaign_performance": {"date": "2024-02-01"}}}
        catalog_dict = _catalog_for_stream("campaign_performance", selected=True)
        args, paths = self._make_args(
            self.make_config(), catalog_dict, state_data=initial_state
        )

        try:
            tap_taboola.do_sync(args)
        finally:
            for p in paths:
                os.unlink(p)

        call_kwargs = mock_sync_perf.call_args[0]
        # Second positional arg is state
        passed_state = call_kwargs[1]
        self.assertEqual(
            singer.get_bookmark(passed_state, "campaign_performance", "date"),
            "2024-02-01",
        )
