"""Unit tests for sync functions in tap_taboola."""

import unittest
from unittest.mock import MagicMock, call, patch

import singer

import tap_taboola
from tests.base import TaboolaBaseTest


# ---------------------------------------------------------------------------
# sync_campaigns
# ---------------------------------------------------------------------------


class TestSyncCampaigns(unittest.TestCase, TaboolaBaseTest):
    """Tests for tap_taboola.sync_campaigns()."""

    def _make_api_campaigns(self, n=3):
        return [self.make_campaign_api_record(i) for i in range(1, n + 1)]

    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.fetch_campaigns")
    def test_writes_one_record_per_campaign(self, mock_fetch, mock_write_record):
        mock_fetch.return_value = self._make_api_campaigns(3)

        tap_taboola.sync_campaigns("tok", "acct-1")

        self.assertEqual(mock_write_record.call_count, 3)

    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.fetch_campaigns")
    def test_written_records_use_campaigns_stream_name(
        self, mock_fetch, mock_write_record
    ):
        mock_fetch.return_value = self._make_api_campaigns(1)

        tap_taboola.sync_campaigns("tok", "acct-1")

        stream_name = mock_write_record.call_args[0][0]
        self.assertEqual(stream_name, "campaigns")

    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.fetch_campaigns")
    def test_record_contains_parsed_fields(self, mock_fetch, mock_write_record):
        mock_fetch.return_value = [self.make_campaign_api_record(7)]

        tap_taboola.sync_campaigns("tok", "acct-1")

        written = mock_write_record.call_args[0][1]
        self.assertEqual(written["id"], 7)
        self.assertIn("name", written)
        self.assertIn("cpc", written)

    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.fetch_campaigns")
    def test_empty_campaign_list_writes_no_records(self, mock_fetch, mock_write_record):
        mock_fetch.return_value = []

        tap_taboola.sync_campaigns("tok", "acct-1")

        mock_write_record.assert_not_called()


# ---------------------------------------------------------------------------
# sync_campaign_performance
# ---------------------------------------------------------------------------


class TestSyncCampaignPerformance(unittest.TestCase, TaboolaBaseTest):
    """Tests for tap_taboola.sync_campaign_performance()."""

    def _make_perf_records(self, n=3, base_date="2024-01-01"):
        import datetime
        base = datetime.date.fromisoformat(base_date)
        return [
            self.make_campaign_performance_api_record(
                campaign_id=i,
                date="{} 00:00:00.000000".format(base + datetime.timedelta(days=i - 1)),
            )
            for i in range(1, n + 1)
        ]

    def _empty_state(self):
        return {}

    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.fetch_campaign_performance")
    def test_writes_one_record_per_performance_row(self, mock_fetch, mock_write_record):
        mock_fetch.return_value = self._make_perf_records(4)

        tap_taboola.sync_campaign_performance(
            self.make_config(), self._empty_state(), "tok", "acct-1"
        )

        self.assertEqual(mock_write_record.call_count, 4)

    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.fetch_campaign_performance")
    def test_records_use_campaign_performance_stream_name(
        self, mock_fetch, mock_write_record
    ):
        mock_fetch.return_value = self._make_perf_records(1)

        tap_taboola.sync_campaign_performance(
            self.make_config(), self._empty_state(), "tok", "acct-1"
        )

        stream_name = mock_write_record.call_args[0][0]
        self.assertEqual(stream_name, "campaign_performance")

    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.fetch_campaign_performance")
    def test_returns_state_with_bookmark(self, mock_fetch, mock_write_record):
        mock_fetch.return_value = self._make_perf_records(3)

        state = tap_taboola.sync_campaign_performance(
            self.make_config(), self._empty_state(), "tok", "acct-1"
        )

        bookmark = singer.get_bookmark(state, "campaign_performance", "date")
        self.assertIsNotNone(bookmark)

    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.fetch_campaign_performance")
    def test_bookmark_is_the_max_date_seen(self, mock_fetch, mock_write_record):
        """The bookmark must advance to the highest date in the synced records."""
        mock_fetch.return_value = [
            self.make_campaign_performance_api_record(1, "2024-01-10 00:00:00.000000"),
            self.make_campaign_performance_api_record(2, "2024-01-20 00:00:00.000000"),
            self.make_campaign_performance_api_record(3, "2024-01-05 00:00:00.000000"),
        ]

        state = tap_taboola.sync_campaign_performance(
            self.make_config(), self._empty_state(), "tok", "acct-1"
        )

        bookmark = singer.get_bookmark(state, "campaign_performance", "date")
        self.assertEqual(bookmark, "2024-01-20")

    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.fetch_campaign_performance")
    def test_bookmark_not_regressed_when_all_records_are_older(
        self, mock_fetch, mock_write_record
    ):
        """An existing bookmark must not be replaced by an older date."""
        initial_state = singer.write_bookmark({}, "campaign_performance", "date", "2024-06-01")
        mock_fetch.return_value = [
            self.make_campaign_performance_api_record(1, "2024-01-01 00:00:00.000000"),
        ]

        state = tap_taboola.sync_campaign_performance(
            self.make_config(), initial_state, "tok", "acct-1"
        )

        bookmark = singer.get_bookmark(state, "campaign_performance", "date")
        self.assertEqual(bookmark, "2024-06-01")

    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.fetch_campaign_performance")
    def test_empty_performance_list_returns_state_unchanged(
        self, mock_fetch, mock_write_record
    ):
        initial_state = singer.write_bookmark({}, "campaign_performance", "date", "2024-03-01")
        mock_fetch.return_value = []

        state = tap_taboola.sync_campaign_performance(
            self.make_config(), initial_state, "tok", "acct-1"
        )

        bookmark = singer.get_bookmark(state, "campaign_performance", "date")
        self.assertEqual(bookmark, "2024-03-01")
        mock_write_record.assert_not_called()


# ---------------------------------------------------------------------------
# is_selected
# ---------------------------------------------------------------------------


class TestIsSelected(unittest.TestCase):
    """Tests for tap_taboola.is_selected()."""

    def _make_entry(self, inclusion=None, selected=None):
        import singer.metadata as singer_metadata
        mdata = {}
        if inclusion is not None:
            mdata["inclusion"] = inclusion
        if selected is not None:
            mdata["selected"] = selected

        entry = MagicMock()
        entry.metadata = singer_metadata.to_list({(): mdata})
        return entry

    def test_unsupported_inclusion_returns_false(self):
        entry = self._make_entry(inclusion="unsupported")
        self.assertFalse(tap_taboola.is_selected(entry))

    def test_automatic_inclusion_returns_true(self):
        entry = self._make_entry(inclusion="automatic")
        self.assertTrue(tap_taboola.is_selected(entry))

    def test_selected_true_overrides_available_inclusion(self):
        entry = self._make_entry(inclusion="available", selected=True)
        self.assertTrue(tap_taboola.is_selected(entry))

    def test_selected_false_overrides_automatic_inclusion(self):
        entry = self._make_entry(inclusion="automatic", selected=False)
        self.assertFalse(tap_taboola.is_selected(entry))

    def test_available_inclusion_without_selected_returns_false(self):
        entry = self._make_entry(inclusion="available")
        self.assertFalse(tap_taboola.is_selected(entry))


# ---------------------------------------------------------------------------
# verify_account_access
# ---------------------------------------------------------------------------


class TestVerifyAccountAccess(unittest.TestCase):
    """Tests for tap_taboola.verify_account_access()."""

    def _mock_request(self, account_id):
        resp = MagicMock()
        resp.json.return_value = {"account_id": account_id}
        return resp

    @patch("tap_taboola.request")
    def test_matching_account_id_returns_original(self, mock_req):
        mock_req.return_value = self._mock_request("acct-123")
        result = tap_taboola.verify_account_access("tok", "acct-123")
        self.assertEqual(result, "acct-123")

    @patch("tap_taboola.request")
    def test_mismatched_account_id_returns_token_account_id(self, mock_req):
        mock_req.return_value = self._mock_request("acct-456")
        result = tap_taboola.verify_account_access("tok", "acct-999")
        self.assertEqual(result, "acct-456")
