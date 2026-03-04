"""Integration tests for tap-taboola start_date filtering."""
from tap_tester.base_suite_tests.start_date_test import StartDateTest

from base import TaboolaBaseTest  # pylint: disable=import-error


class TaboolaStartDateTest(StartDateTest, TaboolaBaseTest):
    """Verify start_date filters campaign_performance records correctly.

    campaigns is a FULL_TABLE stream that does not pass start_date to the API,
    so it is excluded.  campaign_performance uses start_date as an API query
    parameter and is included.
    """

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_taboola_start_date_test"

    start_date_1 = "2023-01-01T00:00:00Z"
    start_date_2 = "2024-10-01T00:00:00Z"

    def streams_to_test(self):
        """Return streams that respect start_date."""
        return {"campaign_performance"}

    def excluded_stream_reasons(self):
        """Return documented reasons for streams excluded from this test."""
        return {
            "campaigns": "Full-table stream: does not filter by start_date via API.",
        }

    def test_excluded_streams_are_documented(self):
        """Verify each excluded stream has a documented reason."""
        excluded = self.expected_stream_names().difference(self.streams_to_test())
        self.assertSetEqual(excluded, set(self.excluded_stream_reasons().keys()))

    def test_replicated_records(self):
        """Verify later start_date does not increase replicated records."""
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                count_1 = StartDateTest.record_count_by_stream_1.get(stream, 0)
                count_2 = StartDateTest.record_count_by_stream_2.get(stream, 0)
                self.assertGreaterEqual(count_1, count_2)
