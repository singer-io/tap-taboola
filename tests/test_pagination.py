"""Integration tests for tap-taboola pagination."""
from tap_tester.base_suite_tests.pagination_test import PaginationTest

from base import TaboolaBaseTest  # pylint: disable=import-error


class TaboolaPaginationTest(PaginationTest, TaboolaBaseTest):
    """Verify all records across all result pages are replicated."""

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_taboola_pagination_test"

    def streams_to_test(self):
        """Return all expected streams."""
        return self.expected_stream_names().difference(self.excluded_stream_reasons().keys())

    def excluded_stream_reasons(self):
        """Return documented reasons for streams excluded from this test."""
        return {
            "campaigns": "Taboola API returns all campaigns in a single response; no pagination.",
            "campaign_performance": "Taboola API returns all performance rows in a single response; no pagination.",
        }

    def test_excluded_streams_are_documented(self):
        """Verify each excluded stream has a documented reason."""
        excluded = self.expected_stream_names().difference(self.streams_to_test())
        self.assertSetEqual(excluded, set(self.excluded_stream_reasons().keys()))
