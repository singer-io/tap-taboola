"""Integration tests for tap-taboola bookmarking."""
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest

from base import TaboolaBaseTest  # pylint: disable=import-error


class TaboolaBookmarkTest(BookmarkTest, TaboolaBaseTest):
    """Verify bookmark behaviour for tap-taboola streams.

    Both streams (campaigns, campaign_performance) are FULL_TABLE and carry
    no replication key, so there are no incremental streams to bookmark-test.
    All streams are documented as excluded.
    """

    bookmark_format = "%Y-%m-%d"
    initial_bookmarks = {}

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_taboola_bookmark_test"

    def streams_to_test(self):
        """Return incremental streams — campaign_performance uses 'date' as a replication key."""
        return {s for s, m in self.expected_metadata().items()
                if m[self.REPLICATION_METHOD] == self.INCREMENTAL}

    def excluded_stream_reasons(self):
        """Document exclusion for full-table streams."""
        return {s: "Full-table stream: bookmark state not applicable."
                for s, m in self.expected_metadata().items()
                if m[self.REPLICATION_METHOD] == self.FULL_TABLE}

    def test_excluded_streams_are_documented(self):
        """Verify each excluded stream has a documented reason."""
        excluded = self.expected_stream_names().difference(self.streams_to_test())
        self.assertSetEqual(excluded, set(self.excluded_stream_reasons().keys()))

    def calculate_new_bookmarks(self):
        """Set bookmark to a date between the two mock records so sync 2 returns fewer rows.

        Mock data has records on 2024-07-01 and 2025-01-01.  Setting the
        bookmark to 2024-12-01 means sync 2 will only fetch the 2025-01-01
        record (1), which is less than sync 1's full two records (2).
        """
        return {'campaign_performance': {'date': '2024-12-01'}}
