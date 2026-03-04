"""Integration tests for tap-taboola basic sync canary."""
from tap_tester.base_suite_tests.sync_canary_test import SyncCanaryTest

from base import TaboolaBaseTest  # pylint: disable=import-error


class TaboolaSyncCanaryTest(SyncCanaryTest, TaboolaBaseTest):
    """Basic smoke test — verify the full pipeline runs and emits records."""

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_taboola_sync_canary_test"

    def streams_to_test(self):
        """Return all expected streams."""
        return self.expected_stream_names()
