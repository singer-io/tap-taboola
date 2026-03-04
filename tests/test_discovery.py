"""Integration tests for tap-taboola stream discovery."""
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest

from base import TaboolaBaseTest  # pylint: disable=import-error


class TaboolaDiscoveryTest(DiscoveryTest, TaboolaBaseTest):
    """Verify discovery returns expected stream metadata."""

    @staticmethod
    def name():
        """Return unique test-run name."""
        return "tap_tester_taboola_discovery_test"

    def streams_to_test(self):
        """Return all expected streams."""
        return self.expected_stream_names()
