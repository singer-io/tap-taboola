import unittest
from unittest.mock import MagicMock, patch

from tap_taboola.discover import discover
from tap_taboola.exceptions import TaboolaForbiddenError


class DiscoveryAccessChecksTest(unittest.TestCase):
    def _client(self):
        client = MagicMock()
        client.config = {"account_id": "acct", "start_date": "2023-01-01T00:00:00Z"}
        return client

    def test_discover_excludes_forbidden_streams(self):
        client = self._client()

        class StreamClass:
            parent = None

            def __init__(self, _result):
                self._result = _result

            def __call__(self, **kwargs):
                instance = MagicMock()
                instance.check_access.return_value = self._result
                return instance

        with patch("tap_taboola.discover.STREAMS") as mock_streams:
            mock_streams.items.return_value = [
                ("campaigns", StreamClass(False)),
                ("campaign_performance", StreamClass(True)),
            ]

            catalog = discover(client)
            names = {entry.tap_stream_id for entry in catalog.streams}

            self.assertEqual(names, {"campaign_performance"})

    def test_discover_raises_when_no_stream_access(self):
        client = self._client()

        class StreamClass:
            parent = None

            def __init__(self, _result):
                self._result = _result

            def __call__(self, **kwargs):
                instance = MagicMock()
                instance.check_access.return_value = self._result
                return instance

        with patch("tap_taboola.discover.STREAMS") as mock_streams:
            mock_streams.items.return_value = [
                ("campaigns", StreamClass(False)),
                ("campaign_performance", StreamClass(False)),
            ]

            with self.assertRaises(TaboolaForbiddenError):
                discover(client)
