import unittest
from unittest.mock import MagicMock

import requests

from tap_taboola.client import raise_for_error
from tap_taboola.discover import discover
from tap_taboola.exceptions import TaboolaForbiddenError
from tap_taboola.streams import Campaign, CampaignPerformance


class DiscoveryAccessChecksTest(unittest.TestCase):
    def _client(self):
        client = MagicMock()
        client.config = {"account_id": "acct", "start_date": "2023-01-01T00:00:00Z"}
        return client

    def test_forbidden_stream_is_excluded_by_real_access_check(self):
        client = self._client()
        client.make_request.side_effect = TaboolaForbiddenError(
            "HTTP-error-code: 403, Error: missing CAMPAIGN_VIEW permission"
        )
        stream = Campaign(config=client.config, client=client)

        with self.assertLogs(level="WARNING") as logs:
            self.assertFalse(stream.check_access())

        output = "\n".join(logs.output)
        self.assertIn("campaigns", output)
        self.assertIn("403", output)
        self.assertIn("missing CAMPAIGN_VIEW permission", output)
        client.make_request.assert_called_once()

    def test_successful_access_check_returns_true(self):
        client = self._client()
        client.make_request.return_value = MagicMock()
        stream = CampaignPerformance(config=client.config, client=client)

        self.assertTrue(stream.check_access())
        client.make_request.assert_called_once_with(
            "GET",
            stream.get_url_endpoint(),
            stream.update_params(),
            {"Accept": "application/json"},
            body=None,
        )

    def test_401_is_not_treated_as_missing_stream_permission(self):
        client = self._client()
        error = requests.HTTPError("401 Unauthorized")
        error.response = MagicMock(status_code=401)
        client.make_request.side_effect = error
        stream = Campaign(config=client.config, client=client)

        with self.assertRaises(requests.HTTPError):
            stream.check_access()

    def test_http_403_is_excluded_and_logs_warning(self):
        client = self._client()
        error = requests.HTTPError("403 Forbidden: reports denied")
        error.response = MagicMock(status_code=403)
        client.make_request.side_effect = error
        stream = CampaignPerformance(config=client.config, client=client)

        with self.assertLogs(level="WARNING") as logs:
            self.assertFalse(stream.check_access())

        output = "\n".join(logs.output)
        self.assertIn("Unauthorized Stream: campaign_performance", output)
        self.assertIn("403 Forbidden: reports denied", output)

    def test_forbidden_error_preserves_api_reason(self):
        response = MagicMock()
        response.status_code = 403
        response.json.return_value = {
            "message": "CAMPAIGN_VIEW permission required"
        }
        response.raise_for_status.side_effect = requests.HTTPError("403")

        with self.assertRaisesRegex(Exception, "CAMPAIGN_VIEW permission required"):
            raise_for_error(response)

    def test_non_forbidden_http_errors_propagate(self):
        client = self._client()
        error = requests.HTTPError("400 Bad Request")
        error.response = MagicMock(status_code=400)
        client.make_request.side_effect = error
        stream = Campaign(config=client.config, client=client)

        with self.assertRaises(requests.HTTPError):
            stream.check_access()

    def test_discovery_excludes_forbidden_stream_and_keeps_allowed_stream(self):
        client = self._client()
        client.make_request.side_effect = [
            TaboolaForbiddenError("HTTP-error-code: 403, Error: campaigns denied"),
            MagicMock(),
        ]

        with self.assertLogs(level="WARNING") as logs:
            catalog = discover(client)

        self.assertEqual(
            {entry.tap_stream_id for entry in catalog.streams},
            {"campaign_performance"},
        )
        self.assertIn(
            "Unauthorized streams excluded from catalog: campaigns",
            "\n".join(logs.output),
        )

    def test_discovery_raises_when_all_streams_are_forbidden(self):
        client = self._client()
        client.make_request.side_effect = [
            TaboolaForbiddenError("HTTP-error-code: 403, Error: campaigns denied"),
            TaboolaForbiddenError("HTTP-error-code: 403, Error: reports denied"),
        ]

        with self.assertLogs(level="WARNING") as logs:
            with self.assertRaises(TaboolaForbiddenError) as raised:
                discover(client)

        output = "\n".join(logs.output)
        self.assertIn("Unauthorized Stream: campaigns", output)
        self.assertIn("Unauthorized Stream: campaign_performance", output)
        self.assertIn("campaigns denied", output)
        self.assertIn("reports denied", output)
        self.assertEqual(
            str(raised.exception),
            "HTTP-error-code: 403, Error: The credentials do not have "
            "'read' access to any supported streams.",
        )
