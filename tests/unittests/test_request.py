"""Unit tests for the request() HTTP helper in tap_taboola."""

import unittest
from unittest.mock import MagicMock, patch

import requests

import tap_taboola


class TestRequest(unittest.TestCase):
    """Tests for tap_taboola.request()."""

    def _make_response(self, status_code=200, json_body=None):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.json.return_value = json_body or {}
        if status_code >= 400:
            mock_resp.raise_for_status.side_effect = requests.HTTPError(
                response=mock_resp
            )
        else:
            mock_resp.raise_for_status.return_value = None
        return mock_resp

    @patch("tap_taboola.requests.get")
    def test_request_success_returns_response(self, mock_get):
        """Successful GET returns the response object."""
        mock_resp = self._make_response(200, {"results": [{"id": 1}]})
        mock_get.return_value = mock_resp

        result = tap_taboola.request("https://example.com/api", "my-token")

        mock_get.assert_called_once()
        self.assertEqual(result, mock_resp)

    @patch("tap_taboola.requests.get")
    def test_request_passes_auth_header(self, mock_get):
        """Bearer token is forwarded in the Authorization header."""
        mock_resp = self._make_response(200)
        mock_get.return_value = mock_resp

        tap_taboola.request("https://example.com/api", "my-token")

        call_kwargs = mock_get.call_args
        headers = call_kwargs[1]["headers"]
        self.assertIn("Bearer my-token", headers["Authorization"])

    @patch("tap_taboola.requests.get")
    def test_request_passes_params(self, mock_get):
        """Query params are forwarded to requests.get."""
        mock_resp = self._make_response(200)
        mock_get.return_value = mock_resp

        tap_taboola.request("https://example.com/api", "tok", params={"page": 1})

        call_kwargs = mock_get.call_args
        self.assertEqual(call_kwargs[1]["params"], {"page": 1})

    @patch("tap_taboola.requests.get")
    def test_request_none_params_defaults_to_empty_dict(self, mock_get):
        """Passing params=None sends an empty dict instead."""
        mock_resp = self._make_response(200)
        mock_get.return_value = mock_resp

        tap_taboola.request("https://example.com/api", "tok", params=None)

        call_kwargs = mock_get.call_args
        self.assertEqual(call_kwargs[1]["params"], {})

    @patch("tap_taboola.requests.get")
    def test_request_raises_on_4xx(self, mock_get):
        """4xx responses propagate as HTTPError (no retry due to giveup)."""
        mock_resp = self._make_response(404)
        mock_get.return_value = mock_resp

        with self.assertRaises(requests.HTTPError):
            tap_taboola.request("https://example.com/api", "tok")

    @patch("tap_taboola.requests.get")
    def test_request_raises_on_connection_error(self, mock_get):
        """A ConnectionError from requests propagates to the caller."""
        mock_get.side_effect = requests.exceptions.ConnectionError("connection failed")

        with self.assertRaises(requests.exceptions.ConnectionError):
            tap_taboola.request.__wrapped__(
                "https://example.com/api", "tok"
            )

    @patch("tap_taboola.requests.get")
    def test_request_calls_raise_for_status(self, mock_get):
        """raise_for_status() is called on every response."""
        mock_resp = self._make_response(200)
        mock_get.return_value = mock_resp

        tap_taboola.request("https://example.com/api", "tok")

        mock_resp.raise_for_status.assert_called_once()
