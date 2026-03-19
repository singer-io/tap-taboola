"""Unit tests for token generation functions in tap_taboola."""

import unittest
from unittest.mock import MagicMock, patch

import tap_taboola


class MockResponse:
    """Lightweight mock of requests.Response."""

    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body


class TestGetTokenPasswordAuth(unittest.TestCase):
    """Tests for get_token_password_auth()."""

    @patch("tap_taboola.requests.post")
    def test_returns_token_on_200(self, mock_post):
        mock_post.return_value = MockResponse(200, {"access_token": "abc123"})

        result = tap_taboola.get_token_password_auth(
            "client_id", "client_secret", "user", "pass"
        )

        self.assertEqual(result, {"token": "abc123"})

    @patch("tap_taboola.requests.post")
    def test_returns_error_on_401(self, mock_post):
        mock_post.return_value = MockResponse(
            401,
            {"error": "invalid_client", "error_description": "Bad credentials"},
        )

        result = tap_taboola.get_token_password_auth(
            "client_id", "client_secret", "user", "wrong"
        )

        self.assertIn("error", result)
        self.assertEqual(result["error"], "invalid_client")

    @patch("tap_taboola.requests.post")
    def test_returns_empty_dict_on_non_200_non_4xx(self, mock_post):
        """A 302 or other non-handled status code returns an empty dict."""
        mock_post.return_value = MockResponse(302, {})

        result = tap_taboola.get_token_password_auth(
            "cid", "csecret", "u", "p"
        )

        self.assertEqual(result, {})


class TestGetTokenClientCredentialsAuth(unittest.TestCase):
    """Tests for get_token_client_credentials_auth()."""

    @patch("tap_taboola.requests.post")
    def test_returns_token_on_200(self, mock_post):
        mock_post.return_value = MockResponse(200, {"access_token": "tok-999"})

        result = tap_taboola.get_token_client_credentials_auth("cid", "csecret")

        self.assertEqual(result, {"token": "tok-999"})

    @patch("tap_taboola.requests.post")
    def test_returns_error_fields_on_400(self, mock_post):
        mock_post.return_value = MockResponse(
            400, {"error": "invalid_grant", "error_description": "Grant expired"}
        )

        result = tap_taboola.get_token_client_credentials_auth("cid", "csecret")

        self.assertEqual(result["error"], "invalid_grant")
        self.assertEqual(result["error_description"], "Grant expired")


class TestGenerateToken(unittest.TestCase):
    """Tests for generate_token()."""

    @patch("tap_taboola.get_token_password_auth")
    def test_returns_token_when_password_auth_succeeds(self, mock_pw):
        mock_pw.return_value = {"token": "pw-token"}

        result = tap_taboola.generate_token("cid", "cs", "user", "pass")

        self.assertEqual(result, "pw-token")
        mock_pw.assert_called_once()

    @patch("tap_taboola.get_token_client_credentials_auth")
    @patch("tap_taboola.get_token_password_auth")
    def test_falls_back_to_client_credentials_when_password_auth_fails(
        self, mock_pw, mock_cc
    ):
        mock_pw.return_value = {"error": "invalid_client"}
        mock_cc.return_value = {"token": "cc-token"}

        result = tap_taboola.generate_token("cid", "cs", "user", "pass")

        self.assertEqual(result, "cc-token")
        mock_cc.assert_called_once()

    @patch("tap_taboola.get_token_client_credentials_auth")
    @patch("tap_taboola.get_token_password_auth")
    def test_raises_exception_when_both_auth_methods_fail(
        self, mock_pw, mock_cc
    ):
        mock_pw.return_value = {"error": "invalid_client", "error_description": "Bad creds"}
        mock_cc.return_value = {
            "error": "invalid_client",
            "error_description": "Bad creds",
        }

        with self.assertRaises(Exception) as ctx:
            tap_taboola.generate_token("cid", "cs", "user", "pass")

        self.assertIn("Unable to authenticate", str(ctx.exception))

    @patch("tap_taboola.get_token_client_credentials_auth")
    @patch("tap_taboola.get_token_password_auth")
    def test_exception_message_includes_error_details(self, mock_pw, mock_cc):
        mock_pw.return_value = {}
        mock_cc.return_value = {
            "error": "server_error",
            "error_description": "Internal error",
        }

        with self.assertRaises(Exception) as ctx:
            tap_taboola.generate_token("cid", "cs", "user", "pass")

        self.assertIn("server_error", str(ctx.exception))
        self.assertIn("Internal error", str(ctx.exception))
