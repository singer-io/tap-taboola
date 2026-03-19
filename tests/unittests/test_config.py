"""Unit tests for validate_config(), load_config() and load_state()."""

import json
import os
import tempfile
import unittest

import tap_taboola
from tests.base import TaboolaBaseTest


class TestValidateConfig(unittest.TestCase, TaboolaBaseTest):
    """Tests for tap_taboola.validate_config()."""

    def test_valid_config_does_not_raise(self):
        tap_taboola.validate_config(self.make_config())

    def test_missing_key_raises_runtime_error(self):
        config = self.make_config()
        del config["username"]
        with self.assertRaises(RuntimeError):
            tap_taboola.validate_config(config)

    def test_null_key_raises_runtime_error(self):
        config = self.make_config()
        config["password"] = None
        with self.assertRaises(RuntimeError):
            tap_taboola.validate_config(config)

    def test_multiple_missing_keys_raise_runtime_error(self):
        with self.assertRaises(RuntimeError):
            tap_taboola.validate_config({})

    def test_all_required_keys_enforced(self):
        """Each required key on its own triggers the error."""
        required_keys = ["username", "password", "account_id",
                         "client_id", "client_secret", "start_date"]
        for key in required_keys:
            config = self.make_config()
            del config[key]
            with self.assertRaises(RuntimeError, msg=f"Expected RuntimeError for missing key: {key}"):
                tap_taboola.validate_config(config)


class TestLoadConfig(unittest.TestCase, TaboolaBaseTest):
    """Tests for tap_taboola.load_config()."""

    def _write_json(self, data):
        """Write *data* to a temp file and return its path."""
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        )
        json.dump(data, tmp)
        tmp.close()
        return tmp.name

    def tearDown(self):
        # Clean up any temp files the test may have created
        pass

    def test_valid_config_file_is_loaded(self):
        path = self._write_json(self.make_config())
        try:
            config = tap_taboola.load_config(path)
            self.assertEqual(config["start_date"], self.DEFAULT_START_DATE)
        finally:
            os.unlink(path)

    def test_invalid_json_raises_runtime_error(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tmp:
            tmp.write("{not valid json")
            path = tmp.name
        try:
            with self.assertRaises(RuntimeError):
                tap_taboola.load_config(path)
        finally:
            os.unlink(path)

    def test_missing_file_raises_runtime_error(self):
        with self.assertRaises(RuntimeError):
            tap_taboola.load_config("/nonexistent/path/config.json")

    def test_config_with_missing_required_keys_raises_runtime_error(self):
        incomplete = {"username": "u", "password": "p"}
        path = self._write_json(incomplete)
        try:
            with self.assertRaises(RuntimeError):
                tap_taboola.load_config(path)
        finally:
            os.unlink(path)


class TestLoadState(unittest.TestCase):
    """Tests for tap_taboola.load_state()."""

    def _write_json(self, data):
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        )
        json.dump(data, tmp)
        tmp.close()
        return tmp.name

    def test_none_filename_returns_empty_dict(self):
        self.assertEqual(tap_taboola.load_state(None), {})

    def test_valid_state_file_is_loaded(self):
        state = {"bookmarks": {"campaign_performance": {"date": "2024-03-01"}}}
        path = self._write_json(state)
        try:
            result = tap_taboola.load_state(path)
            self.assertEqual(result, state)
        finally:
            os.unlink(path)

    def test_empty_state_file_returns_empty_dict(self):
        path = self._write_json({})
        try:
            result = tap_taboola.load_state(path)
            self.assertEqual(result, {})
        finally:
            os.unlink(path)

    def test_invalid_json_raises_runtime_error(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tmp:
            tmp.write("{not valid}")
            path = tmp.name
        try:
            with self.assertRaises(RuntimeError):
                tap_taboola.load_state(path)
        finally:
            os.unlink(path)

    def test_missing_file_raises_runtime_error(self):
        with self.assertRaises(RuntimeError):
            tap_taboola.load_state("/no/such/state.json")
