"""Integration test: basic sync canary — verify the full pipeline runs
and emits records for all streams."""
import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import tap_taboola as taboola

try:
    from .base import TaboolaBaseTest
except ImportError:
    from base import TaboolaBaseTest


class SyncCanaryIntegrationTest(TaboolaBaseTest, unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.tmpdir, 'config.json')
        with open(self.config_path, 'w') as f:
            json.dump(self.default_config, f)
        self.state_path = os.path.join(self.tmpdir, 'state.json')
        with open(self.state_path, 'w') as f:
            json.dump({}, f)
        self.catalog_path = os.path.join(self.tmpdir, 'catalog.json')
        with open(self.catalog_path, 'w') as f:
            json.dump(self._make_selected_catalog(), f)

    def _make_args(self, config=None, state=None, catalog=None):
        return SimpleNamespace(
            config=config or self.config_path,
            state=state or self.state_path,
            catalog=catalog or self.catalog_path,
        )

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.singer.write_schema")
    @patch("tap_taboola.streams.request")
    @patch("tap_taboola.request")
    @patch("tap_taboola.generate_token", return_value="mock-token")
    def test_full_pipeline_emits_records(
        self,
        mock_gen_token, mock_init_request, mock_stream_request,
        mock_write_schema, mock_write_record, mock_write_state,
    ):
        """Smoke test — run the full sync pipeline via do_sync and verify
        at least one record is written for each stream."""
        mock_init_request.side_effect = self._mock_request()
        mock_stream_request.side_effect = self._mock_request()

        taboola.do_sync(self._make_args())

        written_streams = {
            call_args[0][0] for call_args in mock_write_record.call_args_list
        }

        for stream in self.expected_metadata():
            with self.subTest(stream=stream):
                self.assertIn(stream, written_streams)
