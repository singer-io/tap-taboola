"""Integration test: basic sync canary — verify the full pipeline runs
and emits records for all streams."""
import unittest
from unittest.mock import patch

import tap_taboola as taboola
import tap_taboola.schemas as schemas

try:
    from .base import TaboolaBaseTest
except ImportError:
    from base import TaboolaBaseTest


class SyncCanaryIntegrationTest(TaboolaBaseTest, unittest.TestCase):

    @patch("tap_taboola.singer.write_state")
    @patch("tap_taboola.singer.write_record")
    @patch("tap_taboola.singer.write_schema")
    @patch("tap_taboola.request")
    def test_full_pipeline_emits_records(
        self,
        mock_request,
        mock_write_schema,
        mock_write_record,
        mock_write_state,
    ):
        """Smoke test — run the full sync pipeline and verify at least one
        record is written for each stream."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)
        state = {}

        # Simulate do_sync flow
        taboola.singer.write_schema(
            'campaigns', schemas.campaign, key_properties=['id'])
        taboola.singer.write_schema(
            'campaign_performance', schemas.campaign_performance,
            key_properties=['campaign_id', 'date'])

        taboola.sync_campaigns('mock-token', config['account_id'])
        taboola.sync_campaign_performance(
            config, state, 'mock-token', config['account_id'])

        written_streams = {
            call_args[0][0] for call_args in mock_write_record.call_args_list
        }

        for stream in self.expected_metadata():
            with self.subTest(stream=stream):
                self.assertIn(stream, written_streams)
