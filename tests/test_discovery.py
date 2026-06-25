"""Integration test: discovery produces correct catalog and metadata."""
import unittest
from tests.base import TaboolaBaseTest


class DiscoveryIntegrationTest(TaboolaBaseTest, unittest.TestCase):

    def test_discovery_expected_streams_and_metadata(self):
        """Verify do_discover() returns all expected streams with correct metadata."""
        catalog = self._run_discover()
        stream_map = {
            entry['tap_stream_id']: entry
            for entry in catalog.get('streams', [])
        }
        expected_streams = self.expected_metadata()

        self.assertEqual(set(stream_map.keys()), set(expected_streams.keys()))

        for stream_name, stream_expected in expected_streams.items():
            with self.subTest(stream=stream_name):
                entry = stream_map[stream_name]

                # Find root metadata
                root_meta = {}
                for meta in entry.get('metadata', []):
                    if meta.get('breadcrumb', []) in ([], ()):
                        root_meta = meta.get('metadata', {})
                        break

                self.assertEqual(
                    set(root_meta.get('table-key-properties', [])),
                    stream_expected[self.PRIMARY_KEYS],
                )
                self.assertEqual(
                    root_meta.get('forced-replication-method'),
                    stream_expected[self.REPLICATION_METHOD],
                )

                actual_rep_keys = root_meta.get('valid-replication-keys', [])
                if isinstance(actual_rep_keys, str):
                    actual_rep_keys = {actual_rep_keys}
                else:
                    actual_rep_keys = set(actual_rep_keys)
                self.assertEqual(
                    actual_rep_keys,
                    stream_expected[self.REPLICATION_KEYS],
                )

    def test_discovery_schema_properties_exist(self):
        """Each stream schema has at least one property."""
        catalog = self._run_discover()
        for entry in catalog.get('streams', []):
            with self.subTest(stream=entry['tap_stream_id']):
                schema = entry.get('schema', {})
                self.assertIn('properties', schema)
                self.assertTrue(len(schema['properties']) > 0)

    def test_discovery_key_properties_match_schema(self):
        """Key properties listed in metadata exist in the schema."""
        catalog = self._run_discover()
        for entry in catalog.get('streams', []):
            with self.subTest(stream=entry['tap_stream_id']):
                schema_props = set(entry.get('schema', {}).get('properties', {}).keys())
                key_props = set(entry.get('key_properties', []))
                self.assertTrue(
                    key_props.issubset(schema_props),
                    f"key_properties {key_props} not in schema properties {schema_props}",
                )
