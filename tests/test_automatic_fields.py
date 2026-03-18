"""Integration test: automatic (primary key / replication key) fields are
always marked as inclusion=automatic in metadata."""
import unittest

try:
    from .base import TaboolaBaseTest
except ImportError:
    from base import TaboolaBaseTest


class AutomaticFieldsIntegrationTest(TaboolaBaseTest, unittest.TestCase):

    def test_primary_and_replication_keys_are_automatic(self):
        """Verify that all primary keys and replication keys are marked
        as inclusion=automatic in discovery metadata."""
        catalog = self._run_discover()

        for entry in catalog.get('streams', []):
            with self.subTest(stream=entry['tap_stream_id']):
                # Find root metadata
                root_meta = {}
                for meta in entry.get('metadata', []):
                    if meta.get('breadcrumb', []) in ([], ()):
                        root_meta = meta.get('metadata', {})
                        break

                key_props = set(root_meta.get('table-key-properties', []))
                rep_keys = root_meta.get('valid-replication-keys', [])
                if isinstance(rep_keys, str):
                    rep_keys = {rep_keys}
                else:
                    rep_keys = set(rep_keys)

                schema_props = set(
                    entry.get('schema', {}).get('properties', {}).keys()
                )
                # Only expect replication keys that exist in the schema
                expected_auto = key_props | (rep_keys & schema_props)

                actual_auto = set()
                for meta in entry.get('metadata', []):
                    breadcrumb = meta.get('breadcrumb', [])
                    if len(breadcrumb) == 2 and breadcrumb[0] == 'properties':
                        if meta.get('metadata', {}).get('inclusion') == 'automatic':
                            actual_auto.add(breadcrumb[1])

                self.assertTrue(
                    expected_auto.issubset(actual_auto),
                    f"Stream '{entry['tap_stream_id']}': expected automatic fields "
                    f"{expected_auto} but got {actual_auto}",
                )
