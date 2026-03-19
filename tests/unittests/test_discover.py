"""Unit tests for the discover() function."""

import unittest
from unittest.mock import patch

from singer.catalog import Catalog

from tap_taboola.discover import discover


class TestDiscover(unittest.TestCase):
    """Tests for tap_taboola.discover.discover()."""

    def test_discover_returns_catalog(self):
        """discover() must return a singer Catalog object."""
        catalog = discover()
        self.assertIsInstance(catalog, Catalog)

    def test_discover_returns_two_streams(self):
        """The tap exposes exactly two streams: campaigns and campaign_performance."""
        catalog = discover()
        self.assertEqual(len(catalog.streams), 2)

    def test_discover_stream_names(self):
        """Both expected stream names appear in the catalog."""
        catalog = discover()
        names = {entry.tap_stream_id for entry in catalog.streams}
        self.assertIn("campaigns", names)
        self.assertIn("campaign_performance", names)

    def test_campaigns_key_properties(self):
        """campaigns stream must have key_properties = ['id']."""
        catalog = discover()
        entry = catalog.get_stream("campaigns")
        self.assertEqual(entry.key_properties, ["id"])

    def test_campaign_performance_key_properties(self):
        """campaign_performance must have key_properties = ['campaign_id', 'date']."""
        catalog = discover()
        entry = catalog.get_stream("campaign_performance")
        self.assertEqual(entry.key_properties, ["campaign_id", "date"])

    def test_campaigns_has_schema(self):
        """campaigns catalog entry must include a non-empty schema."""
        catalog = discover()
        entry = catalog.get_stream("campaigns")
        self.assertIsNotNone(entry.schema)
        self.assertTrue(entry.schema.to_dict().get("properties"))

    def test_campaign_performance_has_schema(self):
        """campaign_performance catalog entry must include a non-empty schema."""
        catalog = discover()
        entry = catalog.get_stream("campaign_performance")
        self.assertIsNotNone(entry.schema)
        self.assertTrue(entry.schema.to_dict().get("properties"))

    def test_discover_error_propagates(self):
        """If get_schemas() raises, discover() must propagate the exception."""
        with patch("tap_taboola.discover.get_schemas", side_effect=FileNotFoundError("missing")):
            with self.assertRaises(Exception):
                discover()

    def test_campaigns_replication_metadata(self):
        """campaigns must be FULL_TABLE with no valid replication keys."""
        import singer.metadata as singer_metadata
        catalog = discover()
        entry = catalog.get_stream("campaigns")
        mdata = singer_metadata.to_map(entry.metadata)
        root = mdata.get((), {})
        self.assertEqual(root.get("forced-replication-method"), "FULL_TABLE")
        self.assertIsNone(root.get("valid-replication-keys"))

    def test_campaign_performance_replication_metadata(self):
        """campaign_performance must be INCREMENTAL with 'date' as the replication key."""
        import singer.metadata as singer_metadata
        catalog = discover()
        entry = catalog.get_stream("campaign_performance")
        mdata = singer_metadata.to_map(entry.metadata)
        root = mdata.get((), {})
        self.assertEqual(root.get("forced-replication-method"), "INCREMENTAL")
        self.assertIn("date", root.get("valid-replication-keys", []))
