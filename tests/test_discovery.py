"""Integration tests for the discover() flow.

These tests exercise the full discovery pipeline — schema.get_schemas() →
discover.discover() — against real JSON schema files on disk, with no mocking
of the I/O layer.
"""

import unittest

import singer.metadata as singer_metadata
from singer.catalog import Catalog

from tap_taboola.discover import discover
from tests.base import TaboolaBaseTest


class TestDiscovery(unittest.TestCase, TaboolaBaseTest):
    """Full-pipeline discovery tests."""

    @classmethod
    def setUpClass(cls):
        cls.catalog = discover()

    # ------------------------------------------------------------------
    # Catalog structure
    # ------------------------------------------------------------------

    def test_discover_returns_catalog_instance(self):
        self.assertIsInstance(self.catalog, Catalog)

    def test_catalog_contains_exactly_two_streams(self):
        self.assertEqual(len(self.catalog.streams), 2)

    def test_catalog_contains_campaigns_stream(self):
        names = {e.tap_stream_id for e in self.catalog.streams}
        self.assertIn("campaigns", names)

    def test_catalog_contains_campaign_performance_stream(self):
        names = {e.tap_stream_id for e in self.catalog.streams}
        self.assertIn("campaign_performance", names)

    def test_stream_name_matches_tap_stream_id(self):
        for entry in self.catalog.streams:
            self.assertEqual(entry.stream, entry.tap_stream_id)

    # ------------------------------------------------------------------
    # campaigns – metadata
    # ------------------------------------------------------------------

    def test_campaigns_key_properties(self):
        entry = self.catalog.get_stream("campaigns")
        self.assertEqual(entry.key_properties, ["id"])

    def test_campaigns_replication_method_is_full_table(self):
        entry = self.catalog.get_stream("campaigns")
        mdata = singer_metadata.to_map(entry.metadata)
        method = mdata.get((), {}).get("forced-replication-method")
        self.assertEqual(method, "FULL_TABLE")

    def test_campaigns_has_no_replication_keys(self):
        entry = self.catalog.get_stream("campaigns")
        mdata = singer_metadata.to_map(entry.metadata)
        valid_rk = mdata.get((), {}).get("valid-replication-keys")
        self.assertIsNone(valid_rk)

    # ------------------------------------------------------------------
    # campaign_performance – metadata
    # ------------------------------------------------------------------

    def test_campaign_performance_key_properties(self):
        entry = self.catalog.get_stream("campaign_performance")
        self.assertEqual(sorted(entry.key_properties), ["campaign_id", "date"])

    def test_campaign_performance_replication_method_is_incremental(self):
        entry = self.catalog.get_stream("campaign_performance")
        mdata = singer_metadata.to_map(entry.metadata)
        method = mdata.get((), {}).get("forced-replication-method")
        self.assertEqual(method, "INCREMENTAL")

    def test_campaign_performance_replication_key_is_date(self):
        entry = self.catalog.get_stream("campaign_performance")
        mdata = singer_metadata.to_map(entry.metadata)
        valid_rk = mdata.get((), {}).get("valid-replication-keys", [])
        self.assertIn("date", valid_rk)

    def test_campaign_performance_date_field_is_automatic(self):
        entry = self.catalog.get_stream("campaign_performance")
        mdata = singer_metadata.to_map(entry.metadata)
        inclusion = mdata.get(("properties", "date"), {}).get("inclusion")
        self.assertEqual(inclusion, "automatic")

    # ------------------------------------------------------------------
    # campaigns – schema shape
    # ------------------------------------------------------------------

    def test_campaigns_schema_has_required_fields(self):
        entry = self.catalog.get_stream("campaigns")
        props = entry.schema.to_dict()["properties"]
        required = {"id", "name", "cpc", "is_active", "status",
                    "start_date", "end_date", "approval_state"}
        for field in required:
            self.assertIn(field, props, msg=f"'{field}' missing from campaigns schema")

    def test_campaigns_id_type_is_nullable_integer(self):
        entry = self.catalog.get_stream("campaigns")
        props = entry.schema.to_dict()["properties"]
        id_type = props["id"]["type"]
        self.assertIn("integer", id_type)
        self.assertIn("null", id_type)

    # ------------------------------------------------------------------
    # campaign_performance – schema shape
    # ------------------------------------------------------------------

    def test_campaign_performance_schema_has_required_fields(self):
        entry = self.catalog.get_stream("campaign_performance")
        props = entry.schema.to_dict()["properties"]
        required = {"campaign_id", "date", "impressions", "clicks",
                    "ctr", "cpc", "spent", "currency"}
        for field in required:
            self.assertIn(field, props, msg=f"'{field}' missing from campaign_performance schema")

    def test_campaign_performance_date_type_is_nullable_string(self):
        entry = self.catalog.get_stream("campaign_performance")
        props = entry.schema.to_dict()["properties"]
        date_type = props["date"]["type"]
        self.assertIn("string", date_type)
        self.assertIn("null", date_type)

    def test_campaign_performance_date_has_date_format(self):
        entry = self.catalog.get_stream("campaign_performance")
        props = entry.schema.to_dict()["properties"]
        self.assertEqual(props["date"].get("format"), "date")

    # ------------------------------------------------------------------
    # Schema / metadata cross-stream consistency
    # ------------------------------------------------------------------

    def test_all_streams_have_metadata(self):
        for entry in self.catalog.streams:
            self.assertTrue(
                len(entry.metadata) > 0,
                msg=f"No metadata for stream {entry.tap_stream_id}",
            )

    def test_all_streams_have_table_key_properties_in_metadata(self):
        for entry in self.catalog.streams:
            mdata = singer_metadata.to_map(entry.metadata)
            self.assertIn(
                "table-key-properties",
                mdata.get((), {}),
                msg=f"table-key-properties missing for {entry.tap_stream_id}",
            )

    def test_schema_record_generation(self):
        """_generate_stream_record() should produce values conforming to schema."""
        for stream_name in self.STREAMS_TO_TEST:
            record = self._generate_stream_record(stream_name)
            self.assertIsInstance(record, dict)
            self.assertGreater(len(record), 0)
