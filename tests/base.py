"""Shared test helpers and mock-data generators for tap-taboola tests."""

import singer.metadata as singer_metadata

from tap_taboola.schema import get_schemas
from tap_taboola.discover import discover


class TaboolaBaseTest:
    """Base mixin providing shared metadata, config helpers, and mock-data
    generators for tap-taboola unit and integration tests."""

    DEFAULT_START_DATE = "2024-01-01"

    # Metadata key constants
    PRIMARY_KEYS = "primary_keys"
    REPLICATION_METHOD = "replication_method"
    REPLICATION_KEYS = "replication_keys"
    FULL_TABLE = "FULL_TABLE"
    INCREMENTAL = "INCREMENTAL"

    STREAMS_TO_TEST = {"campaigns", "campaign_performance"}

    @classmethod
    def expected_metadata(cls):
        """Expected stream metadata for all tap-taboola streams."""
        return {
            "campaigns": {
                cls.PRIMARY_KEYS: ["id"],
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: None,
            },
            "campaign_performance": {
                cls.PRIMARY_KEYS: ["campaign_id", "date"],
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: ["date"],
            },
        }

    @staticmethod
    def make_config(start_date=None):
        """Return a minimal tap config dict suitable for unit tests."""
        return {
            "start_date": start_date or TaboolaBaseTest.DEFAULT_START_DATE,
            "username": "test_user",
            "password": "test_pass",
            "account_id": "test-account-123",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
        }

    @staticmethod
    def make_campaign_api_record(campaign_id=1):
        """Return a raw API campaign record (as returned by Taboola API)."""
        return {
            "id": campaign_id,
            "advertiser_id": "advertiser-001",
            "name": "Test Campaign {}".format(campaign_id),
            "tracking_code": "tracking-{}".format(campaign_id),
            "cpc": 0.5,
            "daily_cap": 100.0,
            "spending_limit": 1000.0,
            "spending_limit_model": "MONTHLY",
            "country_targeting": {"type": "INCLUDE", "value": ["US", "GB"]},
            "platform_targeting": {"type": "ALL", "value": []},
            "publisher_targeting": {"type": "EXCLUDE", "value": []},
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "approval_state": "APPROVED",
            "is_active": True,
            "spent": 250.0,
            "status": "RUNNING",
        }

    @staticmethod
    def make_campaign_performance_api_record(campaign_id=1, date="2024-01-15 00:00:00.000000"):
        """Return a raw API campaign performance record (as returned by Taboola API)."""
        return {
            "campaign": str(campaign_id),
            "campaign_name": "Test Campaign {}".format(campaign_id),
            "date": date,
            "impressions": 10000,
            "clicks": 200,
            "ctr": 0.02,
            "cpc": 0.5,
            "cpa": 5.0,
            "cpa_actions_num": 10,
            "cpa_conversion_rate": 0.05,
            "cpm": 1.0,
            "spent": 100.0,
            "conversions_value": 500.0,
            "currency": "USD",
        }

    @staticmethod
    def make_token_response(token="test-access-token"):
        """Return a mock successful OAuth token response body."""
        return {"access_token": token}

    @staticmethod
    def _get_selected_stream(stream_name):
        """Return a CatalogEntry for *stream_name* with ``selected=True``."""
        catalog = discover()
        stream_entry = catalog.get_stream(stream_name)
        meta_map = singer_metadata.to_map(stream_entry.metadata)
        meta_map[()]["selected"] = True
        stream_entry.metadata = singer_metadata.to_list(meta_map)
        return stream_entry

    @staticmethod
    def _schema_type(schema):
        """Return the first non-null type from a JSON-Schema type definition."""
        prop_type = schema.get("type")
        if isinstance(prop_type, list):
            non_null = [t for t in prop_type if t != "null"]
            return non_null[0] if non_null else "null"
        return prop_type

    @classmethod
    def _generate_value(cls, schema, date_value=None):
        """Recursively generate a placeholder value that conforms to *schema*."""
        date_value = date_value or cls.DEFAULT_START_DATE
        prop_type = cls._schema_type(schema)
        fmt = schema.get("format")

        if fmt in ("date-time", "date"):
            return date_value
        if prop_type == "string":
            return "mock_value"
        if prop_type in ("integer", "number"):
            return 1
        if prop_type == "boolean":
            return True
        if prop_type == "array":
            items_schema = schema.get("items", {})
            return [cls._generate_value(items_schema, date_value)]
        if prop_type == "object":
            properties = schema.get("properties", {})
            return {k: cls._generate_value(v, date_value) for k, v in properties.items()}
        return None

    @classmethod
    def _generate_stream_record(cls, stream_name, date_value=None):
        """Return a synthetic record dict for *stream_name* built from its schema."""
        date_value = date_value or cls.DEFAULT_START_DATE
        schemas, _ = get_schemas()
        schema = schemas[stream_name]
        return cls._generate_value(schema, date_value)
