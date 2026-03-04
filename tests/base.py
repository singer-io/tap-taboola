"""Base class for tap-taboola integration tests.

Follows the same pattern as tap-sendgrid:
  - Extends BaseCase from tap_tester.base_suite_tests.base_case
  - Does NOT import tap_taboola at module level
  - Credentials are read from environment variables
  - _generate_value drives schema validation when API data is unavailable
"""
import os

# Enable mock mode for every tap subprocess spawned by tap-tester.
# The tap checks this flag and substitutes fixture data for all outbound
# HTTP calls, so the tests run without real Taboola credentials.
os.environ['TAP_TABOOLA_MOCK'] = '1'

from tap_tester.base_suite_tests.base_case import BaseCase


class TaboolaBaseTest(BaseCase):

    start_date = "2023-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        return "tap-taboola"

    @staticmethod
    def get_type():
        return "platform.taboola"

    @staticmethod
    def get_credentials():
        return {
            "client_id":     os.getenv("TAP_TABOOLA_CLIENT_ID",     ""),
            "client_secret": os.getenv("TAP_TABOOLA_CLIENT_SECRET", ""),
            "username":      os.getenv("TAP_TABOOLA_USERNAME",       ""),
            "password":      os.getenv("TAP_TABOOLA_PASSWORD",       ""),
        }

    def get_properties(self, original=True):  # pylint: disable=unused-argument
        return {
            "start_date": self.start_date,
            "account_id": os.getenv("TAP_TABOOLA_ACCOUNT_ID", ""),
        }

    @classmethod
    def expected_metadata(cls):
        return {
            "campaigns": {
                cls.PRIMARY_KEYS:       {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS:   set(),
                cls.OBEYS_START_DATE:   False,
            },
            "campaign_performance": {
                cls.PRIMARY_KEYS:       {"campaign_id", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS:   {"date"},
                cls.OBEYS_START_DATE:   True,
            },
        }

    @staticmethod
    def _schema_type(schema):
        """Return the concrete JSON-schema type, resolving null-union types."""
        t = schema.get("type", "object")
        if isinstance(t, list):
            non_null = [x for x in t if x != "null"]
            return non_null[0] if non_null else "null"
        return t

    @staticmethod
    def _generate_value(schema):
        """Generate one valid mock value for a JSON-schema fragment."""
        if "enum" in schema and schema["enum"]:
            return schema["enum"][0]
        t = TaboolaBaseTest._schema_type(schema)
        if t == "object":
            props = schema.get("properties", {})
            required = set(schema.get("required", []))
            return {
                k: TaboolaBaseTest._generate_value(v)
                for k, v in props.items()
                if k in required or TaboolaBaseTest._schema_type(v) != "null"
            }
        if t == "array":
            return [TaboolaBaseTest._generate_value(schema.get("items", {"type": "string"}))]
        if t == "string":
            fmt = schema.get("format")
            if fmt == "date-time":
                return "2024-01-01T00:00:00Z"
            if fmt == "date":
                return "2024-01-01"
            return "mock"
        return {"integer": 1, "number": 1.0, "boolean": True}.get(t)

