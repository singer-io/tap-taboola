"""Unit tests for parse_campaign() and parse_campaign_performance()."""

import unittest

import tap_taboola
from tests.base import TaboolaBaseTest


class TestParseCampaign(unittest.TestCase, TaboolaBaseTest):
    """Tests for tap_taboola.parse_campaign()."""

    def _raw(self, **overrides):
        record = self.make_campaign_api_record()
        record.update(overrides)
        return record

    def test_returns_all_expected_fields(self):
        result = tap_taboola.parse_campaign(self._raw())
        expected_keys = {
            "id", "advertiser_id", "name", "tracking_code", "cpc",
            "daily_cap", "spending_limit", "spending_limit_model",
            "country_targeting", "platform_targeting", "publisher_targeting",
            "start_date", "end_date", "approval_state", "is_active",
            "spent", "status",
        }
        self.assertEqual(set(result.keys()), expected_keys)

    def test_id_is_integer(self):
        result = tap_taboola.parse_campaign(self._raw(id=42))
        self.assertIsInstance(result["id"], int)
        self.assertEqual(result["id"], 42)

    def test_cpc_is_float(self):
        result = tap_taboola.parse_campaign(self._raw(cpc="1.25"))
        self.assertIsInstance(result["cpc"], float)
        self.assertAlmostEqual(result["cpc"], 1.25)

    def test_is_active_is_bool(self):
        result = tap_taboola.parse_campaign(self._raw(is_active=True))
        self.assertIsInstance(result["is_active"], bool)
        self.assertTrue(result["is_active"])

    def test_none_start_date_defaults_to_sentinel(self):
        result = tap_taboola.parse_campaign(self._raw(start_date=None))
        self.assertEqual(result["start_date"], "9999-12-31")

    def test_none_end_date_defaults_to_sentinel(self):
        result = tap_taboola.parse_campaign(self._raw(end_date=None))
        self.assertEqual(result["end_date"], "9999-12-31")

    def test_provided_dates_are_preserved(self):
        result = tap_taboola.parse_campaign(
            self._raw(start_date="2024-06-01", end_date="2024-12-31")
        )
        self.assertEqual(result["start_date"], "2024-06-01")
        self.assertEqual(result["end_date"], "2024-12-31")

    def test_targeting_objects_passed_through(self):
        country = {"type": "INCLUDE", "value": ["US"]}
        result = tap_taboola.parse_campaign(self._raw(country_targeting=country))
        self.assertEqual(result["country_targeting"], country)


class TestParseCampaignPerformance(unittest.TestCase, TaboolaBaseTest):
    """Tests for tap_taboola.parse_campaign_performance()."""

    def _raw(self, **overrides):
        record = self.make_campaign_performance_api_record()
        record.update(overrides)
        return record

    def test_returns_all_expected_fields(self):
        result = tap_taboola.parse_campaign_performance(self._raw())
        expected_keys = {
            "campaign_id", "impressions", "ctr", "cpc",
            "cpa_actions_num", "cpa", "cpm", "clicks", "currency",
            "cpa_conversion_rate", "spent", "date", "campaign_name",
            "conversions_value",
        }
        self.assertEqual(set(result.keys()), expected_keys)

    def test_campaign_id_is_integer(self):
        result = tap_taboola.parse_campaign_performance(self._raw(campaign="99"))
        self.assertIsInstance(result["campaign_id"], int)
        self.assertEqual(result["campaign_id"], 99)

    def test_date_is_parsed_to_date_string(self):
        result = tap_taboola.parse_campaign_performance(
            self._raw(date="2024-03-15 00:00:00.000000")
        )
        self.assertEqual(result["date"], "2024-03-15")

    def test_numeric_fields_are_floats(self):
        result = tap_taboola.parse_campaign_performance(self._raw())
        for field in ("ctr", "cpc", "cpa", "cpm", "cpa_conversion_rate",
                      "spent", "conversions_value"):
            self.assertIsInstance(result[field], float, msg=f"{field} is not float")

    def test_integer_fields_are_ints(self):
        result = tap_taboola.parse_campaign_performance(self._raw())
        for field in ("impressions", "clicks", "cpa_actions_num"):
            self.assertIsInstance(result[field], int, msg=f"{field} is not int")

    def test_defaults_for_missing_numeric_fields(self):
        """Fields missing from the raw record should default to 0 / 0.0."""
        result = tap_taboola.parse_campaign_performance({"campaign": "1", "date": "2024-01-01 00:00:00.000000"})
        self.assertEqual(result["impressions"], 0)
        self.assertEqual(result["clicks"], 0)
        self.assertAlmostEqual(result["ctr"], 0.0)
        self.assertEqual(result["currency"], "")
