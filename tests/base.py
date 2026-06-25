"""
Base test class for mock integration tests, modeled on tap-3plcentral.

These tests run the real tap code against mocked API responses — no external
tap-tester dependency required.
"""
import json
import os
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch

import tap_taboola as taboola


class MockResponse:
    """Minimal requests.Response stand-in used in mock mode."""

    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


class TaboolaBaseTest:
    """Shared helpers and metadata expectations for mock integration tests."""

    default_start_date = "2023-01-01T00:00:00Z"
    PRIMARY_KEYS = "primary_keys"
    REPLICATION_METHOD = "replication_method"
    REPLICATION_KEYS = "replication_keys"
    OBEYS_START_DATE = "obeys_start_date"

    default_config = {
        "username": "test_user",
        "password": "test_password",
        "account_id": "test-account-id",
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "start_date": "2023-01-01T00:00:00Z",
    }

    MOCK_CAMPAIGNS = [
        {
            'id': '1',
            'advertiser_id': 'mock-advertiser',
            'name': 'Mock Campaign Alpha',
            'tracking_code': 'trk-alpha',
            'cpc': '0.50',
            'daily_cap': '100.0',
            'spending_limit': '1000.0',
            'spending_limit_model': 'ENTIRE',
            'country_targeting': {'type': 'INCLUDE', 'value': ['US']},
            'platform_targeting': None,
            'publisher_targeting': None,
            'start_date': '2024-01-01',
            'end_date': None,
            'approval_state': 'APPROVED',
            'is_active': True,
            'spent': '50.0',
            'status': 'RUNNING',
        },
        {
            'id': '2',
            'advertiser_id': 'mock-advertiser',
            'name': 'Mock Campaign Beta',
            'tracking_code': '',
            'cpc': '0.30',
            'daily_cap': '75.0',
            'spending_limit': '500.0',
            'spending_limit_model': 'MONTHLY',
            'country_targeting': None,
            'platform_targeting': None,
            'publisher_targeting': None,
            'start_date': None,
            'end_date': None,
            'approval_state': 'PENDING',
            'is_active': False,
            'spent': '0.0',
            'status': 'PAUSED',
        },
    ]

    MOCK_CAMPAIGN_PERFORMANCE = [
        {
            'campaign': '1',
            'impressions': '8000',
            'ctr': '0.03',
            'cpc': '0.40',
            'cpa_actions_num': '3',
            'cpa': '0.8',
            'cpm': '2.0',
            'clicks': '300',
            'currency': 'USD',
            'cpa_conversion_rate': '0.01',
            'spent': '120.0',
            'date': '2023-07-01 00:00:00.000000',
            'campaign_name': 'Mock Campaign Alpha',
            'conversions_value': '240.0',
        },
        {
            'campaign': '1',
            'impressions': '10000',
            'ctr': '0.05',
            'cpc': '0.50',
            'cpa_actions_num': '5',
            'cpa': '1.0',
            'cpm': '2.5',
            'clicks': '500',
            'currency': 'USD',
            'cpa_conversion_rate': '0.01',
            'spent': '250.0',
            'date': '2024-07-01 00:00:00.000000',
            'campaign_name': 'Mock Campaign Alpha',
            'conversions_value': '500.0',
        },
        {
            'campaign': '2',
            'impressions': '5000',
            'ctr': '0.02',
            'cpc': '0.30',
            'cpa_actions_num': '2',
            'cpa': '1.5',
            'cpm': '1.8',
            'clicks': '100',
            'currency': 'USD',
            'cpa_conversion_rate': '0.005',
            'spent': '30.0',
            'date': '2025-01-15 00:00:00.000000',
            'campaign_name': 'Mock Campaign Beta',
            'conversions_value': '36.0',
        },
    ]

    @classmethod
    def expected_metadata(cls):
        return {
            "campaigns": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: "FULL_TABLE",
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
            },
            "campaign_performance": {
                cls.PRIMARY_KEYS: {"campaign_id", "date"},
                cls.REPLICATION_METHOD: "INCREMENTAL",
                cls.REPLICATION_KEYS: {"date"},
                cls.OBEYS_START_DATE: True,
            },
        }

    @staticmethod
    def _run_discover():
        """Run do_discover() and capture the catalog from stdout."""
        captured = StringIO()
        with patch('sys.stdout', captured):
            taboola.do_discover()
        captured.seek(0)
        return json.loads(captured.read())

    @staticmethod
    def _make_selected_catalog(stream_names=None):
        """Build a catalog dict with selected=True for the given streams.
        If stream_names is None, select all streams."""
        captured = StringIO()
        with patch('sys.stdout', captured):
            taboola.do_discover()
        captured.seek(0)
        catalog = json.loads(captured.read())

        for entry in catalog.get('streams', []):
            stream_name = entry.get('tap_stream_id', entry.get('stream'))
            is_selected = stream_names is None or stream_name in stream_names

            for meta in entry.get('metadata', []):
                breadcrumb = meta.get('breadcrumb', [])
                if breadcrumb == [] or breadcrumb == ():
                    meta['metadata']['selected'] = is_selected
                elif len(breadcrumb) == 2 and breadcrumb[0] == 'properties':
                    meta['metadata']['selected'] = is_selected

        return catalog

    @classmethod
    def _mock_request(cls, campaigns=None, performance=None,
                      account_id='test-account-id'):
        """Create a mock side_effect for tap_taboola.request."""
        if campaigns is None:
            campaigns = cls.MOCK_CAMPAIGNS
        if performance is None:
            performance = cls.MOCK_CAMPAIGN_PERFORMANCE

        def mock_fn(url, access_token, params=None):
            params = params or {}
            if 'token-details' in url:
                return MockResponse({'account_id': account_id})
            if 'reports/campaign-summary' in url:
                start_date = params.get('start_date', '')
                if start_date:
                    start_str = str(start_date)[:10]
                    results = [
                        r for r in performance
                        if r['date'][:10] >= start_str
                    ]
                else:
                    results = list(performance)
                return MockResponse({'results': results})
            if '/campaigns/' in url:
                return MockResponse({'results': campaigns})
            return MockResponse({})

        return mock_fn

