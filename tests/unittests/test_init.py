"""Unit tests for tap_taboola/__init__.py"""

import json
import unittest
from unittest.mock import MagicMock, mock_open, patch

import requests

from tap_taboola import (
    generate_token,
    get_token_client_credentials_auth,
    get_token_password_auth,
    load_config,
    load_state,
    validate_config,
    verify_account_access,
)
from tap_taboola.client import request
from tap_taboola.streams import (
    parse_campaign,
    parse_campaign_performance,
    sync_campaigns,
    sync_campaign_performance,
)

from tests.base import TaboolaBaseTest


# ---------------------------------------------------------------------------
# parse_campaign_performance
# ---------------------------------------------------------------------------

class TestParseCampaignPerformance(unittest.TestCase):

    def _make_raw(self, **overrides):
        base = {
            'campaign': '42',
            'impressions': '1000',
            'ctr': '0.05',
            'cpc': '0.25',
            'cpa_actions_num': '3',
            'cpa': '1.50',
            'cpm': '5.00',
            'clicks': '50',
            'currency': 'USD',
            'cpa_conversion_rate': '0.06',
            'spent': '12.50',
            'date': '2024-01-15 00:00:00.000000',
            'campaign_name': 'Test Campaign',
            'conversions_value': '75.00',
        }
        base.update(overrides)
        return base

    def test_returns_correct_types(self):
        result = parse_campaign_performance(self._make_raw())
        self.assertIsInstance(result['campaign_id'], int)
        self.assertIsInstance(result['impressions'], int)
        self.assertIsInstance(result['ctr'], float)
        self.assertIsInstance(result['cpc'], float)
        self.assertIsInstance(result['cpa_actions_num'], int)
        self.assertIsInstance(result['cpa'], float)
        self.assertIsInstance(result['cpm'], float)
        self.assertIsInstance(result['clicks'], int)
        self.assertIsInstance(result['currency'], str)
        self.assertIsInstance(result['cpa_conversion_rate'], float)
        self.assertIsInstance(result['spent'], float)
        self.assertIsInstance(result['date'], str)
        self.assertIsInstance(result['campaign_name'], str)
        self.assertIsInstance(result['conversions_value'], float)

    def test_returns_correct_values(self):
        result = parse_campaign_performance(self._make_raw())
        self.assertEqual(result['campaign_id'], 42)
        self.assertEqual(result['impressions'], 1000)
        self.assertAlmostEqual(result['ctr'], 0.05)
        self.assertAlmostEqual(result['cpc'], 0.25)
        self.assertEqual(result['cpa_actions_num'], 3)
        self.assertAlmostEqual(result['cpa'], 1.50)
        self.assertAlmostEqual(result['cpm'], 5.00)
        self.assertEqual(result['clicks'], 50)
        self.assertEqual(result['currency'], 'USD')
        self.assertAlmostEqual(result['cpa_conversion_rate'], 0.06)
        self.assertAlmostEqual(result['spent'], 12.50)
        self.assertEqual(result['date'], '2024-01-15')
        self.assertEqual(result['campaign_name'], 'Test Campaign')
        self.assertAlmostEqual(result['conversions_value'], 75.00)

    def test_missing_numeric_fields_default_to_zero(self):
        raw = {'campaign': '99', 'date': '2024-03-01 00:00:00.000000'}
        result = parse_campaign_performance(raw)
        self.assertEqual(result['impressions'], 0)
        self.assertAlmostEqual(result['ctr'], 0.0)
        self.assertEqual(result['clicks'], 0)
        self.assertEqual(result['currency'], '')

    def test_date_is_date_only_string(self):
        result = parse_campaign_performance(self._make_raw())
        # Should be YYYY-MM-DD with no time component
        self.assertRegex(result['date'], r'^\d{4}-\d{2}-\d{2}$')

    def test_date_parsing_formats_correctly(self):
        raw = self._make_raw(date='2023-12-31 12:34:56.789000')
        result = parse_campaign_performance(raw)
        self.assertEqual(result['date'], '2023-12-31')


# ---------------------------------------------------------------------------
# parse_campaign
# ---------------------------------------------------------------------------

class TestParseCampaign(unittest.TestCase):

    def _make_raw(self, **overrides):
        base = {
            'id': '101',
            'advertiser_id': 'adv-1',
            'name': 'My Campaign',
            'tracking_code': 'trk123',
            'cpc': '0.30',
            'daily_cap': '100.0',
            'spending_limit': '500.0',
            'spending_limit_model': 'monthly',
            'country_targeting': {'type': 'INCLUDE', 'value': ['US']},
            'platform_targeting': None,
            'publisher_targeting': None,
            'start_date': '2024-01-01',
            'end_date': '2024-06-30',
            'approval_state': 'APPROVED',
            'is_active': True,
            'spent': '250.0',
            'status': 'RUNNING',
        }
        base.update(overrides)
        return base

    def test_returns_correct_types(self):
        result = parse_campaign(self._make_raw())
        self.assertIsInstance(result['id'], int)
        self.assertIsInstance(result['advertiser_id'], str)
        self.assertIsInstance(result['name'], str)
        self.assertIsInstance(result['cpc'], float)
        self.assertIsInstance(result['daily_cap'], float)
        self.assertIsInstance(result['spending_limit'], float)
        self.assertIsInstance(result['is_active'], bool)
        self.assertIsInstance(result['spent'], float)

    def test_returns_correct_values(self):
        result = parse_campaign(self._make_raw())
        self.assertEqual(result['id'], 101)
        self.assertEqual(result['advertiser_id'], 'adv-1')
        self.assertEqual(result['name'], 'My Campaign')
        self.assertEqual(result['tracking_code'], 'trk123')
        self.assertAlmostEqual(result['cpc'], 0.30)
        self.assertEqual(result['start_date'], '2024-01-01')
        self.assertEqual(result['end_date'], '2024-06-30')
        self.assertEqual(result['approval_state'], 'APPROVED')
        self.assertTrue(result['is_active'])
        self.assertEqual(result['status'], 'RUNNING')

    def test_none_start_date_defaults_to_sentinel(self):
        result = parse_campaign(self._make_raw(start_date=None))
        self.assertEqual(result['start_date'], '9999-12-31')

    def test_none_end_date_defaults_to_sentinel(self):
        result = parse_campaign(self._make_raw(end_date=None))
        self.assertEqual(result['end_date'], '9999-12-31')

    def test_missing_start_and_end_date_default_to_sentinel(self):
        raw = self._make_raw()
        del raw['start_date']
        del raw['end_date']
        result = parse_campaign(raw)
        self.assertEqual(result['start_date'], '9999-12-31')
        self.assertEqual(result['end_date'], '9999-12-31')

    def test_is_active_defaults_to_false_when_missing(self):
        raw = self._make_raw()
        del raw['is_active']
        result = parse_campaign(raw)
        self.assertFalse(result['is_active'])

    def test_numeric_defaults_when_missing(self):
        raw = {'id': '5'}
        result = parse_campaign(raw)
        self.assertAlmostEqual(result['cpc'], 0.0)
        self.assertAlmostEqual(result['daily_cap'], 0.0)
        self.assertAlmostEqual(result['spending_limit'], 0.0)
        self.assertAlmostEqual(result['spent'], 0.0)
        self.assertEqual(result['advertiser_id'], '')
        self.assertEqual(result['name'], '')
        self.assertEqual(result['status'], '')


# ---------------------------------------------------------------------------
# validate_config
# ---------------------------------------------------------------------------

class TestValidateConfig(unittest.TestCase):

    def _valid_config(self):
        return {
            'username': 'user',
            'password': 'pass',
            'account_id': 'acct',
            'client_id': 'cid',
            'client_secret': 'csecret',
            'start_date': '2020-01-01',
        }

    def test_valid_config_does_not_raise(self):
        validate_config(self._valid_config())  # should not raise

    def test_missing_key_raises_runtime_error(self):
        config = self._valid_config()
        del config['username']
        with self.assertRaises(RuntimeError):
            validate_config(config)

    def test_null_key_raises_runtime_error(self):
        config = self._valid_config()
        config['password'] = None
        with self.assertRaises(RuntimeError):
            validate_config(config)

    def test_multiple_missing_keys_raise_runtime_error(self):
        with self.assertRaises(RuntimeError):
            validate_config({})

    def test_all_required_keys_present_and_nonnull(self):
        # No exception for a fully-populated config
        config = self._valid_config()
        validate_config(config)


# ---------------------------------------------------------------------------
# load_state
# ---------------------------------------------------------------------------

class TestLoadState(unittest.TestCase):

    def test_returns_empty_dict_when_filename_is_none(self):
        result = load_state(None)
        self.assertEqual(result, {})

    def test_returns_parsed_json_from_file(self):
        state = {'start_date': '2024-01-01'}
        m = mock_open(read_data=json.dumps(state))
        with patch('builtins.open', m):
            result = load_state('state.json')
        self.assertEqual(result, state)

    def test_raises_runtime_error_on_invalid_json(self):
        m = mock_open(read_data='{not valid json}')
        with patch('builtins.open', m):
            with self.assertRaises(RuntimeError):
                load_state('bad_state.json')


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

class TestLoadConfig(unittest.TestCase):

    def _valid_config(self):
        return {
            'username': 'user',
            'password': 'pass',
            'account_id': 'acct',
            'client_id': 'cid',
            'client_secret': 'csecret',
            'start_date': '2020-01-01',
        }

    def test_returns_config_for_valid_file(self):
        m = mock_open(read_data=json.dumps(self._valid_config()))
        with patch('builtins.open', m):
            result = load_config('config.json')
        self.assertEqual(result['username'], 'user')

    def test_raises_runtime_error_on_invalid_json(self):
        m = mock_open(read_data='{not valid')
        with patch('builtins.open', m):
            with self.assertRaises(RuntimeError):
                load_config('bad_config.json')

    def test_raises_runtime_error_when_required_key_missing(self):
        config = self._valid_config()
        del config['start_date']
        m = mock_open(read_data=json.dumps(config))
        with patch('builtins.open', m):
            with self.assertRaises(RuntimeError):
                load_config('config.json')


# ---------------------------------------------------------------------------
# get_token_password_auth
# ---------------------------------------------------------------------------

class TestGetTokenPasswordAuth(unittest.TestCase):

    def _make_response(self, status_code, body):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.json.return_value = body
        return mock_resp

    @patch('tap_taboola.requests.post')
    def test_returns_token_on_200(self, mock_post):
        mock_post.return_value = self._make_response(
            200, {'access_token': 'tok123'})
        result = get_token_password_auth('cid', 'csec', 'user', 'pass')
        self.assertEqual(result, {'token': 'tok123'})

    @patch('tap_taboola.requests.post')
    def test_returns_error_on_4xx(self, mock_post):
        mock_post.return_value = self._make_response(
            401, {'error': 'unauthorized', 'error_description': 'bad creds'})
        result = get_token_password_auth('cid', 'csec', 'user', 'bad')
        self.assertEqual(result['error'], 'unauthorized')
        self.assertEqual(result['error_description'], 'bad creds')
        self.assertNotIn('token', result)

    @patch('tap_taboola.requests.post')
    def test_returns_empty_dict_on_other_status(self, mock_post):
        mock_post.return_value = self._make_response(500, {})
        result = get_token_password_auth('cid', 'csec', 'user', 'pass')
        self.assertEqual(result, {})

    @patch('tap_taboola.requests.post')
    def test_uses_password_grant_type(self, mock_post):
        mock_post.return_value = self._make_response(200, {'access_token': 'x'})
        get_token_password_auth('cid', 'csec', 'user', 'pass')
        _, kwargs = mock_post.call_args
        self.assertEqual(kwargs['params']['grant_type'], 'password')
        self.assertEqual(kwargs['params']['username'], 'user')


# ---------------------------------------------------------------------------
# get_token_client_credentials_auth
# ---------------------------------------------------------------------------

class TestGetTokenClientCredentialsAuth(unittest.TestCase):

    def _make_response(self, status_code, body):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.json.return_value = body
        return mock_resp

    @patch('tap_taboola.requests.post')
    def test_returns_token_on_200(self, mock_post):
        mock_post.return_value = self._make_response(
            200, {'access_token': 'cc_tok'})
        result = get_token_client_credentials_auth('cid', 'csec')
        self.assertEqual(result, {'token': 'cc_tok'})

    @patch('tap_taboola.requests.post')
    def test_returns_error_on_4xx(self, mock_post):
        mock_post.return_value = self._make_response(
            403, {'error': 'forbidden', 'error_description': 'no access'})
        result = get_token_client_credentials_auth('cid', 'csec')
        self.assertEqual(result['error'], 'forbidden')

    @patch('tap_taboola.requests.post')
    def test_uses_client_credentials_grant_type(self, mock_post):
        mock_post.return_value = self._make_response(200, {'access_token': 'x'})
        get_token_client_credentials_auth('cid', 'csec')
        _, kwargs = mock_post.call_args
        self.assertEqual(kwargs['params']['grant_type'], 'client_credentials')


# ---------------------------------------------------------------------------
# generate_token
# ---------------------------------------------------------------------------

class TestGenerateToken(unittest.TestCase):

    @patch('tap_taboola.get_token_client_credentials_auth')
    @patch('tap_taboola.get_token_password_auth')
    def test_returns_token_from_password_auth_when_successful(
            self, mock_pw, mock_cc):
        mock_pw.return_value = {'token': 'pw_token'}
        result = generate_token('cid', 'csec', 'user', 'pass')
        self.assertEqual(result, 'pw_token')
        mock_cc.assert_not_called()

    @patch('tap_taboola.get_token_client_credentials_auth')
    @patch('tap_taboola.get_token_password_auth')
    def test_falls_back_to_client_credentials_when_password_auth_fails(
            self, mock_pw, mock_cc):
        mock_pw.return_value = {'error': 'invalid_grant', 'error_description': 'Bad credentials'}
        mock_cc.return_value = {'token': 'cc_token'}
        result = generate_token('cid', 'csec', 'user', 'pass')
        self.assertEqual(result, 'cc_token')
        mock_cc.assert_called_once()

    @patch('tap_taboola.get_token_client_credentials_auth')
    @patch('tap_taboola.get_token_password_auth')
    def test_raises_exception_when_both_auth_methods_fail(
            self, mock_pw, mock_cc):
        mock_pw.return_value = {'error': 'e1', 'error_description': 'desc1'}
        mock_cc.return_value = {'error': 'e2', 'error_description': 'desc2'}
        with self.assertRaises(Exception) as ctx:
            generate_token('cid', 'csec', 'user', 'pass')
        self.assertIn('Unable to authenticate', str(ctx.exception))


# ---------------------------------------------------------------------------
# request (HTTP helper)
# ---------------------------------------------------------------------------

class TestRequest(unittest.TestCase):

    def _make_response(self, status_code=200):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.raise_for_status = MagicMock()
        return mock_resp

    @patch('tap_taboola.client.requests.get')
    def test_successful_request_returns_response(self, mock_get):
        mock_resp = self._make_response(200)
        mock_get.return_value = mock_resp
        result = request('http://example.com', 'token123')
        self.assertEqual(result, mock_resp)
        mock_resp.raise_for_status.assert_called_once()

    @patch('tap_taboola.client.requests.get')
    def test_passes_bearer_token_in_header(self, mock_get):
        mock_get.return_value = self._make_response()
        request('http://example.com', 'mytoken')
        _, kwargs = mock_get.call_args
        self.assertIn('Authorization', kwargs['headers'])
        self.assertEqual(kwargs['headers']['Authorization'], 'Bearer mytoken')

    @patch('tap_taboola.client.requests.get')
    def test_passes_params(self, mock_get):
        mock_get.return_value = self._make_response()
        params = {'start_date': '2024-01-01'}
        request('http://example.com', 'tok', params)
        _, kwargs = mock_get.call_args
        self.assertEqual(kwargs['params'], params)

    @patch('tap_taboola.client.requests.get')
    def test_raises_for_status_on_error(self, mock_get):
        mock_resp = self._make_response(404)
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError('404')
        mock_get.return_value = mock_resp
        with self.assertRaises(requests.exceptions.HTTPError):
            request('http://example.com', 'tok')


# ---------------------------------------------------------------------------
# verify_account_access
# ---------------------------------------------------------------------------

class TestVerifyAccountAccess(unittest.TestCase):

    @patch('tap_taboola.request')
    def test_returns_account_id_when_matching(self, mock_req):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'account_id': 'acct123'}
        mock_req.return_value = mock_resp
        result = verify_account_access('tok', 'acct123')
        self.assertEqual(result, 'acct123')

    @patch('tap_taboola.request')
    def test_returns_token_account_id_when_mismatch(self, mock_req):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'account_id': 'token_acct'}
        mock_req.return_value = mock_resp
        result = verify_account_access('tok', 'config_acct')
        # Should return the token's account_id, not the config one
        self.assertEqual(result, 'token_acct')


# ---------------------------------------------------------------------------
# sync_campaigns / sync_campaign_performance (unit-level tests)
# ---------------------------------------------------------------------------

class TestSyncCampaigns(TaboolaBaseTest, unittest.TestCase):

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_empty_campaigns_no_crash(
        self, mock_request, mock_write_record, mock_write_state,
    ):
        """sync_campaigns completes without errors when API returns empty list."""
        mock_request.side_effect = self._mock_request(campaigns=[])
        sync_campaigns('mock-token', self.default_config['account_id'])
        campaign_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaigns'
        ]
        self.assertEqual(len(campaign_records), 0)

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_write_record_called_for_each_campaign(
        self, mock_request, mock_write_record, mock_write_state,
    ):
        """write_record is called once per campaign."""
        mock_request.side_effect = self._mock_request()
        sync_campaigns('mock-token', self.default_config['account_id'])
        campaign_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaigns'
        ]
        self.assertEqual(len(campaign_records), len(self.MOCK_CAMPAIGNS))

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_campaign_parse_types(
        self, mock_request, mock_write_record, mock_write_state,
    ):
        """Campaign fields are correctly typed after parsing."""
        mock_request.side_effect = self._mock_request()
        sync_campaigns('mock-token', self.default_config['account_id'])
        record = mock_write_record.call_args_list[0][0][1]
        self.assertIsInstance(record['id'], int)
        self.assertIsInstance(record['cpc'], float)
        self.assertIsInstance(record['is_active'], bool)
        self.assertIsInstance(record['name'], str)

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_campaign_null_end_date_uses_sentinel(
        self, mock_request, mock_write_record, mock_write_state,
    ):
        """Campaigns with None end_date use '9999-12-31' sentinel."""
        mock_request.side_effect = self._mock_request()
        sync_campaigns('mock-token', self.default_config['account_id'])
        record = mock_write_record.call_args_list[0][0][1]
        self.assertEqual(record['end_date'], '9999-12-31')

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_all_campaign_fields_present(
        self, mock_request, mock_write_record, mock_write_state,
    ):
        """All expected fields are present in campaign records."""
        expected_fields = {
            'id', 'advertiser_id', 'name', 'tracking_code', 'cpc',
            'daily_cap', 'spending_limit', 'spending_limit_model',
            'country_targeting', 'platform_targeting', 'publisher_targeting',
            'start_date', 'end_date', 'approval_state', 'is_active', 'spent',
            'status',
        }
        mock_request.side_effect = self._mock_request()
        sync_campaigns('mock-token', self.default_config['account_id'])
        for call_args in mock_write_record.call_args_list:
            if call_args[0][0] == 'campaigns':
                self.assertEqual(set(call_args[0][1].keys()), expected_fields)

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_campaigns_does_not_write_state(
        self, mock_request, mock_write_record, mock_write_state,
    ):
        """sync_campaigns should not write bookmark state (full table stream)."""
        mock_request.side_effect = self._mock_request()
        sync_campaigns('mock-token', self.default_config['account_id'])
        mock_write_state.assert_not_called()


class TestSyncCampaignPerformance(TaboolaBaseTest, unittest.TestCase):

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_empty_performance_no_crash(
        self, mock_request, mock_write_record, mock_write_state,
    ):
        """sync_campaign_performance completes without errors when API returns empty list."""
        mock_request.side_effect = self._mock_request(performance=[])
        config = dict(self.default_config)
        sync_campaign_performance(config, {}, 'mock-token', config['account_id'])
        perf_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        ]
        self.assertEqual(len(perf_records), 0)

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_write_record_called_for_each_performance_row(
        self, mock_request, mock_write_record, mock_write_state,
    ):
        """write_record is called once per performance row."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)
        sync_campaign_performance(config, {}, 'mock-token', config['account_id'])
        perf_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        ]
        self.assertEqual(len(perf_records), len(self.MOCK_CAMPAIGN_PERFORMANCE))

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_performance_parse_types(
        self, mock_request, mock_write_record, mock_write_state,
    ):
        """campaign_performance fields are correctly typed after parsing."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)
        sync_campaign_performance(config, {}, 'mock-token', config['account_id'])
        record = mock_write_record.call_args_list[0][0][1]
        self.assertIsInstance(record['campaign_id'], int)
        self.assertIsInstance(record['impressions'], int)
        self.assertIsInstance(record['ctr'], float)
        self.assertIsInstance(record['clicks'], int)
        self.assertIsInstance(record['date'], str)
        self.assertRegex(record['date'], r'^\d{4}-\d{2}-\d{2}$')

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_all_performance_fields_present(
        self, mock_request, mock_write_record, mock_write_state,
    ):
        """All expected fields are present in campaign_performance records."""
        expected_fields = {
            'campaign_id', 'impressions', 'ctr', 'cpc',
            'cpa_actions_num', 'cpa', 'cpm', 'clicks', 'currency',
            'cpa_conversion_rate', 'spent', 'date', 'campaign_name',
            'conversions_value',
        }
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)
        sync_campaign_performance(config, {}, 'mock-token', config['account_id'])
        for call_args in mock_write_record.call_args_list:
            if call_args[0][0] == 'campaign_performance':
                self.assertTrue(expected_fields.issubset(set(call_args[0][1].keys())))

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_start_date_filters_records(
        self, mock_request, mock_write_record, mock_write_state,
    ):
        """Records before start_date are filtered out."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)
        config['start_date'] = '2024-06-01T00:00:00Z'
        sync_campaign_performance(config, {}, 'mock-token', config['account_id'])
        written_records = [
            c[0][1] for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        ]
        self.assertEqual(len(written_records), 2)
        for record in written_records:
            self.assertGreaterEqual(record['date'], '2024-06-01')

    @patch("tap_taboola.streams.singer.write_state")
    @patch("tap_taboola.streams.singer.write_record")
    @patch("tap_taboola.streams.request")
    def test_later_start_date_yields_fewer_records(
        self, mock_request, mock_write_record, mock_write_state,
    ):
        """A later start_date should yield fewer records."""
        mock_request.side_effect = self._mock_request()
        config = dict(self.default_config)
        config['start_date'] = '2025-01-01T00:00:00Z'
        sync_campaign_performance(config, {}, 'mock-token', config['account_id'])
        perf_records = [
            c for c in mock_write_record.call_args_list
            if c[0][0] == 'campaign_performance'
        ]
        self.assertEqual(len(perf_records), 1)


if __name__ == '__main__':
    unittest.main()
