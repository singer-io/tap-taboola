#!/usr/bin/env python3

from decimal import Decimal

import argparse
import datetime
import json
import sys
import singer
from singer import utils
from singer import metadata
import requests

import backoff

import tap_taboola.schema as schemas
from tap_taboola.streams import STREAMS
from tap_taboola.discover import discover

LOGGER = singer.get_logger()

BASE_URL = 'https://backstage.taboola.com'

# ---------------------------------------------------------------------------
# Mock mode – activated by setting TAP_TABOOLA_MOCK=1 in the environment.
# When enabled every outbound HTTP call is replaced with in-process fixture
# data so the tap can be exercised by tap-tester without real credentials.
# ---------------------------------------------------------------------------
_MOCK_MODE = os.environ.get('TAP_TABOOLA_MOCK', '').lower() in ('1', 'true', 'yes')

_MOCK_CAMPAIGNS = [
    {
        'id': '1',
        'advertiser_id': 'mock-advertiser',
        'name': 'Mock Campaign',
        'tracking_code': '',
        'cpc': '0.50',
        'daily_cap': '100.0',
        'spending_limit': '1000.0',
        'spending_limit_model': 'ENTIRE',
        'country_targeting': None,
        'platform_targeting': None,
        'publisher_targeting': None,
        'start_date': '2024-01-01',
        'end_date': None,
        'approval_state': 'APPROVED',
        'is_active': True,
        'spent': '50.0',
        'status': 'RUNNING',
    }
]

_MOCK_CAMPAIGN_PERFORMANCE = [
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
        'date': '2024-07-01 00:00:00.000000',
        'campaign_name': 'Mock Campaign',
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
        'date': '2025-01-01 00:00:00.000000',
        'campaign_name': 'Mock Campaign',
        'conversions_value': '500.0',
    },
]


class _MockResponse:
    """Minimal requests.Response stand-in used in mock mode."""

    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def _mock_request(url, params=None):
    """Return a _MockResponse whose payload depends on the requested URL."""
    if 'token-details' in url:
        return _MockResponse({'account_id': 'mock-account'})
    if 'reports/campaign-summary' in url:
        start_date = (params or {}).get('start_date', '')
        if start_date:
            # Normalise to YYYY-MM-DD for comparison with the record date stub.
            start_date_str = str(start_date)[:10]
            results = [
                r for r in _MOCK_CAMPAIGN_PERFORMANCE
                if r['date'][:10] >= start_date_str
            ]
        else:
            results = _MOCK_CAMPAIGN_PERFORMANCE
        return _MockResponse({'results': results})
    if '/campaigns/' in url:
        return _MockResponse({'results': _MOCK_CAMPAIGNS})
    return _MockResponse({})


def do_discover():

    LOGGER.info("Starting discovery")
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def is_selected(stream_catalog):
    metadata = singer.metadata.to_map(stream_catalog.metadata)
    stream_metadata = metadata.get((), {})

    inclusion = stream_metadata.get("inclusion")

    selected = stream_metadata.get("selected")

    if inclusion == "unsupported":
        return False

    elif selected is not None:
        return selected

    return inclusion == "automatic"


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500, # pylint: disable=line-too-long
                      factor=2)
def request(url, access_token, params={}):
    if _MOCK_MODE:
        LOGGER.info("[mock] GET %s", url)
        return _mock_request(url, params)

    LOGGER.info("Making request: GET {} {}".format(url, params))

    try:
        response = requests.get(
            url,
            headers={'Authorization': 'Bearer {}'.format(access_token),
                     'Accept': 'application/json'},
            params=params)
    except Exception as exception:
        LOGGER.exception(exception)

    LOGGER.info("Got response code: {}".format(response.status_code))

    response.raise_for_status()
    return response

def get_token_password_auth(client_id, client_secret, username, password):
    if _MOCK_MODE:
        LOGGER.info("[mock] password auth – returning mock token")
        return {'token': 'mock-access-token'}

    url = '{}/backstage/oauth/token'.format(BASE_URL)
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'username': username,
        'password': password,
        'grant_type': 'password',
    }

    response = requests.post(
        url,
        headers={'Content-Type': 'application/x-www-form-urlencoded',
                 'Accept': 'application/json'},
        params=params)

    LOGGER.info("Got response code: {}".format(response.status_code))

    result = {}
    if response.status_code == 200:
        LOGGER.info("Got an access token.")
        result = {"token": response.json().get('access_token', None)}
    elif response.status_code >= 400 and response.status_code < 500:
        result = {k: response.json().get(k) for k in ('error','error_description')}

    return result

def get_token_client_credentials_auth(client_id, client_secret):
    if _MOCK_MODE:
        LOGGER.info("[mock] client-credentials auth – returning mock token")
        return {'token': 'mock-access-token'}

    url = '{}/backstage/oauth/token'.format(BASE_URL)
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }

    response = requests.post(
        url,
        headers={'Content-Type': 'application/x-www-form-urlencoded',
                 'Accept': 'application/json'},
        params=params)

    LOGGER.info("Got response code: {}".format(response.status_code))

    result = {}
    if response.status_code == 200:
        LOGGER.info("Got an access token.")
        result = {"token": response.json().get('access_token', None)}
    elif response.status_code >= 400 and response.status_code < 500:
        result = {k: response.json().get(k) for k in ('error','error_description')}

    return result


def generate_token(client_id, client_secret, username, password):
    LOGGER.info("Generating new token with password auth")
    token_result = get_token_password_auth(client_id, client_secret, username, password)
    if 'token' not in token_result:
        LOGGER.info("Retrying with client credentials authentication.")
        token_result = get_token_client_credentials_auth(client_id, client_secret)

    token = token_result.get('token')
    if token is None:
        raise Exception('Unable to authenticate, response from Taboola - {}: {}'
                        .format(token_result.get('error'),
                                token_result.get('error_description')))

    return token

def parse_campaign_performance(campaign_performance):
    return {
        'campaign_id': int(campaign_performance.get('campaign')),
        'impressions': int(campaign_performance.get('impressions', 0)),
        'ctr': float(campaign_performance.get('ctr', 0.0)),
        'cpc': float(campaign_performance.get('cpc', 0.0)),
        'cpa_actions_num': int(campaign_performance.get('cpa_actions_num', 0)),
        'cpa': float(campaign_performance.get('cpa', 0.0)),
        'cpm': float(campaign_performance.get('cpm', 0.0)),
        'clicks': int(campaign_performance.get('clicks', 0)),
        'currency': str(campaign_performance.get('currency', '')),
        'cpa_conversion_rate': float(campaign_performance.get(
            'cpa_conversion_rate', 0.0)),
        'spent': float(campaign_performance.get('spent', 0.0)),
        'date': str(datetime.datetime.strptime(
            campaign_performance.get('date'),
            '%Y-%m-%d %H:%M:%S.%f'
        ).date()),
        'campaign_name': str(campaign_performance.get('campaign_name', '')),
        'conversions_value': float(campaign_performance.get('conversions_value', 0.0)),
    }

def fetch_campaign_performance(config, state, access_token, account_id):
    url = ('{}/backstage/api/1.0/{}/reports/campaign-summary/dimensions/campaign_day_breakdown' #pylint: disable=line-too-long
           .format(BASE_URL, account_id))

    # Prefer the saved bookmark date; fall back to config start_date.
    bookmark_date = state.get('bookmarks', {}).get('campaign_performance', {}).get('date')
    start_date = bookmark_date or state.get('start_date', config.get('start_date'))

    params = {
        'start_date': start_date,
        'end_date': datetime.date.today(),
    }

    campaign_performance = request(url, access_token, params)
    return campaign_performance.json().get('results')


def sync_campaign_performance(config, state, access_token, account_id,
                              selected_fields=None):
    performance = fetch_campaign_performance(config, state, access_token,
                                             account_id)

    time_extracted = utils.now()

    LOGGER.info("Got {} campaign performance records."
                .format(len(performance)))

    max_date = state.get('start_date', config.get('start_date', ''))
    for record in performance:
        parsed_performance = parse_campaign_performance(record)

        # Track max date before any field filtering so the bookmark is always updated.
        record_date = parsed_performance.get('date', '')
        if record_date and record_date > max_date:
            max_date = record_date

        if selected_fields is not None:
            parsed_performance = {k: v for k, v in parsed_performance.items()
                                  if k in selected_fields}

        singer.write_record('campaign_performance',
                            parsed_performance,
                            time_extracted=time_extracted)

    if max_date:
        singer.write_state({'bookmarks': {'campaign_performance': {'date': max_date}}})

    LOGGER.info("Done syncing campaign_performance.")


def parse_campaign(campaign):
    start_date = campaign.get('start_date')
    end_date = campaign.get('end_date')

    return {
        'id': int(campaign.get('id')),
        'advertiser_id': str(campaign.get('advertiser_id', '')),
        'name': str(campaign.get('name', '')),
        'tracking_code': str(campaign.get('tracking_code', '')),
        'cpc': float(campaign.get('cpc', 0.0)),
        'daily_cap': float(campaign.get('daily_cap', 0.0)),
        'spending_limit': float(campaign.get('spending_limit', 0.0)),
        'spending_limit_model': str(campaign.get('spending_limit_model', '')),
        'country_targeting': campaign.get('country_targeting'),
        'platform_targeting': campaign.get('platform_targeting'),
        'publisher_targeting': campaign.get('publisher_targeting'),
        'start_date': str('9999-12-31' if start_date is None else start_date),
        'end_date': str('9999-12-31' if end_date is None else end_date),
        'approval_state': str(campaign.get('approval_state', '')),
        'is_active': bool(campaign.get('is_active', False)),
        'spent': float(campaign.get('spent', 0.0)),
        'status': str(campaign.get('status', '')),
    }

def fetch_campaigns(access_token, account_id):
    url = '{}/backstage/api/1.0/{}/campaigns/'.format(BASE_URL, account_id)

    response = request(url, access_token)
    return response.json().get('results')


def sync_campaigns(access_token, account_id, selected_fields=None):
    campaigns = fetch_campaigns(access_token, account_id)
    time_extracted = utils.now()

    LOGGER.info('Synced {} campaigns.'.format(len(campaigns)))

    for record in campaigns:
        parsed_campaigns = parse_campaign(record)
        if selected_fields is not None:
            parsed_campaigns = {k: v for k, v in parsed_campaigns.items()
                                if k in selected_fields}

        singer.write_record('campaigns',
                            parsed_campaigns,
                            time_extracted=time_extracted)

    LOGGER.info("Done syncing campaigns.")


def verify_account_access(access_token, account_id):
    url = '{}/backstage/api/1.0/token-details/'.format(BASE_URL)

    result = request(url, access_token)

    token_account_id = result.json().get('account_id')
    if token_account_id != account_id:
        LOGGER.warn(("The provided `account_id` ({}) doesn't match the "
                     "`account_id` of the token issued ({})").format(account_id, token_account_id))
        return token_account_id

    LOGGER.info("Verified account access via token details endpoint.")
    return account_id

def validate_config(config):
    required_keys = ['username', 'password', 'account_id',
                     'client_id', 'client_secret', 'start_date']
    missing_keys = []
    null_keys = []
    has_errors = False

    for required_key in required_keys:
        if required_key not in config:
            missing_keys.append(required_key)

        elif config.get(required_key) is None:
            null_keys.append(required_key)

    if missing_keys:
        LOGGER.fatal("Config is missing keys: {}"
                     .format(", ".join(missing_keys)))
        has_errors = True

    if null_keys:
        LOGGER.fatal("Config has null keys: {}"
                     .format(", ".join(null_keys)))
        has_errors = True

    if has_errors:
        raise RuntimeError


def load_config(filename):
    config = {}

    try:
        with open(filename) as config_file:
            config = json.load(config_file)
    except:
        LOGGER.fatal("Failed to decode config file. Is it valid json?")
        raise RuntimeError

    validate_config(config)

    return config


def load_state(filename):
    if filename is None:
        return {}

    try:
        with open(filename) as state_file:
            return json.load(state_file)
    except:
        LOGGER.fatal("Failed to decode state file. Is it valid json?")
        raise RuntimeError


def _build_catalog_entry(stream_name, schema, key_properties,
                         valid_replication_keys=None,
                         replication_method="FULL_TABLE"):
    """Return a Singer catalog entry with inclusion metadata."""
    metadata = [
        {
            "breadcrumb": [],
            "metadata": {
                "table-key-properties": key_properties,
                "forced-replication-method": replication_method,
                "valid-replication-keys": valid_replication_keys or [],
            }
        }
    ]
    for prop in schema.get("properties", {}).keys():
        inclusion = "automatic" if prop in key_properties else "available"
        metadata.append({
            "breadcrumb": ["properties", prop],
            "metadata": {"inclusion": inclusion}
        })
    return {
        "stream": stream_name,
        "tap_stream_id": stream_name,
        "schema": schema,
        "metadata": metadata,
        "key_properties": key_properties,
    }


def do_discover():
    """Write a Singer catalog to stdout and exit."""
    catalog = {
        "streams": [
            _build_catalog_entry("campaigns", schemas.campaign, ["id"],
                                 valid_replication_keys=[]),
            _build_catalog_entry("campaign_performance",
                                 schemas.campaign_performance,
                                 ["campaign_id", "date"],
                                 valid_replication_keys=["date"],
                                 replication_method="INCREMENTAL"),
        ]
    }
    json.dump(catalog, sys.stdout, indent=2)
    sys.stdout.write("\n")
    sys.stdout.flush()


def _get_selected_fields(catalog, stream_name):
    """Return the set of field names selected in the catalog for *stream_name*.

    A field is included when its breadcrumb-level metadata has
    ``selected: true`` OR its inclusion is ``automatic`` (primary keys are
    always emitted regardless of explicit selection).  Returns ``None`` when
    no catalog is provided so callers know to emit all fields.
    """
    if catalog is None:
        return None
    for entry in catalog.get('streams', []):
        if entry.get('stream') != stream_name and \
                entry.get('tap_stream_id') != stream_name:
            continue
        selected = set()
        for meta in entry.get('metadata', []):
            breadcrumb = meta.get('breadcrumb', [])
            if len(breadcrumb) == 2 and breadcrumb[0] == 'properties':
                field_meta = meta.get('metadata', {})
                if field_meta.get('inclusion') == 'automatic' or \
                        field_meta.get('selected') is True:
                    selected.add(breadcrumb[1])
        return selected or None
    return None


def _load_catalog(catalog_path):
    """Load a catalog JSON file; return None if no path given."""
    if not catalog_path:
        return None
    try:
        with open(catalog_path) as f:
            return json.load(f)
    except Exception:  # pylint: disable=broad-except
        LOGGER.warning("Could not load catalog from %s", catalog_path)
        return None


def do_sync(args):
    LOGGER.info("Starting sync.")

    config = load_config(args.config)
    state = load_state(args.state)
    catalog = _load_catalog(getattr(args, 'catalog', None))

    # Load catalog
    try:
        with open(args.catalog) as f:
            raw_catalog = json.load(f)
    except Exception as e:
        LOGGER.fatal("Failed to load catalog: {}".format(e))
        raise

    catalog = singer.catalog.Catalog.from_dict(raw_catalog)

    access_token = generate_token(
        client_id=config.get("client_id"),
        client_secret=config.get("client_secret"),
        username=config.get("username"),
        password=config.get("password"),
    )

    config["account_id"] = verify_account_access(access_token, config["account_id"])


    for entry in catalog.streams:
        if not is_selected(entry):
            continue

        for StreamClass in STREAMS:
            if StreamClass.matches_catalog(entry):
                stream = StreamClass(config, state, entry)
                stream.write_schema()
                stream.sync(access_token)


def main_impl():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')
    parser.add_argument('-d', '--discover', help='Discovery mode', action='store_true')
    parser.add_argument( "--catalog", help="catalog mode")
    args = parser.parse_args()

    if args.discover:
        do_discover()
        return

    try:

        if args.discover:
            do_discover()
        elif args.catalog:
            do_sync(args)
    except RuntimeError:
        LOGGER.fatal("Run failed.")
        exit(1)

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc



if __name__ == '__main__':
    main()
