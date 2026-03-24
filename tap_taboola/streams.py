
import datetime
import singer
from datetime import timedelta
from singer import utils

from tap_taboola.client import request, BASE_URL


DEFAULT_TRAILING_DAYS  = timedelta(days=30)
DEFAULT_TIMESTAMP = "2005-01-01T00:00:00Z"
LOGGER = singer.get_logger()


class Stream:
    name = None
    replication_method = None
    replication_keys = None
    key_properties = None
    parent_stream = None

    def __init__(self, config, state, catalog_entry):
        self.config = config
        self.state = state
        self.catalog_entry = catalog_entry

    @classmethod
    def matches_catalog(cls, catalog_entry):
        return catalog_entry.tap_stream_id == cls.name

    def write_schema(self):
        schema = self.catalog_entry.schema.to_dict()
        singer.write_schema(self.name, schema, self.key_properties)

    def sync(self, access_token):
        raise NotImplementedError(
            "sync() not implemented for {}".format(self.__class__.__name__))


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


def sync_campaigns(access_token, account_id):
    campaigns = fetch_campaigns(access_token, account_id)
    time_extracted = utils.now()

    LOGGER.info('Synced {} campaigns.'.format(len(campaigns)))

    for record in campaigns:
        parsed_campaigns = parse_campaign(record)

        singer.write_record('campaigns',
                            parsed_campaigns,
                            time_extracted=time_extracted)

    LOGGER.info("Done syncing campaigns.")


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

    params = {
        'start_date': state.get('start_date', config.get('start_date')),
        'end_date': datetime.date.today(),
    }

    campaign_performance = request(url, access_token, params)
    return campaign_performance.json().get('results')


def sync_campaign_performance(config, state, access_token, account_id):
    performance = fetch_campaign_performance(config, state, access_token,
                                             account_id)

    time_extracted = utils.now()

    LOGGER.info("Got {} campaign performance records."
                .format(len(performance)))

    max_date = None
    for record in performance:
        parsed_performance = parse_campaign_performance(record)

        singer.write_record('campaign_performance',
                            parsed_performance,
                            time_extracted=time_extracted)

        record_date = parsed_performance.get('date')
        if record_date and (max_date is None or record_date > max_date):
            max_date = record_date

    if max_date:
        state['start_date'] = max_date
        singer.write_state(state)

    LOGGER.info("Done syncing campaign_performance.")


class Campaign(Stream):
    name = "campaigns"
    key_properties = ["id"]
    replication_keys = "created_at"
    replication_method = "INCREMENTAL"

    def sync(self, access_token):
        sync_campaigns(access_token, self.config['account_id'])


class CampaignPerformance(Stream):
    name = "campaign_performance"
    key_properties = ["id"]
    replication_keys = "created_at"
    replication_method = "INCREMENTAL"

    def sync(self, access_token):
        sync_campaign_performance(
            self.config, self.state, access_token, self.config['account_id'])


STREAMS = {"campaigns": Campaign, "campaign_performance": CampaignPerformance}
