
import singer
from datetime import timedelta


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


class Campaign(Stream):
    name = "campaigns"
    key_properties = ["id"]
    replication_keys = "created_at"
    replication_method = "INCREMENTAL"

    def sync(self, access_token):
        from tap_taboola import sync_campaigns
        sync_campaigns(access_token, self.config['account_id'])


class CampaignPerformance(Stream):
    name = "campaign_performance"
    key_properties = ["id"]
    replication_keys = "created_at"
    replication_method = "INCREMENTAL"

    def sync(self, access_token):
        from tap_taboola import sync_campaign_performance
        sync_campaign_performance(
            self.config, self.state, access_token, self.config['account_id'])


STREAMS = {"campaigns": Campaign, "campaign_performance": CampaignPerformance}
