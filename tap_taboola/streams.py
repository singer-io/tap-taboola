
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

class Campaign(Stream):
    name = "campaigns"
    key_properties = ["id"]
    replication_keys = None
    replication_method = "FULL_TABLE"


class CampaignPerformance(Stream):
    name = "campaign_performance"
    key_properties = ["campaign_id", "date"]
    replication_keys = ["date"]
    replication_method = "INCREMENTAL"


STREAMS = {"campaigns": Campaign, "campaign_performance": CampaignPerformance}
