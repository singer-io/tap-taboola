
import singer
from datetime import timedelta


TRAILING_DAYS = timedelta(days=30)
DEFAULT_TIMESTAMP = "2005-01-01T00:00:00Z"
LOGGER = singer.get_logger()


class Stream:
    name = None
    replication_method = None
    replication_keys = None
    key_properties = None
    parent_stream = None

    # To write schema in output
    def write_schema(self, schema, stream_name, sync_streams, selected_streams):
        """
        To write schema in output
        """
        try:
            # Write_schema for the stream if it is selected in catalog
            if stream_name in selected_streams and stream_name in sync_streams:
                singer.write_schema(stream_name, schema, self.key_properties)
        except OSError as err:
            LOGGER.error("OS Error writing schema for: {}".format(stream_name))
            raise err

class Campaign(Stream):
    name = "campaign"
    key_properties = ["id"]
    replication_keys = "created_at"
    replication_method = "INCREMENTAL"


class CampaignPerformance(Stream):
    name = "Campaign Performance"
    key_properties = ["id"]
    replication_keys = "created_at"
    replication_method = "INCREMENTAL"


STREAMS = {"campaigns": Campaign, "campaign_performance": CampaignPerformance}
