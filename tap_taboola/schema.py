# pylint: disable=invalid-name
import os
import json
from singer import metadata
from tap_taboola.streams import STREAMS


def get_abs_path(path):
    """
    Return full path for the specified file
    """

    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas():
    """
    Fetch and return metadata and schema for all the streams
    """

    schemas = {}
    field_metadata = {}

    for stream_name, stream_metadata in STREAMS.items():

        schema_path = get_abs_path("schemas/{}.json".format(stream_name))

        with open(schema_path, "r") as file:
            schema = json.load(file)
        schemas[stream_name] = schema

        mdata = metadata.new()

        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_metadata.key_properties,
            valid_replication_keys=stream_metadata.replication_keys,
            replication_method=stream_metadata.replication_method,
        )

        mdata = metadata.to_map(mdata)
        # Loop through all keys and make replication keys of automatic inclusion
        for field_name in schema["properties"].keys():

            if (
                stream_metadata.replication_keys
                and field_name in stream_metadata.replication_keys
            ):
                mdata = metadata.write(
                    mdata,
                    ("properties", field_name),
                    "inclusion",
                    "automatic",
                )

        mdata = metadata.to_list(mdata)
        field_metadata[stream_name] = mdata

    return schemas, field_metadata


campaign = {
    "type": "object",
    "properties": {
        "id": {
            "type": "integer",
            "description": "The ID of this campaign",
        },
        "advertiser_id": {
            "type": "string",
            "description": "i.e. taboola-demo-advertiser",
        },
        "name": {
            "type": "string",
            "description": "i.e. Demo Campaign",
        },
        "tracking_code": {
            "type": "string",
            "description": "i.e. taboola-track",
        },
        "cpc": {
            "type": "number",
            "description": "Cost per click for the whole campaign, i.e. 0.25",
        },
        "daily_cap": {
            "type": "number",
            "description": "i.e. 100",
        },
        "spending_limit": {
            "type": "number",
            "description": "i.e. 1000",
        },
        "spending_limit_model": {
            "type": "string",
            "description": 'i.e. "MONTHLY"',
        },
        "country_targeting": {
            "type": ["object", "null"],
            "description": (
                'Country codes to target. Type is like "INCLUDE", '
                'value is like ["AU", "GB"]'
            ),
            "properties": {
                "type": {
                    "type": "string",
                },
                "value": {
                    "type": "array",
                    "items": {
                        "type": "string",
                    },
                },
            },
        },
        "platform_targeting": {
            "type": ["object", "null"],
            "description": (
                'Platforms to target. Type is like "INCLUDE", '
                'value is like ["TBLT","PHON"].'
            ),
            "properties": {
                "type": {
                    "type": "string",
                },
                "value": {
                    "type": "array",
                    "items": {
                        "type": "string",
                    },
                },
            },
        },
        "publisher_targeting": {
            "type": ["object", "null"],
            "description": ("Publishers to target."),
            "properties": {
                "type": {
                    "type": "string",
                },
                "value": {
                    "type": "array",
                    "items": {
                        "type": "string",
                    },
                },
            },
        },
        "start_date": {
            "type": "string",
            "format": "date",
            "description": "The start date for this campaign.",
        },
        "end_date": {
            "type": "string",
            "format": "date",
            "description": "The end date for this campaign.",
        },
        "approval_state": {
            "type": "string",
            "description": 'Approval state for the campign, i.e. "APPROVED".',
        },
        "is_active": {
            "type": "boolean",
            "description": "Whether or not the campaign is active.",
        },
        "spent": {
            "type": "number",
            "description": "i.e. 2.23",
        },
        "status": {
            "type": "string",
            "description": 'i.e. "RUNNING"',
        },
    },
}

campaign_performance = {
    "type": "object",
    "properties": {
        "campaign_id": {
            "type": "integer",
        },
        "date": {
            "type": "string",
            "format": "date",
            "description": "The start date for this campaign.",
        },
        "impressions": {
            "type": "integer",
            "description": "Total number of impressions",
        },
        "campaign_name": {
            "type": ["string", "null"],
            "description": "Human-readable campaign name",
        },
        "ctr": {
            "type": "number",
            "description": "CTR, calculated as clicks/impressions",
        },
        "clicks": {
            "type": "integer",
            "description": "Total number of clicks",
        },
        "cpc": {
            "type": "number",
            "description": "CPC, calculated as spend/clicks",
        },
        "cpm": {
            "type": "number",
            "description": (
                "CPM (cost per 1000 impressions), calculated " "as spend/impressions"
            ),
        },
        "cpa_conversion_rate": {
            "type": "number",
            "description": "Conversion rate calculated as actions/clicks",
        },
        "cpa_actions_num": {
            "type": "integer",
            "description": "Total actions (a.k.a. conversions)",
        },
        "cpa": {
            "type": "number",
            "description": "CPA, calculated as spend/actions",
        },
        "spent": {
            "type": "number",
            "description": "Total spent amount",
        },
        "conversions_value": {
            "type": ["number", "null"],
            "description": "Total revenue from conversions",
        },
        "currency": {
            "type": "string",
            "description": "ISO4217 currency code for columns of type money",
        },
    },
}