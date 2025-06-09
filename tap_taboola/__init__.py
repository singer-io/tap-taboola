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

BASE_URL = "https://backstage.taboola.com"


def do_discover():

    LOGGER.info("Starting discovery")
    catalog = discover()
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")




def is_selected(stream_catalog):
    metadata = singer.metadata.to_map(stream_catalog.metadata)
    stream_metadata = metadata.get((), {})

    inclusion = stream_metadata.get('inclusion')

    if stream_metadata.get('selected') is not None:
        selected = stream_metadata.get('selected')
    else:
        selected = stream_metadata.get('selected-by-default')

    if inclusion == 'unsupported':
        return False

    elif selected is not None:
        return selected

    return inclusion == 'automatic'



def get_streams_to_replicate(config, state, catalog):
    streams = []
    campaign_substreams = []
    list_substreams = []

    if not catalog:
        return streams, campaign_substreams, list_substreams

    for stream_catalog in catalog.streams:
        if not is_selected(stream_catalog):
            LOGGER.info("'{}' is not marked selected, skipping."
                        .format(stream_catalog.stream))
            continue

        for available_stream in STREAMS:
            if available_stream.matches_catalog(stream_catalog):
                if not available_stream.requirements_met(catalog):
                    raise RuntimeError(
                        "{} requires that that the following are selected: {}"
                        .format(stream_catalog.stream,
                                ','.join(available_stream.REQUIRES)))

                to_add = available_stream(
                    config, state, stream_catalog)

                if stream_catalog.stream in ['campaigns', 'campaign_performance']:
                    # the others will be triggered by these streams
                    streams.append(to_add)

                elif stream_catalog.stream.startswith('campaigns'):
                    campaign_substreams.append(to_add)
                    to_add.write_schema()



    return streams, campaign_substreams, list_substreams


@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException),
    max_tries=5,
    giveup=lambda e: e.response is not None
    and 400 <= e.response.status_code < 500,  # pylint: disable=line-too-long
    factor=2,
)
def request(url, access_token, params={}):
    LOGGER.info("Making request: GET {} {}".format(url, params))

    try:
        response = requests.get(
            url,
            headers={
                "Authorization": "Bearer {}".format(access_token),
                "Accept": "application/json",
            },
            params=params,
        )
    except Exception as exception:
        LOGGER.exception(exception)

    LOGGER.info("Got response code: {}".format(response.status_code))

    response.raise_for_status()
    return response


def get_token_password_auth(client_id, client_secret, username, password):
    url = "{}/backstage/oauth/token".format(BASE_URL)
    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password,
        "grant_type": "password",
    }

    response = requests.post(
        url,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        params=params,
    )

    LOGGER.info("Got response code: {}".format(response.status_code))

    result = {}
    if response.status_code == 200:
        LOGGER.info("Got an access token.")
        result = {"token": response.json().get("access_token", None)}
    elif response.status_code >= 400 and response.status_code < 500:
        result = {k: response.json().get(k) for k in ("error", "error_description")}

    return result


def get_token_client_credentials_auth(client_id, client_secret):
    url = "{}/backstage/oauth/token".format(BASE_URL)
    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }

    response = requests.post(
        url,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        params=params,
    )

    LOGGER.info("Got response code: {}".format(response.status_code))

    result = {}
    if response.status_code == 200:
        LOGGER.info("Got an access token.")
        result = {"token": response.json().get("access_token", None)}
    elif response.status_code >= 400 and response.status_code < 500:
        result = {k: response.json().get(k) for k in ("error", "error_description")}

    return result


def generate_token(client_id, client_secret, username, password):
    LOGGER.info("Generating new token with password auth")
    token_result = get_token_password_auth(client_id, client_secret, username, password)

    if "token" not in token_result:
        LOGGER.info("Retrying with client credentials authentication.")
        token_result = get_token_client_credentials_auth(client_id, client_secret)

    token = token_result.get("token")
    if token is None:
        raise Exception(
            "Unable to authenticate, response from Taboola - {}: {}".format(
                token_result.get("error"), token_result.get("error_description")
            )
        )

    return token


def parse_campaign_performance(campaign_performance):
    return {
        "campaign_id": int(campaign_performance.get("campaign")),
        "impressions": int(campaign_performance.get("impressions", 0)),
        "ctr": float(campaign_performance.get("ctr", 0.0)),
        "cpc": float(campaign_performance.get("cpc", 0.0)),
        "cpa_actions_num": int(campaign_performance.get("cpa_actions_num", 0)),
        "cpa": float(campaign_performance.get("cpa", 0.0)),
        "cpm": float(campaign_performance.get("cpm", 0.0)),
        "clicks": int(campaign_performance.get("clicks", 0)),
        "currency": str(campaign_performance.get("currency", "")),
        "cpa_conversion_rate": float(
            campaign_performance.get("cpa_conversion_rate", 0.0)
        ),
        "spent": float(campaign_performance.get("spent", 0.0)),
        "date": str(
            datetime.datetime.strptime(
                campaign_performance.get("date"), "%Y-%m-%d %H:%M:%S.%f"
            ).date()
        ),
        "campaign_name": str(campaign_performance.get("campaign_name", "")),
        "conversions_value": float(campaign_performance.get("conversions_value", 0.0)),
    }


def fetch_campaign_performance(config, state, access_token, account_id):
    url = "{}/backstage/api/1.0/{}/reports/campaign-summary/dimensions/campaign_day_breakdown".format(  # pylint: disable=line-too-long
        BASE_URL, account_id
    )

    params = {
        "start_date": state.get("start_date", config.get("start_date")),
        "end_date": datetime.date.today(),
    }

    campaign_performance = request(url, access_token, params)
    return campaign_performance.json().get("results")


def sync_campaign_performance(config, state, access_token, account_id):
    performance = fetch_campaign_performance(config, state, access_token, account_id)

    time_extracted = utils.now()

    LOGGER.info("Got {} campaign performance records.".format(len(performance)))

    for record in performance:
        parsed_performance = parse_campaign_performance(record)

        singer.write_record(
            "campaign_performance", parsed_performance, time_extracted=time_extracted
        )

    LOGGER.info("Done syncing campaign_performance.")


def parse_campaign(campaign):
    start_date = campaign.get("start_date")
    end_date = campaign.get("end_date")

    return {
        "id": int(campaign.get("id")),
        "advertiser_id": str(campaign.get("advertiser_id", "")),
        "name": str(campaign.get("name", "")),
        "tracking_code": str(campaign.get("tracking_code", "")),
        "cpc": float(campaign.get("cpc", 0.0)),
        "daily_cap": float(campaign.get("daily_cap", 0.0)),
        "spending_limit": float(campaign.get("spending_limit", 0.0)),
        "spending_limit_model": str(campaign.get("spending_limit_model", "")),
        "country_targeting": campaign.get("country_targeting"),
        "platform_targeting": campaign.get("platform_targeting"),
        "publisher_targeting": campaign.get("publisher_targeting"),
        "start_date": str("9999-12-31" if start_date is None else start_date),
        "end_date": str("9999-12-31" if end_date is None else end_date),
        "approval_state": str(campaign.get("approval_state", "")),
        "is_active": bool(campaign.get("is_active", False)),
        "spent": float(campaign.get("spent", 0.0)),
        "status": str(campaign.get("status", "")),
    }


def fetch_campaigns(access_token, account_id):
    url = "{}/backstage/api/1.0/{}/campaigns/".format(BASE_URL, account_id)

    response = request(url, access_token)
    return response.json().get("results")


def sync_campaigns(access_token, account_id):
    campaigns = fetch_campaigns(access_token, account_id)
    time_extracted = utils.now()

    LOGGER.info("Synced {} campaigns.".format(len(campaigns)))

    for record in campaigns:
        parsed_campaigns = parse_campaign(record)

        singer.write_record(
            "campaigns", parsed_campaigns, time_extracted=time_extracted
        )

    LOGGER.info("Done syncing campaigns.")


def verify_account_access(access_token, account_id):
    url = "{}/backstage/api/1.0/token-details/".format(BASE_URL)

    result = request(url, access_token)

    token_account_id = result.json().get("account_id")
    if token_account_id != account_id:
        LOGGER.warn(
            (
                "The provided `account_id` ({}) doesn't match the "
                "`account_id` of the token issued ({})"
            ).format(account_id, token_account_id)
        )
        return token_account_id

    LOGGER.info("Verified account access via token details endpoint.")
    return account_id




def load_config(filename):
    config = {}

    try:
        with open(filename) as config_file:

            config = json.load(config_file)
    except:
        LOGGER.fatal("Failed to decode config file. Is it valid json?")
        raise RuntimeError


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

def get_streams_to_replicate(config, state, catalog):
    streams = []
    campaign_substreams = []
    list_substreams = []

    if not catalog:
        return streams, campaign_substreams, list_substreams

    for stream_catalog in catalog.streams:
        if not is_selected(stream_catalog):
            LOGGER.info("'{}' is not marked selected, skipping."
                        .format(stream_catalog.stream))
            continue

        for available_stream in STREAMS:
            if available_stream.matches_catalog(stream_catalog):
                if not available_stream.requirements_met(catalog):
                    raise RuntimeError(
                        "{} requires that that the following are selected: {}"
                        .format(stream_catalog.stream,
                                ','.join(available_stream.REQUIRES)))

                to_add = available_stream(
                    config, state, stream_catalog)

                if stream_catalog.stream in ['campaigns', 'campaign_performance']:
                    # the others will be triggered by these streams
                    streams.append(to_add)

                elif stream_catalog.stream.startswith('campaign_'):
                    campaign_substreams.append(to_add)
                    to_add.write_schema()

                elif stream_catalog.stream.startswith('campaign_performance'):
                    list_substreams.append(to_add)
                    to_add.write_schema()

    return streams, campaign_substreams, list_substreams


def do_sync(args):
    LOGGER.info("Starting sync.")

    config = load_config(args.config)
    state = load_state(args.state)

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

    get_streams_to_replicate(
            args.config, state, args.catalog)

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

    parser.add_argument("-c", "--config", help="Config file", required=True)
    parser.add_argument("-s", "--state", help="State file")
    parser.add_argument("-d", "--discover", help="Discovery mode", action="store_true")
    parser.add_argument("-p", "--catalog", help="catalog mode")
    args = parser.parse_args()

    try:

        if args.discover:
            do_discover()
        else:
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
