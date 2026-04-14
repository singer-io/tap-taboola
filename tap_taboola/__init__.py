#!/usr/bin/env python3

import argparse
import json
import sys
import singer
import requests

from tap_taboola.client import request, BASE_URL
from tap_taboola.streams import STREAMS
from tap_taboola.discover import discover

LOGGER = singer.get_logger()


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


def get_token_password_auth(client_id, client_secret, username, password):
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


def verify_account_access(access_token, account_id):
    url = '{}/backstage/api/1.0/token-details/'.format(BASE_URL)

    result = request(url, access_token)

    token_account_id = result.json().get('account_id')
    if token_account_id != account_id:
        LOGGER.warning(("The provided `account_id` ({}) doesn't match the "
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
    except Exception:
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
    except Exception:
        LOGGER.fatal("Failed to decode state file. Is it valid json?")
        raise RuntimeError


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


    for entry in catalog.streams:
        if not is_selected(entry):
            continue

        matched = False
        for StreamClass in STREAMS.values():
            if StreamClass.matches_catalog(entry):
                matched = True
                stream = StreamClass(config, state, entry)
                stream.write_schema()
                stream.sync(access_token)
        if not matched:
            LOGGER.warning("Selected stream '{}' has no matching stream implementation."
                           .format(entry.tap_stream_id))


def main_impl():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')
    parser.add_argument('-d', '--discover', help='Discovery mode', action='store_true')
    parser.add_argument( "--catalog", help="catalog mode")
    args = parser.parse_args()

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