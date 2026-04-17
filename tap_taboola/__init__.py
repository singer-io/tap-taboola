#!/usr/bin/env python3

import argparse
import json
import sys
import singer

from tap_taboola.client import generate_token, verify_account_access
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
    except json.JSONDecodeError as ex:
        raise Exception("Failed to decode config file. Is it valid json?") from ex
    except IOError as ex:
        raise Exception("Failed to open config file: {}".format(filename)) from ex

    validate_config(config)

    return config


def load_state(filename):
    if filename is None:
        return {}

    try:
        with open(filename) as state_file:
            return json.load(state_file)
    except json.JSONDecodeError as ex:
        raise Exception("Failed to decode state file. Is it valid json?") from ex
    except IOError as ex:
        raise Exception("Failed to open state file: {}".format(filename)) from ex


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

        StreamClass = STREAMS.get(entry.tap_stream_id)
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
