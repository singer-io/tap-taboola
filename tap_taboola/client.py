import singer
import requests
import backoff

LOGGER = singer.get_logger()

BASE_URL = 'https://backstage.taboola.com'


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500, # pylint: disable=line-too-long
                      factor=2)
def request(url, access_token, params=None):
    if params is None:
        params = {}
    LOGGER.info("Making request: GET {} {}".format(url, params))

    response = requests.get(
        url,
        headers={'Authorization': 'Bearer {}'.format(access_token),
                 'Accept': 'application/json'},
        params=params)

    LOGGER.info("Got response code: {}".format(response.status_code))

    response.raise_for_status()
    return response
