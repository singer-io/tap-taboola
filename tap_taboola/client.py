import singer
import requests
import backoff

LOGGER = singer.get_logger()

BASE_URL = 'https://backstage.taboola.com'


def _giveup_on_client_error(exc):
    """Give up retrying on 4xx errors except 429 (rate limit)."""
    return (exc.response is not None
            and 400 <= exc.response.status_code < 500
            and exc.response.status_code != 429)


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=_giveup_on_client_error,
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
        result = {k: response.json().get(k) for k in ('error', 'error_description')}

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
        result = {k: response.json().get(k) for k in ('error', 'error_description')}

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
