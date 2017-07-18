import urllib.request
import urllib.parse
import urllib.error

import time

import hashlib
import hmac
import base64

import requests

_DOMAIN = 'api.kraken.com'
_API_VERSION = '0'
_BASE_URL = 'https://{}'.format(_DOMAIN)

_api_key = None
_secret_key = None
_requests_session = None


def connect(api_key, secret_key):
    """

    :param api_key:
    :param secret_key:
    :return:
    """
    global _requests_session
    global _api_key
    global _secret_key
    _api_key = api_key
    _secret_key = secret_key
    _requests_session = requests.session()


def _api_call(url_path, options, headers=None):
    if _requests_session is None:
        raise Exception('not initialized: call connect(api_key, secret_key) first')

    url = _BASE_URL + url_path

    if headers is None:
        headers = {}

    response = _requests_session.post(url, data=options, headers=headers)
    return response.json()


def api_call_public(method, options=None):
    if options is None:
        options = {}

    url_path = '/' + '/'.join([_API_VERSION, 'public', method])
    return _api_call(url_path, options)


def api_call_private(method, options=None):
    if options is None:
        options = {}

    url_path = '/' + '/'.join([_API_VERSION, 'private', method])

    nonce = int(1000 * time.time())
    options['nonce'] = nonce
    post_data = urllib.parse.urlencode(options)
    encoded = (str(nonce) + post_data).encode()
    message = url_path.encode() + hashlib.sha256(encoded).digest()
    signature = hmac.new(base64.b64decode(_secret_key), message, hashlib.sha512)

    headers = {
        'API-Key': _api_key,
        'API-Sign': base64.b64encode(signature.digest()).decode()
    }

    return _api_call(url_path, options, headers)


def get_balances():
    return api_call_private('Balance')