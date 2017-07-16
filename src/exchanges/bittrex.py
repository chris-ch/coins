import hmac
import time
import hashlib
import logging
import requests

from requests import Request

_BASE_URL = 'https://bittrex.com/api/v1.1'
_REQUEST_ACCOUNT_BALANCES = '/account/getbalances'
_REQUEST_ORDER_HISTORY = '/account/getorderhistory'

_requests_session = None
_api_key = None
_secret_key = None


def connect(api_key, secret_key):
    global _requests_session
    global _api_key
    global _secret_key
    _api_key = api_key
    _secret_key = secret_key
    _requests_session = requests.session()


def api_call(method, options=None):
    if _requests_session is None:
        raise Exception('not initialized: call connect(api_key, secret_key) first')

    if not options:
        options = {}

    nonce = str(int(time.time() * 1000))
    params = {'apikey': _api_key, 'nonce': nonce}
    params.update(options)
    request = Request('GET', _BASE_URL + method, params=params)
    prepared_request = request.prepare()
    logging.info(_secret_key)
    logging.info(prepared_request.url)
    signature = hmac.new(_secret_key.encode(), prepared_request.url.encode(), digestmod=hashlib.sha512)
    prepared_request.headers.update({'apisign': signature.hexdigest()})
    logging.info('headers: {}'.format(prepared_request.headers))
    response = _requests_session.send(prepared_request)
    if not response.json()['success']:
        logging.error('{}'.format(response.json()))
        raise Exception('failed to retrieve data: {}'.format(response.json()['message']))

    return response.json()['result']


def get_balances():
    """
    
    :return:
    """
    return api_call(_REQUEST_ACCOUNT_BALANCES)


def get_order_history():
    """

    :return:
    """
    return api_call(_REQUEST_ORDER_HISTORY, options={'market': 'BTC-LTC', 'count': 10})
