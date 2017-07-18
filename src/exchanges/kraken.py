import urllib.parse
import logging
import time
from datetime import datetime

import hashlib
import hmac
import base64

import requests

import pandas

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
    if response.status_code != requests.codes.ok:
        logging.error('failed requesting data: status {}'.format(response.status_code))
        response.raise_for_status()

    logging.info('response: "{}"'.format(response.text))
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
    return api_call_private('Balance')['result']


def merge_dicts(dict1, *dicts):
    dict1_copy = dict1.copy()
    for other_dict in dicts:
        dict1_copy.update(other_dict)

    return dict1_copy


def get_closed_orders():
    closed_orders = api_call_private('ClosedOrders', options={'trades': False, 'closetime': 'close'})['result'][
        'closed']
    records = list()
    for order_id, closed_order in closed_orders.items():
        if closed_order['status'] != 'canceled':
            description = closed_order.pop('descr')
            record = merge_dicts(closed_order, {'order_id': order_id}, description)
            record.pop('trades')
            record.pop('userref')
            record.pop('status')
            record.pop('reason')
            record.pop('refid')
            record.pop('expiretm')
            record.pop('misc')
            record.pop('oflags')
            record.pop('order')
            record.pop('starttm')
            record.pop('opentm')
            record['closetm'] = datetime.fromtimestamp(record['closetm'])
            records.append(record)

    return pandas.DataFrame(records)


def get_ledgers_info(options=None):
    ledgers_info = api_call_private('Ledgers', options=options)['result']['ledger']
    records = list()
    for ledger_id, ledger in ledgers_info.items():
        record = merge_dicts(ledger, {'ledger_id': ledger_id})
        record['time'] = datetime.fromtimestamp(record['time'])
        records.append(record)

    return pandas.DataFrame(records)


def get_deposits():
    return get_ledgers_info({'type': 'deposit'})


def get_withdrawals():
    return get_ledgers_info({'type': 'withdrawal'})
