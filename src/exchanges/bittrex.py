import hmac
import time
import hashlib
import logging

import requests
from decimal import Decimal
from requests import Request
import pandas
from datetime import datetime

_BASE_URL = 'https://bittrex.com/api/v1.1'
_REQUEST_ACCOUNT_BALANCES = '/account/getbalances'
_REQUEST_ORDER_HISTORY = '/account/getorderhistory'
_REQUEST_WITHDRAWAL_HISTORY = '/account/getwithdrawalhistory'
_REQUEST_DEPOSIT_HISTORY = '/account/getdeposithistory'

_requests_session = None
_api_key = None
_secret_key = None


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


def api_call(method, options=None):
    """

    :param method:
    :param options:
    :return:
    """
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

    return response.json(parse_float=Decimal)['result']


def get_balances():
    """

    :return:
    """
    return api_call(_REQUEST_ACCOUNT_BALANCES)


def get_order_history():
    """

    :return:
    """
    return api_call(_REQUEST_ORDER_HISTORY)


def get_deposit_history():
    """

    :return:
    """
    return api_call(_REQUEST_DEPOSIT_HISTORY)


def get_withdrawal_history():
    """

    :return:
    """
    return api_call(_REQUEST_WITHDRAWAL_HISTORY)


def parse_flows(withdrawals, deposits):
    """

    :return:
    """
    movements = list()
    for withdrawal in withdrawals:
        item = {
            'date': datetime.strptime(withdrawal['LastUpdated'], '%Y-%m-%dT%H:%M:%S.%f'),
            'amount': withdrawal['Amount'] * -1,
            'asset': withdrawal['Currency'],
            'exchange': 'bittrex'
        }
        movements.append(item)

    for deposit in deposits:
        item = {
            'date': datetime.strptime(deposit['LastUpdated'], '%Y-%m-%dT%H:%M:%S.%f'),
            'amount': deposit['Amount'],
            'asset': deposit['Currency'],
            'exchange': 'bittrex'
        }
        movements.append(item)

    flows = pandas.DataFrame(movements)
    return flows


def to_decimal(value):
    """
    Safe conversion to decimal.
    :param value:
    :return:
    """
    output = None
    if value is not None:
        output = Decimal(value)

    return output


def parse_orders(order_history):
    """

    :param order_history:
    :return:
    """
    parsed = list()
    for order in order_history:
        sign = 1
        if 'SELL' in order['OrderType']:
            sign = -1

        traded_qty = (to_decimal(order['Quantity']) - to_decimal(order['QuantityRemaining'])) * sign

        unit_price = None
        if traded_qty != 0:
            unit_price = float(order['Price']) / abs(float(traded_qty))

        pair = order['Exchange'].split('-')
        first_leg_asset = pair[0]
        second_leg_asset = pair[1]
        item_long = {
            'date': datetime.strptime(order['TimeStamp'], '%Y-%m-%dT%H:%M:%S.%f'),
            'asset': first_leg_asset,
            'qty':  traded_qty,
            'fees': to_decimal(order['Commission']),
            'unit_price': unit_price,
            'amount': to_decimal(order['Price']),
            'exchange': 'bittrex'
            }
        parsed.append(item_long)
        item_short = {
            'date': datetime.strptime(order['TimeStamp'], '%Y-%m-%dT%H:%M:%S.%f'),
            'asset': second_leg_asset,
            'qty':  traded_qty * -1,
            'fees': to_decimal(order['Commission']),
            'unit_price': unit_price,
            'amount': to_decimal(order['Price']) * -1,
            'exchange': 'bittrex'
            }
        parsed.append(item_short)

    result = pandas.DataFrame(parsed)
    return result.dropna(subset=('unit_price',))
