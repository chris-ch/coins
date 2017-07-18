import hmac
import time
import hashlib
import logging

import requests
from decimal import Decimal
from requests import Request
import pandas
from datetime import datetime

_API_VERSION = 'v1.1'
_DOMAIN = 'bittrex.com'
_BASE_URL = 'https://{}/api/{}'.format(_DOMAIN, _API_VERSION)
_REQUEST_ACCOUNT_BALANCES = '/account/getbalances'
_REQUEST_ORDER_HISTORY = '/account/getorderhistory'
_REQUEST_WITHDRAWAL_HISTORY = '/account/getwithdrawalhistory'
_REQUEST_DEPOSIT_HISTORY = '/account/getdeposithistory'

_requests_session = None
_api_key = None
_secret_key = None


def retrieve_data(api_key, secret_key):
    """

    :param api_key:
    :param secret_key:
    :return: (flows, trades, currencies)
    """
    connect(api_key, secret_key)
    deposits = get_deposit_history()
    withdrawals = get_withdrawal_history()
    order_history = get_order_history()

    orders_parsed = parse_orders(order_history)
    orders_currencies = set()
    if not orders_parsed.empty:
        orders_currencies = set(orders_parsed['asset'].tolist())

    flows_parsed = parse_flows(withdrawals, deposits)
    flows_currencies = set()
    if not flows_parsed.empty:
        flows_currencies = set(flows_parsed['asset'].tolist())

    currencies = flows_currencies.union(orders_currencies)
    flows = parse_flows(withdrawals, deposits).set_index('date')
    trades = parse_orders(order_history)

    return flows, trades, currencies


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


def _api_call(method, options=None):
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
    return _api_call(_REQUEST_ACCOUNT_BALANCES)


def get_order_history():
    """

    :return:
    """
    return _api_call(_REQUEST_ORDER_HISTORY)


def get_deposit_history():
    """

    :return:
    """
    return _api_call(_REQUEST_DEPOSIT_HISTORY)


def get_withdrawal_history():
    """

    :return:
    """
    return _api_call(_REQUEST_WITHDRAWAL_HISTORY)


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
    logging.debug('processing orders:\n{}'.format(order_history))
    for order in order_history:

        traded_qty = (to_decimal(order['Quantity']) - to_decimal(order['QuantityRemaining']))

        # Example:
        # {
        # 'OrderUuid': '17fd64d1-f4bd-4fb6-adb9-42ec68b8697d',
        # 'Exchange': 'BTC-XRP',
        # 'TimeStamp': '2017-07-08T20:38:58.317',
        # 'OrderType': 'LIMIT_BUY',
        # 'Limit': Decimal('0.00002950'),
        # 'Quantity': Decimal('667.03644955'),
        # 'QuantityRemaining': Decimal('0E-8'),
        # 'Commission': Decimal('0.00004921'),
        # 'Price': Decimal('0.01968424'),
        # 'PricePerUnit': Decimal('0.00002950'),
        # 'IsConditional': False,
        # 'Condition': None,
        # 'ConditionTarget': None,
        # 'ImmediateOrCancel': False
        # }
        #
        # --->  1. SELL 0.01968424 BTC
        #       2. BUY 667.03644955 XRP

        pair = order['Exchange'].split('-')

        first_leg_asset = pair[0]
        first_leg_qty = order['Price'] * -1

        second_leg_asset = pair[1]
        second_leg_qty = traded_qty

        sign = 1
        if 'SELL' in order['OrderType']:
            sign = -1

        item_first = {
            'date': datetime.strptime(order['TimeStamp'], '%Y-%m-%dT%H:%M:%S.%f'),
            'asset': first_leg_asset,
            'qty':  first_leg_qty * sign,
            'fees': to_decimal(order['Commission']),  # all fees for first leg
            'exchange': 'bittrex'
            }
        item_second = {
            'date': datetime.strptime(order['TimeStamp'], '%Y-%m-%dT%H:%M:%S.%f'),
            'asset': second_leg_asset,
            'qty':  second_leg_qty * sign,
            'fees': 0.,  # all fees for first leg
            'exchange': 'bittrex'
            }

        parsed.append(item_first)
        parsed.append(item_second)

    result = pandas.DataFrame(parsed)
    return result
