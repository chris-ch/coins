import urllib.parse
import logging
from datetime import datetime
import time

import hashlib
import hmac
import base64

import requests
import pandas
import tenacity
from decimal import Decimal

_DOMAIN = 'api.kraken.com'
_API_VERSION = '0'
_BASE_URL = 'https://{}'.format(_DOMAIN)

_api_key = None
_secret_key = None
_requests_session = None


def retrieve_data(api_key, secret_key):
    """

    :param api_key:
    :param secret_key:
    :return: (flows: DataFrame ('date', 'amount', 'asset', 'fee', 'exchange'), trades, currencies: set(currency codes))
    """
    connect(api_key, secret_key)

    orders = get_trades_history()
    orders_parsed = parse_trades(orders)
    orders_currencies = set()
    if not orders_parsed.empty:
        orders_currencies = set(orders_parsed['asset'].tolist())

    deposits = get_deposits()
    withdrawals = get_withdrawals()
    flows = parse_flows(withdrawals, deposits)
    flows_currencies = set()
    if not flows.empty:
        flows_currencies = set(flows['asset'].tolist())

    currencies = flows_currencies.union(orders_currencies)
    return flows, orders_parsed, currencies


def connect(api_key=None, secret_key=None):
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
    logging.info('connected with keys ({}, {})'.format(_api_key, _secret_key))


@tenacity.retry(wait=tenacity.wait_fixed(3) + tenacity.wait_random(0, 3),
                retry=tenacity.retry_if_exception_type(requests.HTTPError),
                stop=tenacity.stop_after_attempt(5)
                )
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

    logging.debug('response: "{}"'.format(response.text))
    json_data = response.json(parse_float=Decimal)
    if 'result' not in json_data:
        logging.error('request "{}" failed (post data: {}, headers: {})'.format(url, options, headers))
        raise Exception('request failed: {}'.format(json_data))

    return json_data


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


def get_tradeable_pairs():
    """

    :return:
    """
    asset_pairs = api_call_public('AssetPairs')['result']
    records = list()
    for pair_code, pair_data in asset_pairs.items():
        record = merge_dicts(pair_data, {'pair_code': pair_code})
        record.pop('aclass_quote', None)
        record.pop('aclass_base', None)
        record.pop('leverage_buy', None)
        record.pop('leverage_sell', None)
        record.pop('fees', None)
        record.pop('fees_maker', None)
        record.pop('fee_volume_currency', None)
        records.append(record)

    return pandas.DataFrame(records)


def get_order_book(pair, depth=5):
    """
    Order book for a given pair.

    :param pair:
    :param depth:
    :return:
    """
    order_book = api_call_public('Depth', options={'pair': pair, 'count': depth})['result'][pair]
    bid_side = order_book['bids']
    ask_side = order_book['asks']
    bid_records = list()
    for count, row_data in enumerate(bid_side):
        price, volume, timestamp = row_data
        record = {'level': count, 'price': Decimal(price), 'volume': Decimal(volume),
                  'timestamp': datetime.fromtimestamp(timestamp)}
        bid_records.append(record)

    ask_records = list()
    for count, row_data in enumerate(ask_side):
        price, volume, timestamp = row_data
        record = {'level': count, 'price': Decimal(price), 'volume': Decimal(volume),
                  'timestamp': datetime.fromtimestamp(timestamp)}
        ask_records.append(record)

    bid_df, ask_df = pandas.DataFrame(bid_records), pandas.DataFrame(ask_records)
    if bid_df.empty or ask_df.empty:
        return None, None

    bid_df.set_index('level', inplace=True)
    ask_df.set_index('level', inplace=True)
    bid_df = bid_df[['timestamp', 'price', 'volume']]
    ask_df = ask_df[['timestamp', 'price', 'volume']]
    return bid_df, ask_df


def get_balances():
    return api_call_private('Balance')['result']


def merge_dicts(dict1, *dicts):
    dict1_copy = dict1.copy()
    for other_dict in dicts:
        dict1_copy.update(other_dict)

    return dict1_copy


def get_closed_orders():
    orders = api_call_private('ClosedOrders', options={'trades': True, 'closetime': 'close'})['result']['closed']
    return orders


def get_ledgers_info(options=None):
    ledgers_info = api_call_private('Ledgers', options=options)['result']['ledger']
    return ledgers_info


def get_trades_history(options=None):
    trades = api_call_private('TradesHistory', options=options)['result']['trades']
    return trades


def get_deposits():
    return get_ledgers_info({'type': 'deposit'})


def get_withdrawals():
    return get_ledgers_info({'type': 'withdrawal'})


def translate_currency(kraken_code):
    """
    Converts from Kraken convention to CryptoCompare.

    TODO: create a values DB for this.

    :param kraken_code:
    :return:
    """
    mapping = {
        'ZEUR': 'EUR',
        'XETH': 'ETH',
        'XXRP': 'XRP',
        'XBT': 'BTC',
        'XXBT': 'BTC',
        'XLTC': 'LTC',
    }
    if kraken_code in mapping:
        return mapping[kraken_code]

    else:
        return kraken_code


def parse_flows(withdrawals, deposits):
    """

    :param withdrawals:
    :param deposits:
    :return: DataFrame ('date', 'amount', 'asset', 'fee', 'exchange')
    """
    records = list()
    for ledger_id, ledger in withdrawals.items():
        record = merge_dicts(ledger, {'ledger_id': ledger_id})
        record['date'] = datetime.fromtimestamp(record['time'])
        record['amount'] = record['amount'] * -1
        record['asset'] = translate_currency(record['asset'])
        records.append(record)

    for ledger_id, ledger in deposits.items():
        record = merge_dicts(ledger, {'ledger_id': ledger_id})
        record['date'] = datetime.fromtimestamp(record['time'])
        record['asset'] = translate_currency(record['asset'])
        records.append(record)

    flows = pandas.DataFrame(records, columns=['date', 'amount', 'asset', 'fee'])
    movements = flows.sort_values('date', ascending=False)
    movements['exchange'] = 'kraken'
    return movements


def parse_trades(raw_trades):
    """

    :param raw_trades:
    :return: DataFrame ('date', 'asset', 'qty', 'fee', 'exchange')
    """
    records = list()
    for trade_id, trade in raw_trades.items():
        record = merge_dicts(trade, {'trade_id': trade_id})
        record.pop('misc')
        record.pop('margin')
        record.pop('ordertype')
        record['time'] = datetime.fromtimestamp(record['time'])
        record['cost'] = Decimal(record['cost'])
        record['fee'] = Decimal(record['fee'])
        record['vol'] = Decimal(record['vol'])
        record['price'] = Decimal(record['price'])
        records.append(record)

    trades = list()
    for index, order in pandas.DataFrame(records).iterrows():
        asset_leg1 = order['pair'][:len(order['pair']) // 2]
        asset_leg2 = order['pair'][len(order['pair']) // 2:]
        sign = 1
        if order['type'] == 'sell':
            sign = -1

        trade_leg1 = {
            'date': order['time'],
            'asset': translate_currency(asset_leg1),
            'qty': order['vol'] * sign,
            'fee': 0,
            'exchange': 'kraken'
        }
        trade_leg2 = {
            'date': order['time'],
            'asset': translate_currency(asset_leg2),
            'qty': order['cost'] * sign * -1,
            'fee': order['fee'],
            'exchange': 'kraken'
        }
        trades.append(trade_leg1)
        trades.append(trade_leg2)

    parsed = pandas.DataFrame(trades).sort_values('date', ascending=False)
    parsed = parsed[['date', 'asset', 'qty', 'fee', 'exchange']]
    print(parsed)
    return parsed
