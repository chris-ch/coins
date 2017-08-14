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


def _translate_currency(kraken_code):
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


def retrieve_data(api_key, secret_key):
    """

    :param api_key:
    :param secret_key:
    :return: (flows: DataFrame ('date', 'asset', 'amount', 'fee', 'exchange'), trades: DataFrame ('date', 'asset',
    'amount', 'fee', 'exchange'), currencies: set(currency codes))
    """
    connect(api_key, secret_key)

    ledger_entries = get_ledgers_info()

    records = list()
    for entry_id, entry in ledger_entries.items():
        record = merge_dicts(entry, {'ledger_id': entry_id})
        record['date'] = datetime.fromtimestamp(record['time'])
        record['asset'] = _translate_currency(record['asset'])
        record['exchange'] = 'kraken'
        records.append(record)

    ledger = pandas.DataFrame(records)
    ledger = ledger[['date', 'asset', 'amount', 'fee', 'exchange', 'type']]
    flows = ledger[(ledger['type'] == 'deposit') | (ledger['type'] == 'withdrawal')].drop('type', axis=1)
    trades = ledger[ledger['type'] == 'trade'].drop('type', axis=1)
    currencies = set(ledger['asset'].tolist())
    return flows, trades, currencies


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
    Creates a DataFrame of pairs data:
    altname  base   lot  lot_decimals  lot_multiplier  margin_call     margin_stop pair_code  pair_decimals quote
0   BCHEUR   BCH  unit             8               1           80               40    BCHEUR              4  ZEUR
1   BCHUSD   BCH  unit             8               1           80               40    BCHUSD              4  ZUSD
2   BCHXBT   BCH  unit             8               1           80               40    BCHXBT              6  XXBT
3  DASHEUR  DASH  unit             8               1           80               40   DASHEUR              5  ZEUR
4  DASHUSD  DASH  unit             8               1           80               40   DASHUSD              5  ZUSD

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


def get_ledgers_info(options=None):
    if options is None:
        options = dict()

    ledgers_info = api_call_private('Ledgers')['result']
    current_entries = ledgers_info['ledger']
    remaining = ledgers_info['count'] - len(current_entries)
    while remaining > 0:
        options.update({'ofs': len(current_entries)})
        ledgers_info = api_call_private('Ledgers', options=options)['result']
        entries = ledgers_info['ledger']
        current_entries.update(entries)
        remaining = ledgers_info['count'] - len(current_entries)

    return current_entries
