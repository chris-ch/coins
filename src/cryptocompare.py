import pandas
import logging
import requests
import json
from datetime import datetime

_DEFAULT_EXCHANGE = 'CCCAGG'
_BASE_CRYPTO_COMPARE_URL = 'https://min-api.cryptocompare.com/data'


def load_crypto_compare_data(target_pairs):
    """

    :param target_pairs: list of currency pairs
    :return: DataFrame of historical prices
    """
    spot_prices = load_pairs_spot(target_pairs)
    hist_prices_hourly = load_pairs_histo_hourly(target_pairs)
    hist_prices_daily = load_pairs_histo_daily(target_pairs)
    all_data = pandas.concat([spot_prices, hist_prices_hourly, hist_prices_daily])
    return all_data.pivot_table(index='date', columns='currency', values='price').reset_index()


def load_pairs_spot(pairs):
    """

    :param pairs:
    :return:
    """
    logging.info('loading pairs: {}'.format(str(pairs)))
    session = requests.Session()
    spot_prices = dict()
    for from_currency, to_currency in pairs:
        payload = {'fsym': from_currency,
                   'tsyms': to_currency,
                   'e': _DEFAULT_EXCHANGE,  # exchange
                   }
        results = session.get('{}/price'.format(_BASE_CRYPTO_COMPARE_URL), params=payload)
        spot_prices[(from_currency, to_currency)] = json.loads(results.text)[to_currency]

    now = datetime.now()
    timestamps = list()
    currencies = list()
    prices = list()
    for source, target in spot_prices:
        timestamps.append(now)
        currencies.append('{}/{}'.format(source, target))
        prices.append(spot_prices[(source, target)])

    spot_df = pandas.DataFrame({'date': timestamps, 'currency': currencies, 'price': prices})
    return spot_df


def load_pairs_histo_hourly(pairs):
    """

    :param pairs:
    :return:
    """
    logging.info('loading pairs: {}'.format(str(pairs)))
    session = requests.Session()
    output = dict()
    for from_currency, to_currency in pairs:
        payload = {'fsym': from_currency,
                   'tsym': to_currency,
                   'e': _DEFAULT_EXCHANGE,  # exchange
                   'limit': 1000
                   }
        results = session.get('{}/histohour'.format(_BASE_CRYPTO_COMPARE_URL), params=payload)
        output[(from_currency, to_currency)] = json.loads(results.text)['Data']

    output = {
        key: [
        {'time': datetime.fromtimestamp(price_data['time']), 'close': price_data['close']}
        for price_data in values] for key, values in output.items()
    }

    data = list()
    for source, target in output:
        for hist_data in output[(source, target)]:
            entry = dict()
            entry['currency'] = '{}/{}'.format(source, target)
            entry['date'] = hist_data['time']
            entry['price'] = hist_data['close']
            data.append(entry)

    hourly_df = pandas.DataFrame(data)
    return hourly_df


def load_pairs_histo_daily(pairs):
    """

    :param pairs:
    :return:
    """
    logging.info('loading pairs: {}'.format(str(pairs)))
    session = requests.Session()
    output = dict()
    for from_currency, to_currency in pairs:
        payload = {'fsym': from_currency,
                   'tsym': to_currency,
                   'e': _DEFAULT_EXCHANGE,  # exchange
                   'limit': 400
                   }
        results = session.get('{}/histoday'.format(_BASE_CRYPTO_COMPARE_URL), params=payload)
        output[(from_currency, to_currency)] = json.loads(results.text)['Data']

    output = {
        key: [
        {'time': datetime.fromtimestamp(price_data['time']), 'close': price_data['close']}
        for price_data in values] for key, values in output.items()
    }

    data = list()
    for source, target in output:
        for hist_data in output[(source, target)]:
            entry = dict()
            entry['currency'] = '{}/{}'.format(source, target)
            entry['date'] = hist_data['time']
            entry['price'] = hist_data['close']
            data.append(entry)

    daily_df = pandas.DataFrame(data)
    return daily_df

