import itertools
import pandas
import logging
import requests
import json
from datetime import datetime

_BASE_CRYPTO_COMPARE_URL = 'https://min-api.cryptocompare.com/data'


def load_crypto_compare_data(currencies, reference_currencies, exchange, time_scale):
    """

    :param currencies: list of currency pairs to retrieve
    :param reference_currencies: for quoting each currency in terms of reference currencies
    :param exchange: exchange as referenced by CryptoCompare
    :param time_scale: one of ('day', 'hour', 'minute', 'spot')
    :return: DataFrame of historical prices
    """
    cross_product = itertools.product(set(reference_currencies).union(set(currencies)), reference_currencies)
    pairs = [pair for pair in cross_product if pair[0] != pair[1]]
    if time_scale == 'spot':
        prices = load_pairs_spot(pairs, exchange)

    elif time_scale == 'minute':
        prices = load_pairs_histo_minute(pairs, exchange)

    elif time_scale == 'hour':
        prices = load_pairs_histo_hourly(pairs, exchange)

    elif time_scale == 'day':
        prices = load_pairs_histo_daily(pairs, exchange)

    else:
        raise NotImplementedError('unavailable time scale: "{}"'.format(time_scale))

    return prices.pivot_table(index='date', columns='currency', values='price').reset_index()


def load_pairs_spot(pairs, exchange):
    """

    :param pairs:
    :param exchange: exchange as referenced by CryptoCompare
    :return:
    """
    logging.debug('loading spot data for pairs: {}'.format(str(pairs)))
    session = requests.Session()
    spot_prices = dict()
    for from_currency, to_currency in pairs:
        payload = {'fsym': from_currency,
                   'tsyms': to_currency,
                   'e': exchange
                   }
        results = session.get('{}/price'.format(_BASE_CRYPTO_COMPARE_URL), params=payload)
        json_result = results.json()
        if 'Message' in json_result:
            json_message = json_result['Message']
            message = 'Error occurred while loading prices from exchange {}: {} ({}/{})'
            raise Exception(message.format(exchange, json_message, from_currency, to_currency))

        spot_prices[(from_currency, to_currency)] = json_result[to_currency]

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


def load_pairs_histo_minute(pairs, exchange):
    """

    :param pairs:
    :param exchange: exchange as referenced by CryptoCompare
    :return:
    """
    logging.debug('loading minute data for pairs: {}'.format(str(pairs)))
    session = requests.Session()
    output = dict()
    for from_currency, to_currency in pairs:
        payload = {'fsym': from_currency,
                   'tsym': to_currency,
                   'e': exchange,
                   'limit': 1000
                   }
        results = session.get('{}/histominute'.format(_BASE_CRYPTO_COMPARE_URL), params=payload)
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

    minute_df = pandas.DataFrame(data)
    return minute_df


def load_pairs_histo_hourly(pairs, exchange):
    """

    :param pairs:
    :param exchange: exchange as referenced by CryptoCompare
    :return:
    """
    logging.debug('loading hourly data for pairs: {}'.format(str(pairs)))
    session = requests.Session()
    output = dict()
    for from_currency, to_currency in pairs:
        payload = {'fsym': from_currency,
                   'tsym': to_currency,
                   'e': exchange,
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


def load_pairs_histo_daily(pairs, exchange):
    """

    :param pairs:
    :param exchange: exchange as referenced by CryptoCompare
    :return:
    """
    logging.debug('loading daily data for pairs: {}'.format(str(pairs)))
    session = requests.Session()
    output = dict()
    for from_currency, to_currency in pairs:
        payload = {'fsym': from_currency,
                   'tsym': to_currency,
                   'e': exchange,
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
