import argparse
import json
import logging
import os
from datetime import datetime
from os import path

import requests
import pandas

from gservices import setup_services, file_by_id, update_sheet, load_sheet

_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE = os.sep.join(('.', 'google-service-account-creds.json'))
_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config.json'))
_DEFAULT_EXCHANGE = 'CCCAGG'
_BASE_CRYPTO_COMPARE_URL = 'https://min-api.cryptocompare.com/data'


def load_crypto_compare_data(target_pairs):
    spot_prices = load_pairs_spot(target_pairs)
    print(spot_prices)

    hist_prices_hourly = load_pairs_histo_hourly(target_pairs)
    hist_prices_daily = load_pairs_histo_daily(target_pairs)


    print(hist_prices_hourly)
    print(hist_prices_daily)


def load_pairs_spot(pairs):
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



def process_spreadsheet(credentials, spreadsheet_id):
    svc_drive, svc_sheets = setup_services(credentials)

    # accessing Google drive
    spreadsheet_name = file_by_id(svc_drive, spreadsheet_id)
    logging.info('prepared Google sheet %s: %s', spreadsheet_name, spreadsheet_id)
    header = ('Head1', 'Head2', 'Head3')
    rows = [
        {'Head1': '1', 'Head2': '2', 'Head3': '3'},
        {'Head1': '4', 'Head2': '5', 'Head3': '6'},
    ]
    #update_sheet(svc_sheets, spreadsheet_id, header, rows)
    logging.info('saved sheet %s', spreadsheet_name)
    values = load_sheet(svc_sheets, spreadsheet_id)
    print(values)


def main():
    parser = argparse.ArgumentParser(description='Updating SCBI spreadsheet',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    help_msg_creds = 'location of Google Service Account Credentials file, using "{}" by default'
    parser.add_argument('--google-creds',
                        metavar='GOOGLE_SERVICE_ACCOUNT_CREDS_JSON',
                        type=str,
                        help=help_msg_creds.format(_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE),
                        default=_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE
                        )
    help_msg_config = 'location of config file, using "{}" by default'
    parser.add_argument('--config',
                        metavar='CONFIG_JSON',
                        type=str,
                        help=help_msg_config.format(_DEFAULT_CONFIG_FILE),
                        default=_DEFAULT_CONFIG_FILE
                        )
    help_msg_pairs = 'comma separated pairs of currencies ("BTC/USD,ETH/USD,ETH/BTC,USDT/USD")'
    parser.add_argument('--pairs',
                        type=str,
                        help=help_msg_pairs,
                        default="BTC/USD,ETH/USD,ETH/BTC,USDT/USD"
                        )

    args = parser.parse_args()
    full_creds_path = os.path.abspath(args.google_creds)
    logging.info('reading Google Service Account credentials from "%s"', full_creds_path)
    if not os.path.isfile(full_creds_path):
        raise RuntimeError('unable to load Google Service Account credentials file: {}'.format(full_creds_path))

    full_config_path = os.path.abspath(args.config)
    logging.info('reading config from "%s"', full_config_path)
    if not os.path.isfile(full_config_path):
        raise RuntimeError('unable to load config file: {}'.format(full_config_path))

    config_json = json.load(open(args.config, 'rt'))

    target_pairs = [pair.split('/') for pair in args.pairs.split(',')]
    load_crypto_compare_data(target_pairs)

    process_spreadsheet(args.google_creds, config_json['target_sheet_id'])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    # logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
    # logging.getLogger('googleapicliet.discovery_cache').setLevel(logging.ERROR)
    file_handler = logging.FileHandler('{}.log'.format(path.basename(__file__).split('.')[0]), mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    try:
        main()

    except:
        logging.exception('error occured')
