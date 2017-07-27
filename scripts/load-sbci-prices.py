import argparse
import json
import logging
import os
from os import path

from cryptocompare import load_crypto_compare_data

_DEFAULT_EXCHANGE = 'CCCAGG'
_DEFAULT_DATA_PATH = '.'
_DEFAULT_TIME_SCALE = 'spot'


def main():
    parser = argparse.ArgumentParser(description='Loading SBCI prices data',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    help_msg_pairs = 'additional comma separated pairs of currencies for price retrieval'
    parser.add_argument('--pairs',
                        type=str,
                        help=help_msg_pairs,
                        default="BTC/USD,ETH/USD,ETH/BTC,USDT/USD"
                        )
    help_msg_ref_cur = 'comma-separated list of reference currencies'
    parser.add_argument('--reference-pairs',
                        type=str,
                        help=help_msg_ref_cur,
                        default='BTC.USD,BTC.EUR,ETH.USD,ETH.EUR'
                        )
    help_msg_prices_exchange = 'exchange for loading prices'
    parser.add_argument('--exchange',
                        type=str,
                        help=help_msg_prices_exchange,
                        default=_DEFAULT_EXCHANGE
                        )
    help_msg_data_path = 'location of data path, using "{}" by default'
    parser.add_argument('--data',
                        metavar='DATA_PATH',
                        type=str,
                        help=help_msg_data_path.format(_DEFAULT_DATA_PATH),
                        default=_DEFAULT_DATA_PATH
                        )
    help_msg_data_path = 'time scale, using "{}" by default'
    parser.add_argument('--time-scale',
                        choices=('day', 'hour', 'minute', 'spot'),
                        help=help_msg_data_path.format(_DEFAULT_TIME_SCALE),
                        default=_DEFAULT_TIME_SCALE
                        )

    args = parser.parse_args()
    reporting_currency = 'ETH'  # TODO: config param

    full_data_path = os.path.abspath(args.data)
    if not os.path.isdir(full_data_path):
        raise RuntimeError('not a directory: {}'.format(full_data_path))

    exchange = args.exchange
    reference_pairs = [(currency.split('.')[0], currency.split('.')[1]) for currency in args.reference_pairs.split(',')]

    with open(os.sep.join([full_data_path, 'currencies.json']), 'r') as currencies_file:
        currencies = json.load(currencies_file)

    currencies.append(reporting_currency)

    reference_currencies = set([currency for pair in reference_pairs for currency in pair])
    prices = load_crypto_compare_data(set(currencies), reference_currencies, exchange, time_scale=args.time_scale)
    prices.to_pickle(os.sep.join([full_data_path, '_'.join([args.time_scale, 'prices']) + '.pkl']))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('{}.log'.format(path.basename(__file__).split('.')[0]), mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    try:
        main()

    except:
        logging.exception('error occured')
