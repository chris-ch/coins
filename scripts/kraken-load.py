import json
import logging
from os import path
import argparse
import os

from exchanges import kraken

_DEFAULT_CONFIG_FILE = 'config.json'


def main():
    parser = argparse.ArgumentParser(description='Updating SCBI spreadsheet',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    help_msg_config = 'location of config file, using "{}" by default'
    parser.add_argument('--config',
                        metavar='CONFIG_JSON',
                        type=str,
                        help=help_msg_config.format(_DEFAULT_CONFIG_FILE),
                        default=_DEFAULT_CONFIG_FILE
                        )

    args = parser.parse_args()
    full_config_path = os.path.abspath(args.config)
    logging.info('reading config from "%s"', full_config_path)
    if not os.path.isfile(full_config_path):
        raise RuntimeError('unable to load config file: {}'.format(full_config_path))

    config_json = json.load(open(args.config, 'rt'))
    api_key = config_json['exchanges']['kraken']['key']
    secret_key = config_json['exchanges']['kraken']['secret']

    kraken.connect(api_key, secret_key)
    balances = kraken.get_balances()
    orders = kraken.get_closed_orders()
    deposits = kraken.get_deposits()
    withdrawals = kraken.get_withdrawals()
    logging.info('balances:\n{}'.format(balances))
    logging.info('orders:\n{}'.format(orders))
    logging.info('deposits:\n{}'.format(deposits))
    logging.info('withdrawals:\n{}'.format(withdrawals))
    orders.to_pickle('output/kraken_orders.pkl')
    deposits.to_pickle('output/kraken_deposits.pkl')
    withdrawals.to_pickle('output/kraken_withdrawals.pkl')


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
