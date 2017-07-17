import json
import logging
from os import path
import argparse
import os

from exchanges import bittrex

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
    api_key = config_json['exchanges']['bittrex']['key']
    secret_key = config_json['exchanges']['bittrex']['secret']
    bittrex.connect(api_key, secret_key)
    results = bittrex.get_balances()
    print(results)
    deposits = bittrex.get_deposit_history()
    print(deposits)
    withdrawals = bittrex.get_withdrawal_history()
    print(withdrawals)
    trades = bittrex.get_order_history()
    print(trades)

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
