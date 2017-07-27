import argparse
import json
import logging
import os
from os import path

_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config.json'))
_DEFAULT_DATA_PATH = '.'


def main():
    parser = argparse.ArgumentParser(description='Loading SBCI ledger data',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    help_msg_config = 'location of config file, using "{}" by default'
    parser.add_argument('--config',
                        metavar='CONFIG_JSON',
                        type=str,
                        help=help_msg_config.format(_DEFAULT_CONFIG_FILE),
                        default=_DEFAULT_CONFIG_FILE
                        )
    help_msg_data_path = 'location of data path, using "{}" by default'
    parser.add_argument('--data',
                        metavar='DATA_PATH',
                        type=str,
                        help=help_msg_data_path.format(_DEFAULT_DATA_PATH),
                        default=_DEFAULT_DATA_PATH
                        )

    args = parser.parse_args()

    full_config_path = os.path.abspath(args.config)
    logging.info('reading config from "%s"', full_config_path)
    if not os.path.isfile(full_config_path):
        raise RuntimeError('unable to load config file: {}'.format(full_config_path))

    config_json = json.load(open(args.config, 'rt'))
    full_data_path = os.path.abspath(args.data)
    if os.path.isfile(full_data_path):
        raise RuntimeError('not a directory: {}'.format(full_data_path))

    os.makedirs(full_data_path, exist_ok=True)

    # Exchange-related part ... TODO: make it generic by reading from config file
    from exchanges import bittrex
    api_key_bittrex = config_json['exchanges']['bittrex']['key']
    secret_key_bittrex = config_json['exchanges']['bittrex']['secret']
    flows_bittrex, trades_bittrex, currencies_bittrex = bittrex.retrieve_data(api_key_bittrex, secret_key_bittrex)

    #from exchanges import kraken
    #api_key_kraken = config_json['exchanges']['kraken']['key']
    #secret_key_kraken = config_json['exchanges']['kraken']['secret']
    #flows_kraken, trades_kraken, currencies_kraken = kraken.retrieve_data(api_key_kraken, secret_key_kraken)

    #flows = flows_kraken
    #trades = trades_kraken
    #currencies = currencies_kraken

    flows = flows_bittrex
    trades = trades_bittrex
    currencies = currencies_bittrex
    flows.to_pickle(os.sep.join([full_data_path, 'flows.pkl']))
    trades.to_pickle(os.sep.join([full_data_path, 'trades.pkl']))
    with open(os.sep.join([full_data_path, 'currencies.json']), 'w') as currencies_file:
        json.dump(list(currencies), currencies_file)

    #


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
