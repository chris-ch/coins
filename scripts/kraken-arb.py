import json
import logging
from os import path
import argparse
import os
from time import sleep

from exchanges import kraken
from exchanges.kraken import get_tradeable_pairs, get_order_book

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

    tradeable_pairs = get_tradeable_pairs()
    assets = set(tradeable_pairs['base'].tolist()).union(tradeable_pairs['quote'].tolist())
    available_pairs = set(tradeable_pairs['pair_code'].tolist())
    logging.info('available pairs: {}'.format(available_pairs))
    logging.info('available assets: {}'.format(assets))
    results = list()
    for common_leg in assets:
        logging.info('trying currency {}'.format(common_leg))
        for leg_pair1 in assets:
            if leg_pair1 == common_leg:
                continue

            for leg_pair2 in assets:
                if leg_pair2 == common_leg or leg_pair2 == leg_pair1:
                    continue

                direct_pair = leg_pair1 + leg_pair2
                indirect_pair_1 = leg_pair1 + common_leg
                indirect_pair_2 = leg_pair2 + common_leg
                if available_pairs.issuperset({direct_pair, indirect_pair_1, indirect_pair_2}):
                    logging.info('trying pair {} with {} and {}'.format(direct_pair, indirect_pair_1, indirect_pair_2))
                    sleep(1)
                    direct_bid, direct_ask = get_order_book(direct_pair)
                    sleep(1)
                    indirect_bid_1, indirect_ask_1 = get_order_book(indirect_pair_1)
                    sleep(1)
                    indirect_bid_2, indirect_ask_2 = get_order_book(indirect_pair_2)
                    logging.info('{}: {} / {}'.format(direct_pair, direct_bid, direct_ask))
                    logging.info('{}: {} / {}'.format(indirect_pair_1, indirect_bid_1, indirect_ask_1))
                    logging.info('{}: {} / {}'.format(indirect_pair_2, indirect_bid_2, indirect_ask_2))
                    result = ''
                    results.append(result)

    logging.info('results:\n{}'.format(results))


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
