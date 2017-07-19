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
        logging.debug('trying currency {}'.format(common_leg))
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
                    if direct_bid is None or direct_ask is None:
                        continue

                    sleep(1)
                    indirect_bid_1, indirect_ask_1 = get_order_book(indirect_pair_1)
                    if indirect_bid_1 is None or indirect_ask_1 is None:
                        continue

                    sleep(1)
                    indirect_bid_2, indirect_ask_2 = get_order_book(indirect_pair_2)
                    if indirect_bid_2 is None or indirect_ask_2 is None:
                        continue

                    currency_start = direct_pair[:4]
                    currency_transition = direct_pair[4:]
                    amount_transition = direct_ask.iloc[0]['price']

                    if currency_transition in indirect_pair_1:
                        next_pair = indirect_pair_1
                        next_bid = indirect_bid_1.iloc[0]
                        next_ask = indirect_ask_1.iloc[0]
                        last_pair = indirect_pair_2
                        last_bid = indirect_bid_2.iloc[0]
                        last_ask = indirect_ask_2.iloc[0]

                    else:
                        next_pair = indirect_pair_2
                        next_bid = indirect_bid_2.iloc[0]
                        next_ask = indirect_ask_2.iloc[0]
                        last_pair = indirect_pair_1
                        last_bid = indirect_bid_1.iloc[0]
                        last_ask = indirect_ask_1.iloc[0]

                    if next_pair.startswith(currency_transition):
                        currency_final = next_pair[4:]
                        amount_final = amount_transition * next_ask['price']

                    else:
                        currency_final = next_pair[:4]
                        amount_final = amount_transition * next_bid['price']


                    if last_pair.startswith(currency_final):
                        amount_start = last_bid['price']

                    else:
                        amount_start = last_ask['price']

                    final_ratio = amount_final / amount_start
                    if final_ratio > 1:
                        logging.info('found profitable strategy')
                        logging.info('selling 1 {} for {} {}'.format(currency_start, amount_transition, currency_transition))
                        logging.info('selling {} {} for {} {}'.format(amount_transition, currency_transition, amount_final, currency_final))
                        logging.info('buying {} {} with {} {}'.format(final_ratio, currency_start, amount_final, currency_final))
                        logging.info('{}\n --> bid:\n{}\n --> ask:\n {}'.format(direct_pair, direct_bid.iloc[0],
                                                                                direct_ask.iloc[0]))
                        logging.info('{}\n --> bid:\n{}\n --> ask:\n {}'.format(indirect_pair_1, indirect_bid_1.iloc[0],
                                                                                indirect_ask_1.iloc[0]))
                        logging.info('{}\n --> bid:\n{}\n --> ask:\n {}'.format(indirect_pair_2, indirect_bid_2.iloc[0],
                                                                                indirect_ask_2.iloc[0]))

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
