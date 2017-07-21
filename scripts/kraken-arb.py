import argparse
import json
import logging
import os
from os import path

from arbitrage import scan_arbitrage_opportunities
from exchanges import kraken

_DEFAULT_CONFIG_FILE = 'config.json'


def main():
    parser = argparse.ArgumentParser(description='Scanning arbitrage opportunities',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )

    kraken.connect()

    tradeable_pairs = kraken.get_tradeable_pairs()

    def order_book_l1():
        def wrapped(pair):
            bid, ask = kraken.get_order_book(pair)
            if bid is None or ask is None:
                return None, None

            return bid.iloc[0], ask.iloc[0]

        return wrapped

    results = scan_arbitrage_opportunities(tradeable_pairs, order_book_callbak=order_book_l1())

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
