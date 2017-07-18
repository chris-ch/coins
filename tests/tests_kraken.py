import unittest
import logging
import os
import json

from decimal import Decimal

import pandas

from exchanges.kraken import parse_orders, parse_flows


class TestKrakenAPI(unittest.TestCase):
    """
    Testing P&L calculation from Bittrex.
    """

    def setUp(self):
        self._orders = pandas.read_pickle(os.path.abspath(os.sep.join(['tests-data', 'kraken-orders.pkl'])))
        self._deposits = pandas.read_pickle(os.path.abspath(os.sep.join(['tests-data', 'kraken-deposits.pkl'])))
        self._withdrawals = pandas.read_pickle(os.path.abspath(os.sep.join(['tests-data', 'kraken-withdrawals.pkl'])))
        logging.info('loading example order history file: {}'.format(self._orders))
        logging.info('loading example withdrawals file: {}'.format(self._withdrawals))
        logging.info('loading example deposits file: {}'.format(self._deposits))

    def test_parsing(self):
        trades = parse_orders(self._orders)
        flows = parse_flows(self._withdrawals, self._deposits)
        self.assertAlmostEqual(float(trades[trades['asset'] == 'BTC']['qty'].sum()), 0.01968424)
        self.assertAlmostEqual(float(flows[flows['asset'] == 'NEOS']['amount'].sum()), 0.09736144)

    def tearDown(self):
        pass

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()