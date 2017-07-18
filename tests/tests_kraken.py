import unittest
import logging
import os

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

    def test_parsing_flows(self):
        flows = parse_flows(self._withdrawals, self._deposits)
        target = pandas.read_pickle(os.path.abspath(os.sep.join(['..', 'output', 'common-flows.pkl'])))
        print(target)
        self.assertAlmostEqual(float(flows[flows['asset'] == 'NEOS']['amount'].sum()), 0.09736144)

    def tearDown(self):
        pass

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()