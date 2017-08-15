import unittest
import logging
import os
import json
from decimal import Decimal

import pandas

from exchanges.bittrex import parse_flows, parse_trades
from sbcireport import compute_trades_pnl, compute_balances, extend_balances


class TestNavSBCI(unittest.TestCase):
    """
    Testing Prices access.
    """

    def setUp(self):
        target_file = os.path.abspath(os.sep.join(['tests-data', 'cryptocompare-prices.pkl']))
        self._example_prices = pandas.read_pickle(target_file)
        logging.info('loading example balances file: {}'.format(self._example_prices))
        self._example_order_hist_file = open(os.path.abspath(os.sep.join(['tests-data', 'bittrex-getorderhist.json'])), 'r')
        self._example_withdrawals_file = open(os.path.abspath(os.sep.join(['tests-data', 'bittrex-withdrawals.json'])), 'r')
        self._example_deposits_file = open(os.path.abspath(os.sep.join(['tests-data', 'bittrex-deposits.json'])), 'r')
        self._example_withdrawals = json.load(self._example_withdrawals_file, parse_float=Decimal)
        self._example_deposits = json.load(self._example_deposits_file, parse_float=Decimal)
        self._example_order_hist = json.load(self._example_order_hist_file, parse_float=Decimal)
        self._test_prices = pandas.read_pickle(os.path.abspath(os.sep.join(['tests-data', 'test-prices.pkl'])))
        self._test_flows = pandas.read_pickle(os.path.abspath(os.sep.join(['tests-data', 'test-flows.pkl'])))
        self._test_trades = pandas.read_pickle(os.path.abspath(os.sep.join(['tests-data', 'test-trades.pkl'])))

    def test_trades_pnl(self):
        trades_pnl = compute_trades_pnl('USD', self._test_prices, self._test_trades)
        print(trades_pnl)
        pnl_xrp = trades_pnl['XRP'].tail(1).sum()
        self.assertAlmostEqual(pnl_xrp, 62.1341177, places=6)

    def tearDown(self):
        self._example_order_hist_file.close()
        self._example_withdrawals_file.close()
        self._example_deposits_file.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()
