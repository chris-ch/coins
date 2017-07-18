import unittest
import logging
import os
import json
from decimal import Decimal

import pandas

from sbcireport import compute_trades_pnl, compute_balances_pnl, compute_pnl_history, compute_balances


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

    def test_trades_pnl(self):
        trades_pnl = compute_trades_pnl('USD', self._example_prices, self._example_order_hist)
        pnl_xrp = trades_pnl[trades_pnl['asset'] == 'XRP'].tail(1)['total_pnl'].sum()
        self.assertAlmostEqual(pnl_xrp, 85.514073, places=6)

    def test_balances_pnl(self):
        balances = compute_balances(self._example_withdrawals, self._example_deposits)
        balances_pnl = compute_balances_pnl('USD', balances, self._example_prices)
        self.assertAlmostEqual(balances_pnl.groupby('asset').sum().loc['START'].sum(), -2.136932, places=6)

    def test_pnl_history(self):
        balances = compute_balances(self._example_withdrawals, self._example_deposits)
        balances_pnl = compute_balances_pnl('USD', balances, self._example_prices)
        pnl_history = compute_pnl_history('USD', self._example_prices, balances_pnl, self._example_order_hist)
        self.assertAlmostEqual(pnl_history.sum(), 7811.912114, places=6)

    def tearDown(self):
        self._example_order_hist_file.close()
        self._example_withdrawals_file.close()
        self._example_deposits_file.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()
