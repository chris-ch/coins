import unittest
import logging
import os
import json

from decimal import Decimal

from exchanges.bittrex import parse_trades, parse_flows


class TestBittrexAPI(unittest.TestCase):
    """
    Testing P&L calculation from Bittrex.
    """

    def setUp(self):
        self._example_balances_file = open(os.path.abspath(os.sep.join(['tests-data', 'bittrex-balances.json'])), 'r')
        self._example_order_hist_file = open(os.path.abspath(os.sep.join(['tests-data', 'bittrex-getorderhist.json'])), 'r')
        self._example_withdrawals_file = open(os.path.abspath(os.sep.join(['tests-data', 'bittrex-withdrawals.json'])), 'r')
        self._example_deposits_file = open(os.path.abspath(os.sep.join(['tests-data', 'bittrex-deposits.json'])), 'r')
        self._example_balances = json.load(self._example_balances_file, parse_float=Decimal)
        self._example_withdrawals = json.load(self._example_withdrawals_file, parse_float=Decimal)
        self._example_deposits = json.load(self._example_deposits_file, parse_float=Decimal)
        self._example_order_hist = json.load(self._example_order_hist_file, parse_float=Decimal)
        logging.info('loading example balances file: {}'.format(self._example_balances))
        logging.info('loading example order history file: {}'.format(self._example_order_hist))
        logging.info('loading example withdrawals file: {}'.format(self._example_withdrawals))
        logging.info('loading example deposits file: {}'.format(self._example_deposits))

    def test_parsing(self):
        trades = parse_trades(self._example_order_hist)
        flows = parse_flows(self._example_withdrawals, self._example_deposits)
        self.assertSequenceEqual(flows.columns.tolist(), ('date', 'asset', 'amount', 'fee', 'exchange'))
        self.assertAlmostEqual(float(trades[trades['asset'] == 'BTC']['amount'].sum()), 0.01968424)
        self.assertAlmostEqual(float(flows[flows['asset'] == 'NEOS']['amount'].sum()), 0.09736144)

    def tearDown(self):
        self._example_balances_file.close()
        self._example_order_hist_file.close()
        self._example_withdrawals_file.close()
        self._example_deposits_file.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()