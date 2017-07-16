import unittest
import logging
import os
import json


class TestBittrexPublicAPI(unittest.TestCase):
    """
    Testing P&L calculation from Bittrex.
    """

    def setUp(self):
        self._example_balances_file = open(os.path.abspath(os.sep.join(['tests-data', 'bittrex-balances.json'])), 'r')
        self._example_order_hist_file = open(os.path.abspath(os.sep.join(['tests-data', 'bittrex-getorderhist.json'])), 'r')
        self._example_balances = json.load(self._example_balances_file)
        self._example_order_hist = json.load(self._example_order_hist_file)
        logging.info('loading example balances file: {}'.format(self._example_balances))
        logging.info('loading example order history file: {}'.format(self._example_order_hist))

    def test_calc_pnl(self):
        print(self._example_balances)
        print(self._example_order_hist)


    def tearDown(self):
        self._example_balances_file.close()
        self._example_order_hist_file.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()