import unittest
import logging
import os
import pandas

from decimal import Decimal

from arbitrage import calculate_arbitrage_opportunity


def load_book_data(pair_code):
    bid_path = os.path.abspath(os.sep.join(['tests-data', '{}-bid.pkl']).format(pair_code))
    ask_path = os.path.abspath(os.sep.join(['tests-data', '{}-ask.pkl']).format(pair_code))
    return pandas.read_pickle(bid_path), pandas.read_pickle(ask_path)


class TestBittrexAPI(unittest.TestCase):
    """
    Testing P&L calculation from Bittrex.
    """

    def setUp(self):
        pass

    def test_simple(self):
        # XETHXXBT with XETHZCAD and XXBTZCAD
        # XXRPXXBT with XXRPZCAD and XXBTZCAD
        # XETHXXBT with XETHZJPY and XXBTZJPY
        # XXRPXXBT with XXRPZJPY and XXBTZJPY
        bid1, ask1 = load_book_data('XETHXXBT')
        bid2, ask2 = load_book_data('XETHZCAD')
        bid3, ask3 = load_book_data('XXBTZCAD')
        arbitrage_ratio = calculate_arbitrage_opportunity('XETHXXBT', bid1, ask1, 'XETHZCAD', bid2, ask2,
                                                          'XXBTZCAD', bid3, ask3)
        arbitrage_ratio = calculate_arbitrage_opportunity('XETHXXBT', bid1, ask1, 'XETHZCAD', bid2, ask2,
                                                          'XXBTZCAD', bid3, ask3)
        arbitrage_ratio = calculate_arbitrage_opportunity('XETHXXBT', bid1, ask1, 'XETHZCAD', bid2, ask2,
                                                          'XXBTZCAD', bid3, ask3)
        print(arbitrage_ratio)

    def tearDown(self):
        pass

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()