import unittest
import logging
import os

import pandas
from decimal import Decimal

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
        self.assertSequenceEqual(flows.columns.tolist(), ('date', 'amount', 'asset', 'fee', 'exchange'))
        self.assertAlmostEqual(float(flows[flows['asset'] == 'EOS']['amount'].sum()), 255.7618, places=6)

    def test_parsing_orders(self):
        kraken_data = [
            {'closetm': pandas.Timestamp('2017-07-18 08:13:32'), 'cost': Decimal('19.932'), 'fee': Decimal('0.031'),
             'leverage': 'none', 'order_id': 'ORRIGN-NSRTN-PKG6VE', 'ordertype': 'limit', 'pair': 'XBTEUR',
             'price': Decimal('1950.000'), 'price2': '0', 'type': 'buy', 'vol': Decimal('0.01025641'),
             'vol_exec': Decimal('0.01025641')},
            {'closetm': pandas.Timestamp('2017-07-17 21:42:18'), 'cost': Decimal('183.46373'),
             'fee': Decimal('0.29354'), 'leverage': 'none', 'order_id': 'OWFI52-YZ57H-6DQ5VJ', 'ordertype': 'limit',
             'pair': 'LTCEUR', 'price': Decimal('36.70000'), 'price2': '0', 'type': 'buy', 'vol': Decimal('5.00000000'),
             'vol_exec': Decimal('5.00000000')},
            {'closetm': pandas.Timestamp('2017-07-16 16:02:31'), 'cost': Decimal('136.80013'),
             'fee': Decimal('0.21888'), 'leverage': 'none', 'order_id': 'OUE5BK-DZMBK-FXQMKA', 'ordertype': 'limit',
             'pair': 'ETHEUR', 'price': Decimal('137.00000'), 'price2': '0', 'type': 'buy',
             'vol': Decimal('1.00000000'), 'vol_exec': Decimal('1.00000000')},
            {'closetm': pandas.Timestamp('2017-07-16 15:46:03'), 'cost': Decimal('50.000000'),
             'fee': Decimal('0.080000'), 'leverage': 'none', 'order_id': 'O5XWA6-P7PA7-P2DIW3', 'ordertype': 'limit',
             'pair': 'XRPEUR', 'price': Decimal('0.126300'), 'price2': '0', 'type': 'buy',
             'vol': Decimal('395.88281868'), 'vol_exec': Decimal('395.88281868')},
            {'closetm': pandas.Timestamp('2017-07-16 15:31:08'), 'cost': Decimal('48.867187'),
             'fee': Decimal('0.127054'), 'leverage': 'none', 'order_id': 'OBRMOO-KEPIU-YWTN4R', 'ordertype': 'limit',
             'pair': 'XRPEUR', 'price': Decimal('0.128000'), 'price2': '0', 'type': 'buy',
             'vol': Decimal('390.62500000'), 'vol_exec': Decimal('390.62500000')},
            {'closetm': pandas.Timestamp('2017-07-14 14:49:06'), 'cost': Decimal('75.97806'), 'fee': Decimal('0.19754'),
             'leverage': 'none', 'order_id': 'OKAZSI-KTFYG-PNJRPC', 'ordertype': 'market', 'pair': 'LTCEUR',
             'price': Decimal('0'), 'price2': '0', 'type': 'sell', 'vol': Decimal('1.99739000'),
             'vol_exec': Decimal('1.99739000')},
            {'closetm': pandas.Timestamp('2017-07-14 14:47:26'), 'cost': Decimal('371.99351'),
             'fee': Decimal('0.59519'), 'leverage': 'none', 'order_id': 'OX3HS2-FWF56-WBWTEV', 'ordertype': 'limit',
             'pair': 'ETHEUR', 'price': Decimal('170.50001'), 'price2': '0', 'type': 'sell',
             'vol': Decimal('2.18178000'), 'vol_exec': Decimal('2.18178000')},
            {'closetm': pandas.Timestamp('2017-07-13 09:33:59'), 'cost': Decimal('2.117642'),
             'fee': Decimal('0.005505'), 'leverage': 'none', 'order_id': 'OSWBSJ-KQI6S-DXTTQM', 'ordertype': 'market',
             'pair': 'EOSETH', 'price': Decimal('0'), 'price2': '0', 'type': 'sell', 'vol': Decimal('253.76180000'),
             'vol_exec': Decimal('253.76180000')},
            {'closetm': pandas.Timestamp('2017-07-13 08:24:28'), 'cost': Decimal('0.004486'),
             'fee': Decimal('0.000012'), 'leverage': 'none', 'order_id': 'OBHUW4-TJ7YP-RLHQ4D', 'ordertype': 'market',
             'pair': 'ETHXBT', 'price': Decimal('0'), 'price2': '0', 'type': 'buy', 'vol': Decimal('0.05000000'),
             'vol_exec': Decimal('0.05000000')},
            {'closetm': pandas.Timestamp('2017-07-13 08:22:29'), 'cost': Decimal('0.009623'),
             'fee': Decimal('0.000025'), 'leverage': 'none', 'order_id': 'O46K5J-WU7YO-COB3FS', 'ordertype': 'market',
             'pair': 'LTCXBT', 'price': Decimal('0'), 'price2': '0', 'type': 'sell', 'vol': Decimal('0.50000000'),
             'vol_exec': Decimal('0.50000000')},
            {'closetm': pandas.Timestamp('2017-07-13 08:19:25'), 'cost': Decimal('0.000900'),
             'fee': Decimal('0.000002'), 'leverage': 'none', 'order_id': 'OSWKIU-PLZMI-VX3UYG', 'ordertype': 'market',
             'pair': 'ETHXBT', 'price': Decimal('0'), 'price2': '0', 'type': 'buy', 'vol': Decimal('0.01000000'),
             'vol_exec': Decimal('0.01000000')},
            {'closetm': pandas.Timestamp('2017-07-13 08:16:01'), 'cost': Decimal('0.000871'),
             'fee': Decimal('0.000002'), 'leverage': 'none', 'order_id': 'OUT46P-3FCZ4-IBIIM3', 'ordertype': 'market',
             'pair': 'ETHXBT', 'price': Decimal('0'), 'price2': '0', 'type': 'buy', 'vol': Decimal('0.00964000'),
             'vol_exec': Decimal('0.00964000')},
            {'closetm': pandas.Timestamp('2017-07-13 08:12:57'), 'cost': Decimal('0.009649'),
             'fee': Decimal('0.000025'), 'leverage': 'none', 'order_id': 'OIXSHR-DYHSO-VGSA6N', 'ordertype': 'limit',
             'pair': 'LTCXBT', 'price': Decimal('0.019298'), 'price2': '0', 'type': 'sell',
             'vol': Decimal('0.50000000'), 'vol_exec': Decimal('0.50000000')},
            {'closetm': pandas.Timestamp('2017-07-12 07:55:30'), 'cost': Decimal('0.188650'),
             'fee': Decimal('0.000302'), 'leverage': 'none', 'order_id': 'OXOKNF-MPYDL-JZLM2I', 'ordertype': 'limit',
             'pair': 'ETHXBT', 'price': Decimal('0.083850'), 'price2': '0', 'type': 'buy', 'vol': Decimal('2.24985092'),
             'vol_exec': Decimal('2.24985092')},
            {'closetm': pandas.Timestamp('2017-07-12 07:52:27'), 'cost': Decimal('0.057900'),
             'fee': Decimal('0.000150'), 'leverage': 'none', 'order_id': 'OWLPQZ-F5RTF-R4DRVS', 'ordertype': 'limit',
             'pair': 'LTCXBT', 'price': Decimal('0.019311'), 'price2': '0', 'type': 'buy', 'vol': Decimal('3.00000000'),
             'vol_exec': Decimal('3.00000000')}]

        orders = parse_orders(pandas.DataFrame(kraken_data))
        eur_value = float(orders[['asset', 'qty']].groupby('asset').sum().loc['EUR']['qty'])
        self.assertSequenceEqual(orders.columns.tolist(), ('date', 'asset', 'qty', 'fee', 'exchange'))
        self.assertAlmostEqual(eur_value, 8.908523, places=6)

    def tearDown(self):
        pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()
