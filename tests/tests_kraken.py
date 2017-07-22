import unittest
import logging
import os

import pandas
from decimal import Decimal

from exchanges.kraken import parse_trades, parse_flows


class TestKrakenAPI(unittest.TestCase):
    """
    Testing P&L calculation from Bittrex.
    """

    def setUp(self):
        pass

    def test_parsing_flows(self):
        deposits = {'L4WZSK-XKUFF-ULRXRT': {'refid': 'QCCILXH-7D774O-Z6YXO6', 'time': Decimal('1500621623.3172'),
                                            'type': 'deposit', 'aclass': 'currency', 'asset': 'ZEUR',
                                            'amount': '500.0000', 'fee': '0.0000', 'balance': '507.3638'},
                    'LN22GR-D7JEM-WREQHG': {'refid': 'Q4G46G5-J4EVIZ-XCEZZJ', 'time': Decimal('1499930741.5098'),
                                            'type': 'deposit', 'aclass': 'currency', 'asset': 'EOS',
                                            'amount': '255.7618000000', 'fee': '2.0000000000',
                                            'balance': '253.7618000000'},
                    'LJNJSI-JASTK-4UCSVT': {'refid': 'QCCHOFH-SNP7O4-ZC7TF4', 'time': Decimal('1499758934.1354'),
                                            'type': 'deposit', 'aclass': 'currency', 'asset': 'ZEUR',
                                            'amount': '500.0000', 'fee': '0.0000', 'balance': '500.0000'}}
        withdrawals = {'L72ZCB-ZJBKY-7ZXENF': {'refid': 'A2B3JVQ-4QMY6G-VF4HWM', 'time': Decimal('1499845194.9223'),
                                               'type': 'withdrawal', 'aclass': 'currency', 'asset': 'XETH',
                                               'amount': '-2.2413500000', 'fee': '0.0050000000',
                                               'balance': '0.0000065900'}}
        flows = parse_flows(withdrawals, deposits)
        self.assertSequenceEqual(flows.columns.tolist(), ('date', 'amount', 'asset', 'fee', 'exchange'))
        self.assertAlmostEqual(float(flows[flows['asset'] == 'EOS']['amount'].sum()), 255.7618, places=6)

    def test_parsing_trades(self):
        kraken_data = {'TT3LTK-3SQ6Z-OHGAGM': {'ordertxid': 'OG2LFR-5PILY-SR72PU', 'pair': 'XXRPZEUR',
                                               'time': Decimal('1500716896.3376'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '0.16098900', 'cost': '160.98900000', 'fee': '0.41857140',
                                               'vol': '1000.00000000', 'margin': '0.00000000', 'misc': ''},
                       'TKZ7Q5-5YIB3-MII3BK': {'ordertxid': 'O54IUK-BCKHA-CHQ77D', 'pair': 'XETHZEUR',
                                               'time': Decimal('1500716390.8499'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '192.20000', 'cost': '192.20000', 'fee': '0.30752',
                                               'vol': '1.00000000', 'margin': '0.00000', 'misc': ''},
                       'TD2SRE-EOASM-NDOV73': {'ordertxid': 'ORRIGN-NSRTN-PKG6VE', 'pair': 'XXBTZEUR',
                                               'time': Decimal('1500358412.1716'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '1943.42800', 'cost': '19.93259', 'fee': '0.03189',
                                               'vol': '0.01025641', 'margin': '0.00000', 'misc': ''},
                       'TXDFWF-WBGBS-7J7DYX': {'ordertxid': 'OWFI52-YZ57H-6DQ5VJ', 'pair': 'XLTCZEUR',
                                               'time': Decimal('1500320538.6229'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '36.69997', 'cost': '104.24887', 'fee': '0.16680',
                                               'vol': '2.84057098', 'margin': '0.00000', 'misc': ''},
                       'TSOBY5-R6NCW-MNI4KT': {'ordertxid': 'OWFI52-YZ57H-6DQ5VJ', 'pair': 'XLTCZEUR',
                                               'time': Decimal('1500320473.3459'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '36.69396', 'cost': '36.32702', 'fee': '0.05812',
                                               'vol': '0.99000000', 'margin': '0.00000', 'misc': ''},
                       'TXBH2R-MID32-UYFQHH': {'ordertxid': 'OWFI52-YZ57H-6DQ5VJ', 'pair': 'XLTCZEUR',
                                               'time': Decimal('1500320473.2885'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '36.69294', 'cost': '36.32601', 'fee': '0.05812',
                                               'vol': '0.99000000', 'margin': '0.00000', 'misc': ''},
                       'TLNY22-3SUN5-JI7MTV': {'ordertxid': 'OWFI52-YZ57H-6DQ5VJ', 'pair': 'XLTCZEUR',
                                               'time': Decimal('1500320463.2831'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '36.52805', 'cost': '4.92984', 'fee': '0.00789',
                                               'vol': '0.13496048', 'margin': '0.00000', 'misc': ''},
                       'T4Q7WN-3YYAJ-ARFUQG': {'ordertxid': 'OWFI52-YZ57H-6DQ5VJ', 'pair': 'XLTCZEUR',
                                               'time': Decimal('1500320297.2712'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '36.69993', 'cost': '1.63199', 'fee': '0.00261',
                                               'vol': '0.04446854', 'margin': '0.00000', 'misc': ''},
                       'TWTEFQ-3NGJG-37ZTW3': {'ordertxid': 'OUE5BK-DZMBK-FXQMKA', 'pair': 'XETHZEUR',
                                               'time': Decimal('1500213751.2916'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '136.80013', 'cost': '136.80013', 'fee': '0.21888',
                                               'vol': '1.00000000', 'margin': '0.00000', 'misc': ''},
                       'TQUMO4-AG42S-2E3DMY': {'ordertxid': 'O5XWA6-P7PA7-P2DIW3', 'pair': 'XXRPZEUR',
                                               'time': Decimal('1500212763.0626'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '0.12630000', 'cost': '50.00000000', 'fee': '0.08000000',
                                               'vol': '395.88281868', 'margin': '0.00000000', 'misc': ''},
                       'TZRI7X-UVZ4V-QKSFPO': {'ordertxid': 'OBRMOO-KEPIU-YWTN4R', 'pair': 'XXRPZEUR',
                                               'time': Decimal('1500211868.0615'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '0.12510000', 'cost': '48.86718750', 'fee': '0.12705469',
                                               'vol': '390.62500000', 'margin': '0.00000000', 'misc': ''},
                       'TUMEYD-Q7ESV-DOXB67': {'ordertxid': 'OKAZSI-KTFYG-PNJRPC', 'pair': 'XLTCZEUR',
                                               'time': Decimal('1500036546.1022'), 'type': 'sell',
                                               'ordertype': 'market', 'price': '38.03867', 'cost': '74.83246',
                                               'fee': '0.19456', 'vol': '1.96727334', 'margin': '0.00000', 'misc': ''},
                       'TKOMOP-6LPCZ-FPJQEO': {'ordertxid': 'OKAZSI-KTFYG-PNJRPC', 'pair': 'XLTCZEUR',
                                               'time': Decimal('1500036546.0922'), 'type': 'sell',
                                               'ordertype': 'market', 'price': '38.03871', 'cost': '1.14560',
                                               'fee': '0.00298', 'vol': '0.03011666', 'margin': '0.00000', 'misc': ''},
                       'T3N7SN-KV66R-7YRVRK': {'ordertxid': 'OX3HS2-FWF56-WBWTEV', 'pair': 'XETHZEUR',
                                               'time': Decimal('1500036446.361'), 'type': 'sell', 'ordertype': 'limit',
                                               'price': '170.50001', 'cost': '371.99351', 'fee': '0.59519',
                                               'vol': '2.18178000', 'margin': '0.00000', 'misc': ''},
                       'TCCGJY-N2HO3-PZ764O': {'ordertxid': 'OSWBSJ-KQI6S-DXTTQM', 'pair': 'EOSETH',
                                               'time': Decimal('1499931239.9109'), 'type': 'sell',
                                               'ordertype': 'market', 'price': '0.00834500', 'cost': '2.11764222',
                                               'fee': '0.00550587', 'vol': '253.76180000', 'margin': '0.00000000',
                                               'misc': ''},
                       'T2IBOD-KIPFH-XR54MB': {'ordertxid': 'OBHUW4-TJ7YP-RLHQ4D', 'pair': 'XETHXXBT',
                                               'time': Decimal('1499927068.1844'), 'type': 'buy', 'ordertype': 'market',
                                               'price': '0.089713', 'cost': '0.004486', 'fee': '0.000012',
                                               'vol': '0.05000000', 'margin': '0.000000', 'misc': ''},
                       'TPWQDL-E6T3S-BIUOQO': {'ordertxid': 'O46K5J-WU7YO-COB3FS', 'pair': 'XLTCXXBT',
                                               'time': Decimal('1499926949.1648'), 'type': 'sell',
                                               'ordertype': 'market', 'price': '0.01924700', 'cost': '0.00962350',
                                               'fee': '0.00002502', 'vol': '0.50000000', 'margin': '0.00000000',
                                               'misc': ''},
                       'TKAXGT-TZ2YV-CFJUDA': {'ordertxid': 'OSWKIU-PLZMI-VX3UYG', 'pair': 'XETHXXBT',
                                               'time': Decimal('1499926765.4976'), 'type': 'buy', 'ordertype': 'market',
                                               'price': '0.089988', 'cost': '0.000900', 'fee': '0.000002',
                                               'vol': '0.01000000', 'margin': '0.000000', 'misc': ''},
                       'TBWKBZ-2HLYF-35Y6LL': {'ordertxid': 'OUT46P-3FCZ4-IBIIM3', 'pair': 'XETHXXBT',
                                               'time': Decimal('1499926561.7059'), 'type': 'buy', 'ordertype': 'market',
                                               'price': '0.090372', 'cost': '0.000871', 'fee': '0.000002',
                                               'vol': '0.00964000', 'margin': '0.000000', 'misc': ''},
                       'TEY4TQ-KKWWB-IH6D5B': {'ordertxid': 'OIXSHR-DYHSO-VGSA6N', 'pair': 'XLTCXXBT',
                                               'time': Decimal('1499926377.6599'), 'type': 'sell', 'ordertype': 'limit',
                                               'price': '0.01929800', 'cost': '0.00964900', 'fee': '0.00002509',
                                               'vol': '0.50000000', 'margin': '0.00000000', 'misc': ''},
                       'TPAM55-GR362-IQJF7E': {'ordertxid': 'OXOKNF-MPYDL-JZLM2I', 'pair': 'XETHXXBT',
                                               'time': Decimal('1499838930.3345'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '0.083850', 'cost': '0.188650', 'fee': '0.000302',
                                               'vol': '2.24985092', 'margin': '0.000000', 'misc': ''},
                       'TFHXK4-6DYO7-RK5NMB': {'ordertxid': 'OWLPQZ-F5RTF-R4DRVS', 'pair': 'XLTCXXBT',
                                               'time': Decimal('1499838747.238'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '0.01930000', 'cost': '0.05790000', 'fee': '0.00015054',
                                               'vol': '3.00000000', 'margin': '0.00000000', 'misc': ''},
                       'TYJDNY-HA434-ISPIEM': {'ordertxid': 'O2ZJEH-L2YVT-JK6BHF', 'pair': 'XXBTZEUR',
                                               'time': Decimal('1499838634.9954'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '2023.44000', 'cost': '165.26600', 'fee': '0.26443',
                                               'vol': '0.08167576', 'margin': '0.00000', 'misc': ''},
                       'TTC6V6-AGN6P-XVINYL': {'ordertxid': 'O2ZJEH-L2YVT-JK6BHF', 'pair': 'XXBTZEUR',
                                               'time': Decimal('1499838634.9862'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '2023.44000', 'cost': '62.33678', 'fee': '0.09974',
                                               'vol': '0.03080733', 'margin': '0.00000', 'misc': ''},
                       'TIZJAC-3O4WF-HHTCAU': {'ordertxid': 'O2ZJEH-L2YVT-JK6BHF', 'pair': 'XXBTZEUR',
                                               'time': Decimal('1499838634.9294'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '2023.44000', 'cost': '198.69375', 'fee': '0.31791',
                                               'vol': '0.09819602', 'margin': '0.00000', 'misc': ''},
                       'T3DLGY-FPVOE-EEYGEQ': {'ordertxid': 'O2ZJEH-L2YVT-JK6BHF', 'pair': 'XXBTZEUR',
                                               'time': Decimal('1499838628.2415'), 'type': 'buy', 'ordertype': 'limit',
                                               'price': '2023.44000', 'cost': '73.16874', 'fee': '0.11707',
                                               'vol': '0.03616057', 'margin': '0.00000', 'misc': ''}}
        trades = parse_trades(kraken_data)
        eur_value = float(trades[['asset', 'qty']].groupby('asset').sum().loc['EUR']['qty'])
        self.assertSequenceEqual(trades.columns.tolist(), ('date', 'asset', 'qty', 'fee', 'exchange'))
        self.assertAlmostEqual(eur_value, -843.74633750, places=8)

    def tearDown(self):
        pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    unittest.main()
