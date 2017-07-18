import json
import logging
from os import path
import argparse
import os

from exchanges import kraken

_DEFAULT_CONFIG_FILE = 'config.json'


def main():
    parser = argparse.ArgumentParser(description='Updating SCBI spreadsheet',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    help_msg_config = 'location of config file, using "{}" by default'
    parser.add_argument('--config',
                        metavar='CONFIG_JSON',
                        type=str,
                        help=help_msg_config.format(_DEFAULT_CONFIG_FILE),
                        default=_DEFAULT_CONFIG_FILE
                        )

    args = parser.parse_args()
    full_config_path = os.path.abspath(args.config)
    logging.info('reading config from "%s"', full_config_path)
    if not os.path.isfile(full_config_path):
        raise RuntimeError('unable to load config file: {}'.format(full_config_path))

    config_json = json.load(open(args.config, 'rt'))
    api_key = config_json['exchanges']['kraken']['key']
    secret_key = config_json['exchanges']['kraken']['secret']

    kraken.connect(api_key, secret_key)
    print(kraken.get_balances())
    print(kraken.get_closed_orders())


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

{'ORRIGN-NSRTN-PKG6VE': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1500358395.7263,
                         'closetm': 1500358412.1722, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'XBTEUR', 'type': 'buy', 'ordertype': 'limit', 'price': '1950.000',
                                   'price2': '0', 'leverage': 'none',
                                   'order': 'buy 0.01025641 XBTEUR @ limit 1950.000'}, 'vol': '0.01025641',
                         'vol_exec': '0.01025641', 'cost': '19.932', 'fee': '0.031', 'price': '1943.427', 'misc': '',
                         'oflags': 'fciq', 'trades': ['TD2SRE-EOASM-NDOV73']},
 'OWFI52-YZ57H-6DQ5VJ': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1500320279.6568,
                         'closetm': 1500320538.6233, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'LTCEUR', 'type': 'buy', 'ordertype': 'limit', 'price': '36.70000',
                                   'price2': '0', 'leverage': 'none',
                                   'order': 'buy 5.00000000 LTCEUR @ limit 36.70000'}, 'vol': '5.00000000',
                         'vol_exec': '5.00000000', 'cost': '183.46373', 'fee': '0.29354', 'price': '36.69275',
                         'misc': '', 'oflags': 'fciq',
                         'trades': ['TXBH2R-MID32-UYFQHH', 'TSOBY5-R6NCW-MNI4KT', 'T4Q7WN-3YYAJ-ARFUQG',
                                    'TLNY22-3SUN5-JI7MTV', 'TXDFWF-WBGBS-7J7DYX']},
 'OUE5BK-DZMBK-FXQMKA': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1500213640.125,
                         'closetm': 1500213751.292, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'ETHEUR', 'type': 'buy', 'ordertype': 'limit', 'price': '137.00000',
                                   'price2': '0', 'leverage': 'none',
                                   'order': 'buy 1.00000000 ETHEUR @ limit 137.00000'}, 'vol': '1.00000000',
                         'vol_exec': '1.00000000', 'cost': '136.80013', 'fee': '0.21888', 'price': '136.80013',
                         'misc': '', 'oflags': 'fciq', 'trades': ['TWTEFQ-3NGJG-37ZTW3']},
 'O5XWA6-P7PA7-P2DIW3': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1500212395.972,
                         'closetm': 1500212763.063, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'XRPEUR', 'type': 'buy', 'ordertype': 'limit', 'price': '0.126300',
                                   'price2': '0', 'leverage': 'none',
                                   'order': 'buy 395.88281868 XRPEUR @ limit 0.126300'}, 'vol': '395.88281868',
                         'vol_exec': '395.88281868', 'cost': '50.000000', 'fee': '0.080000', 'price': '0.126300',
                         'misc': '', 'oflags': 'fciq', 'trades': ['TQUMO4-AG42S-2E3DMY']},
 'OBRMOO-KEPIU-YWTN4R': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1500211865.4638,
                         'closetm': 1500211868.0617, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'XRPEUR', 'type': 'buy', 'ordertype': 'limit', 'price': '0.128000',
                                   'price2': '0', 'leverage': 'none',
                                   'order': 'buy 390.62500000 XRPEUR @ limit 0.128000'}, 'vol': '390.62500000',
                         'vol_exec': '390.62500000', 'cost': '48.867187', 'fee': '0.127054', 'price': '0.125100',
                         'misc': '', 'oflags': 'fciq', 'trades': ['TZRI7X-UVZ4V-QKSFPO']},
 'OKAZSI-KTFYG-PNJRPC': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1500036544.5181,
                         'closetm': 1500036546.1024, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'LTCEUR', 'type': 'sell', 'ordertype': 'market', 'price': '0', 'price2': '0',
                                   'leverage': 'none', 'order': 'sell 1.99739000 LTCEUR @ market'}, 'vol': '1.99739000',
                         'vol_exec': '1.99739000', 'cost': '75.97806', 'fee': '0.19754', 'price': '38.03867',
                         'misc': '', 'oflags': 'fciq', 'trades': ['TKOMOP-6LPCZ-FPJQEO', 'TUMEYD-Q7ESV-DOXB67']},
 'OX3HS2-FWF56-WBWTEV': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1500036370.2567,
                         'closetm': 1500036446.3614, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'ETHEUR', 'type': 'sell', 'ordertype': 'limit', 'price': '170.50001',
                                   'price2': '0', 'leverage': 'none',
                                   'order': 'sell 2.18178000 ETHEUR @ limit 170.50001'}, 'vol': '2.18178000',
                         'vol_exec': '2.18178000', 'cost': '371.99351', 'fee': '0.59519', 'price': '170.50001',
                         'misc': '', 'oflags': 'fciq', 'trades': ['T3N7SN-KV66R-7YRVRK']},
 'OSWBSJ-KQI6S-DXTTQM': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1499931235.3944,
                         'closetm': 1499931239.9111, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'EOSETH', 'type': 'sell', 'ordertype': 'market', 'price': '0', 'price2': '0',
                                   'leverage': 'none', 'order': 'sell 253.76180000 EOSETH @ market'},
                         'vol': '253.76180000', 'vol_exec': '253.76180000', 'cost': '2.117642', 'fee': '0.005505',
                         'price': '0.008345', 'misc': '', 'oflags': 'fciq', 'trades': ['TCCGJY-N2HO3-PZ764O']},
 'OBHUW4-TJ7YP-RLHQ4D': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1499927067.5728,
                         'closetm': 1499927068.1846, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'ETHXBT', 'type': 'buy', 'ordertype': 'market', 'price': '0', 'price2': '0',
                                   'leverage': 'none', 'order': 'buy 0.05000000 ETHXBT @ market'}, 'vol': '0.05000000',
                         'vol_exec': '0.05000000', 'cost': '0.004486', 'fee': '0.000012', 'price': '0.089720',
                         'misc': '', 'oflags': 'fciq', 'trades': ['T2IBOD-KIPFH-XR54MB']},
 'O46K5J-WU7YO-COB3FS': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1499926947.7688,
                         'closetm': 1499926949.165, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'LTCXBT', 'type': 'sell', 'ordertype': 'market', 'price': '0', 'price2': '0',
                                   'leverage': 'none', 'order': 'sell 0.50000000 LTCXBT @ market'}, 'vol': '0.50000000',
                         'vol_exec': '0.50000000', 'cost': '0.009623', 'fee': '0.000025', 'price': '0.019247',
                         'misc': '', 'oflags': 'fcib', 'trades': ['TPWQDL-E6T3S-BIUOQO']},
 'OSWKIU-PLZMI-VX3UYG': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1499926762.8833,
                         'closetm': 1499926765.4977, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'ETHXBT', 'type': 'buy', 'ordertype': 'market', 'price': '0', 'price2': '0',
                                   'leverage': 'none', 'order': 'buy 0.01000000 ETHXBT @ market'}, 'vol': '0.01000000',
                         'vol_exec': '0.01000000', 'cost': '0.000900', 'fee': '0.000002', 'price': '0.090000',
                         'misc': '', 'oflags': 'fciq', 'trades': ['TKAXGT-TZ2YV-CFJUDA']},
 'OUT46P-3FCZ4-IBIIM3': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1499926557.1823,
                         'closetm': 1499926561.706, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'ETHXBT', 'type': 'buy', 'ordertype': 'market', 'price': '0', 'price2': '0',
                                   'leverage': 'none', 'order': 'buy 0.00964000 ETHXBT @ market'}, 'vol': '0.00964000',
                         'vol_exec': '0.00964000', 'cost': '0.000871', 'fee': '0.000002', 'price': '0.090353',
                         'misc': '', 'oflags': 'fciq', 'trades': ['TBWKBZ-2HLYF-35Y6LL']},
 'OIXSHR-DYHSO-VGSA6N': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1499926375.1952,
                         'closetm': 1499926377.6601, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'LTCXBT', 'type': 'sell', 'ordertype': 'limit', 'price': '0.019298',
                                   'price2': '0', 'leverage': 'none',
                                   'order': 'sell 0.50000000 LTCXBT @ limit 0.019298'}, 'vol': '0.50000000',
                         'vol_exec': '0.50000000', 'cost': '0.009649', 'fee': '0.000025', 'price': '0.019298',
                         'misc': '', 'oflags': 'fcib', 'trades': ['TEY4TQ-KKWWB-IH6D5B']},
 'OXOKNF-MPYDL-JZLM2I': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1499838927.3371,
                         'closetm': 1499838930.3352, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'ETHXBT', 'type': 'buy', 'ordertype': 'limit', 'price': '0.083850',
                                   'price2': '0', 'leverage': 'none',
                                   'order': 'buy 2.24985092 ETHXBT @ limit 0.083850'}, 'vol': '2.24985092',
                         'vol_exec': '2.24985092', 'cost': '0.188650', 'fee': '0.000302', 'price': '0.083850',
                         'misc': '', 'oflags': 'fciq', 'trades': ['TPAM55-GR362-IQJF7E']},
 'OWLPQZ-F5RTF-R4DRVS': {'refid': None, 'userref': None, 'status': 'closed', 'reason': None, 'opentm': 1499838746.4842,
                         'closetm': 1499838747.2381, 'starttm': 0, 'expiretm': 0,
                         'descr': {'pair': 'LTCXBT', 'type': 'buy', 'ordertype': 'limit', 'price': '0.019311',
                                   'price2': '0', 'leverage': 'none',
                                   'order': 'buy 3.00000000 LTCXBT @ limit 0.019311'}, 'vol': '3.00000000',
                         'vol_exec': '3.00000000', 'cost': '0.057900', 'fee': '0.000150', 'price': '0.019300',
                         'misc': '', 'oflags': 'fciq', 'trades': ['TFHXK4-6DYO7-RK5NMB']}}
