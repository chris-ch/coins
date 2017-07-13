import argparse
import json
import logging
import os
from os import path
import pandas

from cryptocompare import load_crypto_compare_data
from gservices import setup_services, file_by_id, update_sheet, load_sheet

_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE = os.sep.join(('.', 'google-service-account-creds.json'))
_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config.json'))


def process_spreadsheet(credentials, spreadsheet_id, prices):
    svc_drive, svc_sheets = setup_services(credentials)
    spreadsheet_name = file_by_id(svc_drive, spreadsheet_id)
    logging.info('prepared Google sheet %s: %s', spreadsheet_name, spreadsheet_id)
    header = [field for field in prices.reset_index().columns.tolist() if field != 'index']
    records = prices.to_dict(orient='records')
    numbers_flag = [field for field in header if field != 'date']
    update_sheet(svc_sheets, spreadsheet_id, header, records, date_columns=('date', ), number_columns=numbers_flag)
    logging.info('saved sheet %s', spreadsheet_name)
    values = load_sheet(svc_sheets, spreadsheet_id)
    print(values)


def main():
    parser = argparse.ArgumentParser(description='Updating SCBI spreadsheet',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter
                                     )
    help_msg_creds = 'location of Google Service Account Credentials file, using "{}" by default'
    parser.add_argument('--google-creds',
                        metavar='GOOGLE_SERVICE_ACCOUNT_CREDS_JSON',
                        type=str,
                        help=help_msg_creds.format(_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE),
                        default=_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE
                        )
    help_msg_config = 'location of config file, using "{}" by default'
    parser.add_argument('--config',
                        metavar='CONFIG_JSON',
                        type=str,
                        help=help_msg_config.format(_DEFAULT_CONFIG_FILE),
                        default=_DEFAULT_CONFIG_FILE
                        )
    help_msg_pairs = 'comma separated pairs of currencies ("BTC/USD,ETH/USD,ETH/BTC,USDT/USD")'
    parser.add_argument('--pairs',
                        type=str,
                        help=help_msg_pairs,
                        default="BTC/USD,ETH/USD,ETH/BTC,USDT/USD"
                        )
    help_msg_prices = 'use indicated prices (pickled DataFrame) instead of querying CryptoCompare'
    parser.add_argument('--prices',
                        type=str,
                        help=help_msg_prices
                        )

    args = parser.parse_args()
    full_creds_path = os.path.abspath(args.google_creds)
    logging.info('reading Google Service Account credentials from "%s"', full_creds_path)
    if not os.path.isfile(full_creds_path):
        raise RuntimeError('unable to load Google Service Account credentials file: {}'.format(full_creds_path))

    full_config_path = os.path.abspath(args.config)
    logging.info('reading config from "%s"', full_config_path)
    if not os.path.isfile(full_config_path):
        raise RuntimeError('unable to load config file: {}'.format(full_config_path))

    target_pairs = [pair.split('/') for pair in args.pairs.split(',')]
    if args.prices:
        prices = pandas.read_pickle(args.prices)

    else:
        prices = load_crypto_compare_data(target_pairs)

    config_json = json.load(open(args.config, 'rt'))
    process_spreadsheet(args.google_creds, config_json['target_sheet_id'], prices)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    # logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
    # logging.getLogger('googleapicliet.discovery_cache').setLevel(logging.ERROR)
    file_handler = logging.FileHandler('{}.log'.format(path.basename(__file__).split('.')[0]), mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    try:
        main()

    except:
        logging.exception('error occured')
