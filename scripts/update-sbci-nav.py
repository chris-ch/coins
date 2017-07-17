import argparse
import json
import logging
import os
from os import path
import pandas
import gspread

from cryptocompare import load_crypto_compare_data
from gservices import save_sheet, authorize_services

_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE = os.sep.join(('.', 'google-service-account-creds.json'))
_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config.json'))
_DEFAULT_EXCHANGE = 'CCCAGG'
_SHEET_TAB_PRICES = 'Prices'


def process_spreadsheet(credentials_file, spreadsheet_id, prices, portfolio, skip_google_update=False):
    authorized_http, credentials = authorize_services(credentials_file)
    svc_sheet = gspread.authorize(credentials)
    header = [field for field in prices.reset_index().columns.tolist() if field != 'index']
    price_records = prices.sort_values('date', ascending=False).to_dict(orient='records')
    if not skip_google_update:
        save_sheet(svc_sheet, spreadsheet_id, _SHEET_TAB_PRICES, header, price_records)


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
    help_msg_pairs = 'additional comma separated pairs of currencies for price retrieval'
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
    help_msg_ref_cur = 'comma-separated list of reference currencies'
    parser.add_argument('--reference-currencies',
                        type=str,
                        help=help_msg_ref_cur,
                        default='USD,BTC,ETH'
                        )
    help_msg_rec_prices = 'record prices to indicated file (using pickle)'
    parser.add_argument('--record-prices',
                        type=str,
                        help=help_msg_rec_prices
                        )
    help_msg_prices_exchange = 'exchange for loading prices'
    parser.add_argument('--exchange',
                        type=str,
                        help=help_msg_prices_exchange,
                        default=_DEFAULT_EXCHANGE
                        )
    help_msg_skip_google_update = 'skip updating Google Sheet'
    parser.add_argument('--skip-google-update',
                        action='store_true',
                        help=help_msg_skip_google_update
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

    portfolio = None  # TODO

    if args.prices:
        prices = pandas.read_pickle(args.prices)

    else:
        portfolio_target_currencies = set(portfolio.columns.tolist())
        portfolio_target_currencies.discard('date')
        prices = load_crypto_compare_data(portfolio_target_currencies, args.reference_currencies.split(','), args.exchange)
        if args.record_prices:
            prices.to_pickle(args.record_prices)

    config_json = json.load(open(args.config, 'rt'))
    process_spreadsheet(args.google_creds, config_json['target_sheet_id'], prices, portfolio, args.skip_google_update)


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
