import argparse
import json
import logging
import os
from os import path
from datetime import datetime

from exchanges import bittrex
import pandas
import gspread

from cryptocompare import load_crypto_compare_data
from exchanges.bittrex import parse_flows, parse_orders
from gservices import save_sheet, authorize_services
from sbcireport import compute_balances, extend_balances, compute_balances_pnl, compute_pnl_history

_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE = os.sep.join(('.', 'google-service-account-creds.json'))
_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config.json'))
_DEFAULT_EXCHANGE = 'CCCAGG'
_SHEET_TAB_PRICES = 'Prices'
_SHEET_TAB_PNL = 'PnL'


def process_spreadsheet(credentials_file, spreadsheet_id, prices, pnl_history, skip_google_update=False,
                        pnl_start=None):
    """

    :param credentials_file:
    :param spreadsheet_id:
    :param prices:
    :param pnl_history:
    :param skip_google_update:
    :param pnl_start:
    :return:
    """
    if not skip_google_update:
        authorized_http, credentials = authorize_services(credentials_file)
        svc_sheet = gspread.authorize(credentials)
        header_prices = [field for field in prices.reset_index().columns.tolist() if field != 'index']
        logging.info('uploading {} rows for prices data'.format(prices.count().max()))
        price_records = prices.sort_values('date', ascending=False).to_dict(orient='records')
        save_sheet(svc_sheet, spreadsheet_id, _SHEET_TAB_PRICES, header_prices, price_records)
        logging.info('uploading {} rows for pnl data'.format(pnl_history.count().max()))
        pnl_history_records = pandas.DataFrame(pnl_history).reset_index().sort_values('date', ascending=False)
        if pnl_start is not None:
            pnl_history_records = pnl_history_records[pnl_history_records['date'] > pnl_start]

        header_pnl = ['date', pnl_history.name]
        save_sheet(svc_sheet, spreadsheet_id, _SHEET_TAB_PNL, header_pnl, pnl_history_records.to_dict(orient='records'))


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
    parser.add_argument('--reference-pairs',
                        type=str,
                        help=help_msg_ref_cur,
                        default='BTC.USD,BTC.EUR,ETH.USD,ETH.EUR'
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

    config_json = json.load(open(args.config, 'rt'))
    api_key = config_json['exchanges']['bittrex']['key']
    secret_key = config_json['exchanges']['bittrex']['secret']

    deposits, withdrawals, order_history, currencies = bittrex.retrieve_data(api_key, secret_key)
    reference_pairs = [(currency.split('.')[0], currency.split('.')[1]) for currency in args.reference_pairs.split(',')]

    if args.prices:
        prices = pandas.read_pickle(args.prices)

    else:
        reference_currencies = set([currency for pair in reference_pairs for currency in pair])
        prices = load_crypto_compare_data(currencies, reference_currencies, args.exchange)
        if args.record_prices:
            prices.to_pickle(args.record_prices)

    reporting_currency = 'USD'

    flows = parse_flows(withdrawals, deposits).set_index('date')
    balances_by_asset = compute_balances(flows)

    extended_balances, prices_selection = extend_balances(reporting_currency, balances_by_asset, prices)
    balances_in_reporting_currency = prices_selection * extended_balances.shift()
    balances_in_reporting_currency = balances_in_reporting_currency.fillna(0)
    balances_total = balances_in_reporting_currency.apply(sum, axis=1)
    balances_total.name = 'Portfolio P&L'

    balances_pnl = compute_balances_pnl(reporting_currency, balances_by_asset, prices)
    trades = parse_orders(order_history)
    pnl_history = compute_pnl_history(reporting_currency, prices, balances_pnl, trades)
    pnl_history.name = 'Portfolio P&L'

    config_json = json.load(open(args.config, 'rt'))
    reporting_pairs = ['/'.join(pair) for pair in reference_pairs]
    remaining_columns = set(prices.columns).difference(set(reporting_pairs))
    remaining_columns.discard('date')
    prices = prices[['date'] + reporting_pairs + list(remaining_columns)]
    process_spreadsheet(args.google_creds, config_json['target_sheet_id'], prices, balances_total,
                        skip_google_update=args.skip_google_update, pnl_start=datetime(2017, 6, 1))


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
