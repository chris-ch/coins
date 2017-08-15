import argparse
import json
import logging
import os
from os import path
from datetime import datetime

import pandas
import gspread

from gservices import save_sheet, authorize_services
from sbcireport import compute_balances, extend_balances, compute_pnl

_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config.json'))
_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE = os.sep.join(('.', 'google-service-account-creds.json'))
_SHEET_TAB_PRICES = 'Prices'
_SHEET_TAB_PNL = 'PnL'
_DEFAULT_DATA_PATH = '.'
_DEFAULT_REPORTING_CURRENCY = 'ETH'
_DEFAULT_INCEPTION_DATE = '2017-06-01'


def process_spreadsheet(credentials_file, spreadsheet_id, prices, pnl_history_records):
    """

    :param credentials_file:
    :param spreadsheet_id:
    :param prices:
    :param balance_history:
    :param skip_google_update:
    :param pnl_start:
    :return:
    """
    header_prices = [field for field in prices.reset_index().columns.tolist() if field != 'index']
    authorized_http, credentials = authorize_services(credentials_file)
    svc_sheet = gspread.authorize(credentials)
    header_pnl = ['date', 'Portfolio P&L'] + [column for column in pnl_history_records.columns if
                                                  column not in ('date', 'Portfolio P&L')]
    pnl_history_records = pnl_history_records[header_pnl]
    price_records = prices.sort_values('date', ascending=False).to_dict(orient='records')
    save_sheet(svc_sheet, spreadsheet_id, _SHEET_TAB_PRICES, header_prices, price_records)
    save_sheet(svc_sheet, spreadsheet_id, _SHEET_TAB_PNL, header_pnl, pnl_history_records.to_dict(orient='records'))


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
    help_msg_creds = 'location of Google Service Account Credentials file, using "{}" by default'
    parser.add_argument('--google-creds',
                        metavar='GOOGLE_SERVICE_ACCOUNT_CREDS_JSON',
                        type=str,
                        help=help_msg_creds.format(_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE),
                        default=_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE
                        )
    help_msg_ref_cur = 'comma-separated list of reference currencies'
    parser.add_argument('--reference-pairs',
                        type=str,
                        help=help_msg_ref_cur,
                        default='BTC.USD,BTC.EUR,ETH.USD,ETH.EUR'
                        )
    help_msg_skip_google_update = 'skip updating Google Sheet'
    parser.add_argument('--skip-google-update',
                        action='store_true',
                        help=help_msg_skip_google_update
                        )
    help_msg_data_path = 'location of data path, using "{}" by default'
    parser.add_argument('--data',
                        metavar='DATA_PATH',
                        type=str,
                        help=help_msg_data_path.format(_DEFAULT_DATA_PATH),
                        default=_DEFAULT_DATA_PATH
                        )
    help_msg_data_path = 'reporting currency, using "{}" by default'
    parser.add_argument('--reporting-currency',
                        type=str,
                        help=help_msg_data_path.format(_DEFAULT_REPORTING_CURRENCY),
                        default=_DEFAULT_REPORTING_CURRENCY
                        )

    help_msg_data_path = 'inception date, using "{}" by default'
    parser.add_argument('--inception-date',
                        type=str,
                        help=help_msg_data_path.format(_DEFAULT_INCEPTION_DATE),
                        default=_DEFAULT_INCEPTION_DATE
                        )

    args = parser.parse_args()
    full_creds_path = os.path.abspath(args.google_creds)
    logging.info('reading Google Service Account credentials from "%s"', full_creds_path)
    if not os.path.isfile(full_creds_path):
        raise RuntimeError('unable to load Google Service Account credentials file: {}'.format(full_creds_path))

    config_json = json.load(open(args.config, 'rt'))
    full_data_path = os.path.abspath(args.data)
    if os.path.isfile(full_data_path):
        raise RuntimeError('not a directory: {}'.format(full_data_path))

    full_data_path = os.path.abspath(args.data)
    if not os.path.isdir(full_data_path):
        raise RuntimeError('not a directory: {}'.format(full_data_path))

    flows = pandas.read_pickle(os.sep.join([full_data_path, 'flows.pkl']))
    trades = pandas.read_pickle(os.sep.join([full_data_path, 'trades.pkl']))
    prices_daily = pandas.read_pickle(os.sep.join([full_data_path, 'day_prices.pkl']))
    prices_hourly = pandas.read_pickle(os.sep.join([full_data_path, 'hour_prices.pkl']))
    prices_spot = pandas.read_pickle(os.sep.join([full_data_path, 'spot_prices.pkl']))
    prices = pandas.concat([prices_daily, prices_hourly, prices_spot]).sort_values('date', ascending=False)
    prices[args.reporting_currency] = 1
    reference_pairs = [(currency.split('.')[0], currency.split('.')[1]) for currency in args.reference_pairs.split(',')]

    reporting_pairs = ['/'.join(pair) for pair in reference_pairs]
    remaining_columns = set(prices.columns).difference(set(reporting_pairs))
    remaining_columns.discard('date')
    prices_out = prices[['date'] + reporting_pairs + list(remaining_columns)]

    reporting_currency = args.reporting_currency
    fund_inception_date = datetime.strptime(args.inception_date, '%Y-%m-%d')

    pnl_history_records = compute_pnl(reporting_currency, flows, prices, trades)
    if fund_inception_date is not None:
        pnl_history_records = pnl_history_records[pnl_history_records['date'] > fund_inception_date]

    if not args.skip_google_update:
        process_spreadsheet(args.google_creds, config_json['target_sheet_id'], prices_out, pnl_history_records)

    else:
        from matplotlib import pyplot
        pnl_history_records.set_index('date')['Portfolio P&L'].plot()
        pyplot.show()

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
