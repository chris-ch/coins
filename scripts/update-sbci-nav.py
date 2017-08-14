import argparse
import json
import logging
import os
from os import path
from datetime import datetime

import pandas
import gspread

from gservices import save_sheet, authorize_services
from sbcireport import compute_balances, extend_balances


_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config.json'))
_DEFAULT_GOOGLE_SVC_ACCT_CREDS_FILE = os.sep.join(('.', 'google-service-account-creds.json'))
_SHEET_TAB_PRICES = 'Prices'
_SHEET_TAB_PNL = 'PnL'
_DEFAULT_DATA_PATH = '.'
_DEFAULT_REPORTING_CURRENCY = 'ETH'
_DEFAULT_INCEPTION_DATE = '2017-06-01'


def process_spreadsheet(credentials_file, spreadsheet_id, prices, balance_history, skip_google_update=False,
                        pnl_start=None):
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
    logging.info('uploading {} rows for prices data'.format(prices.count().max()))
    price_records = prices.sort_values('date', ascending=False).to_dict(orient='records')
    logging.info('uploading {} rows for pnl data'.format(balance_history.count().max()))
    pnl_history_records = balance_history.sort_values('date', ascending=False)
    if pnl_start is not None:
        pnl_history_records = pnl_history_records[pnl_history_records['date'] > pnl_start]

    header_pnl = ['date', 'Portfolio P&L'] + [column for column in pnl_history_records.columns if
                                                  column not in ('date', 'Portfolio P&L')]
    pnl_history_records = pnl_history_records[header_pnl]

    from matplotlib import pyplot
    pnl_history_records.set_index('date')['Portfolio P&L'].plot()
    pyplot.show()
    if not skip_google_update:
        authorized_http, credentials = authorize_services(credentials_file)
        svc_sheet = gspread.authorize(credentials)
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

    balances_by_asset = compute_balances(flows, trades)
    extended_balances, prices_selection = extend_balances(reporting_currency, balances_by_asset, prices)
    balances_in_reporting_currency = prices_selection * extended_balances
    balances_in_reporting_currency = balances_in_reporting_currency.fillna(0)
    balances_in_reporting_currency['Portfolio P&L'] = balances_in_reporting_currency.apply(sum, axis=1)
    balances_in_reporting_currency.reset_index(inplace=True)

    segments = breakdown_flows(balances_by_asset, balances_in_reporting_currency)
    # linking segments and normalizing
    previous_level = 1
    normalized = pandas.Series()
    for segment in segments:
        if not segment.empty:
            logging.info('processing segment:\n{}'.format(segment))
            current_normalized = segment * previous_level / segment.iloc[0]
            normalized = normalized.append(current_normalized)
            logging.info('normalized segment:\n{}'.format(current_normalized))
            previous_level = current_normalized.iloc[-1]

    balances_in_reporting_currency['Portfolio P&L'] = normalized
    balances_in_reporting_currency['Portfolio P&L'].ffill(inplace=True)
    balances_in_reporting_currency['Portfolio P&L'].fillna(1, inplace=True)
    process_spreadsheet(args.google_creds, config_json['target_sheet_id'], prices_out, balances_in_reporting_currency,
                        skip_google_update=args.skip_google_update, pnl_start=fund_inception_date)


def breakdown_flows(balances_by_asset, balances):
    logging.info('flows:\n{}'.format(balances_by_asset))
    flow_dates = balances_by_asset.reset_index()['date']
    timespans = pandas.DataFrame({'start': flow_dates, 'end': flow_dates.shift(-1)}, columns=['start', 'end'])
    balances_segments = list()
    for index, timespan in timespans.iterrows():
        start_date = timespan['start']
        end_date = timespan['end']
        logging.info('{} --> {}'.format(start_date, end_date))
        if end_date == pandas.NaT:
            timespan_filter = (balances['date'] >= start_date)

        else:
            timespan_filter = ((balances['date'] >= start_date)
                               & (balances['date'] < end_date))

        current_balances_by_asset = balances[timespan_filter]
        balances_segments.append(current_balances_by_asset['Portfolio P&L'])

    return balances_segments

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
