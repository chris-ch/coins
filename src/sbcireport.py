import pandas
from collections import defaultdict
import logging

from pnl import AverageCostProfitAndLoss


def _select_prices(reporting_currency, prices):
    """

    :param reporting_currency:
    :param prices:
    :return:
    """
    all_prices = prices.set_index('date')
    filter_columns = [column for column in all_prices.columns if column.endswith(reporting_currency)]
    prices_selection = all_prices[filter_columns]
    prices_selection.columns = [column.split('/')[0] for column in prices_selection.columns]
    return prices_selection


def _include_indices(target_df, source_df):
    """
    Adds missing indices from source_df into target_df.

    :param target_df:
    :param source_df:
    :return:
    """
    complete_index = target_df.index.append(source_df.index)
    reindexed = target_df.reindex(complete_index)
    return reindexed.sort_index()


def compute_balances(flows, trades):
    """
    Balances by currency.
    :param flows:
    :param trades:
    :return:
    """
    flows = flows.append(trades).set_index('date')
    flows_by_asset = flows.pivot(columns='asset', values='amount').apply(pandas.to_numeric)
    balances = flows_by_asset.fillna(0).cumsum()
    return balances


def extend_balances(reporting_currency, balances, prices):
    """

    :param balances:
    :param reporting_currency:
    :param prices:
    :return:
    """
    prices_selection = _select_prices(reporting_currency, prices)
    # removes duplicates (TODO: find bug)
    prices_selection = prices_selection[~prices_selection.index.duplicated(keep='first')]
    prices_selection = _include_indices(prices_selection, balances).ffill()
    extended_balances = _include_indices(balances, prices_selection).ffill()
    # removing duplicates
    extended_balances = extended_balances.groupby('date').first()
    return extended_balances, prices_selection


def compute_balances_pnl(reporting_currency, balances, prices):
    """
    Output format (data expressed in terms of reporting currency):

              asset                       date  pnl
        10947   XRP 2017-07-17 09:00:00.000000  0.0
        10948   XRP 2017-07-17 10:00:00.000000  0.0
        10949   XRP 2017-07-17 10:04:06.200048  0.0
        10950   XRP 2017-07-17 10:35:09.143000  0.0
        10951   XRP 2017-07-17 10:35:09.143000  0.0

    :param reporting_currency:
    :param prices:
    :param balances:
    :return:
    """
    extended_balances, prices_selection = extend_balances(reporting_currency, balances, prices)
    performances = prices_selection.diff() * extended_balances.shift()
    cum_perf = performances.cumsum()
    formatted = cum_perf.unstack().reset_index().fillna(0).rename(columns={'level_0': 'asset', 0: 'pnl'})
    return formatted


def compute_trades_pnl(reporting_currency, prices, trades):
    """
    Trades P&L by asset expressed in the reporting currency.

    :param reporting_currency:
    :param prices:
    :param trades:
    :return: DataFrame (<index 'date'>, list of asset codes) containing pnl history for each asset
    """
    logging.debug('loaded orders:\n{}'.format(trades))
    if trades.empty:
        result = pandas.DataFrame({'asset': [], 'date': [], 'realized_pnl': [], 'total_pnl': [], 'unrealized_pnl': []})

    else:
        trades = trades.set_index('date')
        prices_selection = _select_prices(reporting_currency, prices)
        prices_selection[reporting_currency] = 1
        prices_selection = _include_indices(prices_selection, trades).ffill()
        pnl_tracker = defaultdict(AverageCostProfitAndLoss)
        pnl_data = list()
        for timestamp, price_row in prices_selection.iterrows():
            if timestamp in trades.index:
                current_trades = trades.loc[timestamp]
                for trade_ts, trade_row in current_trades.iterrows():
                    fees = trade_row['fee']
                    asset = trade_row['asset']
                    fill_qty = float(trade_row['amount'])
                    fill_price = price_row[asset]
                    pnl_tracker[asset].add_fill(fill_qty, fill_price, fees)
                    pnl_asset_data = {
                        'date': trade_ts,
                        'asset': asset,
                        'unrealized_pnl': pnl_tracker[asset].get_unrealized_pnl(fill_price),
                        'realized_pnl': pnl_tracker[asset].realized_pnl,
                        'total_pnl': pnl_tracker[asset].get_total_pnl(fill_price),
                    }
                    pnl_data.append(pnl_asset_data)
                    logging.info('*trade* added pnl data: {}'.format(pnl_asset_data))

            else:
                for asset in pnl_tracker:
                    pnl_asset_data = {
                        'date': timestamp,
                        'asset': asset,
                        'unrealized_pnl': pnl_tracker[asset].get_unrealized_pnl(price_row[asset]),
                        'realized_pnl': pnl_tracker[asset].realized_pnl,
                        'total_pnl': pnl_tracker[asset].get_total_pnl(price_row[asset]),
                    }
                    pnl_data.append(pnl_asset_data)
                    logging.info('added pnl data: {}'.format(pnl_asset_data))

        result = pandas.DataFrame(pnl_data)

    result_filtered = result[['date', 'asset', 'total_pnl']]
    pnl_by_currency = result_filtered.pivot_table(index='date', columns='asset', values='total_pnl')
    return pnl_by_currency.ffill()


def compute_pnl_history(reporting_currency, prices, balances_pnl, trades):
    """
    Output format (data expressed in terms of reporting currency):

      asset                                BTC  ETH  EUR  LTC      NEOS     START      STRAT         XRP
      date
      2017-07-17 08:00:00.000000 -2.703352e+06  0.0  0.0  0.0  0.005842  0.000238   0.041495  212.745259
      2017-07-17 09:00:00.000000 -2.766600e+06  0.0  0.0  0.0  0.006815  0.000801   0.067430  219.148809
      2017-07-17 10:00:00.000000 -2.744735e+06  0.0  0.0  0.0  0.000974 -0.000836  -0.015561  216.213848
      2017-07-17 10:04:06.200048 -2.744481e+06  0.0  0.0  0.0  0.000000  0.000000   0.000000  216.213848
      2017-07-17 10:35:09.143000           NaN  NaN  NaN  NaN       NaN       NaN        NaN         NaN

    :param reporting_currency: str currency code
    :param prices:
    :param withdrawals:
    :param deposits:
    :param trades:
    :return:
    """
    trades_pnl = compute_trades_pnl(reporting_currency, prices, trades)
    balances_pnl_by_asset = balances_pnl.groupby(['date', 'asset']).sum().unstack()['pnl']
    if trades_pnl.empty:
        pnl_history = balances_pnl_by_asset

    else:
        trades_pnl_by_asset = trades_pnl.groupby(['date', 'asset']).sum().unstack()['total_pnl']
        trades_pnl_by_asset = trades_pnl_by_asset.reindex(columns=balances_pnl_by_asset.columns).fillna(0)
        pnl_history = balances_pnl_by_asset + trades_pnl_by_asset

    return pnl_history.fillna(0)
