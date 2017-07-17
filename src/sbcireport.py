import pandas
from collections import defaultdict

from exchanges.bittrex import parse_orders, parse_flows
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

    :param target_df:
    :param source_df:
    :return:
    """
    return target_df.reindex(target_df.index.append(source_df.index)).sort_index()


def compute_balances_pnl(reporting_currency, prices, withdrawals, deposits):
    """
    Output format:

              asset                       date  pnl
        10947   XRP 2017-07-17 09:00:00.000000  0.0
        10948   XRP 2017-07-17 10:00:00.000000  0.0
        10949   XRP 2017-07-17 10:04:06.200048  0.0
        10950   XRP 2017-07-17 10:35:09.143000  0.0
        10951   XRP 2017-07-17 10:35:09.143000  0.0

    :param reporting_currency:
    :param prices:
    :param withdrawals:
    :param deposits:
    :return:
    """
    flows = parse_flows(withdrawals, deposits).set_index('date')
    flows_by_asset = flows.pivot(columns='asset', values='amount').apply(pandas.to_numeric)
    balances = flows_by_asset.fillna(0).cumsum()
    prices_selection = _select_prices(reporting_currency, prices)
    prices_selection = _include_indices(prices_selection, balances).ffill()
    balances = _include_indices(balances, prices_selection).ffill()
    performances = prices_selection.diff() * balances.shift()
    return performances.unstack().reset_index().fillna(0).rename(columns={'level_0': 'asset', 0: 'pnl'})


def compute_trades_pnl(reporting_currency, prices, order_history):
    """
    Output format:

              asset                           date  realized_pnl      total_pnl       unrealized_pnl
      413       XRP     2017-07-17 09:00:00.000000           0.0   2.191488e+02         2.191488e+02
      414       BTC     2017-07-17 10:00:00.000000           0.0  -2.744735e+06        -2.744735e+06
      415       XRP     2017-07-17 10:00:00.000000           0.0   2.162138e+02         2.162138e+02
      416       BTC     2017-07-17 10:04:06.200048           0.0  -2.744481e+06        -2.744481e+06
      417       XRP     2017-07-17 10:04:06.200048           0.0   2.162138e+02         2.162138e+02

    :param reporting_currency:
    :param prices:
    :param order_history:
    :return:
    """
    trades = parse_orders(order_history).set_index('date')
    prices_selection = _select_prices(reporting_currency, prices)
    prices_selection = _include_indices(prices_selection, trades)
    pnl_tracker = defaultdict(AverageCostProfitAndLoss)
    pnl_data = list()
    for timestamp, price_row in prices_selection.iterrows():
        if timestamp in trades.index:
            current_trades = trades.loc[timestamp]
            for trade_ts, trade_row in current_trades.iterrows():
                fill_qty = float(trade_row['qty'])
                fill_price = float(trade_row['unit_price'])
                fees = trade_row['fees']
                asset = trade_row['asset']
                pnl_tracker[asset].add_fill(fill_qty, fill_price, fees)
                pnl_asset_data = {
                    'date': trade_ts,
                    'asset': asset,
                    'unrealized_pnl': pnl_tracker[asset].get_unrealized_pnl(fill_price),
                    'realized_pnl': pnl_tracker[asset].realized_pnl,
                    'total_pnl': pnl_tracker[asset].get_total_pnl(fill_price),
                }
                pnl_data.append(pnl_asset_data)

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

    return pandas.DataFrame(pnl_data)


def compute_pnl_history(reporting_currency, prices, withdrawals, deposits, order_history):
    balances_pnl = compute_balances_pnl(reporting_currency, prices, withdrawals, deposits)
    trades_pnl = compute_trades_pnl(reporting_currency, prices, order_history)
    balances_pnl_by_asset = balances_pnl.groupby(['date', 'asset']).sum().unstack()['pnl']
    trades_pnl_by_asset = trades_pnl.groupby(['date', 'asset']).sum().unstack()['total_pnl']
    trades_pnl_by_asset = trades_pnl_by_asset.reindex(columns=balances_pnl_by_asset.columns).fillna(0)
    pnl_history = balances_pnl_by_asset + trades_pnl_by_asset
    return pnl_history
