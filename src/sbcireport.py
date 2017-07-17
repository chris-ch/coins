import pandas
from collections import defaultdict

from exchanges.bittrex import parse_orders
from pnl import AverageCostProfitAndLoss


def compute_trades_pnl(reporting_currency, prices, order_history):
    """

    :param reporting_currency:
    :param prices:
    :param order_history:
    :return:
    """
    trades = parse_orders(order_history).set_index('date')
    all_prices = prices.set_index('date')
    filter_columns = [column for column in all_prices.columns if column.endswith(reporting_currency)]
    prices = all_prices[filter_columns]
    prices.columns = [column.split('/')[0] for column in prices.columns]
    prices = prices.reindex(prices.index.append(trades.index)).sort_index()
    pnl_tracker = defaultdict(AverageCostProfitAndLoss)
    pnl_data = list()
    for timestamp, price_row in prices.iterrows():
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