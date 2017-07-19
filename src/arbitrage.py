import logging
from time import sleep


def trade_pair(pair_code, bid, ask, volume):
    """
    Computes the balance after the operation takes place.
    Example:
        XXLMXXBT 38092.21 0.000008210 0.000008340 121.618 --> With a volume of 1 we go long 0.000008210 XXBT and short 1 XXLM

    :param pair_code:
    :param bid:
    :param ask:
    :param volume:
    :return:
    """
    currency_first = pair_code[:4]
    currency_second = pair_code[4:]
    result = {currency_first: 0, currency_second: 0}
    if volume > 0:
        allowed_volume = min(volume, bid['volume'])
        if allowed_volume < volume:
            logging.warning('volume capped at {} instead of expected {}'.format(allowed_volume, volume))

        result = {currency_first: allowed_volume * -1, currency_second: allowed_volume * bid['price']}

    elif volume < 0:
        allowed_volume = min(abs(volume), ask['volume'])
        if allowed_volume < abs(volume):
            logging.warning('volume capped at {} instead of expected {}'.format(allowed_volume, abs(volume)))

        result = {currency_first: allowed_volume, currency_second: allowed_volume * ask['price'] * -1}

    return result


def buy_currency_using_pair(currency, volume, pair_code, bid, ask):
    """

    :param currency:
    :param volume: amount to buy denominated in currency
    :param pair_code:
    :param bid:
    :param ask:
    :return:
    """
    logging.info('buying {} {} using {}'.format(volume, currency, pair_code))
    if pair_code[4:] == currency:
        # Direct quotation
        logging.info('direct quotation')
        target_volume = volume / bid['price']
        result = trade_pair(pair_code, bid, ask, round(target_volume, 10))

    else:
        # Indirect quotation
        logging.info('indirect quotation')
        result = trade_pair(pair_code, bid, ask, volume * -1)

    return result


def sell_currency_using_pair(currency, volume, pair_code, bid, ask):
    """

    :param currency:
    :param volume: amount to buy denominated in currency
    :param pair_code:
    :param bid:
    :param ask:
    :return:
    """
    logging.info('selling {} {} using {}'.format(volume, currency, pair_code))
    if pair_code[4:] == currency:
        # Direct quotation
        logging.info('direct quotation')
        target_volume = -1 * volume / ask['price']
        result = trade_pair(pair_code, bid, ask, round(target_volume, 10))

    else:
        # Indirect quotation
        logging.info('indirect quotation')
        result = trade_pair(pair_code, bid, ask, volume)

    return result


def calculate_arbitrage_opportunity(direct_ask, direct_bid, direct_pair, indirect_ask_1, indirect_ask_2, indirect_bid_1,
                                    indirect_bid_2, indirect_pair_1, indirect_pair_2):
    currency_initial = direct_pair[4:]
    currency_final = direct_pair[:4]
    initial_bid = direct_bid
    initial_ask = direct_ask
    if currency_initial in indirect_pair_1:
        next_pair = indirect_pair_1
        next_bid = indirect_bid_1
        next_ask = indirect_ask_1
        final_pair = indirect_pair_2
        final_bid = indirect_bid_2
        final_ask = indirect_ask_2

    else:
        next_pair = indirect_pair_2
        next_bid = indirect_bid_2
        next_ask = indirect_ask_2
        final_pair = indirect_pair_1
        final_bid = indirect_bid_1
        final_ask = indirect_ask_1

    if next_pair[:4] != currency_initial:
        currency_next = next_pair[:4]

    else:
        currency_next = next_pair[4:]

    logging.info('currency initial: {}'.format(currency_initial))
    logging.info('currency next: {}'.format(currency_next))
    logging.info('currency final: {}'.format(currency_final))

    balance_initial = buy_currency_using_pair(currency_initial, 1, direct_pair, initial_bid, initial_ask)
    logging.info('balance 1: {}'.format(balance_initial))
    balance_next = sell_currency_using_pair(currency_initial, balance_initial[currency_initial], next_pair, next_bid, next_ask)
    logging.info('balance 2: {}'.format(balance_next))
    balance_final = sell_currency_using_pair(currency_next, balance_next[currency_next], final_pair, final_bid, final_ask)
    logging.info('balance 3: {}'.format(balance_final))

    amount_transition = 0
    amount_final = 0
    currency_final = 0
    final_ratio = 0
    if final_ratio > 1:
        logging.info('found profitable strategy')
        logging.info('')
        logging.info('selling {} {} for {} {}'.format(1, currency_initial, amount_transition, currency_next))
        logging.info(
            'selling {} {} for {} {}'.format(amount_transition, currency_next, amount_final, currency_final))
        logging.info('buying {} {} with {} {}'.format(final_ratio, currency_initial, amount_final, currency_final))
        logging.info('{}\n --> bid:\n{}\n --> ask:\n {}'.format(direct_pair, direct_bid,
                                                                direct_ask))
        logging.info('{}\n --> bid:\n{}\n --> ask:\n {}'.format(indirect_pair_1, indirect_bid_1,
                                                                indirect_ask_1))
        logging.info('{}\n --> bid:\n{}\n --> ask:\n {}'.format(indirect_pair_2, indirect_bid_2,
                                                                indirect_ask_2))

    return final_ratio


def scan_arbitrage_opportunities(tradeable_pairs, order_book_callbak):
    assets = set(tradeable_pairs['base'].tolist()).union(tradeable_pairs['quote'].tolist())
    available_pairs = set(tradeable_pairs['pair_code'].tolist())
    logging.info('available pairs: {}'.format(available_pairs))
    logging.info('available assets: {}'.format(assets))
    results = list()
    for common_leg in assets:
        logging.debug('trying currency {}'.format(common_leg))
        for leg_pair1 in assets:
            if leg_pair1 == common_leg:
                continue

            for leg_pair2 in assets:
                if leg_pair2 == common_leg or leg_pair2 == leg_pair1:
                    continue

                direct_pair = leg_pair1 + leg_pair2
                indirect_pair_1 = leg_pair1 + common_leg
                indirect_pair_2 = leg_pair2 + common_leg
                if available_pairs.issuperset({direct_pair, indirect_pair_1, indirect_pair_2}):
                    logging.info('trying pair {} with {} and {}'.format(direct_pair, indirect_pair_1, indirect_pair_2))
                    sleep(1)
                    direct_bid, direct_ask = order_book_callbak(direct_pair)
                    if direct_bid is None or direct_ask is None:
                        continue

                    sleep(1)
                    indirect_bid_1, indirect_ask_1 = order_book_callbak(indirect_pair_1)
                    if indirect_bid_1 is None or indirect_ask_1 is None:
                        continue

                    sleep(1)
                    indirect_bid_2, indirect_ask_2 = order_book_callbak(indirect_pair_2)
                    if indirect_bid_2 is None or indirect_ask_2 is None:
                        continue

                    arbitrage_ratio = calculate_arbitrage_opportunity(direct_ask, direct_bid, direct_pair,
                                                                      indirect_ask_1, indirect_ask_2,
                                                                      indirect_bid_1, indirect_bid_2, indirect_pair_1,
                                                                      indirect_pair_2)

                    result = ''
                    results.append(result)
    return results