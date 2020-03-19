from __future__ import absolute_import
from time import sleep
import sys
from datetime import datetime
from os.path import getmtime
import random
import requests
import atexit
import signal

from market_maker import bitmex
from market_maker.settings import settings
from math import ceil, floor, pow
from math import log as logn
from market_maker.utils import log, constants, errors, math

# Used for reloading the bot - saves modified times of key files
import os
watched_files_mtimes = [(f, getmtime(f)) for f in settings.WATCHED_FILES]


#
# Helpers
#
logger = log.setup_custom_logger('root')


class ExchangeInterface:
    def __init__(self, dry_run=False):
        self.dry_run = dry_run
        if len(sys.argv) > 1:
            self.symbol = sys.argv[1]
        else:
            self.symbol = settings.SYMBOL
        self.bitmex = bitmex.BitMEX(base_url=settings.BASE_URL, symbol=self.symbol,
                                    apiKey=settings.API_KEY, apiSecret=settings.API_SECRET,
                                    orderIDPrefix=settings.ORDERID_PREFIX, postOnly=settings.POST_ONLY,
                                    timeout=settings.TIMEOUT)

    def cancel_order(self, order):
        tickLog = self.get_instrument()['tickLog']
        logger.info("Canceling: %s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))
        while True:
            try:
                self.bitmex.cancel(order['orderID'])
                sleep(settings.API_REST_INTERVAL)
            except ValueError as e:
                logger.info(e)
                sleep(settings.API_ERROR_INTERVAL)
            else:
                break

    def cancel_all_orders(self):
        if self.dry_run:
            return

        logger.info("Resetting current position. Canceling all existing orders.")
        tickLog = self.get_instrument()['tickLog']

        # In certain cases, a WS update might not make it through before we call this.
        # For that reason, we grab via HTTP to ensure we grab them all.
        orders = self.bitmex.http_open_orders()

        for order in orders:
            logger.info("Canceling: %s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))

        if len(orders):
            self.bitmex.cancel([order['orderID'] for order in orders])

        sleep(settings.API_REST_INTERVAL)

    def get_portfolio(self):
        contracts = settings.CONTRACTS
        portfolio = {}
        for symbol in contracts:
            position = self.bitmex.position(symbol=symbol)
            instrument = self.bitmex.instrument(symbol=symbol)

            if instrument['isQuanto']:
                future_type = "Quanto"
            elif instrument['isInverse']:
                future_type = "Inverse"
            elif not instrument['isQuanto'] and not instrument['isInverse']:
                future_type = "Linear"
            else:
                raise NotImplementedError("Unknown future type; not quanto or inverse: %s" % instrument['symbol'])

            if instrument['underlyingToSettleMultiplier'] is None:
                multiplier = float(instrument['multiplier']) / float(instrument['quoteToSettleMultiplier'])
            else:
                multiplier = float(instrument['multiplier']) / float(instrument['underlyingToSettleMultiplier'])

            portfolio[symbol] = {
                "currentQty": float(position['currentQty']),
                "futureType": future_type,
                "multiplier": multiplier,
                "markPrice": float(instrument['markPrice']),
                "spot": float(instrument['indicativeSettlePrice'])
            }

        return portfolio

    def calc_delta(self):
        """Calculate currency delta for portfolio"""
        portfolio = self.get_portfolio()
        spot_delta = 0
        mark_delta = 0
        for symbol in portfolio:
            item = portfolio[symbol]
            if item['futureType'] == "Quanto":
                spot_delta += item['currentQty'] * item['multiplier'] * item['spot']
                mark_delta += item['currentQty'] * item['multiplier'] * item['markPrice']
            elif item['futureType'] == "Inverse":
                spot_delta += (item['multiplier'] / item['spot']) * item['currentQty']
                mark_delta += (item['multiplier'] / item['markPrice']) * item['currentQty']
            elif item['futureType'] == "Linear":
                spot_delta += item['multiplier'] * item['currentQty']
                mark_delta += item['multiplier'] * item['currentQty']
        basis_delta = mark_delta - spot_delta
        delta = {
            "spot": spot_delta,
            "mark_price": mark_delta,
            "basis": basis_delta
        }
        return delta

    def get_delta(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.get_position(symbol)['currentQty']

    def get_instrument(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.instrument(symbol)

    def get_margin(self):
        if self.dry_run:
            return {'marginBalance': float(settings.DRY_BTC), 'availableFunds': float(settings.DRY_BTC)}
        return self.bitmex.funds()

    def get_orders(self):
        if self.dry_run:
            return []
        return self.bitmex.open_orders()

    def get_filled_orders(self):
        if self.dry_run:
            return []
        return self.bitmex.filled_orders()

    def get_highest_buy(self):
        buys = [o for o in self.get_orders() if o['side'] == 'Buy']
        if not len(buys):
            return {'price': -2**32}
        highest_buy = max(buys or [], key=lambda o: o['price'])
        return highest_buy if highest_buy else {'price': -2**32}

    def get_lowest_sell(self):
        sells = [o for o in self.get_orders() if o['side'] == 'Sell']
        if not len(sells):
            return {'price': 2**32}
        lowest_sell = min(sells or [], key=lambda o: o['price'])
        return lowest_sell if lowest_sell else {'price': 2**32}  # ought to be enough for anyone

    def get_position(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.position(symbol)
        
    def get_trade_bin_1m(self):
        return self.bitmex.trade_bin_1m()

    def get_trade_bin_5m(self):
        return self.bitmex.trade_bin_5m()
        
    def get_trade_bin_1h(self):
        return self.bitmex.trade_bin_1h()

    def get_ticker(self, symbol=None):
        if symbol is None:
            symbol = self.symbol
        return self.bitmex.ticker_data(symbol)

    def is_open(self):
        """Check that websockets are still open."""
        return not self.bitmex.ws.exited

    def check_market_open(self):
        instrument = self.get_instrument()
        if instrument["state"] != "Open" and instrument["state"] != "Closed":
            raise errors.MarketClosedError("The instrument %s is not open. State: %s" %
                                           (self.symbol, instrument["state"]))

    def check_if_orderbook_empty(self):
        """This function checks whether the order book is empty"""
        instrument = self.get_instrument()
        if instrument['midPrice'] is None:
            raise errors.MarketEmptyError("Orderbook is empty, cannot quote")

    def amend_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        return self.bitmex.amend_bulk_orders(orders)

    def create_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        return self.bitmex.create_bulk_orders(orders)

    def cancel_bulk_orders(self, orders):
        if self.dry_run:
            return orders
        return self.bitmex.cancel([order['orderID'] for order in orders])


class OrderManager:
    def __init__(self):
        self.exchange = ExchangeInterface(settings.DRY_RUN)
        # Once exchange is created, register exit handler that will always cancel orders
        # on any error.
        atexit.register(self.exit)
        signal.signal(signal.SIGTERM, self.exit)

        logger.info("Using symbol %s." % self.exchange.symbol)

        if settings.DRY_RUN:
            logger.info("Initializing dry run. Orders printed below represent what would be posted to BitMEX.")
        else:
            logger.info("Order Manager initializing, connecting to BitMEX. Live run: executing real trades.")

        self.start_time = datetime.now()
        self.instrument = self.exchange.get_instrument()
        self.starting_qty = self.exchange.get_delta()
        self.running_qty = self.starting_qty
        self.reset()

    def reset(self):
        #self.exchange.cancel_all_orders()
        self.print_status()

        # Create orders and converge.
        self.place_orders()

    def print_status(self):
        """Print the current MM status."""

        margin = self.exchange.get_margin()
        position = self.exchange.get_position()
        self.running_qty = self.exchange.get_delta()
        tickLog = self.exchange.get_instrument()['tickLog']
        self.start_XBt = margin["marginBalance"]
        
        logger.info("Current Wallet Balance: %s" % margin['walletBalance'])
        logger.info("Current Contract Position: %d" % self.running_qty)
        logger.info("Mark price: %.4f" % self.instrument['markPrice'])
        logger.info("Fair price: %.4f" % self.instrument['fairPrice'])
        logger.info("Indicative settle price: %.4f" % self.instrument['indicativeSettlePrice'])
        if settings.CHECK_POSITION_LIMITS:
            logger.info("Position limits: %d/%d" % (settings.MIN_POSITION, settings.MAX_POSITION))
        if position['currentQty'] != 0:
            logger.info("Avg Entry Price: %.*f" % (tickLog, float(position['avgEntryPrice'])))
            logger.info("Break Even Price: %.*f" % (tickLog, float(position['breakEvenPrice'])))
            logger.info("Liquidation Price: %.*f" % (tickLog, float(position['liquidationPrice'])))
        logger.info("Funding rate: %s" % (self.instrument['fundingRate']))
        logger.info("Predicted Funding rate: %s" % (self.instrument['indicativeFundingRate']))
        logger.info("Contracts Traded This Run: %d" % (self.running_qty - self.starting_qty))
        logger.info("Total Contract Delta: %.4f XBT" % self.exchange.calc_delta()['spot'])

    ###
    # Orders
    ###
    
    def calc_first_buy_price(self, avg_entry_price, trade_count):
        suma = 0

        for x in range(trade_count):
            suma += pow(1-settings.INTERVAL, x)

        return avg_entry_price*trade_count/suma

    def get_buy_price(self, first_trade_price, trade_number):
        return first_trade_price*pow(1-settings.INTERVAL, trade_number)    

    def calc_first_sell_price(self, avg_entry_price, trade_count):
        suma = 0

        for x in range(trade_count):
            suma += pow(1+settings.INTERVAL, x)

        return avg_entry_price*trade_count/suma

    def get_sell_price(self, first_trade_price, trade_number):
        return first_trade_price*pow(1+settings.INTERVAL, trade_number)

    def place_orders(self):
        """Create order items for use in convergence."""
        ticker = self.exchange.get_ticker()
        position = self.exchange.get_position()
        trade_bin_1m = self.exchange.get_trade_bin_1m();
        trade_bin_5m = self.exchange.get_trade_bin_5m();
        trade_bin_1h = self.exchange.get_trade_bin_1h();
        margin = self.exchange.get_margin()

        buy_orders = []
        sell_orders = []

        top_buy_price = ticker["buy"] + self.instrument['tickSize']
        top_sell_price = ticker["sell"] - self.instrument['tickSize']

        if top_buy_price >= ticker["sell"]:
            top_buy_price = ticker["buy"]
        
        if top_sell_price <= ticker["buy"]:
            top_sell_price = ticker["sell"]

        vwap = self.instrument['vwap']
         
        if len(trade_bin_1h) > 0:
            vwap1h = trade_bin_1h[-1]['vwap']
        else:
            vwap1h = vwap
            
        if len(trade_bin_5m) > 0:
            vwap5m = trade_bin_5m[-1]['vwap']
        else:
            vwap5m = vwap1h
            
        if len(trade_bin_1m) > 0:
            vwap1m = trade_bin_1m[-1]['vwap']
        else:
            vwap1m = vwap5m
#         if len(trade_bin_1h) > 0:
#             vwap = max(vwap, trade_bin_1h[-1]['vwap'])
        
        logger.info("VWAP 24h: %s" % (vwap))
        logger.info("VWAP 1h: %s" % (vwap1h))
        logger.info("VWAP 5m: %s" % (vwap5m))
        logger.info("VWAP 1m: %s" % (vwap1m))

        funds = XBt_to_XBT(margin['walletBalance'])

        max_buy_orders = ceil(logn(1-settings.COVERAGE_LONG)/logn(1-settings.INTERVAL))
        start_order_long = ceil(settings.MAX_LEVERAGE_LONG*funds/max_buy_orders*1e8)/1e8

        max_sell_orders = ceil(logn(1+settings.COVERAGE_SHORT)/logn(1+settings.INTERVAL))
        start_order_short = ceil(settings.MAX_LEVERAGE_SHORT*funds/max_sell_orders*1e8)/1e8

        logger.info("Max sell orders: %s:" % max_sell_orders)
        logger.info("Start order short: %s:" % start_order_short)
        logger.info("Max buy orders: %s:" % max_buy_orders)
        logger.info("Start order long: %s:" % start_order_long)

        im_taker = False

        if position['currentQty'] != 0:
            current_qty = abs(position['currentQty'])

            leverage = (current_qty/self.instrument['markPrice'])/funds

            logger.info("Current Leverage %s" % (leverage))                
            
            if position['currentQty'] < 0:
                trade_count = max(1, round((current_qty/position['avgEntryPrice'])/start_order_short))
                
                logger.info("trade count %s" % trade_count)
                
                first_trade_price = self.calc_first_sell_price(position['avgEntryPrice'], trade_count)
                last_trade_price = self.get_sell_price(first_trade_price, trade_count-1)
                next_trade_price = self.get_sell_price(first_trade_price, trade_count)
                spread = abs(first_trade_price-last_trade_price)/first_trade_price
                profit = settings.PROFIT
                
                logger.info("first trade price %s" % (first_trade_price))
                logger.info("last trade price %s" % (last_trade_price))
                logger.info("next trade price %s" % (next_trade_price))
                logger.info("spread %s" % spread)
                logger.info("profit %s" % profit)
                
                if funds > 0:
                    sell_quantity = 0
                    total_sell_quantity = current_qty
                    total_delta = current_qty/position['avgEntryPrice']
                    next_price = last_trade_price
                    order_count = 0
                    first_price = None

                    while order_count < settings.ORDER_PAIRS:
                        while True:
                            next_price *= 1+settings.INTERVAL
                            sell_quantity += max(1, round(start_order_short*next_price))

                            if next_price >= ticker['buy'] and sell_quantity/next_price >= total_delta*settings.RE_ENTRY_FACTOR:
                                break;

                        sell_price = max(top_sell_price, math.toNearestCeil(next_price, self.instrument['tickSize']))

                        new_leverage = ((total_sell_quantity+sell_quantity)/self.instrument['markPrice'])/funds  

                        if sell_quantity > 0 and new_leverage <= settings.MAX_LEVERAGE_SHORT and sell_price < position['liquidationPrice']:
                            if first_price is None:
                                first_price = sell_price
                            elif (sell_price-first_price)/first_price > settings.RANGE:
                                break
                            sell_orders.append({'price': sell_price, 'orderQty': sell_quantity, 'side': "Sell", 'execInst': 'ParticipateDoNotInitiate'})
                            total_sell_quantity += sell_quantity
                            total_delta += sell_quantity/sell_price
                            sell_quantity = 0
                            order_count += 1
                        else:
                            break

                break_even_price = min(position['avgEntryPrice'], position['breakEvenPrice']*(1-abs(self.instrument['makerFee'])))
                buy_price_as_taker = break_even_price*(1-profit-abs(self.instrument['takerFee']))
                buy_price_as_taker = math.toNearestFloor(buy_price_as_taker, self.instrument['tickSize'])

                if buy_price_as_taker >= ticker['sell']:
                    close_short_at = buy_price_as_taker
                    im_taker = True
                    buy_orders.append({'price': buy_price_as_taker, 'orderQty': current_qty, 'side': "Buy"})
                else:
                    buy_price = min(top_buy_price, math.toNearestFloor(break_even_price*(1-profit), self.instrument['tickSize']))
                    close_short_at = buy_price
                    im_taker = False
                    buy_orders.append({'price': buy_price, 'orderQty': current_qty, 'side': "Buy", 'execInst': 'ParticipateDoNotInitiate'})

            elif position['currentQty']>0:
                trade_count = max(1, round((current_qty/position['avgEntryPrice'])/start_order_long))
                
                logger.info("trade count %s" % trade_count)
                
                first_trade_price = self.calc_first_buy_price(position['avgEntryPrice'], trade_count)
                last_trade_price = self.get_buy_price(first_trade_price, trade_count-1)
                next_trade_price = self.get_buy_price(first_trade_price, trade_count)
                
                spread = abs(first_trade_price-last_trade_price)/first_trade_price
                profit = settings.PROFIT
                
                logger.info("first buy price %s" % (first_trade_price))
                logger.info("last buy price %s" % (last_trade_price))
                logger.info("next buy price %s" % (next_trade_price))
                logger.info("spread %s" % spread)
                logger.info("target profit %s" % profit)
                
                if funds > 0:
                    buy_quantity = 0
                    total_buy_quantity = current_qty
                    total_delta = current_qty/position['avgEntryPrice']
                    next_price = last_trade_price
                    order_count = 0
                    first_price = None

                    while order_count < settings.ORDER_PAIRS:
                        while True:
                            next_price *= 1-settings.INTERVAL
                            buy_quantity += max(1, round(start_order_long*next_price))

                            if next_price <= ticker['sell'] and buy_quantity/next_price >= total_delta*settings.RE_ENTRY_FACTOR:
                                break;

                        buy_price = min(top_buy_price, math.toNearestCeil(next_price, self.instrument['tickSize']))

                        new_leverage = ((total_buy_quantity+buy_quantity)/self.instrument['markPrice'])/funds

                        if buy_quantity > 0 and new_leverage <= settings.MAX_LEVERAGE_LONG and buy_price > position['liquidationPrice']:
                            if first_price is None:
                                first_price = buy_price
                            elif (first_price-buy_price)/first_price > settings.RANGE:
                                break
                            buy_orders.append({'price': buy_price, 'orderQty': buy_quantity, 'side': "Buy", 'execInst': 'ParticipateDoNotInitiate'})
                            total_buy_quantity += buy_quantity
                            total_delta += buy_quantity/buy_price
                            buy_quantity = 0
                            order_count += 1
                        else:
                            break
                
                break_even_price = max(position['avgEntryPrice'], position['breakEvenPrice']*(1+abs(self.instrument['makerFee'])))
                sell_price_as_taker = break_even_price*(1+profit+abs(self.instrument['takerFee']))
                sell_price_as_taker = math.toNearestCeil(sell_price_as_taker, self.instrument['tickSize'])

                if sell_price_as_taker <= ticker['buy']:
                    close_long_at = sell_price_as_taker
                    im_taker = True
                    sell_orders.append({'price': sell_price_as_taker, 'orderQty': current_qty, 'side': "Sell"})
                else:
                    sell_price = max(top_sell_price, math.toNearestCeil(break_even_price*(1+profit), self.instrument['tickSize']))
                    close_long_at = sell_price
                    im_taker = False
                    sell_orders.append({'price': sell_price, 'orderQty': current_qty, 'side': "Sell", 'execInst': 'ParticipateDoNotInitiate'})
        
        if funds > 0:
#             if False:
            #Should I go short?
            if position['currentQty'] >= 0 and start_order_short >= 0.0025 and self.instrument['fundingRate'] >= 0:# and self.instrument['indicativeFundingRate'] >= 0:
                if vwap is None:
                    next_price = top_sell_price
                else:
                    next_price = max(top_sell_price, vwap1m)

                if position['currentQty']>0:
                    next_price = max(next_price, close_long_at*(1+settings.INTERVAL))
                    logger.info("close long at %s " % close_long_at)

                total_sell_quantity = 0
                total_delta = 0
                sell_quantity = ceil(start_order_short*next_price)
    
                order_count = 0
                first_price = None
    
                while order_count < settings.ORDER_PAIRS:
                    if order_count > 0:
                        while True:
                            next_price *= 1+settings.INTERVAL
                            sell_quantity += max(1, round(start_order_short*next_price))
        
                            if next_price > ticker['buy'] and sell_quantity/next_price >= total_delta*settings.RE_ENTRY_FACTOR:
                                break;
    
                    sell_price = max(top_sell_price, math.toNearestCeil(next_price, self.instrument['tickSize']))
    
                    new_leverage = ((total_sell_quantity+sell_quantity)/self.instrument['markPrice'])/funds
    
                    if sell_quantity > 0 and new_leverage <= settings.MAX_LEVERAGE_SHORT:
                        if first_price is None:
                            first_price = sell_price
                        elif (sell_price-first_price)/first_price > settings.RANGE:
                            break
                        
                        sell_orders.append({'price': sell_price, 'orderQty': sell_quantity, 'side': "Sell", 'execInst': 'ParticipateDoNotInitiate'})
                        total_sell_quantity += sell_quantity
                        total_delta += sell_quantity/sell_price
                        sell_quantity = 0
                        order_count += 1
                    else:
                        break

#             if False:
#             # Should I go long?
            if position['currentQty'] <= 0 and start_order_long >= 0.0025 and self.instrument['fundingRate'] <= 0:# and self.instrument['indicativeFundingRate'] <= 0:
                if vwap is None:
                    next_price = top_buy_price
                else:
                    next_price = min(top_buy_price, vwap1m)

                if position['currentQty'] < 0:
                    next_price = min(next_price, close_short_at*(1-settings.INTERVAL))
                    logger.info("close short at %s " % close_short_at)
    
                total_buy_quantity = 0
                total_delta = 0
                buy_quantity = ceil(start_order_long*next_price)
    
                order_count = 0
                first_price = None
    
                while order_count < settings.ORDER_PAIRS:
                    if order_count > 0:
                        while True:
                            next_price *= 1-settings.INTERVAL
                            buy_quantity += max(1, round(start_order_long*next_price))
        
                            if next_price < ticker['sell'] and buy_quantity/next_price >= total_delta*settings.RE_ENTRY_FACTOR:
                                break;
    
                    buy_price = min(top_buy_price, math.toNearestFloor(next_price, self.instrument['tickSize']))
    
                    new_leverage = ((total_buy_quantity+buy_quantity)/self.instrument['markPrice'])/funds
    
                    if buy_quantity > 0 and new_leverage <= settings.MAX_LEVERAGE_LONG:
                        if first_price is None:
                            first_price = buy_price
                        elif (first_price-buy_price)/first_price > settings.RANGE:
                            break
                            
                        buy_orders.append({'price': buy_price, 'orderQty': buy_quantity, 'side': "Buy", 'execInst': 'ParticipateDoNotInitiate'})
                        total_buy_quantity += buy_quantity
                        total_delta += buy_quantity/buy_price
                        buy_quantity = 0
                        order_count += 1
                    else:
                        break

        return self.converge_orders(buy_orders, sell_orders)


    def converge_orders(self, buy_orders, sell_orders):
        """Converge the orders we currently have in the book with what we want to be in the book.
           This involves amending any open orders and creating new ones if any have filled completely.
           We start from the closest orders outward."""

        tickLog = self.exchange.get_instrument()['tickLog']
        to_amend = []
        to_create = []
        to_cancel = []
        buys_matched = 0
        sells_matched = 0
        existing_orders = self.exchange.get_orders()

        for order in buy_orders:
            if 'execInst' not in order:
                to_create.append(order)

        for order in sell_orders:
            if 'execInst' not in order:
                to_create.append(order)

        buy_orders = [o for o in buy_orders if 'execInst' in o]
        sell_orders = [o for o in sell_orders if 'execInst' in o]

        #buy_orders.sort(key=lambda o : o['price'], reverse=True)
        #sell_orders.sort(key=lambda o : o['price'])

        # Check all existing orders and match them up with what we want to place.
        # If there's an open one, we might be able to amend it to fit what we want.
        for order in existing_orders:
            try:
                if order['side'] == 'Buy':
                    desired_order = buy_orders[buys_matched]
                    buys_matched += 1
                else:
                    desired_order = sell_orders[sells_matched]
                    sells_matched += 1

                # Found an existing order. Do we need to amend it?
                if desired_order['orderQty'] != order['leavesQty'] or (
                        # If price has changed, and the change is more than our RELIST_INTERVAL, amend.
                        desired_order['price'] != order['price'] and
                        abs((desired_order['price'] / order['price']) - 1) > settings.RELIST_INTERVAL):
                    to_amend.append({'orderID': order['orderID'], 'orderQty': order['cumQty'] + desired_order['orderQty'],
                                     'price': desired_order['price'], 'side': order['side']})
            except IndexError:
                # Will throw if there isn't a desired order to match. In that case, cancel it.
                to_cancel.append(order)

        while buys_matched < len(buy_orders):
            to_create.append(buy_orders[buys_matched])
            buys_matched += 1

        while sells_matched < len(sell_orders):
            to_create.append(sell_orders[sells_matched])
            sells_matched += 1

        if len(to_amend) > 0:
            for amended_order in reversed(to_amend):
                reference_order = [o for o in existing_orders if o['orderID'] == amended_order['orderID']][0]
                logger.info("Amending %4s: %d @ %.*f to %d @ %.*f (%+.*f)" % (
                    amended_order['side'],
                    reference_order['leavesQty'], tickLog, reference_order['price'],
                    (amended_order['orderQty'] - reference_order['cumQty']), tickLog, amended_order['price'],
                    tickLog, (amended_order['price'] - reference_order['price'])
                ))
            # This can fail if an order has closed in the time we were processing.
            # The API will send us `invalid ordStatus`, which means that the order's status (Filled/Canceled)
            # made it not amendable.
            # If that happens, we need to catch it and re-tick.
            try:
                self.exchange.amend_bulk_orders(to_amend)
            except requests.exceptions.HTTPError as e:
                errorObj = e.response.json()
                if errorObj['error']['message'] == 'Invalid ordStatus':
                    logger.warn("Amending failed. Waiting for order data to converge and retrying.")
                    sleep(0.5)
                    return self.place_orders()
                else:
                    logger.error("Unknown error on amend: %s. Exiting" % errorObj)
                    sys.exit(1)

        if len(to_create) > 0:
            logger.info("Creating %d orders:" % (len(to_create)))
            for order in reversed(to_create):
                logger.info("%4s %d @ %.*f" % (order['side'], order['orderQty'], tickLog, order['price']))
            self.exchange.create_bulk_orders(to_create)

        # Could happen if we exceed a delta limit
        if len(to_cancel) > 0:
            logger.info("Canceling %d orders:" % (len(to_cancel)))
            for order in reversed(to_cancel):
                logger.info("%4s %d @ %.*f" % (order['side'], order['leavesQty'], tickLog, order['price']))
            self.exchange.cancel_bulk_orders(to_cancel)

    ###
    # Position Limits
    ###

    def short_position_limit_exceeded(self):
        """Returns True if the short position limit is exceeded"""
        if not settings.CHECK_POSITION_LIMITS:
            return False
        position = self.exchange.get_delta()
        return position <= settings.MIN_POSITION

    def long_position_limit_exceeded(self):
        """Returns True if the long position limit is exceeded"""
        if not settings.CHECK_POSITION_LIMITS:
            return False
        position = self.exchange.get_delta()
        return position >= settings.MAX_POSITION

   

    ###
    # Running
    ###

    def check_file_change(self):
        """Restart if any files we're watching have changed."""
        for f, mtime in watched_files_mtimes:
            if getmtime(f) > mtime:
                self.restart()

    def check_connection(self):
        """Ensure the WS connections are still open."""
        return self.exchange.is_open()

    def exit(self):
        logger.info("Shutting down. All open orders will be cancelled.")
        try:
            #self.exchange.cancel_all_orders()
            self.exchange.bitmex.exit()
        except errors.AuthenticationError as e:
            logger.info("Was not authenticated; could not cancel orders.")
        except Exception as e:
            logger.info("Unable to cancel orders: %s" % e)

        sys.exit()

    def run_loop(self):
        while True:
            sys.stdout.write("-----\n")
            sys.stdout.flush()

            self.check_file_change()
            sleep(settings.LOOP_INTERVAL)

            # This will restart on very short downtime, but if it's longer,
            # the MM will crash entirely as it is unable to connect to the WS on boot.
            if not self.check_connection():
                logger.error("Realtime data connection unexpectedly closed, restarting.")
                self.restart()

            self.print_status()  # Print skew, delta, etc
            self.place_orders()  # Creates desired orders and converges to existing orders

    def restart(self):
        logger.info("Restarting the market maker...")
        os.execv(sys.executable, [sys.executable] + sys.argv)

#
# Helpers
#


def XBt_to_XBT(XBt):
    return float(XBt) / constants.XBt_TO_XBT


def cost(instrument, quantity, price):
    mult = instrument["multiplier"]
    P = mult * price if mult >= 0 else mult / price
    return abs(quantity * P)


def margin(instrument, quantity, price):
    return cost(instrument, quantity, price) * instrument["initMargin"]


def run():
    logger.info('BitMEX Market Maker Version: %s\n' % constants.VERSION)

    om = OrderManager()
    # Try/except just keeps ctrl-c from printing an ugly stacktrace
    try:
        om.run_loop()
    except (KeyboardInterrupt, SystemExit):
        sys.exit()
