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

    def get_trade_bin_5m(self):
        return self.bitmex.trade_bin_5m()

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
        self.exchange.cancel_all_orders()
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

        logger.info("Current XBT Balance: %.6f" % XBt_to_XBT(self.start_XBt))
        logger.info("Current Contract Position: %d" % self.running_qty)
        if settings.CHECK_POSITION_LIMITS:
            logger.info("Position limits: %d/%d" % (settings.MIN_POSITION, settings.MAX_POSITION))
        if position['currentQty'] != 0:
            logger.info("Avg Cost Price: %.*f" % (tickLog, float(position['avgCostPrice'])))
            logger.info("Avg Entry Price: %.*f" % (tickLog, float(position['avgEntryPrice'])))
            logger.info("Break Even Price: %.*f" % (tickLog, float(position['breakEvenPrice'])))
            logger.info("Funding rate: %s" % (self.instrument['fundingRate']))
            logger.info("Indicative funding rate: %s" % (self.instrument['indicativeFundingRate']))
        logger.info("Contracts Traded This Run: %d" % (self.running_qty - self.starting_qty))
        logger.info("Total Contract Delta: %.4f XBT" % self.exchange.calc_delta()['spot'])


    ###
    # Orders
    ###

    def calc_first_trade_price(self, break_even_price, qty):
        trade_count = floor(qty/settings.ORDER_START_SIZE)
        incomplete_trade_qty = qty % settings.ORDER_START_SIZE
       
        suma = incomplete_trade_qty/pow(1+settings.INTERVAL, trade_count)

        for x in range(trade_count):
            suma += settings.ORDER_START_SIZE/pow(1+settings.INTERVAL, x)

        return break_even_price*suma/(trade_count*settings.ORDER_START_SIZE+incomplete_trade_qty)

    def get_trade_price(self, first_trade_price, trade_number):
        return first_trade_price*pow(1+settings.INTERVAL, trade_number)

    def place_orders(self):
        """Create order items for use in convergence."""
        ticker = self.exchange.get_ticker()
        position = self.exchange.get_position()
        trade_bin_5m = self.exchange.get_trade_bin_5m();

        buy_orders = []
        sell_orders = []

        top_buy_price = ticker["buy"] + self.instrument['tickSize']
        top_sell_price = ticker["sell"] - self.instrument['tickSize']

        if top_buy_price >= ticker["sell"]:
            top_buy_price = ticker["buy"]
        
        if top_sell_price <= ticker["buy"]:
            top_sell_price = ticker["sell"]

        if len(trade_bin_5m) > 0:
            vwap = trade_bin_5m[-1]['vwap']
            logger.info("VWAP %s" %(trade_bin_5m[-1]))
        else:
            vwap = self.instrument['vwap']
        
        logger.info("VWAP: %s" % (vwap))    

        if position['currentQty'] != 0:
            current_qty = abs(position['currentQty'])
            trade_count = ceil(float(current_qty) / settings.ORDER_START_SIZE)
            
            logger.info("trade count %s" % (trade_count))

            leverage = (current_qty/self.instrument['markPrice'])/XBt_to_XBT(self.start_XBt)

            logger.info("Current Leverage %s" % (leverage))                
            
            if position['currentQty'] < 0:
                break_even_price = min(position['avgEntryPrice'], position['breakEvenPrice'])
                
                first_trade_price = self.calc_first_trade_price(break_even_price, current_qty)
                last_trade_price = self.get_trade_price(first_trade_price, trade_count-1)
                next_trade_price = self.get_trade_price(first_trade_price, trade_count)
                
                logger.info("first trade price %s" % (first_trade_price))
                logger.info("last trade price %s" % (last_trade_price))
                logger.info("next trade price %s" % (next_trade_price))
                
                if self.instrument['makerFee'] < 0 and self.start_XBt > 0:
                    sell_quantity = -(current_qty % settings.ORDER_START_SIZE)

                    total_sell_quantity = current_qty
                    total_delta = current_qty/position['avgEntryPrice']
                    next_price = last_trade_price
                    order_count = 0

                    while order_count < 10:
                        while True:
                            sell_quantity += settings.ORDER_START_SIZE
                            next_price *= 1+settings.INTERVAL

                            if next_price >= ticker['buy'] and sell_quantity/next_price >= total_delta:
                                break;

                        sell_price = max(top_sell_price, math.toNearestCeil(next_price, self.instrument['tickSize']))

                        new_leverage = ((total_sell_quantity+sell_quantity)/self.instrument['markPrice'])/XBt_to_XBT(self.start_XBt)

                        logger.info("New Leverage %s" % (new_leverage))   

                        if sell_quantity > 0 and new_leverage <= settings.MAX_LEVERAGE:
                            sell_orders.append({'price': sell_price, 'orderQty': sell_quantity, 'side': "Sell", 'execInst': 'ParticipateDoNotInitiate'})
                            total_sell_quantity += sell_quantity
                            total_delta += sell_quantity/sell_price
                            sell_quantity = 0
                            order_count += 1
                        else:
                            break

                buy_price_as_taker = break_even_price*(1-abs(self.instrument['makerFee'])-abs(self.instrument['takerFee']))
                buy_price_as_taker = math.toNearestFloor(buy_price_as_taker, self.instrument['tickSize'])

                if buy_price_as_taker >= ticker['sell']:
                    buy_orders.append({'price': buy_price_as_taker, 'orderQty': current_qty, 'side': "Buy"})
                else:
                    buy_price = min(top_buy_price, math.toNearestFloor(break_even_price, self.instrument['tickSize']))
                    buy_orders.append({'price': buy_price, 'orderQty': current_qty, 'side': "Buy", 'execInst': 'ParticipateDoNotInitiate'})

            elif position['currentQty']>0:
                avg_entry_price = math.toNearestCeil(position['avgEntryPrice'], self.instrument['tickSize'])
                sell_orders.append({'price': max(top_sell_price, avg_entry_price), 'orderQty': abs(position['currentQty']), 'side': "Sell", 'execInst': 'ParticipateDoNotInitiate'})
            
        elif self.instrument['makerFee'] < 0 and self.instrument['fundingRate'] >= 0 and self.start_XBt > 0:
            sell_quantity = settings.ORDER_START_SIZE
            total_sell_quantity = 0
            total_delta = 0
            if vwap == None:
                next_price = top_sell_price
            else:
                next_price = max(top_sell_price, vwap/(1+settings.INTERVAL))
            order_count = 0

            while order_count < 10:
                if order_count > 0:
                    while True:
                        sell_quantity += settings.ORDER_START_SIZE
                        next_price *= 1+settings.INTERVAL
    
                        if next_price > ticker['buy'] and sell_quantity/next_price >= total_delta:
                            break;

                sell_price = max(top_sell_price, math.toNearestCeil(next_price, self.instrument['tickSize']))

                new_leverage = ((total_sell_quantity+sell_quantity)/self.instrument['markPrice'])/XBt_to_XBT(self.start_XBt)

                logger.info("New Leverage %s" % (new_leverage))   

                if sell_quantity > 0 and new_leverage <= settings.MAX_LEVERAGE:
                    sell_orders.append({'price': sell_price, 'orderQty': sell_quantity, 'side': "Sell", 'execInst': 'ParticipateDoNotInitiate'})
                    total_sell_quantity += sell_quantity
                    total_delta += sell_quantity/sell_price
                    sell_quantity = 0
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
            self.exchange.cancel_all_orders()
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
