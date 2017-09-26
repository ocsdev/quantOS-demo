# encoding: UTF-8

from abc import abstractmethod
from framework import common
from framework.basic.order import *
from framework.basic.trade import Trade
from framework.basic.position import Position
from util.sequence import SequenceGenerator
from framework.jzcalendar import *


class OrderDeprecated(object):
    def __init__(self):
        self.order_type = common.ORDER_TYPE.LIMIT
        self.entrust_no = ''
        self.security = ''
        self.entrust_action = ''
        self.entrust_price = 0.0
        self.entrust_size = 0
        self.entrust_date = 0
        self.entrust_time = ''
        self.order_status = ''
        self.fill_size = 0
        self.fill_price = 0.0
        self.cancel_size = 0
        self.errmsg = ''

    def isFinished(self):
        if self.order_status == common.ORDER_STATUS.FILLED or self.order_status == common.ORDER_STATUS.CANCELLED or self.order_status == common.ORDER_STATUS.REJECTED:
            return True
        else:
            return False

    def initFromQuote(self, quote):
        self.entrust_date = quote.getDate()
        self.entrust_time = quote.time

    def copy(self, order):
        self.order_type = order.order_type
        self.entrust_no = order.entrust_no
        self.security = order.security
        self.entrust_action = order.entrust_action
        self.entrust_price = order.entrust_price
        self.entrust_size = order.entrust_size
        self.entrust_date = order.entrust_date
        self.entrust_time = order.entrust_time
        self.order_status = order.order_status
        self.fill_size = order.fill_size
        self.fill_price = order.fill_price
        self.cancel_size = order.cancel_size
        self.errmsg = order.errmsg


class TradeIndDeprecated(object):
    def __init__(self):
        self.entrust_no = ''
        self.security = ''
        self.entrust_action = ''

        self.fill_price = 0.0
        self.fill_size = 0.0
        self.fill_no = ''
        self.fill_date = 0
        self.fill_time = ''

        self.entrust_price = 0.0
        self.entrust_size = 0

        self.refquote = None
        self.refcode = ''


class OrderStatusInd(object):
    def __init__(self):
        self.entrust_no = ''

        self.security = ''

        self.entrust_action = ''
        self.entrust_price = 0.0
        self.entrust_size = 0
        self.entrust_date = 0
        self.entrust_time = 0

        self.order_status = ''

        self.fill_size = 0
        self.fill_price = 0.0

    def init_from_order(self, order):
        self.entrust_no = order.entrust_no

        self.security = order.security

        self.entrust_action = order.entrust_action
        self.entrust_price = order.entrust_price
        self.entrust_size = order.entrust_size
        self.entrust_date = order.entrust_date
        self.entrust_time = order.entrust_time

        self.order_status = order.order_status

        self.fill_size = order.fill_size
        self.fill_price = order.fill_price


class TradeCallback(object):
    @abstractmethod
    def on_trade_ind(self, trade):
        pass

    @abstractmethod
    def on_order_status(self, orderstatus):
        pass

    @abstractmethod
    def on_order_rsp(self, order, result, msg):
        pass


class TradeStat(object):
    def __init__(self):
        self.security = ""
        self.buy_filled_size = 0
        self.buy_want_size = 0
        self.sell_filled_size = 0
        self.sell_want_size = 0


class PositionDeprecated(object):
    def __init__(self):
        self.trade_date = 0
        self.security = ''
        self.init_size = 0
        self.curr_size = 0


class PortfolioManager(TradeCallback):
    """
    Used to store relevant context of the strategy.

    Attributes
    ----------
    orders : list of framework.basic.Order objects
    trades : list of framework.basic.Trade objects
    positions : dict of {security + trade_date : framework.basic.Position}
    strategy : Strategy
    holding_securities : set of securities

    Methods
    -------

    """

    # TODO want / frozen update
    def __init__(self, strategy=None):
        self.orders = {}
        self.trades = []
        self.positions = {}
        self.holding_securities = set()
        self.tradestat = {}
        self.strategy = strategy

    @staticmethod
    def _make_position_key(security, trade_date):
        return '@'.join((security, str(trade_date)))

    def on_order_rsp(self, order, result, msg):
        if result:
            self.add_order(order)

    def get_position(self, security, date):
        key = self._make_position_key(security, date)
        position = self.positions.get(key, None)
        return position

    def on_new_day(self, date, pre_date):
        for key, pos in self.positions.items():
            sec, td = key.split('@')
            if str(pre_date) == td:
                new_key = self._make_position_key(sec, date)
                pre_position = pos

                new_position = Position()
                new_position.curr_size = pre_position.curr_size
                new_position.init_size = new_position.curr_size
                new_position.security = pre_position.security
                new_position.trade_date = date
                self.positions[new_key] = new_position

        """
        for sec in self.holding_securities:
            pre_key = self._make_position_key(sec, pre_date)
            new_key = self._make_position_key(sec, date)
            if pre_key in self.positions:
                pre_position = self.positions.get(pre_key)
                new_position = Position()
                new_position.curr_size = pre_position.curr_size
                new_position.init_size = new_position.curr_size
                new_position.security = pre_position.security
                new_position.trade_date = date
                self.positions[new_key] = new_position
        """

    def add_order(self, order):
        """
        Add order to orders, create position and tradestat if necessary.

        Parameters
        ----------
        order : Order

        """
        if order.entrust_no in self.orders:
            print 'duplicate entrust_no ' + order.entrust_no
            return False

        new_order = Order()
        new_order.copy(order)  # TODO why copy?
        self.orders[order.entrust_no] = new_order

        position_key = self._make_position_key(order.security, self.strategy.trade_date)
        if position_key not in self.positions:
            position = Position()
            position.security = order.security
            self.positions[position_key] = position

        if order.security not in self.tradestat:
            tradestat = TradeStat()
            tradestat.security = order.security
            self.tradestat[order.security] = tradestat

        tradestat = self.tradestat.get(order.security)

        if order.entrust_action == common.ORDER_ACTION.BUY:
            tradestat.buy_want_size += order.entrust_size
        else:
            tradestat.sell_want_size += order.entrust_size

    def on_order_status(self, ind):
        if ind.order_status is None:
            return

        if ind.order_status == common.ORDER_STATUS.CANCELLED or ind.order_status == common.ORDER_STATUS.REJECTED:
            entrust_no = ind.entrust_no
            order = self.orders.get(entrust_no, None)
            if order is not None:
                order.order_status = ind.order_status

                tradestat = self.tradestat.get(ind.security)
                release_size = ind.entrust_size - ind.fill_size

                if ind.entrust_action == common.ORDER_ACTION.BUY:
                    tradestat.buy_want_size -= release_size
                else:
                    tradestat.sell_want_size -= release_size
            else:
                raise ValueError("order {} does not exist".format(entrust_no))

    def on_trade_ind(self, ind):
        entrust_no = ind.entrust_no

        order = self.orders.get(entrust_no, None)
        if order is None:
            print 'cannot find order for entrust_no' + entrust_no
            return

        self.trades.append(ind)

        order.fill_size += ind.fill_size

        if order.fill_size == order.entrust_size:
            order.order_status = common.ORDER_STATUS.FILLED
        else:
            order.order_status = common.ORDER_STATUS.ACCEPTED

        position_key = self._make_position_key(ind.security, self.strategy.trade_date)
        position = self.positions.get(position_key)
        tradestat = self.tradestat.get(ind.security)

        if (ind.entrust_action == common.ORDER_ACTION.BUY
            or ind.entrust_action == common.ORDER_ACTION.COVER
            or ind.entrust_action == common.ORDER_ACTION.COVERYESTERDAY
            or ind.entrust_action == common.ORDER_ACTION.COVERTODAY):

            tradestat.buy_filled_size += ind.fill_size
            tradestat.buy_want_size -= ind.fill_size

            position.curr_size += ind.fill_size

        elif (ind.entrust_action == common.ORDER_ACTION.SELL
              or ind.entrust_action == common.ORDER_ACTION.SELLTODAY
              or ind.entrust_action == common.ORDER_ACTION.SELLYESTERDAY
              or ind.entrust_action == common.ORDER_ACTION.SHORT):

            tradestat.sell_filled_size += ind.fill_size
            tradestat.sell_want_size -= ind.fill_size

            position.curr_size -= ind.fill_size

        if position.curr_size != 0:
            self.holding_securities.add(ind.security)
        else:
            self.holding_securities.remove(ind.security)

    def market_value(self, ref_date, ref_prices, suspensions=None):
        """
        Calculate total market value according to all current positions.
        NOTE for now this func only support stocks.

        Parameters
        ----------
        ref_date : int
            The date we refer to to get security position.
        ref_prices : dict of {security: price}
            The prices we refer to to get security price.
        suspensions : list of securities
            Securities that are suspended.

        Returns
        -------
        market_value : float

        """
        # TODO some securities could not be able to be traded
        if suspensions is None:
            suspensions = []

        market_value = 0.0
        for sec in self.holding_securities:
            if sec in suspensions:
                continue

            size = self.get_position(sec, ref_date).curr_size
            # TODO PortfolioManager object should not access price
            price = ref_prices[sec]
            market_value += price * size * 100

        return market_value


class Gateway(object):
    def __init__(self):
        self.callback = None

    def register_callback(self, callback):
        self.callback = callback

    @abstractmethod
    def init_from_config(self, props):
        pass

    @abstractmethod
    def sendOrder(self, order, algo, param):
        pass

    @abstractmethod
    def processQuote(self, quote):
        pass

    @abstractmethod
    def closeDay(self, trade_date):
        pass


class BaseGateway(object):
    """
    Strategy communicates with Gateway using APIs defined by ourselves;
    Gateway communicates with brokers using brokers' APIs;
    Gateway can also communicate with simulator.
    See the flow chart below:
          +--------+
          |Strategy|
          +--------+
              |
    portfolio | Custom API
         goal |
              |
      +-------v--------+
      |ManagementSystem|
      +----------------+
              |
       orders | Custom API
              |
          +---v---+
          |Gateway|
          +-------+
              |
       orders | Broker's API
              |
              |
         +----v-------------+
         |Exchange/Simulator|
         +------------------+

    Note: Gateway knows nothing about task_id but entrust_no,
          so does Simulator.

    """

    def __init__(self):
        self.callback = None
        self.cb_on_trade_ind = None
        self.cb_on_order_status = None
        self.cb_pm = None
        pass

    @abstractmethod
    def init_from_config(self, props):
        pass

    def register_callback(self, type_, callback):
        if type_ == 'on_trade_ind':
            self.cb_on_trade_ind = callback
        elif type_ == 'on_order_status':
            self.cb_on_order_status = callback
        elif type_ == 'portfolio manager':
            self.cb_pm = callback
        else:
            raise NotImplementedError("callback of type {}".format(type_))

    def on_new_day(self, trade_date):
        pass

    def place_order(self, order):
        """
        Send an order with determined task_id and entrust_no.

        Parameters
        ----------
        order : Order

        Returns
        -------
        task_id : str
            Task ID generated by entrust_order.
        err_msg : str.

        """
        # do sth.
        # return task_id, err_msg
        pass

    def cancel_order(self, task_id):
        """Cancel all want orders of a task according to its task ID.

        Parameters
        ----------
        task_id : str
            ID of the task.
            NOTE we CANNOT cancel order by entrust_no because this may break the execution of algorithm.

        Returns
        -------
        result : str
            Indicate whether the cancel succeed.
        err_msg : str

        """
        # do sth.
        # return result, err_msg
        pass

    def goal_portfolio(self, goals):
        """
        Let the system automatically generate orders according to portfolio positions goal.
        If there are want orders of any security in the strategy universe, this order will be rejected.

        Parameters
        -----------
        goals : list of GoalPosition
            This must include positions of all securities in the strategy universe.
            Use former value if there is no change.

        Returns
        --------
        result : bool
            Whether this command is accepted. True means the system's acceptance, instead of positions have changed.
        err_msg : str

        """
        pass

    def on_trade_ind(self, ind):
        self.cb_on_trade_ind(ind)


class DailyStockSimGateway(BaseGateway):
    def __init__(self):
        BaseGateway.__init__(self)

        self.simulator = StockSimulatorDaily()

    def init_from_config(self, props):
        pass

    def on_new_day(self, trade_date):
        self.simulator.on_new_day(trade_date)

    def place_order(self, order):
        err_msg = self.simulator.add_order(order)
        return err_msg

    def cancel_order(self, entrust_no):
        order_status_ind, err_msg = self.simulator.cancel_order(entrust_no)
        self.cb_on_order_status(order_status_ind)
        return err_msg

    @property
    def match_finished(self):
        return self.simulator.match_finished

    @abstractmethod
    def match(self, price_dict, date=19700101, time=0):
        """
        Match un-fill orders in simulator. Return trade indications.

        Parameters
        ----------
        price_dict : dict

        Returns
        -------
        list

        """
        return self.simulator.match(price_dict, date, time)


class StockSimulatorDaily(object):
    """This is not event driven!

    Attributes
    ----------
    __orders : list of Order
        Store orders that have not been filled.
    __order_id : int
        Current order id

    """

    def __init__(self):
        # TODO heap is better for insertion and deletion. We only need implement search of heapq module.
        self.__orders = dict()
        self.seq_gen = SequenceGenerator()

        self.date = 0
        self.time = 0

    def on_new_day(self, trade_date):
        self.date = trade_date
        self.time = 150000
        # self._refresh_orders() #TODO sometimes we do not want to refresh (multi-days match)

    def _refresh_orders(self):
        self.__orders.clear()

    def _next_fill_no(self):
        return str(self.date * 10000 + self.seq_gen.get_next('fill_no'))

    @property
    def match_finished(self):
        return len(self.__orders) == 0

    @staticmethod
    def _validate_order(order):
        # TODO to be enhanced
        assert order is not None

    @staticmethod
    def _validate_price(price_dic):
        # TODO to be enhanced
        assert price_dic is not None

    def add_order(self, order):
        """
        Add one order to the simulator.

        Parameters
        ----------
        order : Order

        Returns
        -------
        err_msg : str
            default ""

        """
        self._validate_order(order)

        if order.entrust_no in self.__orders:
            err_msg = "order with entrust_no {} already exists in simulator".format(order.entrust_no)
        self.__orders[order.entrust_no] = order
        err_msg = ""
        return err_msg

    def cancel_order(self, entrust_no):
        """
        Cancel an order.

        Parameters
        ----------
        entrust_no : str

        Returns
        -------
        err_msg : str
            default ""

        """
        popped = self.__orders.pop(entrust_no, None)
        if popped is None:
            err_msg = "No order with entrust_no {} in simulator.".format(entrust_no)
            order_status_ind = None
        else:
            err_msg = ""
            order_status_ind = OrderStatusInd()
            order_status_ind.init_from_order(popped)
            order_status_ind.order_status = common.ORDER_STATUS.CANCELLED
        return order_status_ind, err_msg

    def match(self, price_dic, date=19700101, time=0):
        self._validate_price(price_dic)

        results = []
        for order in self.__orders.values():  # TODO viewvalues()
            security = order.security
            df = price_dic[security]
            if 'VWAP' not in df.columns:
                df.loc[:, 'VWAP'] = df.loc[:, 'TURNOVER'] / df.loc[:, 'VOLUME']

            # get fill price
            if isinstance(order, FixedPriceTypeOrder):
                price_target = order.price_target
                fill_price = df.loc[:, price_target.upper()].values[0]
            elif isinstance(order, VwapOrder):
                if order.start != -1:
                    raise NotImplementedError("Vwap of a certain time range")
                fill_price = df.loc[:, 'VWAP'].values[0]
            elif isinstance(order, Order):
                # TODO
                fill_price = df.loc[:, 'CLOSE'].values[0]
            else:
                raise NotImplementedError("order class {} not support!".format(order.__class__))

            # get fill size
            fill_size = order.entrust_size - order.fill_size

            # create trade indication
            trade_ind = Trade()
            trade_ind.init_from_order(order)
            trade_ind.send_fill_info(fill_price, fill_size,
                                     date, time,
                                     self._next_fill_no())
            results.append(trade_ind)

            # update order status
            order.fill_price = (order.fill_price * order.fill_size
                                + fill_price * fill_size) / (order.fill_size + fill_size)
            order.fill_size += fill_size
            if order.fill_size == order.entrust_size:
                order.order_status = common.ORDER_STATUS.FILLED

        self.__orders = {k: v for k, v in self.__orders.viewitems() if not v.is_finished}
        # self.cancel_order(order.entrust_no)  # TODO DEBUG

        return results


class OrderBook(object):
    def __init__(self):
        self.orders = []
        self.trade_id = long(0)
        self.order_id = long(0)

    def nextTradeId(self):
        self.trade_id += 1
        return str(self.trade_id)

    def nextOrderId(self):
        self.order_id += 1
        return str(self.order_id)

    def addOrder(self, order):
        neworder = Order()
        # to do
        order.entrust_no = self.nextOrderId()
        neworder.copy(order)
        self.orders.append(neworder)

    def makeTrade(self, quote):

        if (quote.type == common.QUOTE_TYPE.TICK):
            return self.makeTickTrade(quote)

        if (quote.type == common.QUOTE_TYPE.MIN
            or quote.type == common.QUOTE_TYPE.FiVEMIN
            or quote.type == common.QUOTE_TYPE.QUARTERMIN
            or quote.type == common.QUOTE_TYPE.SPECIALBAR):
            return self.makeBarTrade(quote)

        if (quote.type == common.QUOTE_TYPE.DAILY):
            return self.makeDaiylTrade(quote)

    def makeBarTrade(self, quote):

        result = []
        # to be optimized
        for i in xrange(len(self.orders)):
            order = self.orders[i]
            if (quote.security != order.security):
                continue
            if (order.is_finished()):
                continue

            if (order.order_type == common.ORDER_TYPE.LIMIT):
                if (order.entrust_action == common.ORDER_ACTION.BUY and order.entrust_price >= quote.low):
                    trade = Trade()
                    trade.fill_no = self.nextTradeId()
                    trade.entrust_no = order.entrust_no
                    trade.security = order.security
                    trade.entrust_action = order.entrust_action
                    trade.fill_size = order.entrust_size
                    trade.fill_price = order.entrust_price
                    trade.fill_date = order.entrust_date
                    trade.fill_time = quote.time

                    order.order_status = common.ORDER_STATUS.FILLED
                    order.fill_size = trade.fill_size
                    order.fill_price = trade.fill_price

                    orderstatusInd = OrderStatusInd()
                    orderstatusInd.init_from_order(order)
                    result.append((trade, orderstatusInd))

                if (order.entrust_action == common.ORDER_ACTION.SELL and order.entrust_price <= quote.high):
                    trade = Trade()
                    trade.fill_no = self.nextTradeId()
                    trade.entrust_no = order.entrust_no
                    trade.security = order.security
                    trade.entrust_action = order.entrust_action
                    trade.fill_size = order.entrust_size
                    trade.fill_price = order.entrust_price
                    trade.fill_date = order.entrust_date
                    trade.fill_time = quote.time

                    order.order_status = common.ORDER_STATUS.FILLED
                    order.fill_size = trade.fill_size
                    order.fill_price = trade.fill_price

                    orderstatusInd = OrderStatusInd()
                    orderstatusInd.init_from_order(order)
                    result.append((trade, orderstatusInd))

            if (order.order_type == common.ORDER_TYPE.STOP):
                if (order.entrust_action == common.ORDER_ACTION.BUY and order.entrust_price <= quote.high):
                    trade = Trade()
                    trade.fill_no = self.nextTradeId()
                    trade.entrust_no = order.entrust_no
                    trade.security = order.security
                    trade.entrust_action = order.entrust_action
                    trade.fill_size = order.entrust_size
                    trade.fill_price = order.entrust_price
                    trade.fill_date = order.entrust_date
                    trade.fill_time = quote.time

                    order.order_status = common.ORDER_STATUS.FILLED
                    order.fill_size = trade.fill_size
                    order.fill_price = trade.fill_price
                    orderstatusInd = OrderStatusInd()
                    orderstatusInd.init_from_order(order)
                    result.append((trade, orderstatusInd))

                if (order.entrust_action == common.ORDER_ACTION.SELL and order.entrust_price >= quote.low):
                    trade = Trade()
                    trade.fill_no = self.nextTradeId()
                    trade.entrust_no = order.entrust_no
                    trade.security = order.security
                    trade.entrust_action = order.entrust_action
                    trade.fill_size = order.entrust_size
                    trade.fill_price = order.entrust_price
                    trade.fill_date = order.entrust_date
                    trade.fill_time = quote.time

                    order.order_status = common.ORDER_STATUS.FILLED
                    order.fill_size = trade.fill_size
                    order.fill_price = trade.fill_price
                    orderstatusInd = OrderStatusInd()
                    orderstatusInd.init_from_order(order)
                    result.append((trade, orderstatusInd))

        return result

    def cancelOrder(self, entrust_no):
        for i in xrange(len(self.orders)):
            order = self.orders[i]

            if (order.is_finished()):
                continue

            if (order.entrust_no == entrust_no):
                order.cancel_size = order.entrust_size - order.fill_size
                order.order_status = common.ORDER_STATUS.CANCELLED

            # todo
            orderstatus = OrderStatusInd()
            orderstatus.init_from_order(order)

            return orderstatus

    def cancelAllOrder(self):

        result = []

        for i in xrange(len(self.orders)):
            order = self.orders[i]

            if (order.is_finished()):
                continue
            order.cancel_size = order.entrust_size - order.fill_size
            order.order_status = common.ORDER_STATUS.CANCELLED

            # todo
            orderstatus = OrderStatusInd()
            orderstatus.init_from_order(order)
            result.append(orderstatus)

        return result


##########################################################################

class BarSimulatorGateway(Gateway):
    # ----------------------------------------------------------------------
    def __init__(self):
        Gateway.__init__(self)
        self.orderbook = OrderBook()

    def init_from_config(self, props):
        pass

    def sendOrder(self, order, algo, param):
        self.orderbook.addOrder(order)
        self.callback.on_order_rsp(order, True, '')

    def processQuote(self, quote):
        return self.orderbook.makeTrade(quote)

    def closeDay(self, trade_date):
        return self.orderbook.cancelAllOrder()
