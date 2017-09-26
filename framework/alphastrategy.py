# encoding: utf-8

from abc import abstractmethod
from collections import defaultdict

import numpy as np

from framework import common
from framework.basic.order import *
from framework.basic.position import GoalPosition
from framework.gateway import PortfolioManager
from util.sequence import SequenceGenerator


class StrategyContext(object):
    """
    Used to store relevant context of the strategy.

    Attributes
    ----------
    data_server : framework.DataServer object
        Data provider for the strategy.
    gateway : gateway.Gateway object
        Broker of the strategy.
    universe : list of str
        Securities that the strategy cares about.
    calendar : framework.Calendar object
        A certain calendar that the strategy refers to.

    Methods
    -------
    add_universe(univ)
        Add new securities.

    """
    def __init__(self):
        self.data_server = None
        self.gateway = None
        self.universe = []
        self.calendar = None

    def add_universe(self, univ):
        """univ could be single security or securities separated by ,"""
        self.universe += univ.split(',')


class BaseStrategy(object):
    """
    Base strategy class.

    Attributes
    ----------
    context : strategy.StrategyContext object
        Used to store relevant context of the strategy.
    run_mode : int
        Whether the strategy is under back-testing or live trading.
    trade_date : int
        current trading date (may be inconsistent with calendar date).
    pm : framework.PortfolioManger
        Responsible for managing orders, trades and positions.

    Methods
    -------

    """
    # TODO we need a better way to deal with err_msg
    def __init__(self):
        self.context = StrategyContext()
        self.run_mode = common.RUN_MODE.BACKTEST

        self.trade_date = 0

        self.pm = PortfolioManager(self)

        self.seq_gen = SequenceGenerator()

        self.task_map = defaultdict(list)

    def init_from_config(self, props):
        univ = props.get('universe', "")
        self.add_universe(univ)
        pass

    def initialize(self, run_mode):
        self.run_mode = run_mode
        self.register_callback()
        pass

    def register_callback(self):
        gw = self.context.gateway
        gw.register_callback('portfolio manager', self.pm)
        gw.register_callback('on_trade_ind', self.on_trade_ind)
        gw.register_callback('on_order_status', self.on_trade_ind)

    def add_universe(self, univ):
        self.context.add_universe(univ)

    def on_new_day(self, trade_date):
        last_date = self.trade_date
        self.trade_date = trade_date
        self.pm.on_new_day(self.trade_date, last_date)

    def _get_next_num(self, key):
        return str(self.trade_date * 10000 + self.seq_gen.get_next(key))

    def place_order(self, security, action, price, size, algo="", algo_param=None):
        """
        Send a request with an order to the system. Execution algorithm will be automatically chosen.
        Returns task_id which can be used to query execution and orders of this task.

        Parameters
        ----------
        security : str
            the symbol of security to be ordered, eg. "000001.SZ".
        action : str
        price : float.
            The price to be ordered at.
        size : int
            The quantity to be ordered at.
        algo : str
            The algorithm to be used. If None then use default algorithm.
        algo_param : dict
            Parameters of the algorithm. Default {}.

        Returns
        -------
        task_id : str
            Task ID generated by entrust_order.
        err_msg : str.

        """
        if algo:
            raise NotImplementedError("algo {}".format(algo))

        order = Order()
        order.task_id = self._get_next_num('task_id')
        order.entrust_no = self._get_next_num('entrust_no')
        order.security = security
        order.entrust_action = action
        order.entrust_price = price
        order.entrust_size = size
        order.entrust_date = self.trade_date
        order.entrust_time = 0  # TODO
        order.order_status = common.ORDER_STATUS.NEW

        self.task_map[order.task_id].append(order.entrust_no)

        self.pm.add_order(order)

        err_msg = self.context.gateway.place_order(order)

        if err_msg:
            return '0', err_msg
        else:
            return order.task_id, err_msg

    def cancel_order(self, task_id):
        """Cancel all uncome orders of a task according to its task ID.

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
        entrust_no_list = self.task_map.get(task_id, None)
        if entrust_no_list is None:
            return False, "No task id {}".format(task_id)

        err_msgs = []
        for entrust_no in entrust_no_list:
            err_msg = self.context.gateway.cancel_order(entrust_no)
            err_msgs.append(err_msg)
        if any(map(lambda s: bool(s), err_msgs)):
            return False, ','.join(err_msgs)
        else:
            return True, ""

    def place_batch_order(self, orders, algo="", algo_param=None):
        """Send a batch of orders to the system together.

        Parameters
        -----------
        orders : list
            a list of framework.model.Order objects.
        algo : str
            The algorithm to be used. If None then use default algorithm.
        algo_param : dict
            Parameters of the algorithm. Default {}.

        Returns
        -------
        task_id : str
            Task ID generated by entrust_order.
        err_msg : str.

        """
        task_id = self._get_next_num('task_id')
        err_msgs = []
        for order in orders:
            order.task_id = task_id
            order.entrust_no = self._get_next_num('entrust_no')

            self.pm.add_order(order)

            err_msg = self.context.gateway.place_order(order)
            err_msgs.append(err_msg)

            self.task_map[order.task_id].append(order.entrust_no)

        return task_id, ','.join(err_msgs)

    def query_portfolio(self):
        """
        Return net positions of all securities in the strategy universe (including zero positions).

        Returns
        --------
        positions : list of framework.model.Position}
            Current position of the strategy.
        err_msg : str

        """
        pass

    def goal_portfolio(self, goals):
        """
        Let the system automatically generate orders according to portfolio positions goal.
        If there are uncome orders of any security in the strategy universe, this order will be rejected.

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

    def query_order(self, task_id):
        """
        Query order information of current day.

        Parameters
        ----------
        task_id : str
            ID of the task. if None, return all orders of the day; else return orders of this task.

        Returns
        -------
        orders : list of framework.model.Order objects.
        err_msg : str.

        """
        pass

    def query_trade(self, task_id):
        """
        Query trade information of current day.

        Parameters
        -----------
        task_id : int
            ID of the task. if None, return all trades of the day; else return trades of this task.

        Returns
        --------
        trades : list of framework.model.Trade objects.
        err_msg : str.

        """
        pass

    def on_trade_ind(self, ind):
        """

        Parameters
        ----------
        ind : TradeInd

        Returns
        -------

        """
        self.pm.on_trade_ind(ind)
        print str(ind)

    def on_order_status(self, ind):
        """

        Parameters
        ----------
        ind : OrderStatusInd

        Returns
        -------

        """
        self.pm.on_order_status(ind)


class AlphaStrategy(BaseStrategy):
    """
    Alpha strategy class.

    Attributes
    ----------
    period : str
        Interval between current and next. {'day', 'week', 'month'}
    days_delay : int
        n'th business day after next period.
    weights : np.array with the same shape with self.context.universe
    benchmark : str
        The benchmark security.

    Methods
    -------

    """
    def __init__(self):
        BaseStrategy.__init__(self)

        self.period = ""
        self.days_delay = 0
        self.cash = 0
        self.position_ratio = 0.0

        self.weights = None

        self.benchmark = ""

    def init_from_config(self, props):
        BaseStrategy.init_from_config(self, props)
        self.cash = props['init_balance']
        self.period = props['period']
        self.days_delay = props['days_delay']
        self.position_ratio = props['position_ratio']

    @abstractmethod
    def calc_weights(self):
        """
        Calculate target weights of each security in the strategy universe.
        This must be implemented by user.

        Returns
        -------
        weights : np.array
            Weights of each security, the order corresponds to universe.
            eg. return np.ones_like(self.context.universe, dtype=float) / len(self.context.universe)

        """
        pass

    def re_weight_suspension(self, suspensions=None):
        """
        How we deal with weights when there are suspension securities.

        Parameters
        ----------
        suspensions : list of securities
            None if no suspension.

        """
        # TODO this can be refine: consider whether we increase or decrease shares on a suspended security.
        if suspensions is None:
            return

        univ = self.context.universe

        if len(suspensions) == len(univ):
            raise ValueError("All suspended")  # TODO custom error

        mask = np.array(map(lambda s: s in suspensions, univ), dtype=bool)
        sus_weight = np.sum(self.weights[mask])
        self.weights[mask] = 0.0

        adjust_ratio = 1.0 / (1.0 - sus_weight)
        self.weights *= adjust_ratio

    def get_univ_prices(self):
        ds = self.context.data_server

        univ_str = ','.join(self.context.universe)

        df_dic = ds.daily(univ_str, self.trade_date, self.trade_date, "")
        return df_dic

    def _re_balance(self, suspensions=None):
        """
        Do portfolio re-balance.
        For now, we stick to the same close price when calculate market value and do re-balance.

        Parameters
        ----------
        suspensions : list of securities, default = None

        """

        self.calc_weights()
        self.re_weight_suspension(suspensions)

        df_dic = self.get_univ_prices()
        prices = {k: v.loc[:, 'CLOSE'].values[0] for k, v in df_dic.items()}

        market_value = self.pm.market_value(self.trade_date, prices, suspensions)  # TODO need close price
        cash_available = self.cash + market_value
        print "\n\n{}, cash all = {:9.4e}".format(self.trade_date, cash_available) #DEBUG
        orders, cash_remain = self.generate_weights_order(dict(zip(self.context.universe, self.weights)),
                                                          cash_available * self.position_ratio,
                                                          prices,
                                                          algo='CLOSE')
        self.cash = cash_remain
        self.liquidate_all()
        self.place_batch_order(orders)

    def liquidate_all(self):
        for sec in self.pm.holding_securities:
            curr_size = self.pm.get_position(sec, self.trade_date).curr_size
            self.place_order(sec, common.ORDER_ACTION.SELL, 1e-3, curr_size)

    def generate_weights_order(self, goals, turnover, prices, algo="close"):
        """
        Send order according subject to total turnover and weights of different securities.

        Parameters
        ----------
        goals : dict of {security: weight}
            Weight of each security.
        turnover : float
            Total turnover goal of all securities.
        prices : dict of {str: float}
            {security: price}
        algo : str
            {'CLOSE', 'open', 'vwap', etc.}

        Returns
        -------
        orders : list of Order
        cash_left : float

        """
        if algo not in ['CLOSE', 'vwap']:
            raise NotImplementedError("Currently we only suport order at close price.")

        cash_left = 0.0
        orders = []
        if algo == 'CLOSE' or 'vwap':  # order a certain amount of shares according to current close price
            for sec, w in goals.items():
                if algo == 'CLOSE':
                    order = FixedPriceTypeOrder()
                    order.price_target = 'CLOSE'
                else:
                    order = VwapOrder()
                order.security = sec

                if w == 0.0:
                    order.entrust_size = 0
                    continue

                price = prices[sec]
                shares_raw = w * turnover / price
                shares = int(round(shares_raw / 100., 0))  # TODO cash may be not enough
                shares_left = shares_raw - shares * 100  # may be negative
                cash_left += shares_left * price

                order.entrust_size = shares
                order.entrust_action = common.ORDER_ACTION.BUY

                orders.append(order)


        return orders, cash_left
