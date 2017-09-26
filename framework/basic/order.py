# encoding:utf-8

from framework import common


class Order(object):
    """
    Basic order class.

    Attributes
    ----------
    task_id : str
        id of the task.
    entrust_no : str
        ID of the order.
    security : str
    entrust_action : str
        Action of the trade.
    entrust_price : double
        Price of the order.
    entrust_size : int
        Quantity of the order.
    entrust_date : int
        Date of the order.
    entrust_time : int
        Time of the order.
    sub_seq : int
        Number of sub-orders, start with 0.
    sub_total : int
        Total number of sub-orders.
    batch_no : int
        Number of batch.
    order_status : str
    fill_price : float
    fill_size : int
    algo : str
    order_type : str (common.ORDER_TYPE)
        market, limit, stop, etc.
    time_in_force : str (common.ORDER_TIME_IN_FORCE)
        FAK, FOK, GTM, etc.

    Methods
    -------

    """
    def __init__(self):
        self.task_id = ""
        self.entrust_no = ""

        self.security = ""

        self.entrust_action = ""
        self.entrust_price = 0.0
        self.entrust_size = 0
        self.entrust_date = 0
        self.entrust_time = 0

        self.sub_seq = 0
        self.sub_total = 0
        self.batch_no = 0

        self.order_status = ""
        self.fill_price = 0.0
        self.fill_size = 0

        self.algo = ""

        self.order_type = ""
        self.time_in_force = ""

        # TODO attributes below only for backward compatibility
        self.errmsg = ""
        self.cancel_size = 0
        self.reforderid = ''

    def __eq__(self, other):
        return self.entrust_no == other.entrust_no

    def __cmp__(self, other):
        return cmp(self.entrust_no, other.entrust_no)

    def copy(self, order):
        self.task_id = order.task_id
        self.entrust_no = order.entrust_no

        self.security = order.security

        self.entrust_action = order.entrust_action
        self.entrust_price = order.entrust_price
        self.entrust_size = order.entrust_size
        self.entrust_date = order.entrust_date
        self.entrust_time = order.entrust_time

        self.sub_seq = order.sub_seq
        self.sub_total = order.sub_total
        self.batch_no = order.batch_no

        self.order_status = order.order_status
        self.fill_size = order.fill_size
        self.fill_price = order.fill_price

        self.algo = order.algo

        self.order_type = order.order_type
        self.time_in_force = order.time_in_force

        self.cancel_size = order.cancel_size
        self.reforderid = order.reforderid
        self.errmsg = order.errmsg

    def is_finished(self):
        return self.order_status == common.ORDER_STATUS.FILLED or self.order_status == common.ORDER_STATUS.CANCELLED or self.order_status == common.ORDER_STATUS.REJECTED


class FixedPriceTypeOrder(Order):
    """
    This type of order aims to be matched at a given price type, eg. CLOSE, OPEN, VWAP, etc.
    Only used in daily resolution backtest.

    Attributes
    ----------
    price_target : str
        The type of price we want.

    Methods
    -------

    """
    def __init__(self, target=""):
        Order.__init__(self)

        self.price_target = target


class VwapOrder(Order):
    """
    This type of order will only be matched once a day.
    Only used in daily resolution backtest.

    Attributes
    ----------
    start : int
        The start of matching time range.
        If start = -1, end will be ignored and the order will be matched in the whole trading session.
    end : int
        The end of matching time range.

    """
    def __init__(self, start=-1, end=-1):
        Order.__init__(self)

        self.start = start
        self.end = end

    @property
    def time_range(self):
        return self.start, self.end


