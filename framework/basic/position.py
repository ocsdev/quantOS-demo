# encoding:utf-8


class Position(object):
    """
    Basic position class.

    Attributes
    ----------
    security : str
        List of securities.
    side : str
        ("Long", "Short"). Positions of different sides will not be merged.
    cost_price : float
        Average cost price of current net position.
    close_pnl : float
    float_pnl : float
    trading_pnl : float
    holding_pnl : float
    init_size : int
        Position at the start of the day.
    enable_size : int
        Position that can be closed.
    current_size : int
    frozen_size : int
    uncome_size : int
        Position which is on its way.
    pre_size : int
        Last day's position.
    cur_size : int
        Today's position.

    Methods
    -------

    """
    def __init__(self):
        self.security = ""

        self.side = ""
        self.cost_price = 0.0

        self.close_pnl = 0.0
        self.float_pnl = 0.0
        self.trading_pnl = 0.0
        self.holding_pnl = 0.0

        self.init_size = 0
        self.enable_size = 0
        self.current_size = 0
        self.frozen_size = 0
        self.uncome_size = 0

        self.pre_size = 0
        self.cur_size = 0


class GoalPosition(object):
    """
    Used in goal_portfolio function to generate orders.

    Attributes
    ----------
    security : str
    ref_price : float
        Reference price, used by risk management, not by order.
    size : int
        Target position size.
    urgency : int
        The urgency to adjust position, used to determine trading algorithm.

    """
    def __init__(self):
        self.security = ""
        self.ref_price = 0.0
        self.size = 0
        self.urgency = 0

