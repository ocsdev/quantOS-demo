# encoding:utf-8


class Trade(object):
    """
    Basic order class.

    Attributes
    ----------
    task_id : str
        id of the task.
    entrust_no : str
        ID of the order.
    entrust_action : str
    security : str
    fill_price : float
    fill_size : int
    fill_date : int
    fill_time : int
    fill_no : str

    Methods
    -------


    """
    def __init__(self):
        self.task_id = ""
        self.entrust_no = ""

        self.entrust_action = ""

        self.security = ""

        self.fill_price = 0.0
        self.fill_size = 0
        self.fill_date = 0
        self.fill_time = 0
        self.fill_no = ""

