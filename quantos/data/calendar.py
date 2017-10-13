# encoding: utf-8

import datetime

import numpy as np
import pandas as pd
from pandas.tseries import offsets

from quantos.data.dbmanager import get_jzts_connection
from quantos.data.dataservice import RemoteDataService
from quantos.util import dtutil


class Calendar(object):
    """
    A calendar for manage trade date.
    
    Attributes
    ----------
    data_api :

    """
    
    def __init__(self, data_api=None):
        self.conn = get_jzts_connection()
        if data_api is None:
            self.data_api = RemoteDataService()
        else:
            self.data_api = data_api

    def get_trade_date_range(self, begin, end):
        """
        Get array of trade dates within given range.
        
        Parameters
        ----------
        begin : int
            YYmmdd
        end : int

        Returns
        -------
        trade_dates_arr : np.ndarray
            dtype = int

        """
        filter_argument = self.data_api._dic2url({'start_date': begin,
                                                  'end_date': end})

        df_raw, msg = self.data_api.query("jz.secTradeCal", fields="trade_date",
                                          filter=filter_argument, orderby="")
        trade_dates_arr = df_raw['trade_date'].values.astype(int)
        return trade_dates_arr

    def get_last_trade_date(self, date):
        """
        
        Parameters
        ----------
        date : int

        Returns
        -------
        res : int

        """
        dt = dtutil.convert_int_to_datetime(date)
        delta = pd.Timedelta(weeks=2)
        dt_old = dt - delta
        date_old = dtutil.convert_datetime_to_int(dt_old)
        
        dates = self.get_trade_date_range(date_old, date)
        mask = dates < date
        res = dates[mask][-1]
        
        return res

    def get_next_trade_date(self, date):
        """
        
        Parameters
        ----------
        date : int

        Returns
        -------
        res : int

        """
        dt = dtutil.convert_int_to_datetime(date)
        delta = pd.Timedelta(weeks=2)
        dt_new = dt + delta
        date_new = dtutil.convert_datetime_to_int(dt_new)
    
        dates = self.get_trade_date_range(date, date_new)
        mask = dates > date
        res = dates[mask][0]
    
        return res


class Calendar_OLD(object):
    """
    A calendar for manage trade date.
    
    """
    
    def __init__(self):
        self.conn = get_jzts_connection()
    
    def get_last_trade_date(self, date):
        sql = 'select max(date) as date from Calendar where date < %d' % (date)
        df = pd.read_sql(sql, self.conn)
        return df['date'][0]
    
    def get_next_trade_date(self, date):
        sql = 'select min(date) as date from Calendar where date > %d' % (date)
        df = pd.read_sql(sql, self.conn)
        return df['date'][0]
    
    @staticmethod
    def get_next_period_day(current, period, n):
        """
        Get the n'th day in next period from current day.

        Parameters
        ----------
        current : int
            Current date in format "%Y%m%d".
        period : str
            Interval between current and next. {'day', 'week', 'month'}
        n : int
            n'th business day after next period.

        Returns
        -------
        nxt : int

        """
        current_dt = dtutil.convert_int_to_datetime(current)
        if period == 'day':
            offset = offsets.BDay()  # move to next business day
        elif period == 'week':
            offset = offsets.Week(weekday=0)  # move to next Monday
        elif period == 'month':
            offset = offsets.BMonthBegin()  # move to first business day of next month
        else:
            raise NotImplementedError("Frequency as {} not support".format(period))
        
        next_dt = current_dt + offset
        if n:
            next_dt = next_dt + n * offsets.BDay()
        nxt = dtutil.convert_datetime_to_int(next_dt)
        return nxt
    
    def get_trade_date_range(self, begin, end):
        sql = 'select date from Calendar where date >= %d  and date <= %d' % (begin, end)
        df = pd.read_sql(sql, self.conn)
        return df['date']
    
    @staticmethod
    def convert_int_to_datetime(dt):
        """Convert int date (%Y%m%d) to datetime.datetime object."""
        if isinstance(dt, pd.Series):
            dt = dt.astype(str)
        elif isinstance(dt, int):
            dt = str(dt)
        return pd.to_datetime(dt, format="%Y%m%d")
    
    @staticmethod
    def convert_datetime_to_int(dt):
        f = lambda x: x.year * 10000 + x.month * 100 + x.day
        if isinstance(dt, datetime.datetime):
            dt = pd.Timestamp(dt)
            res = f(dt)
        elif isinstance(dt, np.datetime64):
            dt = pd.Timestamp(dt)
            res = f(dt)
        else:
            dt = pd.Series(dt)
            res = dt.apply(f)
        return res
    
    @staticmethod
    def shift(date, n_weeks=0):
        """Shift date backward or forward for n weeks.
        
        Parameters
        ----------
        date : int or datetime
            The date to be shifted.
        n_weeks : int, optional
            Positive for increasing date, negative for decreasing date.
            Default 0 (no shift).
        
        Returns
        -------
        res : int or datetime
        
        """
        delta = pd.Timedelta(weeks=n_weeks)
        
        is_int = isinstance(date, (int, np.integer))
        if is_int:
            dt = dtutil.convert_int_to_datetime(date)
        else:
            dt = date
        res = dt + delta
        if is_int:
            res = dtutil.convert_datetime_to_int(res)
        return res

