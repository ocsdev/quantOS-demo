# encoding: UTF-8

from abc import abstractmethod
from datetime import datetime

import numpy as np
import pandas as pd
from framework import common
from jzdataapi.data_api import DataApi
from framework.pubsub import Publisher
from framework.jzcalendar import JzCalendar


class Quote(object):
    # ----------------------------------------------------------------------
    def __init__(self, type):
        self.type = type
        self.frequency = 0
        self.security = ''
        self.refsecurity = ''
        self.date = 0
        self.time = 0
        self.open = 0.0
        self.high = 0.0
        self.low = 0.0
        self.close = 0.0
        self.volume = 0.0
        self.turnover = 0.0
        self.oi = 0.0
        self.settle = 0.0
        self.preclose = 0.0
        self.presettle = 0.0
        self.bidprice = []
        self.bidvol = []
        self.askprice = []
        self.askvol = []
        
        self.format = '%Y%m%d %H:%M:%S %f'
    
    def getDate(self):
        """
        dt = datetime.strptime(self.time, self.format)
        return int(dt.strftime('%Y%m%d'))
        """
        return self.date
    
    def getTime(self):
        """
        dt = datetime.strptime(self.time, self.format)
        return int(dt.strftime('%H%M%S'))
        """
        return self.time
    
    def show(self):
        print self.type, self.time, self.security, self.open, self.high, self.low, self.close, self.volume, self.turnover


class BaseDataServer(Publisher):
    """
    DataServer is a base class providing both historic and live data
    from various data sources.
    Current API version: 1.6

    Derived classes of DataServer hide different data source, but use the same API.

    Attributes
    ----------
    source_name : str
        Name of data source.

    Methods
    -------
    subscribe
    quote
    daily
    bar
    tick
    query

    """
    # TODO we need a uniform convert from str/int to standard date object, for all derived classes.
    def __init__(self, name=""):
        Publisher.__init__(self)
        
        if name:
            self.source_name = name
        else:
            self.source_name = str(self.__class__.__name__)
    
    def init_from_config(self, props):
        pass
    
    def initialize(self):
        pass
    
    # TODO deprecated
    def add_batch_subscribe(self, subscriber, securities):
        """
        Add subscriber to multiple securities at once.

        Parameters
        ----------
        subscriber : object
        securities : list or str separated by ','

        """
        if isinstance(securities, (str, unicode)):
            l = securities.split(',')
        else:
            l = securities
        for sec in l:
            self.add_subscriber(subscriber, sec)
    
    def subscribe(self, targets, callback):
        """
        Subscribe real time market data, including bar and tick,
        processed by respective callback function.

        Parameters
        ----------
        targets : str
            Security and type, eg. "000001.SH/tick,cu1709.SHF/1m"
        callback : dict of {'on_tick': func1, 'on_bar': func2}
            Call back functions.

        """
        # TODO for now it will not publish event
        for target in targets.split(','):
            sec, data_type = target.split('/')
            if data_type == 'tick':
                func = callback['on_tick']
            else:
                func = callback['on_bar']
            self.add_subscriber(func, target)
    
    @abstractmethod
    def quote(self, security, fields=""):
        """
        Query latest market data in DataFrame.
        
        Parameters
        ----------
        security : str
        fields : str, optional
            default ""

        Returns
        -------
        df : pd.DataFrame
        msg : str
            error code and error message joined by comma

        """
        pass
    
    @abstractmethod
    def daily(self, security, start_date, end_date, fields="", adjust_mode=None):
        """
        Query dar bar,
        support auto-fill suspended securities data,
        support auto-adjust for splits, dividends and distributions.

        Parameters
        ----------
        security : str
            support multiple securities, separated by comma.
        start_date : int or str
            YYYMMDD or 'YYYY-MM-DD'
        end_date : int or str
            YYYMMDD or 'YYYY-MM-DD'
        fields : str, optional
            separated by comma ',', default "" (all fields included).
        adjust_mode : str or None, optional
            None for no adjust;
            'pre' for forward adjust;
            'post' for backward adjust.

        Returns
        -------
        df : pd.DataFrame
            columns:
                security, code, trade_date, open, high, low, close, volume, turnover, vwap, oi, suspended
        msg : str
            error code and error message joined by comma

        Examples
        --------
        df, msg = api.daily("00001.SH,cu1709.SHF",start_date=20170503, end_date=20170708,
                            fields="open,high,low,last,volume", fq=None, skip_suspended=True)

        """
        pass
    
    @abstractmethod
    def bar(self, security, start_time=200000, end_time=160000, trade_date=None, cycle='1m', fields=""):
        """
        Query minute bars of various type, return DataFrame.

        Parameters
        ----------
        security : str
            support multiple securities, separated by comma.
        start_time : int (HHMMSS) or str ('HH:MM:SS')
            Default is market open time.
        end_time : int (HHMMSS) or str ('HH:MM:SS')
            Default is market close time.
        trade_date : int (YYYMMDD) or str ('YYYY-MM-DD')
            Default is current trade_date.
        fields : str, optional
            separated by comma ',', default "" (all fields included).
        cycle : framework.common.MINBAR_TYPE, optional
            {'1m', '5m', '15m'}, Minute bar type, default is '1m'

        Returns
        -------
        df : pd.DataFrame
            columns:
                security, code, date, time, trade_date, cycle, open, high, low, close, volume, turnover, vwap, oi
        msg : str
            error code and error message joined by comma

        Examples
        --------
        df, msg = api.bar("000001.SH,cu1709.SHF", start_time="09:56:00", end_time="13:56:00",
                          trade_date="20170823", fields="open,high,low,last,volume", cycle="5m")

        """
        # TODO data_server DOES NOT know "current date".
        pass
    
    @abstractmethod
    def tick(self, security, start_time=200000, end_time=160000, trade_date=None, fields=""):
        """
        Query tick data in DataFrame.
        
        Parameters
        ----------
        security : str
        start_time : int (HHMMSS) or str ('HH:MM:SS')
            Default is market open time.
        end_time : int (HHMMSS) or str ('HH:MM:SS')
            Default is market close time.
        trade_date : int (YYYMMDD) or str ('YYYY-MM-DD')
            Default is current trade_date.
        fields : str, optional
            separated by comma ',', default "" (all fields included).

        Returns
        -------
        df : pd.DataFrame
        err_msg : str
            error code and error message joined by comma
            
        """
        pass
    
    @abstractmethod
    def query(self, view, filter, fields):
        """
        Query reference data.
        Input query type and parameters, return DataFrame.
        
        Parameters
        ----------
        view : str
            Type of reference data. See doc for details.
        filter : str
            Query conditions, separated by '&'.
        fields : str
            Fields to return, separated by ','.

        Returns
        -------
        df : pd.DataFrame
        err_msg : str
            error code and error message joined by comma

        """
        pass
    
    @abstractmethod
    def get_split_dividend(self):
        pass
    
    def get_suspensions(self):
        pass


class JzDataServer(BaseDataServer):
    """
    JzDataServer uses data from jz's local database.

    """
    # TODO no validity check for input parameters
    
    def __init__(self):
        BaseDataServer.__init__(self)
        
        address = 'tcp://10.1.0.210:8910'
        self.api = DataApi(address, use_jrpc=False)
        self.api.set_timeout(60)
        r, msg = self.api.login("test", "123")
        if not r:
            print msg
        else:
            print "DataAPI login success."
        
        self.REPORT_DATE_FIELD_NAME = 'report_date'

    def daily(self, security, start_date, end_date,
              fields="", adjust_mode=None):
        df, err_msg = self.api.daily(security=security, start_date=start_date, end_date=end_date,
                                     fields=fields, adjust_mode=adjust_mode, data_format="")
        return df, err_msg

    def bar(self, security,
            start_time=200000, end_time=160000, trade_date=None,
            cycle='1m', fields=""):
        df, msg = self.api.bar(security=security, fields=fields,
                               start_time=start_time, end_time=end_time, trade_date=trade_date,
                               cycle='1m', data_format="")
        return df, msg
    
    def query(self, view, filter="", fields="", **kwargs):
        """
        Get various reference data.
        
        Parameters
        ----------
        view : str
            data source.
        fields : str
            Separated by ','
        filter : str
            filter expressions.
        kwargs

        Returns
        -------
        df : pd.DataFrame
        msg : str
            error code and error message, joined by ','
        
        Examples
        --------
        res3, msg3 = ds.query("wd.secDailyIndicator", fields="price_level,high_52w_adj,low_52w_adj",
                              filter="start_date=20170907&end_date=20170907",
                              orderby="trade_date",
                              data_format='pandas')
            view does not change. fileds can be any field predefined in reference data api.

        """
        df, msg = self.api.query(view, fields=fields, filter=filter, data_format="", **kwargs)
        return df, msg
    
    def get_suspensions(self):
        return None
    
    def get_trade_date(self, start_date, end_date, security=None, is_datetime=False):
        if security is None:
            security = '000300.SH'
        df, msg = self.daily(security, start_date, end_date, fields="close")
        res = df.loc[:, 'trade_date'].values
        if is_datetime:
            res = JzCalendar.convert_int_to_datetime(res)
        return res
    
    @staticmethod
    def _dic2url(d):
        """
        Convert a dict to str like 'k1=v1&k2=v2'
        
        Parameters
        ----------
        d : dict

        Returns
        -------
        str

        """
        l = ['='.join([key, str(value)]) for key, value in d.items()]
        return '&'.join(l)

    def query_wd_fin_stat(self, type_, security, start_date, end_date, fields=""):
        """
        Helper function to call data_api.query with 'wd.income' more conveniently.
        
        Parameters
        ----------
        type_ : {'income', 'balance_sheet', 'cash_flow'}
        security : str
            separated by ','
        start_date : int
            Annoucement date in results will be no earlier than start_date
        end_date : int
            Annoucement date in results will be no later than start_date
        fields : str, optional
            separated by ',', default ""

        Returns
        -------
        df : pd.DataFrame
            index date, columns fields
        msg : str

        """
        view_map = {'income': 'wd.income', 'cash_flow': 'wd.cashFlow', 'balance_sheet': 'wd.balanceSheet'}
        view_name = view_map.get(type_, None)
        if view_name is None:
            raise NotImplementedError("type_ = {:s}".format(type_))
        
        filter_argument = self._dic2url({'security': security,
                                         'start_date': start_date,
                                         'end_date': end_date,
                                         'report_type': '408002000',  # joint sheet
                                         'update_flag': '0'})  # 0 means first time, not update
        
        return self.query(view_name, fields=fields, filter=filter_argument,
                          order_by=self.REPORT_DATE_FIELD_NAME)

    def query_wd_balance_sheet(self, security, start_date, end_date, fields="", extend=0):
        """
        Helper function to call data_api.query with 'wd.income' more conveniently.
        
        Parameters
        ----------
        security : str
            separated by ','
        start_date : int
            Annoucement date in results will be no earlier than start_date
        end_date : int
            Annoucement date in results will be no later than start_date
        fields : str, optional
            separated by ',', default ""
        extend : int, optional
            If not zero, extend for weeks.

        Returns
        -------
        df : pd.DataFrame
            index date, columns fields
        msg : str

        """
        # extend 1 year
        if extend:
            start_dt = JzCalendar.convert_int_to_datetime(start_date)
            start_dt = start_dt - pd.Timedelta(weeks=extend)
            start_date = JzCalendar.convert_datetime_to_int(start_dt)
    
        filter_argument = self._dic2url({'security': security,
                                         'start_date': start_date,
                                         'end_date': end_date,
                                         'report_type': '408002000'})
    
        return self.query("wd.balanceSheet", fields=fields, filter=filter_argument,
                          order_by=self.REPORT_DATE_FIELD_NAME)

    def query_wd_cash_flow(self, security, start_date, end_date, fields="", extend=0):
        """
        Helper function to call data_api.query with 'wd.income' more conveniently.
        
        Parameters
        ----------
        security : str
            separated by ','
        start_date : int
            Annoucement date in results will be no earlier than start_date
        end_date : int
            Annoucement date in results will be no later than start_date
        fields : str, optional
            separated by ',', default ""
        extend : int, optional
            If not zero, extend for weeks.

        Returns
        -------
        df : pd.DataFrame
            index date, columns fields
        msg : str

        """
        # extend 1 year
        if extend:
            start_dt = JzCalendar.convert_int_to_datetime(start_date)
            start_dt = start_dt - pd.Timedelta(weeks=extend)
            start_date = JzCalendar.convert_datetime_to_int(start_dt)
    
        filter_argument = self._dic2url({'security': security,
                                         'start_date': start_date,
                                         'end_date': end_date,
                                         'report_type': '408002000'})
    
        return self.query("wd.cashFlow", fields=fields, filter=filter_argument,
                          order_by=self.REPORT_DATE_FIELD_NAME)
    
    def query_wd_dailyindicator(self, security, start_date, end_date, fields=""):
        """
        Helper function to call data_api.query with 'wd.secDailyIndicator' more conveniently.
        
        Parameters
        ----------
        security : str
            separated by ','
        start_date : int
        end_date : int
        fields : str, optional
            separated by ',', default ""

        Returns
        -------
        df : pd.DataFrame
            index date, columns fields
        msg : str
        
        """
        filter_argument = self._dic2url({'security': security,
                                         'start_date': start_date,
                                         'end_date': end_date})
    
        return self.query("wd.secDailyIndicator",
                          fields=fields,
                          filter=filter_argument,
                          orderby="trade_date")

    def get_index_comp(self, index, start_date, end_date):
        """
        Return all securities that have been in index during start_date and end_date.
        
        Parameters
        ----------
        index : str
            separated by ','
        start_date : int
        end_date : int

        Returns
        -------
        list

        """
        filter_argument = self._dic2url({'index_code': index,
                                         'start_date': start_date,
                                         'end_date': end_date})
    
        df_io, msg = self.query("wd.indexCons", fields="",
                                filter=filter_argument, orderby="security")
        if msg != '0,':
            print msg
        return list(np.unique(df_io.loc[:, 'security']))
    
    def get_index_comp_df(self, index, start_date, end_date):
        """
        Get index components on each day during start_date and end_date.
        
        Parameters
        ----------
        index : str
            separated by ','
        start_date : int
        end_date : int

        Returns
        -------
        res : pd.DataFrame
            index dates, columns all securities that have ever been components,
            values are 0 (not in) or 1 (in)
        msg : str

        """
        filter_argument = self._dic2url({'index_code': index,
                                         'start_date': start_date,
                                         'end_date': end_date})
    
        df_io, msg = self.query("wd.indexCons", fields="",
                             filter=filter_argument, orderby="security")
        if msg != '0,':
            print msg
        
        def str2int(s):
            if isinstance(s, (str, unicode)):
                return int(s) if s else 99999999
            elif isinstance(s, (int, np.integer, float, np.float)):
                return s
            else:
                raise NotImplementedError("type s = {}".format(type(s)))
        df_io.loc[:, 'in_date'] = df_io.loc[:, 'in_date'].apply(str2int)
        df_io.loc[:, 'out_date'] = df_io.loc[:, 'out_date'].apply(str2int)
        
        # df_io.set_index('security', inplace=True)
        dates = self.get_trade_date(start_date=start_date, end_date=end_date, security=index)

        dic = dict()
        gp = df_io.groupby(by='security')
        for sec, df in gp:
            mask = np.zeros_like(dates, dtype=int)
            for idx, row in df.iterrows():
                bool_index = np.logical_and(dates > row['in_date'], dates < row['out_date'])
                mask[bool_index] = 1
            dic[sec] = mask
            
        res = pd.DataFrame(index=dates, data=dic)
        
        return res


class JzEventServer(JzDataServer):
    def __init__(self):
        super(JzEventServer, self).__init__()
        
        self.bar_type = common.QUOTE_TYPE.MIN
        self.security = ''

        self.daily_quotes_cache = None

    def get_daily_quotes(self, target_date):
        self.daily_quotes_cache = self.make_cache(target_date)
        return self.daily_quotes_cache
    
    @staticmethod
    def make_time(timestamp):
        return timestamp.strftime('%Y%m%d %H:%M:%S %f')
    
    def make_cache(self, target_date):
        """Return a list of quotes of a single day. If any error, print error msg and return None.

        """
        topic_list = self.get_topics()
        
        for sec in topic_list:
            pd_bar, msg = self.bar(sec, start_time=200000, end_time=160000,
                                   trade_date=target_date, cycle='1m', fields="")
            
            if pd_bar is not None:
                cache = []
                
                dict_bar = pd_bar.transpose().to_dict()
                keys = sorted(dict_bar.keys())
                
                for j in xrange(len(keys)):
                    key = keys[j]
                    bar = dict_bar.get(key)
                    quote = Quote(self.bar_type)
                    quote.security = bar['security']
                    quote.open = bar['open']
                    quote.close = bar['close']
                    quote.high = bar['high']
                    quote.low = bar['low']
                    quote.volume = bar['volume']
                    quote.turnover = bar['turnover']
                    quote.oi = bar['oi']
                    quote.date = bar['trade_date']
                    quote.time = bar['time']
                    # quote.time = self.make_time(key)
                    
                    cache.append(quote)
                return cache
            else:
                print msg
        
        return None
    
    
class DataServer(Publisher):
    def __init__(self):
        Publisher.__init__(self)
    
    @abstractmethod
    def init_from_config(self, props):
        pass
    
    @abstractmethod
    def initialize(self):
        pass
    
    @abstractmethod
    def start(self):
        pass
    
    @abstractmethod
    def join(self):
        pass
    
    @abstractmethod
    def stop(self):
        pass
    
    @abstractmethod
    def getNextQuote(self):
        pass
    
    def add_batch_subscribe(self, subscriber, univlist):
        for i in xrange(len(univlist)):
            self.add_subscriber(subscriber, univlist[i])


class JshHistoryBarDataServer(DataServer):
    # ----------------------------------------------------------------------
    def __init__(self):
        self.api = None
        self.addr = ''
        self.bar_type = common.QUOTE_TYPE.MIN
        self.security = ''
        
        self.daily_quotes_cache = None
        
        DataServer.__init__(self)
    
    def init_from_config(self, props):
        self.addr = props.get('jsh.addr')
        self.bar_type = props.get('bar_type')
        self.security = props.get('security')
    
    def start(self):
        pass
    
    def join(self):
        pass
    
    def stop(self):
        pass
    
    """
    def getNextQuote(self):
        if (self.daily_quotes_cache is not None and self.cache_pos < len(self.daily_quotes_cache)):
            quote = self.daily_quotes_cache[self.cache_pos]
            self.cache_pos = self.cache_pos + 1
            return quote
        
        else:
            self.daily_quotes_cache = None
            self.cache_pos = 0
            
            while (self.daily_quotes_cache is None or len(self.daily_quotes_cache) == 0):
                if (self.next_day_pos > self.end_date):
                    return None
                
                self.daily_quotes_cache = self.makeCache(self.next_day_pos)
                self.next_day_pos = self.getNextDate(self.next_day_pos)
            
            return self.getNextQuote()
    """
    
    def get_daily_quotes(self, target_date):
        self.daily_quotes_cache = self.makeCache(target_date)
        return self.daily_quotes_cache
    
    """
    def getNextDate(self, read_pos):
        dt = datetime.strptime(str(read_pos) , '%Y%m%d')
        # next_dt = dt + timedelta(days = 1)
        # next_pos = int(next_dt.strftime('%Y%m%d'))
        # today = self.calendar.transferDtToInt(dt)
        next_pos = self.calendar.getNextTradeDate(read_pos)
        return next_pos
    """
    
    def makeTime(self, timestamp):
        return timestamp.strftime('%Y%m%d %H:%M:%S %f')
    
    def makeCache(self, target_date):
        """Return a list of quotes of a single day. If any error, print error msg and return None.

        """
        topic_list = self.get_topics()
        
        for sec in topic_list:
            pd_bar, msg = self.bar('rb1710.SHF,600662.SH', start_time=200000, end_time=160000, trade_date=20170831, fields="")
            pd_bar, msg = self.api.jz_unified('jsh', sec, fields='', date=target_date,
                                              start_time='', end_time='', bar_size=self.bar_type.value)
            
            if pd_bar is not None:
                cache = []
                
                dict_bar = pd_bar.transpose().to_dict()
                keys = sorted(dict_bar.keys())
                
                for j in xrange(len(keys)):
                    key = keys[j]
                    bar = dict_bar.get(key)
                    quote = Quote(self.bar_type)
                    quote.security = bar['SYMBOL']
                    quote.open = bar['OPEN']
                    quote.close = bar['close']
                    quote.high = bar['HIGH']
                    quote.low = bar['LOW']
                    quote.volume = bar['VOLUME']
                    quote.turnover = bar['TURNOVER']
                    quote.oi = bar['OPENINTEREST']
                    quote.time = self.makeTime(key)
                    
                    cache.append(quote)
                return cache
            else:
                print msg
        
        return None
