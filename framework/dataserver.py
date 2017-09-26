# encoding: UTF-8

from abc import abstractmethod
from datetime import datetime

from framework import common
from jzdataapi.data_api import DataApi
from pubsub import Publisher


class Quote(object):
    # ----------------------------------------------------------------------
    def __init__(self, type):
        self.type = type
        self.frequency = 0
        self.security = ''
        self.refsecurity = ''
        self.time = ''
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
        dt = datetime.strptime(self.time, self.format)
        return int(dt.strftime('%Y%m%d'))
    
    def getTime(self):
        dt = datetime.strptime(self.time, self.format)
        return int(dt.strftime('%H%M%S'))
    
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
    def quote(self, security, field):
        pass
    
    @abstractmethod
    def daily(self, security, begin_date, end_date, field="", adjust_mode=None):
        """
        Query dar bar,
        support auto-fill suspended securities data,
        support auto-adjust for splits, dividends and distributions.

        Parameters
        ----------
        security : str
            support multiple securities, separated by comma.
        begin_date : int (YYYMMDD) or str ('YYYY-MM-DD')
        end_date : int (YYYMMDD) or str ('YYYY-MM-DD')
        field : separated by comma ','
            Default is all fields included.
        adjust_mode : str or None
            None for no adjust;
            'pre' for forward adjust;
            'post' for backward adjust.

        Returns
        -------
        df : dict of {security: pd.DataFrame}
            columns:
                security, code, trade_date, open, high, low, close, volume, turnover, vwap, oi, suspended

        Examples
        --------
        df, msg = api.daily("00001.SH,cu1709.SHF",begin_date=20170503, end_date=20170708,
                            field="open,high,low,last,volume", fq=None, skip_suspended=True)

        """
        pass
    
    @abstractmethod
    def bar(self, security, begin_time=200000, end_time=160000, trade_date=None, field="", cycle='1m'):
        """
        Query minute bars of various type, return DataFrame.

        Parameters
        ----------
        security : str
            support multiple securities, separated by comma.
        begin_time : int (HHMMSS) or str ('HH:MM:SS')
            Default is market open time.
        end_time : int (HHMMSS) or str ('HH:MM:SS')
            Default is market close time.
        trade_date : int (YYYMMDD) or str ('YYYY-MM-DD')
            Default is current trade_date.
        field : separated by comma ','
            Default is all fields included.
        cycle : framework.common.MINBAR_TYPE {'1m', '5m', '15m'}
            Minute bar type, default is '1m'

        Returns
        -------
        df : dict of {security: pd.DataFrame}
            columns:
                security, code, date, time, trade_date, cycle, open, high, low, close, volume, turnover, vwap, oi

        Examples
        --------
        df, msg = api.bar("000001.SH,cu1709.SHF", begin_time="09:56:00", end_time="13:56:00",
                          trade_date="20170823", field="open,high,low,last,volume", cycle="5m")

        """
        # TODO data_server DOES NOT know "current date".
        pass
    
    @abstractmethod
    def tick(self, security, begin_time=None, end_time=None, trade_date=None, field=""):
        pass
    
    @abstractmethod
    def query(self, view, field, filter):
        """
        Query reference data.
        Input query type and parameters, return DataFrame.
        
        Parameters
        ----------
        view : str
            Type of reference data. See doc for details.
        field : str
            Fields to return, separated by ','.
        filter : str
            Query conditions, separated by '&'.

        Returns
        -------
        df : pd.DataFrame
        err_msg : str

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
        self.api.connect()
    
    def daily(self, security, field="",
              begin_date=0, end_date=0,
              adjust_mode=None, data_format=""):
        df, err_msg = self.api.daily(security=security, begin_date=begin_date, end_date=end_date,
                                     fields=field, adjust_mode=adjust_mode, data_format=data_format)
        return df, err_msg

    def bar(self, security, field="",
            begin_time=200000, end_time=160000, trade_date=0,
            cycle="1m", data_format=""):
        df, msg = self.api.bar(security=security, fields=field,
                               begin_time=begin_time, end_time=end_time, trade_date=trade_date,
                               cycle='1m', data_format=data_format)
        return df, msg
    
    def query(self, view, field, filter_="", **kwargs):
        df, msg = self.api.query(view, field, filter=filter_, **kwargs)
        return df, msg
    
    def get_suspensions(self):
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
    
    def initialize(self):
        # TODO obsolete api
        self.api = jzquant_api.get_jzquant_api(address=self.addr, user="TODO", password="TODO")
    
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


def _test_old():
    props = dict()
    props['jsh.addr'] = 'tcp://10.2.0.14:61616'
    props['bar_type'] = common.QUOTE_TYPE.MIN
    props['security'] = '600030.SH'
    
    server = JshHistoryBarDataServer()
    server.init_from_config(props)
    
    server.initialize()
    
    server.add_batch_subscribe(None, ['600030.SH'])
    
    quotes = server.get_daily_quotes(20170712)
    for quote in quotes:
        print quote.security, quote.time, quote.open, quote.high


def test_jz_data_server():
    ds = JzDataServer()
    res, msg = ds.daily('rb1710.SHF,600662.SH', "", 20170828, 20170831)
    rb = res.loc[res.loc[:, 'security'] == 'rb1710.SHF', :]
    stk = res.loc[res.loc[:, 'security'] == '600662.SH', :]
    assert rb.shape == (4, 13)
    assert msg == '0,'
    # TODO format of volume
    # assert stk.loc[:, 'volume'].values[0] == 7174813.00
    
    res2, msg2 = ds.bar('rb1710.SHF,600662.SH', "", 200000, 160000, 20170831)
    rb = res2.loc[res2.loc[:, 'security'] == 'rb1710.SHF', :]
    stk = res2.loc[res2.loc[:, 'security'] == '600662.SH', :]
    assert rb.shape == (345, 14)
    assert stk.shape == (240, 14)
    assert msg2 == '0,'

    # unit: 1e4 RMB
    res3, msg3 = ds.query("wd.secDailyIndicator", "",
                          filter_="security=600848.SH&start_date=20170101&end_date=20170801",
                          orderby="date",
                          data_format='pandas')
    print msg3
    print res3
    print "Test passed."


# 直接运行脚本可以进行测试
if __name__ == '__main__':
    # test_old()
    test_jz_data_server()
