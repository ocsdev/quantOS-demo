# encoding: UTF-8

from abc import abstractmethod
from datetime import datetime
from framework import common

from pubsub import Publisher
from jzquant import jzquant_api


class Quote(object):
    #----------------------------------------------------------------------
    def __init__(self, type):
        self.type = type
        self.frequency   = 0
        self.symbol      = ''
        self.refsymbol   = ''
        self.time        = ''
        self.open        = 0.0
        self.high        = 0.0
        self.low         = 0.0
        self.close       = 0.0
        self.volume      = 0.0
        self.turnover    = 0.0
        self.oi          = 0.0
        self.settle      = 0.0
        self.preclose    = 0.0
        self.presettle   = 0.0
        self.bidprice    = []
        self.bidvol      = []
        self.askprice    = []
        self.askvol      = []
        
        self.format      = '%Y%m%d %H:%M:%S %f'
        
    def getDate(self):
        dt = datetime.strptime(self.time , self.format)
        return int(dt.strftime('%Y%m%d'))
    
    def getTime(self):
        dt = datetime.strptime(self.time , self.format)
        return int(dt.strftime('%H%M%S'))
    
    def show(self):
        print self.type, self.time, self.symbol, self.open, self.high, self.low, self.close, self.volume, self.turnover


class BaseDataServer(Publisher):
    """
    DataServer is a base class providing both historic and live data
    from various data sources.

    Derived classes of DataServer hide different data source, but use the same API.

    Attributes
    ----------
    source_name : str
        Name of data source.

    Methods
    -------
    subscribe(securities, call_back)
    quote(security, field)
    daily(security, begin_date, end_date, field="", fq=None, skip_paused=False)
    bar(security, begin_time=[MARKET_OPEN], end_time=[NOW, MARKET_CLOSE], trade_date=[TODAY], field="", cycle='1m')
    tick(security, begin_time=[MARKET_OPEN], end_time=[NOW, MARKET_CLOSE], trade_date=[TODAY], field="")
    query(query_type, param, field)

    """
    def __init__(self):
        Publisher.__init__(self)

        self.source_name = ""

    def init_from_config(self, props):
        pass

    def initialize(self):
        pass

    def add_batch_subsribe(self, subscriber, securities):
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
        for target in targets.split(','):
            sec, data_type = target.split('/')
            if data_type == 'tick':
                func = callback['on_tick']
            else:
                func = callback['on_bar']
            self.add_subscriber(func, target)

    def quote(self, security, field):
        pass


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
    
    def subscribe(self, subscriber, univlist):
        
        for i in xrange(len(univlist)):
            self.add_subscriber(subscriber, univlist[i])
    

class JshHistoryBarDataServer(DataServer):
    
    #----------------------------------------------------------------------
    def __init__(self):
        self.api    = None
        self.addr   = ''
        self.bar_type   = common.QUOTE_TYPE.MIN
        self.symbol     = ''
        
        self.daily_quotes_cache      = None

        DataServer.__init__(self)
    
    def init_from_config(self, props):
        self.addr       = props.get('jsh.addr')
        self.bar_type   = props.get('bar_type')
        self.symbol     = props.get('symbol')
    
    def initialize(self):
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
                    quote.symbol   = bar['SYMBOL']
                    quote.open     = bar['OPEN']
                    quote.close    = bar['CLOSE']
                    quote.high     = bar['HIGH']
                    quote.low      = bar['LOW']
                    quote.volume   = bar['VOLUME']
                    quote.turnover = bar['TURNOVER']
                    quote.oi       = bar['OPENINTEREST']
                    quote.time     = self.makeTime(key)
                    
                    cache.append(quote)
                return cache
            else:
                print msg

        return None


# 直接运行脚本可以进行测试
if __name__ == '__main__':
    
    props = dict()
    props['jsh.addr'] = 'tcp://10.2.0.14:61616'
    props['bar_type'] = common.QUOTE_TYPE.MIN
    props['symbol'] = '600030.SH'
    
    server = JshHistoryBarDataServer()
    server.init_from_config(props)
    
    server.initialize()
    
    server.subscribe(None, ['600030.SH'])

    quotes = server.get_daily_quotes(20170712)
    for quote in quotes:
        print quote.symbol, quote.time, quote.open, quote.high

