# encoding: UTF-8

from event.eventEngine import Event
from event.eventType import EVENT
from framework.jzcalendar import JzCalendar
from framework.pnlreport import PnlManager
from framework import common
from pubsub import *
from framework.alphabacktest import BacktestInstance


########################################################################
class EventBacktestInstance(BacktestInstance):
    # ----------------------------------------------------------------------
    def __init__(self):
        super(EventBacktestInstance, self).__init__()
        pass
        
    def init_from_config(self, props, data_api, gateway, strategy):
        self.props = props
        self.instanceid = props.get("instanceid")
        
        self.start_date = props.get("start_date")
        self.end_date = props.get("end_date")
        
        data_api.init_from_config(props)
        data_api.initialize()
        
        gateway.init_from_config(props)
        
        strategy.context.dataserver = data_api
        strategy.context.gateway = gateway
        strategy.context.calendar = self.calendar
        
        gateway.register_callback(strategy.pm)
        
        strategy.init_from_config(props)
        strategy.initialize(common.RUN_MODE.BACKTEST)
        
        self.strategy = strategy
        
        self.pnlmgr = PnlManager()
        self.pnlmgr.setStrategy(strategy)
        self.pnlmgr.initFromConfig(props, data_api)
        
        return True
    
    def run(self):
        
        data_server = self.strategy.context.dataserver
        universe = self.strategy.context.universe
        
        data_server.add_batch_subscribe(self, universe)
        
        last_trade_date = 0
        while (True):
            quote = data_server.getNextQuote()
            
            if quote is None:
                break
            
            trade_date = quote.getDate()
            
            # switch to a new day
            if (trade_date != last_trade_date):
                
                if (last_trade_date > 0):
                    self.close_day(last_trade_date)
                
                self.strategy.trade_date = trade_date
                self.strategy.pm.on_new_day(trade_date, last_trade_date)
                self.strategy.onNewday(trade_date)
                last_trade_date = trade_date
            
            self.process_quote(quote)
    
    def get_next_trade_date(self, current):
        next_dt = self.calendar.get_next_trade_date(current)
        return next_dt
    
    def run2(self):
        data_server = self.strategy.context.dataserver
        universe = self.strategy.context.universe
        
        data_server.add_batch_subscribe(self, universe)
        
        self.current_date = self.start_date
        
        # ------------
        def __extract(func):
            return lambda event: func(event.data, **event.kwargs)
        
        ee = self.strategy.eventEngine  # TODO event-driven way of lopping, is it proper?
        ee.register(EVENT.CALENDAR_NEW_TRADE_DATE, __extract(self.strategy.onNewday))
        ee.register(EVENT.MD_QUOTE, __extract(self.process_quote))
        ee.register(EVENT.MARKET_CLOSE, __extract(self.close_day))
        
        # ------------
        
        while self.current_date <= self.end_date:  # each loop is a new trading day
            quotes = data_server.get_daily_quotes(self.current_date)
            if quotes is not None:
                # gateway.oneNewDay()
                e_newday = Event(EVENT.CALENDAR_NEW_TRADE_DATE)
                e_newday.data = self.current_date
                ee.put(e_newday)
                ee.process_once()  # this line should be done on another thread
                
                # self.strategy.onNewday(self.current_date)
                self.strategy.pm.on_new_day(self.current_date, self.last_date)
                self.strategy.trade_date = self.current_date
                
                for quote in quotes:
                    # self.processQuote(quote)
                    e_quote = Event(EVENT.MD_QUOTE)
                    e_quote.data = quote
                    ee.put(e_quote)
                    ee.process_once()
                
                # self.strategy.onMarketClose()
                # self.closeDay(self.current_date)
                e_close = Event(EVENT.MARKET_CLOSE)
                e_close.data = self.current_date
                ee.put(e_close)
                ee.process_once()
                # self.strategy.onSettle()
                
                self.last_date = self.current_date
            else:
                # no quotes because of holiday or other issues. We don't update last_date
                print "in backtest.py: function run(): {} quotes is None, continue.".format(self.last_date)
            
            self.current_date = self.get_next_trade_date(self.current_date)
            
            # self.strategy.onTradingEnd()
    
    def process_quote(self, quote):
        result = self.strategy.context.gateway.process_quote(quote)
        
        for (tradeInd, statusInd) in result:
            self.strategy.pm.on_trade_ind(tradeInd)
            self.strategy.pm.on_order_status(statusInd)
        
        self.strategy.onQuote(quote)
    
    # close one trade day, cancel all orders    
    def close_day(self, trade_date):
        print 'close trade_date ' + str(trade_date)
        result = self.strategy.context.gateway.close_day(trade_date)
        
        for statusInd in result:
            self.strategy.pm.on_order_status(statusInd)
    
    def generate_report(self):
        return self.pnlmgr.generateReport()
