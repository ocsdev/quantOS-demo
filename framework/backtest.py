# encoding: UTF-8

from framework import common
from objectfactory import *

from pubsub import *
from framework.strategy import StrategyContext
from framework.jzcalendar import JzCalendar
from framework.pnlreport import PnlManager
import datetime
from event.eventType import EVENT
from event.eventEngine import Event

########################################################################
class BacktestInstance(Subscriber):
    
    #----------------------------------------------------------------------
    def __init__(self):
        self.instanceid = ''
        self.strategy   = None
        self.start_date = 0
        self.end_date   = 0
        self.current_date = 0
        self.last_date = 0
        self.folder     = ''

        self.calendar = JzCalendar()

    def initFromConfig(self, props, data_server, gateway, strategy):
        self.instanceid = props.get("instanceid")
        
        self.start_date = props.get("start_date")
        self.end_date   = props.get("end_date")
        self.folder     = props.get("folder")
        
        data_server.init_from_config(props)
        data_server.initialize()
        
        gateway.init_from_config(props)
        
        strategy.context.dataserver = data_server
        strategy.context.gateway    = gateway
        strategy.context.calendar   = self.calendar
        gateway.register_callback(strategy.pm)
        
        strategy.init_from_config(props)
        strategy.initialize(common.RUN_MODE.BACKTEST)
        
        self.pnlmgr = PnlManager()
        self.pnlmgr.setStrategy(strategy)
        self.pnlmgr.initFromConfig(props)
        self.strategy = strategy
        
        return True
    
    def run(self):
        
        data_server = self.strategy.context.dataserver
        universe   = self.strategy.context.universe
        
        data_server.add_batch_subscribe(self, universe)
        
        last_trade_date = 0
        while (True):
            quote = data_server.getNextQuote()
            
            if quote is None:
                break
            
            trade_date = quote.getDate()
            
            # switch to a new day
            if (trade_date != last_trade_date) :
                
                if (last_trade_date > 0):
                    self.closeDay(last_trade_date)
                
                self.strategy.trade_date = trade_date
                self.strategy.pm.on_new_day(trade_date, last_trade_date)
                self.strategy.onNewday(trade_date)
                last_trade_date = trade_date
                
            self.processQuote(quote)

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
        ee.register(EVENT.MD_QUOTE, __extract(self.processQuote))
        ee.register(EVENT.MARKET_CLOSE, __extract(self.closeDay))

        # ------------

        while self.current_date <= self.end_date:  # each loop is a new trading day
            quotes = data_server.get_daily_quotes(self.current_date)
            if quotes is not None:
                # gateway.oneNewDay()
                e_newday = Event(EVENT.CALENDAR_NEW_TRADE_DATE)
                e_newday.data = self.current_date
                ee.put(e_newday)
                ee.process_once() # this line should be done on another thread

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
                e_quote.data = self.current_date
                ee.put(e_close)
                ee.process_once()
                # self.strategy.onSettle()

                self.last_date = self.current_date
            else:
                # no quotes because of holiday or other issues. We don't update last_date
                print "in backtest.py: function run(): {} quotes is None, continue.".format(self.last_date)

            self.current_date = self.get_next_trade_date(self.current_date)

        # self.strategy.onTradingEnd()

    def processQuote(self, quote):
        result = self.strategy.context.gateway.processQuote(quote)
        
        for (tradeInd, statusInd) in result:
            self.strategy.pm.on_trade_ind(tradeInd)
            self.strategy.pm.on_order_status(statusInd)
        
        self.strategy.onQuote(quote)
    
    # close one trade day, cancel all orders    
    def closeDay(self, trade_date):
        print 'close trade_date ' + str(trade_date)
        result = self.strategy.context.gateway.closeDay(trade_date)
        
        for statusInd in result:
            self.strategy.pm.on_order_status(statusInd)
            
    def generateReport(self):
        return self.pnlmgr.generateReport()    