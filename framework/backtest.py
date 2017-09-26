# encoding: UTF-8

from framework import common
from objectfactory import *

from pubsub import *
from framework.strategy import StrategyContext
from framework.jzcalendar import JzCalendar
from framework.pnlreport import PnlManager
import datetime

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

    def initFromConfig(self, instanceid, props, dataserver, gateway, strategy):
        self.instanceid = instanceid
        
        self.start_date = props.get("start_date")
        self.end_date   = props.get("end_date")
        self.folder     = props.get("folder")
        
        dataserver.initConfig(props)
        dataserver.initialization()
        
        gateway.initConfig(props)        
        
        strategy.context.dataserver = dataserver
        strategy.context.gateway    = gateway
        strategy.context.calendar   = self.calendar
        gateway.registerCallback(strategy.pm)
        
        strategy.initConfig(props)
        strategy.initialization(common.RUN_MODE.BACKTEST)
        
        self.pnlmgr = PnlManager()
        self.pnlmgr.setStrategy(strategy)
        self.pnlmgr.initFromConfig(props)
        self.strategy = strategy
        
        return True
    
    def run(self):
        
        dataserver = self.strategy.context.dataserver
        universe   = self.strategy.context.universe
        
        dataserver.subscribe(self, universe)
        
        last_trade_date = 0
        while (True):
            quote = dataserver.getNextQuote()
            
            if quote is None:
                break
            
            trade_date = quote.getDate()
            
            # switch to a new day
            if (trade_date != last_trade_date) :
                
                if (last_trade_date > 0):
                    self.closeDay(last_trade_date)
                
                self.strategy.trade_date = trade_date
                self.strategy.pm.onNewDay(trade_date, last_trade_date)
                self.strategy.onNewday(trade_date)
                last_trade_date = trade_date
                
            self.processQuote(quote)

    def get_next_trade_date(self, current):
        next_dt = self.calendar.getNextTradeDate(current)
        return next_dt

    def run2(self):
        dataserver = self.strategy.context.dataserver
        universe = self.strategy.context.universe

        dataserver.subscribe(self, universe)

        self.current_date = self.start_date
        while self.current_date <= self.end_date:  # each loop is a new trading day
            quotes = dataserver.get_daily_quotes(self.current_date)
            if quotes is not None:
                # gateway.oneNewDay()
                self.strategy.onNewday(self.current_date)
                self.strategy.pm.onNewDay(self.current_date, self.last_date)
                self.strategy.trade_date = self.current_date

                for quote in quotes:
                    self.processQuote(quote)

                # self.strategy.onMarketClose()
                self.closeDay(self.current_date)
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
            self.strategy.pm.onTradeInd(tradeInd)
            self.strategy.pm.onOrderStatusInd(statusInd)
        
        self.strategy.onQuote(quote)
    
    # close one trade day, cancel all orders    
    def closeDay(self, trade_date):
        print 'close trade_date ' + str(trade_date)
        result = self.strategy.context.gateway.closeDay(trade_date)
        
        for statusInd in result:
            self.strategy.pm.onOrderStatusInd(statusInd)
            
    def generateReport(self):
        return self.pnlmgr.generateReport()    