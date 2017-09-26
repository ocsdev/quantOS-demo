# encoding: UTF-8

from common import *
from objectfactory import *

from pubsub import *
from framework.strategy import StrategyContext
from framework.jzcalendar import JzCalendar
from framework.pnlreport import PnlManager

########################################################################
class BacktestInstance(Subscriber):
    
    #----------------------------------------------------------------------
    def __init__(self):
        self.instanceid = ''
        self.strategy   = None
        self.start_date = 0
        self.end_date   = 0
        self.folder     = ''
        
    def initFromConfig(self, instanceid, props, dataserver, gateway, strategy):
        self.instanceid = instanceid
        
        self.start_date = props.get("start_date")
        self.end_date   = props.get("end_date")
        self.folder     = props.get("folder")
        
        calendar = JzCalendar()
        
        dataserver.initConfig(props)
        dataserver.initialization()
        
        gateway.initConfig(props)        
        
        strategy.context.dataserver = dataserver
        strategy.context.gateway    = gateway
        strategy.context.calendar   = calendar
        gateway.registerCallback(strategy.pm)
        
        strategy.initConfig(props)
        strategy.initialization(RUNMODE_BACKTEST)
        
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

    def run2(self):
        dataserver = self.strategy.context.dataserver
        universe = self.strategy.context.universe

        dataserver.subscribe(self, universe)

        while True:  # each loop is a new trading day
            dataserver.onNewDay()
            if dataserver.current_date > self.end_date:
                break
            # gateway.oneNewDay()
            self.strategy.onNewday(dataserver.current_date)
            self.strategy.pm.onNewDay(dataserver.current_date, dataserver.last_date)
            self.strategy.trade_date = dataserver.current_date

            quote_generator = dataserver.quoteGenerator()
            for quote in quote_generator:
                self.processQuote(quote)

            # self.strategy.onMarketClose()

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