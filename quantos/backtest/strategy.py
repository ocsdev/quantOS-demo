# encoding: UTF-8

from abc import abstractmethod

from quantos.backtest.gateway import PortfolioManager

from backtest import common
from event import EventEngine
from pubsub import Subscriber
from quantos.backtest.event import eventType
from quantos.backtest.alphastrategy import StrategyContext


class Strategy(object):
    # ----------------------------------------------------------------------
    def __init__(self):
        self.context = None  # StrategyContext()
        self.pm = PortfolioManager()
        self.pm.strategy = self
        self.instanceid = ""
        self.trade_date = 0
        self.runmode = common.RUN_MODE.REALTIME
        
        self.initbalance = 0.0
    
    @abstractmethod
    def init_from_config(self, props):
        pass
    
    @abstractmethod
    def initialize(self, runmode):
        pass
    
    def initUniverse(self, universe):
        self.context.add_universe(universe)
    
    def getUniverse(self):
        return self.context.universe
    
    # -------------------------------------------
    def sendOrder(self, order, algo, param):
        self.context.gateway.send_order(order, algo, param)
    
    def cancelOrder(self, order):
        self.context.gateway.cancelOrder(order)


class EventDrivenStrategy(Strategy, Subscriber):
    def __init__(self):
        
        Strategy.__init__(self)
        
        self.eventEngine = EventEngine()
        self.eventEngine.register(eventType.EVENT_TIMER, self.onCycle)
        self.eventEngine.register(eventType.EVENT_MD_QUOTE, self.onQuote)
        self.eventEngine.register(eventType.EVENT_TRADE_IND, self.pm.on_trade_ind)
        self.eventEngine.register(eventType.EVENT_ORDERSTATUS_IND, self.pm.on_order_status)
    
    @abstractmethod
    def onNewDay(self, trade_date):
        pass
    
    @abstractmethod
    def onQuote(self, quote):
        pass
    
    @abstractmethod
    def onCycle(self):
        pass
    
    def initialize(self, runmode):
        if runmode == common.RUN_MODE.REALTIME:
            self.subscribeEvents()
    
    def subscribeEvents(self):
        universe = self.context.universe
        data_server = self.context.dataserver
        for i in xrange(len(universe)):
            self.subscribe(data_server, universe[i])
    
    def subscribe(self, publisher, topic):
        publisher.add_subscriber(self, topic)
    
    def start(self):
        self.eventEngine.start(False)
    
    def stop(self):
        self.eventEngine.stop()
    
    def registerEvent(self, event):
        self.eventEngine.put(event)
