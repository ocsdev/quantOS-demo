# encoding: UTF-8

from framework import common
from abc import abstractmethod

from event import EventEngine, eventType
from pubsub import Subscriber
from framework.gateway import PortfolioManager

########################################################################
class StrategyContext(object):

    #----------------------------------------------------------------------
    def __init__(self):
        self.dataserver = None
        self.gateway  = None
        self.universe = []
        self.calendar = None

    def addUniverse(self, univ):
        self.universe += univ.split(',')

########################################################################
class Strategy(object):

    #----------------------------------------------------------------------
    def __init__(self):
        self.context = StrategyContext()
        self.pm = PortfolioManager()
        self.pm.strategy = self
        self.instanceid = ""
        self.trade_date = 0
        self.runmode = common.RUN_MODE.REALTIME
        
        self.initbalance = 0.0
    
    @abstractmethod    
    def initConfig(self, props):
        pass
    
    @abstractmethod
    def initialization(self, runmode):
        pass
    
    def initUniverse(self, universe):
        self.context.addUniverse(universe)

    def getUniverse(self):
        return self.context.universe
        
    #-------------------------------------------
    def sendOrder(self, order, algo, param):
        self.context.gateway.sendOrder(order, algo, param)    

    def cancelOrder(self, order):
        self.context.gateway.cancelOrder(order)
        
########################################################################
class EventDrivenStrategy(Strategy, Subscriber):
    
    #----------------------------------------------------------------------
    def __init__(self):
        
        Strategy.__init__(self)        
        
        self.eventEngine = EventEngine()
        self.eventEngine.register(eventType.EVENT_TIMER, self.onCycle)
        self.eventEngine.register(eventType.EVENT_MD_QUOTE, self.onQuote)
        self.eventEngine.register(eventType.EVENT_TRADE_IND, self.pm.onTradeInd)
        self.eventEngine.register(eventType.EVENT_ORDERSTATUS_IND, self.pm.onOrderStatusInd)
    
    @abstractmethod    
    def onNewDay(self, trade_date):
        pass    

    @abstractmethod 
    def onQuote(self, quote):
        pass
    
    @abstractmethod 
    def onCycle(self):
        pass
    
    def initialization(self, runmode):
        if runmode == common.RUN_MODE.REALTIME :
            self.subscribeEvents()
            
    def subscribeEvents(self):
        universe = self.context.universe
        dataserver = self.context.dataserver
        for i in xrange(len(universe)):
            self.subscribe(dataserver, universe[i])
    
    def subscribe(self, publisher, topic):
        publisher.onSubscribe(self, topic)
    
    
    def start(self):
        self.eventEngine.start(False)    
    
    def stop(self):
        self.eventEngine.stop()
        
    def registerEvent(self, event):
        self.eventEngine.put(event)   
    