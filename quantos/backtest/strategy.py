# encoding: UTF-8

from abc import abstractmethod

from quantos.backtest.gateway import PortfolioManager

from backtest import common
from event import EventEngine
from pubsub import Subscriber
from quantos.backtest.event import eventType
from quantos.backtest.alphastrategy import Strategy


class EventDrivenStrategy(Strategy, Subscriber):
    def __init__(self):
        
        Strategy.__init__(self)
        
        self.pm = PortfolioManager()
        self.pm.strategy = self
        
        self.eventEngine = EventEngine()
        self.eventEngine.register(eventType.EVENT_TIMER, self.on_cycle)
        self.eventEngine.register(eventType.EVENT_MD_QUOTE, self.on_quote)
        self.eventEngine.register(eventType.EVENT_TRADE_IND, self.pm.on_trade_ind)
        self.eventEngine.register(eventType.EVENT_ORDERSTATUS_IND, self.pm.on_order_status)
    
    @abstractmethod
    def on_new_day(self, trade_date):
        pass
    
    @abstractmethod
    def on_quote(self, quote):
        pass
    
    @abstractmethod
    def on_cycle(self):
        pass
    
    def initialize(self, runmode):
        if runmode == common.RUN_MODE.REALTIME:
            self.subscribe_events()
    
    def subscribe_events(self):
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
    
    def register_event(self, event):
        self.eventEngine.put(event)
