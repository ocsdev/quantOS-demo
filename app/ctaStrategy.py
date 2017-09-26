# encoding: UTF-8

from framework import common
from framework.gateway import Order
from framework.strategy import EventDrivenStrategy


########################################################################
class CtaStrategy(EventDrivenStrategy):
    # ----------------------------------------------------------------------
    def __init__(self):
        EventDrivenStrategy.__init__(self)
        self.security = ''
    
    def init_from_config(self, props):
        self.security = props.get('security')
        self.initbalance = props.get('init_balance')
    
    def initialize(self, runmode):
        self.initUniverse(self.security)
    
    def onCycle(self):
        pass
    
    def onQuote(self, quote):
        # quote.show()
        
        time = quote.getTime()
        if (time == 100000):
            quote.show()
            order = Order()
            order.entrust_action = common.ORDER_ACTION.BUY
            order.entrust_date = quote.getDate()
            order.entrust_time = quote.time
            order.security = quote.security
            order.entrust_size = 10000
            order.entrust_price = quote.close
            self.context.gateway.sendOrder(order, '', '')
            print 'send order %s: %s %s %f' % (
            order.entrust_id, order.security, order.entrust_action, order.entrust_price)
        if (time == 140000):
            quote.show()
            order = Order()
            order.entrust_action = common.ORDER_ACTION.SELL
            order.entrust_date = quote.getDate()
            order.entrust_time = quote.time
            order.security = quote.security
            order.entrust_size = 5000
            order.entrust_price = quote.close
            self.context.gateway.sendOrder(order, '', '')
            print 'send order %s: %s %s %f' % (
            order.entrust_id, order.security, order.entrust_action, order.entrust_price)
    
    def onNewday(self, trade_date):
        print 'new day comes ' + str(trade_date)
