# encoding: UTF-8

import framework
from framework.strategy import EventDrivenStrategy
from framework.gateway import Order
from framework.common import *


########################################################################
class CtaStrategy(EventDrivenStrategy):
    #----------------------------------------------------------------------
    def __init__(self):
        EventDrivenStrategy.__init__(self)
        self.symbol = ''
        
    def initConfig(self, props):
        self.symbol = props.get('symbol')
        self.initbalance = props.get('init_balance')
        
        
    def initialization(self, runmode):
        self.initUniverse(self.symbol)
        
    def onCycle(self):
        pass
    
    def onQuote(self, quote):
        #quote.show()
        
        time = quote.getTime()
        if (time == 100000):
            quote.show()
            order = Order()
            order.action = ORDER_ACTION_BUY
            order.initFromQuote(quote)            
            order.symbol = quote.symbol
            order.order_size = 10000
            order.order_price = quote.close
            self.context.gateway.sendOrder(order, '','') 
            print 'send order %s: %s %s %f'%(order.orderid, order.symbol, order.action, order.order_price)         
        if (time == 140000):
            quote.show()
            order = Order()
            order.action = ORDER_ACTION_SELL
            order.initFromQuote(quote)            
            order.symbol = quote.symbol
            order.order_size = 5000
            order.order_price = quote.close
            self.context.gateway.sendOrder(order, '','') 
            print 'send order %s: %s %s %f'%(order.orderid, order.symbol, order.action, order.order_price) 
            
    def onNewday(self, trade_date):
        print 'new day comes ' + str(trade_date)
        
    