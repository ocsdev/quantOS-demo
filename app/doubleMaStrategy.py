import framework
from framework.strategy import EventDrivenStrategy
from framework.gateway import Order
from framework.common import *
import numpy as np

class DoubleMaStrategy(EventDrivenStrategy):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        EventDrivenStrategy.__init__(self)
        self.symbol = ''
        self.fastN = 5
        self.slowN = 10
        self.bar = None
        self.bufferCount = 0
        self.bufferSize = 20
        self.closeArray = np.zeros(self.bufferSize)
        self.fastMa = 0
        self.slowMa = 0
        self.pos = 0
        
    def initConfig(self, props):
        self.symbol = props.get('symbol')
        self.initbalance = props.get('init_balance')
    
    def initialization(self, runmode):
        self.initUniverse(self.symbol)
        
    def onCycle(self):
        pass
    
    def createOrder(self, quote, price, size):
        order = Order()
        order.initFromQuote(quote)            
        order.symbol = quote.symbol
        order.order_size = size
        order.order_price = price
        return order
        
    def buy(self, quote, price, size):
        order = self.createOrder(quote, price, size)
        order.action = ORDER_ACTION_BUY        
        self.context.gateway.sendOrder(order, '','') 
        
    def sell(self, quote, price, size):
        order = self.createOrder(quote, price, size)
        order.action = ORDER_ACTION_SELL        
        self.context.gateway.sendOrder(order, '','')
        
    def cover(self, quote, price, size):
        order = self.createOrder(quote, price, size)
        order.action = ORDER_ACTION_BUY       
        self.context.gateway.sendOrder(order, '','') 
         
    def short(self, quote, price, size):
        order = self.createOrder(quote, price, size)
        order.action = ORDER_ACTION_SELL        
        self.context.gateway.sendOrder(order, '','') 
    def onNewday(self, trade_date):
        print 'new day comes ' + str(trade_date)    
    def onQuote(self, quote):
        date = quote.getDate()
        key = quote.symbol + '.' + str(date)
        self.pos = self.pm.getPosition(key)
        
        self.closeArray[0:self.bufferSize-1] = self.closeArray[1:self.bufferSize]
        self.closeArray[-1] = quote.close
        self.bufferCount += 1
        
        if self.bufferCount <= self.bufferSize:
            return
        self.fastMa = self.closeArray[-self.fastN-1:-1].mean()
        self.slowMa = self.closeArray[-self.slowN-1:-1].mean()        
      
        
        if self.fastMa > self.slowMa:
            if self.pos == 0:
                self.buy(quote, quote.close+3, 1)
           
            elif self.pos < 0:
                self.cover(quote, quote.close+3, 1)
                self.buy(quote, quote.close+3, 1)
       
        elif self.fastMa < self.slowMa:
            if self.pos == 0:
                self.short(quote, quote.close-3, 1)
            elif self.pos > 0:
                self.sell(quote, quote.close-3, 1)
                self.short(quote, quote.close-3, 1)
        
        
        