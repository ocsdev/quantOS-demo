# encoding: UTF-8

from quantos.backtest import common
from quantos.backtest.strategy import EventDrivenStrategy
from quantos.data.basic.order import Order


class CtaStrategy(EventDrivenStrategy):
    # ----------------------------------------------------------------------
    def __init__(self):
        EventDrivenStrategy.__init__(self)
        self.symbol = ''
    
    def init_from_config(self, props):
        self.symbol = props.get('symbol')
        self.init_balance = props.get('init_balance')
    
    def initialize(self, runmode):
        self.initUniverse(self.symbol)
    
    def on_cycle(self):
        pass
    
    def on_quote(self, quote):
        # quote.show()
        
        time = quote.time
        if (time == 100000):
            quote.show()
            order = Order()
            order.entrust_action = common.ORDER_ACTION.BUY
            order.entrust_date = quote.date
            order.entrust_time = quote.time
            order.symbol = quote.symbol
            order.entrust_size = 10000
            order.entrust_price = quote.close
            self.context.gateway.send_order(order, '', '')
            print 'send order %s: %s %s %f' % (
            order.entrust_id, order.symbol, order.entrust_action, order.entrust_price)
        if (time == 140000):
            quote.show()
            order = Order()
            order.entrust_action = common.ORDER_ACTION.SELL
            order.entrust_date = quote.date
            order.entrust_time = quote.time
            order.symbol = quote.symbol
            order.entrust_size = 5000
            order.entrust_price = quote.close
            self.context.gateway.send_order(order, '', '')
            print 'send order %s: %s %s %f' % (
            order.entrust_id, order.symbol, order.entrust_action, order.entrust_price)
    
    def on_new_day(self, trade_date):
        print 'new day comes ' + str(trade_date)
