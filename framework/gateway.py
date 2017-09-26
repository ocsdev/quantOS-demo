# encoding: UTF-8

from abc import abstractmethod
from framework import common
from framework.jzcalendar import *
import jzquant
from jzquant import jzquant_api
########################################################################
class Order(object):
    #----------------------------------------------------------------------
    def __init__(self):
        self.ordertype  = common.ORDER_TYPE.LIMITORDER
        self.orderid    = ''
        self.reforderid = ''
        self.symbol  = ''
        self.action    = ''
        self.order_price = 0.0
        self.order_size  = 0
        self.order_date  = 0
        self.order_time  = ''
        self.order_status = ''
        self.fill_size   = 0
        self.fill_price  = 0.0
        self.cancel_size = 0
        self.errmsg      = ''
    
    def isFinished(self):
        if self.order_status == common.ORDER_STATUS.FILLED or self.order_status == common.ORDER_STATUS.CANCELLED or self.order_status == common.ORDER_STATUS.REJECTED:
            return True
        else:
            return False 
    def initFromQuote(self,quote):
        self.order_date = quote.getDate()
        self.order_time = quote.time
            
    def copy(self, order):
        self.ordertype    = order.ordertype   
        self.orderid      = order.orderid     
        self.reforderid   = order.reforderid  
        self.symbol       = order.symbol      
        self.action       = order.action      
        self.order_price  = order.order_price 
        self.order_size   = order.order_size  
        self.order_date   = order.order_date  
        self.order_time   = order.order_time  
        self.order_status = order.order_status
        self.fill_size    = order.fill_size   
        self.fill_price   = order.fill_price  
        self.cancel_size  = order.cancel_size 
        self.errmsg       = order.errmsg   
             
########################################################################
class TradeInd(object) :
    #----------------------------------------------------------------------
    def __init__(self):
        self.tradeid   = ''
        self.reforderid = ''
        self.entrustno = ''
        self.symbol  = ''
        self.action    = ''
        self.fill_price = 0.0
        self.fill_size  = 0.0
        self.fill_no    = ''
        self.fill_date  = 0
        self.fill_time  = ''
        self.order_price = 0.0
        self.order_size  = 0
        self.refquote    = None
        self.refcode     = ''

class OrderStatusInd(object) :
    #----------------------------------------------------------------------
    def __init__(self):
        self.orderid    = ''
        self.reforderid = ''
        self.symbol  = ''
        self.action    = ''
        self.order_price = 0.0
        self.order_size  = 0
        self.order_date  = 0
        self.order_time  = ''
        self.order_status = ''
        self.fill_size   = 0
        self.fill_price  = 0.0    
        
    def initOrderStatus(self, order):
        self.orderid      = order.orderid     
        self.reforderid   = order.reforderid  
        self.symbol       = order.symbol         
        self.action       = order.action       
        self.order_price  = order.order_price
        self.order_size   = order.order_size 
        self.order_date   = order.order_date 
        self.order_time   = order.order_time 
        self.order_status = order.order_status
        self.fill_size    = order.fill_size  
        self.fill_price   = order.fill_price 

########################################################################
class TradeCallback(object):
    
    @abstractmethod    
    def onTradeInd(self, trade):
        pass
    
    @abstractmethod
    def onOrderStatusInd(self, orderstatus):
        pass
    
    @abstractmethod    
    def orderRsp(self, order, result, msg):
        pass

########################################################################
class TradeStat(object):
    #----------------------------------------------------------------------
    def __init__(self):
        self.symbol = ''
        self.buy_filled_size  = 0
        self.buy_uncome_size  = 0
        self.sell_filled_size = 0
        self.sell_uncome_size = 0

########################################################################
class Position(object):
    #----------------------------------------------------------------------
    def __init__(self):
        self.trade_date = 0
        self.symbol     = ''
        self.init_size  = 0
        self.curr_size  = 0
    
########################################################################
class PortfolioManager(TradeCallback):
    #----------------------------------------------------------------------
    def __init__(self):
        self.orders    = {}
        self.trades    = []
        self.universe  = []
        self.positions = {}
        self.tradestat = {}
        self.strategy  = None
    
    def setUniverse(self, universe): 
        self.universe = universe.split(',')
        
    def makePositionKey(self, symbol, trade_date):
        return symbol + "." + str(trade_date);
    
    
    def orderRsp(self, order, result, msg):
        if result == True:
            self.addNewOrder(order)
            
    def getPosition(self, key):
        position = self.positions.get(key, None)
        if position != None:
            return position.curr_size
        else : 
            return 0
        
    def onNewDay(self, date, pre_date):
        for code in self.universe:
            pre_key = self.makePositionKey(code, pre_date)
            new_key = self.makePositionKey(code, date)
            if self.positions.has_key(pre_key):
                pre_position = self.positions.get(pre_key)
                new_position = Position()
                new_position.curr_size = pre_position.curr_size
                new_position.init_size = new_position.curr_size
                new_position.symbol = pre_position.symbol
                new_position.trade_date = date
                self.positions[new_key] = new_position
                
    def addNewOrder(self, order):
        if (self.orders.has_key(order.orderid)):
            print 'duplicate orderid ' + order.orderid
            return False
        new_order = Order()
        new_order.copy(order)
        self.orders[order.orderid] = new_order
        positionKey = self.makePositionKey(order.symbol, self.strategy.trade_date)
        if (self.positions.has_key(positionKey) == False):
            position = Position()
            position.trade_date = self.strategy.trade_date
            position.symbol = order.symbol
            position.init_size = 0
            position.curr_size = 0
            self.positions[positionKey] = position
        
        if (self.tradestat.has_key(order.symbol) == False):
            tradestat = TradeStat()
            tradestat.symbol = order.symbol
            tradestat.buy_filled_size  = 0
            tradestat.buy_uncome_size  = 0
            tradestat.sell_filled_size = 0
            tradestat.sell_uncome_size = 0 
            self.tradestat[order.symbol] = tradestat

        tradestat = self.tradestat.get(order.symbol)
        if (order.action == common.ORDER_ACTION.BUY):
            tradestat.buy_uncome_size += order.order_size
        else:
            tradestat.sell_uncome_size += order.order_size
        
        return True
    
    def onOrderStatusInd(self, ind):
        if (ind.order_status is None):
            return
        
        if (ind.order_status == common.ORDER_STATUS.CANCELLED or ind.order_status == common.ORDER_STATUS.REJECTED):
            orderid = ind.orderid
            if (self.orders.has_key(orderid)):
                order = self.orders.get(orderid)
                order.order_status = ind.order_status
                
                tradestat = self.tradestat.get(ind.symbol)
                release_size = ind.order_size - ind.fill_size
                
                if (ind.action == common.ORDER_ACTION.BUY):
                    tradestat.buy_uncome_size -= release_size
                else:
                    tradestat.sell_uncome_size -= release_size
            
    def onTradeInd(self, ind):
        orderid = ind.reforderid
        
        if (self.orders.has_key(orderid) == False):
            print 'cannot find order for orderid' + orderid
            return
        
        self.trades.append(ind)
        
        order = self.orders.get(orderid)
        order.fill_size += ind.fill_size
        
        if (order.fill_size == order.order_size):
            order.order_status = common.ORDER_STATUS.FILLED
        else:
            order.order_status = common.ORDER_STATUS.ACCEPTED
        
        tradestat = self.tradestat.get(ind.symbol)
        positionKey = self.makePositionKey(ind.symbol, self.strategy.trade_date)
        position  = self.positions.get(positionKey)
        
        if (ind.action == common.ORDER_ACTION.BUY
            or ind.action == common.ORDER_ACTION.COVER
            or ind.action == common.ORDER_ACTION.COVERYESTERDAY
            or ind.action == common.ORDER_ACTION.COVERTODAY):
            
            tradestat.buy_filled_size += ind.fill_size;
            tradestat.buy_uncome_size -= ind.fill_size;
            
            position.curr_size += ind.fill_size;
            
        elif (ind.action == common.ORDER_ACTION.SELL
              or ind.action == common.ORDER_ACTION.SELLTODAY
              or ind.action == common.ORDER_ACTION.SELLYESTERDAY
              or ind.action == common.ORDER_ACTION.SHORT):
            
            tradestat.sell_filled_size += ind.fill_size;
            tradestat.sell_uncome_size -= ind.fill_size;
            
            position.curr_size -= ind.fill_size;
         

########################################################################
class Gateway(object):
    #----------------------------------------------------------------------
    def __init__(self):
        self.callback    = None
    
    def registerCallback(self, callback):
        self.callback = callback
    
    @abstractmethod
    def initConfig(self, props):
        pass
    
    @abstractmethod
    def sendOrder(self, order, algo, param):
        pass
    
    @abstractmethod
    def processQuote(self, quote):
        pass

    @abstractmethod
    def closeDay(self, trade_date):
        pass

########################################################################
class OrderBook(object):
    #----------------------------------------------------------------------
    def __init__(self):    
        self.orders = []
        self.trade_id = long(0)
        self.order_id = long(0)
        
    def nextTradeId(self):
        self.trade_id += 1
        return str(self.trade_id)
    
    def nextOrderId(self):
        self.order_id += 1
        return str(self.order_id)
    
    def addOrder(self, order):
        neworder = Order()        
        # to do
        order.orderid = self.nextOrderId()
        order.reforderid = order.orderid       
        neworder.copy(order)        
        self.orders.append(neworder)

    def makeTrade(self, quote):
        
        if (quote.type == common.QUOTE_TYPE.TICK):
            return self.makeTickTrade(quote)

        if (quote.type == common.QUOTE_TYPE.MINBAR
            or quote.type == common.QUOTE_TYPE.FiVEMINBAR
            or quote.type == common.QUOTE_TYPE.QUARTERBAR
            or quote.type == common.QUOTE_TYPE.SPECIALBAR):
            return self.makeBarTrade(quote)
        
        if (quote.type == common.QUOTE_TYPE.DAILY):
            return self.makeDaiylTrade(quote)

    def makeBarTrade(self, quote):
        
        result = []
        # to be optimized
        for i in xrange(len(self.orders)):
            order = self.orders[i]
            if (quote.symbol != order.symbol):
                continue
            if (order.isFinished()):
                continue
            
            if (order.ordertype == common.ORDER_TYPE.LIMITORDER):
                if (order.action == common.ORDER_ACTION.BUY and order.order_price >= quote.low):
                    trade = TradeInd()
                    trade.tradeid = self.nextTradeId()
                    trade.reforderid = order.orderid
                    trade.symbol  = order.symbol
                    trade.action  = order.action
                    trade.fill_size  = order.order_size
                    trade.fill_price = order.order_price
                    trade.fill_date  = order.order_date
                    trade.fill_time  = quote.time
                    
                    order.order_status = common.ORDER_STATUS.FILLED
                    order.fill_size = trade.fill_size
                    order.fill_price = trade.fill_price
                    
                    orderstatusInd = OrderStatusInd()
                    orderstatusInd.initOrderStatus(order)
                    result.append((trade, orderstatusInd))                
                
                if (order.action == common.ORDER_ACTION.SELL and order.order_price <= quote.high):
                    trade = TradeInd()
                    trade.tradeid = self.nextTradeId()
                    trade.reforderid = order.orderid
                    trade.symbol  = order.symbol
                    trade.action  = order.action
                    trade.fill_size  = order.order_size
                    trade.fill_price = order.order_price
                    trade.fill_date  = order.order_date
                    trade.fill_time  = quote.time
                    
                    order.order_status = common.ORDER_STATUS.FILLED
                    order.fill_size = trade.fill_size
                    order.fill_price = trade.fill_price
                    
                    orderstatusInd = OrderStatusInd()
                    orderstatusInd.initOrderStatus(order)
                    result.append((trade, orderstatusInd))                

            if (order.ordertype == common.ORDER_TYPE.STOPORDER):
                if (order.action == common.ORDER_ACTION.BUY and order.order_price <= quote.high):
                    
                    trade = TradeInd()
                    trade.tradeid = self.nextTradeId()
                    trade.reforderid = order.orderid
                    trade.symbol  = order.symbol
                    trade.action  = order.action
                    trade.fill_size  = order.order_size
                    trade.fill_price = order.order_price
                    trade.fill_date  = order.order_date
                    trade.fill_time  = quote.time
                    
                    order.order_status = common.ORDER_STATUS.FILLED
                    order.fill_size = trade.fill_size
                    order.fill_price = trade.fill_price
                    orderstatusInd = OrderStatusInd()
                    orderstatusInd.initOrderStatus(order)
                    result.append((trade, orderstatusInd))  
                    
                if (order.action == common.ORDER_ACTION.SELL and order.order_price >= quote.low):
                    
                    trade = TradeInd()
                    trade.tradeid = self.nextTradeId()
                    trade.reforderid = order.orderid
                    trade.symbol  = order.symbol
                    trade.action  = order.action
                    trade.fill_size  = order.order_size
                    trade.fill_price = order.order_price
                    trade.fill_date  = order.order_date
                    trade.fill_time  = quote.time
                    
                    order.order_status = common.ORDER_STATUS.FILLED
                    order.fill_size = trade.fill_size
                    order.fill_price = trade.fill_price
                    orderstatusInd = OrderStatusInd()
                    orderstatusInd.initOrderStatus(order)
                    result.append((trade, orderstatusInd))  
                
        return result
    
    def cancelOrder(self, orderid):
        for i in xrange(len(self.orders)):
            order = self.orders[i]
            
            if (order.isFinished()):
                continue
            
            if (order.orderid == orderid):
                order.cancel_size = order.order_size - order.fill_size
                order.order_status = common.ORDER_STATUS.CANCELLED
            
            # todo
            orderstatus = OrderStatusInd()            
            orderstatus.initOrderStatus(order)        
            
            return orderstatus
    
    def cancelAllOrder(self):
        
        result = []
        
        for i in xrange(len(self.orders)):
            order = self.orders[i]
            
            if (order.isFinished()):
                continue
            order.cancel_size = order.order_size - order.fill_size
            order.order_status = common.ORDER_STATUS.CANCELLED
            
            # todo
            orderstatus = OrderStatusInd()
            orderstatus.initOrderStatus(order)
            result.append(orderstatus)
        
        return result
##########################################################################

class BarSimulatorGateway(Gateway):
    #----------------------------------------------------------------------
    def __init__(self):    
        Gateway.__init__(self)
        self.orderbook = OrderBook()
        
    def initConfig(self, props):
        pass
        
    def sendOrder(self, order, algo, param):
        self.orderbook.addOrder(order) 
        self.callback.orderRsp(order, True, '')       
        
    def processQuote(self, quote):
        return self.orderbook.makeTrade(quote)
    
    def closeDay(self, trade_date):
        return self.orderbook.cancelAllOrder()

    