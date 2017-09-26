from framework.jzcalendar import *
from instrument import  * 
import jzquant
import datetime as dt
from jzquant import *
from Cython.Compiler.Symtab import Entry
from framework.gateway import TradeInd
import matplotlib.animation as animation
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

#%matplotlib inline


class Pnl(object):
    def __init__(self):
        self.date = 0
        self.symbol = '' 
        self.hold_pnl = 0.0
        self.trade_pnl = 0.0
        
class PnlReport:
    def __init__(self):
        self.daily_pnls = []
        self.trades = None
        self.hold_pnl = 0.0
        self.trade_pnl = 0.0
        self.trade_amount = 0.0
        self.trade_count = 0.0
        self.total_pnl = 0.0
        self.tax = 0.0
        self.commission = 0.0
        self.win_rate = 0.0
        self.maxdrawdown = 0.0
        self.sharp = 0.0 
        
class DailyPnlReport(object):     
    def __init__(self):
        self.positions = {}
        self.total_pnl = 0.0
        self.hold_pnl = 0.0
        self.trade_pnl = 0.0 
        self.accum_total_pnl = 0.0 
        self.trade_amount = 0.0  
        self.trade_count = 0      
        self.commission = 0.0
        self.tax = 0.0        
        self.date = 0
        
    def copy(self, pnl):  
        self.positions    = pnl.positions            
        self.total_pnl    = pnl.total_pnl      
        self.hold_pnl     = pnl.hold_pnl        
        self.trade_pnl    = pnl.trade_pnl      
        self.trade_amount = pnl.trade_amount
        self.trade_count  = pnl.trade_count
        self.commission   = pnl.commission    
        self.tax          = pnl.tax                  
        self.date         = pnl.date  
                          
class PnlManager(object):    
    def __init__(self): 
        self.calendar = JzCalendar()
        self.instmgr = InstManager()
        self.jzquant_api = None 
        self.strategy = None
        self.pnls = []
        self.begin_date = 0
        self.end_date = 0
        self.close_prices = {} 
        self.universe = []
    def setStrategy(self, sta):
        self.strategy = sta
        
    def tradeToDataframe(self, trades):

        key_list = ['tradeid', 'reforderid', 'symbol', 'action', 'fill_price', 'fill_size', 'fill_date', 'fill_time' ]

        result={}

        for p in trades:
            pos = {}
            for key in key_list:
                if key == 'tradeid':
                    pos[key] = int(p.__getattribute__(key))
                else:
                    pos[key] = p.__getattribute__(key)
            result[int(p.tradeid)] = pos
    
        return pd.DataFrame(result).transpose().sort()
    
    def generateStatisticReport(self, pnls):
        report = PnlReport()
        report.daily_pnls = pnls
        report.trades = self.tradeToDataframe(self.strategy.pm.trades)
        win_count = 0
        i = 0
        pre_value = self.strategy.initbalance
        cur_value = self.strategy.initbalance
        rtn = []
        
        for pnl in pnls:
            cur_value = pre_value + pnl.total_pnl
            rtn.append(cur_value/pre_value - 1.0)            
            pre_value = cur_value
            report.trade_amount += pnl.trade_amount
            report.trade_count += pnl.trade_count
            report.commission += pnl.commission
            report.tax += pnl.tax
            report.total_pnl += pnl.total_pnl
            pnl.accum_total_pnl = report.total_pnl
            if report.maxdrawdown > pnl.accum_total_pnl:
                report.maxdrawdown = pnl.accum_total_pnl
            if pnl.total_pnl > 0.0:
                win_count += 1
      
        date_num = float(len(pnls))
        report.win_rate = win_count / date_num      
        s = pd.Series(rtn)
        mean_value = s.mean()       
        std_value = s.std()
        report.sharp = mean_value/std_value * np.sqrt(240.0)
        return report
    
    def generateReport(self):       
        daily_pnls = self.calcPnl(self.strategy.pm.trades)
        report = self.generateStatisticReport(daily_pnls)
        print "Total PNL: %f"%(report.total_pnl)
        print "Total trade number: %d"%(report.trade_count)
        print "Total trade amount: %f"%(report.trade_amount)
        print "Win rate: %f"%(report.win_rate)
        print "Max drawdown: %f"%(report.maxdrawdown)
        print "Tax: %f"%(report.tax)
        print "Commission: %f"%(report.commission)
        print "Sharp: %f"%(report.sharp)
        trade_pnl = []
        hold_pnl = []
        total_pnl = []
        dates = []
        trade_amount = []
        
        for pnl in daily_pnls:            
            dates.append(str(pnl.date))
            trade_pnl.append(pnl.trade_pnl)
            hold_pnl.append(pnl.hold_pnl)
            total_pnl.append(pnl.accum_total_pnl)
            trade_amount.append(pnl.trade_amount)
        
        plt.figure(figsize=(19, 12))
        plt.subplot(111)
        plt.title("Strategy Backtest Result")
        plt.ylabel("PNL")
        plt.xlabel("Date")
        xs = [dt.datetime.strptime(d, '%Y%m%d').date() for d in dates]
        plt.plot(xs, trade_pnl, color = 'r', label='Trading PNL', linewidth = "2")
        plt.plot(xs, hold_pnl , color = 'c', label='Holding PNL', linewidth = "2")
        plt.plot(xs, total_pnl, color = 'm', label='Total PNL', linewidth = "2")
        plt.legend(loc='best')
        plt.grid(True, linestyle = "--", color = "black", linewidth = "1") 
#        legend = ax.legend(loc='upper center', shadow=True, fontsize='x-large')
        plt.show()
        return report
        
    def initFromConfig(self, props):
        self.api = jzquant_api.connect(addr=props.get("jsh.addr"),user="TODO", password="TODO")
        self.begin_date = props.get('begin_date')
        self.end_date   = props.get('end_date') 
        self.fut_commission = props.get('future_commission_rate', 0.0)  
        self.stk_commission = props.get('stock_commission_rate', 0.0)  
        self.stk_tax = props.get('stock_tax_rate', 0.0)
        self.universe = []
        universe_str = props.get('symbol')
        for code in universe_str.split(','):
            self.universe.append(code.strip())
        self.prepareData() 
           
    def prepareData(self):         
        begindate = self.calendar.getPreTradeDate(self.begin_date)
        enddate = self.calendar.getNextTradeDate(self.end_date)
        begin_str = dt.datetime.strptime(str(begindate) , '%Y%m%d').strftime('%Y-%m-%d')
        end_str = dt.datetime.strptime(str(enddate) , '%Y%m%d').strftime('%Y-%m-%d')
        for code in self.universe:          
            df,msg =  self.api.jsd(code, fields="", start_date=begin_str, end_date=end_str)
            for i in range(0, len(df.index)):
                date = (df['DATE'][i])
                close = df['CLOSE'][i]
                if self.close_prices.has_key(date) == False:
                    self.close_prices[date] = {}
                self.close_prices[date][code] = close
                
    def isBuyAction(self, action):
        if (action == ORDER_ACTION_BUY 
            or action == ORDER_ACTION_COVER
            or action == ORDER_ACTION_COVERYESTERDAY
            or action == ORDER_ACTION_COVERTODAY) :
            return True
        else :
            return False
        
    def isSellAction(self, action):
        if (action == ORDER_ACTION_SELL
            or action == ORDER_ACTION_SELLTODAY
            or action == ORDER_ACTION_SELLYESTERDAY
            or action == ORDER_ACTION_SHORT):
            return True
        else :
            return False      
    
    def calculatePnls(self):
        hold_pnls = self.calculateHoldPnl()
        trade_pnls = self.calculateTradePnl()
        total_pnls = []
        i = 0
        for hold_pnl in hold_pnls:
            trade_pnl = trade_pnls[i]
            pnl = DailyPnlReport()
            pnl.date = hold_pnl.date
            pnl.hold_pnl = hold_pnl.hold_pnl
            pnl.trade_amount = trade_pnl.trade_amount
            pnl.trade_pnl = trade_pnl.trade_pnl
            pnl.commission = trade_pnl.commision
            pnl.tax = trade_pnl.tax
            pnl.total_pnl = pnl.hold_pnl + pnl.trade_pnl
            total_pnls.append(pnl)
            if hold_pnl.date == trade_pnl.date:
                i+=1
        return total_pnls    
                
    def calculateHoldPnl(self):
        dates = self.calendar.getTradeDates(self.begin_date, self.end_date)        
        hold_pnl = []
        for date in dates : 
            hold_pnl.append(self.calcOneDayHoldPnl(date))
        return hold_pnl
       
    def calcOneDayHoldPnl(self, date, position):
        pnl = 0.0        
        pre_date = self.calendar.getPreTradeDate(date)
        pre_close_prices = self.close_prices[pre_date]
        cur_close_prices = self.close_prices[date]        
        for code,hold_size in position.items():
            # hold pnl calc          
         
            pre_close = pre_close_prices.get(code,0.0)
            close = cur_close_prices.get(code,0.0)
            inst = self.instmgr.getInst(code)
            mult = inst.multiplier            
            hold_pnl = hold_size * mult * (close - pre_close) 
            pnl += hold_pnl         
        return pnl
        
    def calcPnl(self, all_trades):
        trades = {}
        trade_pnls = {} 
        pnls = []      
        for trade in all_trades:
            code = trade.symbol
            date = trade.fill_date
            if trades.has_key(date) == False:
                trades[date]={}
            if trades[date].has_key(code) == False:
                trades[date][code] = []
            trades[date][code].append(trade)
            
        for key, value in trades.items():
            pnl = self.calcOneDayTradePnl(key, value)   
            trade_pnls[pnl.date] = pnl 
        dates = self.calendar.getTradeDates(self.begin_date, self.end_date) 
        i = 0     
        pre_position = {} 
        cur_position = {}   
        for date in dates:
            pnl = DailyPnlReport()  
            pnl.date = date          
            tpnl = trade_pnls.get(date, None)
            if tpnl != None:
                pnl.copy(tpnl)
            pnl.positions = self.combinePosition(pre_position, pnl.positions)  
            pnl.hold_pnl = self.calcOneDayHoldPnl(date, pre_position) 
            pnl.total_pnl = pnl.hold_pnl + pnl.trade_pnl - pnl.tax - pnl.commission
            pre_position = pnl.positions 
            pnls.append(pnl)
        self.pnls = pnls
        return pnls
#     def calcTradePnl(self, trade, inst, close):         
#         action = trade.action
#         pnl = 0.0
#         if self.isBuyAction(action):
#             pnl = (close - trade.fill_price) * trade.fill_size * inst.multiplier 
#         elif self.isSellAction(action):
#             pnl = (trade.fill_price - close) * trade.fill_size * inst.multiplier    
#         else :
#             return 0.0
#         return pnl
    
    #combine src position to dst_position
    def combinePosition(self, cur_position, pre_position): 
        position = {}
        for k,v in cur_position.items():
            position[k] = v
        for k,v in pre_position.items():
            if position.has_key(k) == False: 
                position[k] = v
            else:
                position[k] += v      
        return position        
          
                 
    def calcOneDayTradePnl(self, date, trade_map):  
        pnl = DailyPnlReport()
        pnl.date = date  
        close_prices = self.close_prices[date]  
        for code,trades in trade_map.items():
            position = 0
            close = close_prices.get(code,0.0) 
            inst = self.instmgr.getInst(code)
            tax_rate = 0.0
            commission_rate = 0.0
            
            if inst.isStock():
                tax_rate =  self.stk_tax
                commission_rate = self.stk_commission    
            elif inst.isFuture():                        
                commission_rate = self.fut_commission     
            for trade in trades:
                tax = 0.0
                action = trade.action
                trade_pnl = 0.0        
                trade_amount =  trade.fill_price * trade.fill_size * inst.multiplier
                commission = commission_rate * trade_amount
                pnl.trade_amount += trade_amount
                pnl.trade_count += 1
                if self.isBuyAction(action):
                    position += trade.fill_size
                    trade_pnl = (close - trade.fill_price) * trade.fill_size * inst.multiplier                    
                elif self.isSellAction(action):
                    position -= trade.fill_size
                    tax = trade_amount * tax_rate
                    trade_pnl = (trade.fill_price - close) * trade.fill_size * inst.multiplier    
                pnl.commission += commission
                pnl.tax += tax
                pnl.trade_pnl += trade_pnl  
                pnl.trade_amount += trade_amount                
            pnl.positions[code] = position
            
        return pnl  
    
if __name__ == '__main__':
    
    props = {}
    props['jsh.addr']   = 'tcp://10.2.0.14:61616'
    props['begin_date'] = 20170702
    props['end_date']   = 20170712
    props['future_commission_rate']  = 0.005
    props['stock_commission_rate']   = 0.005
    props['stock_tax_rate']          = 0.002
    props['symbol']     = '600030.SH'   
    pnlmgr = PnlManager()
    pnlmgr.initFromConfig(props)
    trades = []
    t1 = TradeInd()
    t1.symbol = '600030.SH'
    t1.action = ORDER_ACTION_BUY
    t1.fill_date = 20170704
    t1.fill_size = 100
    t1.fill_price = 16.72
    t2 = TradeInd()
    t2.symbol = '600030.SH'
    t2.action = ORDER_ACTION_SELL
    t2.fill_date = 20170706
    t2.fill_size = 50
    t2.fill_price = 16.69
    t3 = TradeInd()
    t3.symbol = '600030.SH'
    t3.action = ORDER_ACTION_SELL
    t3.fill_date = 20170707
    t3.fill_size = 50
    t3.fill_price = 16.75
    trades.append(t1)
    trades.append(t2)
    trades.append(t3)
    pnls = pnlmgr.calcPnl(trades)
    for pnl in pnls:
        print pnl.date, pnl.trade_pnl, pnl.hold_pnl, pnl.positions.get('600030.SH')
        
    
        