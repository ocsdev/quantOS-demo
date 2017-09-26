
from dbmanager import *
import MySQLdb
import pandas as pd
import numpy as np


class JzCalendar(object):
    
    #----------------------------------------------------------------------
    def __init__(self):               
        self.conn = getJztsConnection()
        
    def getPreTradeDate(self, date):
        sql = 'select max(date) as date from Calendar where date < %d'%(date)
        df = pd.read_sql(sql, self.conn)
        return df['date'][0]
    
    def getNextTradeDate(self, date):
        sql = 'select min(date) as date from Calendar where date > %d'%(date)
        df = pd.read_sql(sql, self.conn)       
        return df['date'][0]
    
    def getTradeDates(self, begin, end):
        sql = 'select date from Calendar where date >= %d  and date <= %d'%(begin,end)
        df = pd.read_sql(sql, self.conn)
        return df['date']
    
    def transferDtToInt(self, now):
        return now.year * 10000 + now.month * 100 + now.day
 
 #### for testing   
if __name__ == '__main__':
    calendar = JzCalendar()
    date = 20170808
    print calendar.getPreTradeDate(date)
    print calendar.getNextTradeDate(date)
    print calendar.getTradeDates(20170701, 20170723)
