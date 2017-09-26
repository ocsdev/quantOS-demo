from common import *
from dbmanager import *
import pandas as pd
class Instrument(object):

    def __init__(self):
        '''
        Constructor
        '''
        self.jzcode = 0;
        self.instcode = 0;
        self.multiplier = 0.0;
        self.insttype = 0
    def isStock(self):
        if self.insttype == 1:
            return True
        else : 
            return False
    def isFuture(self):
        if (self.insttype == 101 or 
            self.insttype == 102 or 
            self.insttype == 103):
            return True
        else : 
            return False 
    
class InstManager(object):
    def __init__(self):        
        self.conn = getJztsConnection()        
        self.instmap = {}
        self.loadInstruments()
        
    def loadInstruments(self):
        sql = "select I.jzcode, concat(I.instcode,'.',M.marketcode) as instcode, I.multiplier, I.insttype \
                from Instrument I, Market M where I.market = M.market"
        df = pd.read_sql(sql, self.conn)
        for i in range(0, len (df.index)):
            inst = Instrument()
            inst.jzcode = df['jzcode'][i]
            inst.instcode = df['instcode'][i]
            inst.multiplier=df['multiplier'][i]
            inst.insttype = df['insttype'][i]
            self.instmap[inst.instcode] = inst
            
    def getInst(self,code):
        return self.instmap.get(code,None)