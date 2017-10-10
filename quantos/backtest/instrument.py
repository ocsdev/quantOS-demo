# encoding: UTF-8

import pandas as pd

from quantos.data.dbmanager import get_jzts_connection


class Instrument(object):
    def __init__(self):
        self.jzcode = 0;
        self.instcode = 0;
        self.multiplier = 0.0;
        self.insttype = 0
    
    def is_stock(self):
        if self.insttype == 1:
            return True
        else:
            return False
    
    def is_future(self):
        if self.insttype == 101 or self.insttype == 102 or self.insttype == 103:
            return True
        else:
            return False


class InstManager(object):
    def __init__(self):
        self.conn = get_jzts_connection()
        self.instmap = {}
        self.load_instruments()
    
    def load_instruments(self):
        sql = "select I.jzcode, concat(I.instcode,'.',M.marketcode) as instcode, I.multiplier, I.insttype \
                from Instrument I, Market M where I.market = M.market"
        df = pd.read_sql(sql, self.conn)
        for _, row in df.iterrows():
            inst = Instrument()
            inst.__dict__.update(row.to_dict())
            self.instmap[inst.instcode] = inst
    
    def get_intruments(self, code):
        return self.instmap.get(code, None)
