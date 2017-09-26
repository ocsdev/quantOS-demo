import _jzquant_api
import pandas as pd

import time

class JzQuantApi:
    def __init__(self, handle):
        self._handle = handle

    def __del__(self):
        self.close()


    @staticmethod
    def _to_date(row):
        date = int(row['DATE'])
        return pd.datetime( year=date/10000, month = date/ 100 % 100, day = date%100)

    @staticmethod
    def _to_datetime(row):
        date = int(row['DATE'])
        time = int(row['TIME'])
        return pd.datetime( year=date/10000, month=date/ 100 % 100, day = date%100,
                            hour=time/10000, minute=time/100%100, second =time%100)

    @staticmethod
    def _to_dataframe(cloumset, index_func = None):
        df = pd.DataFrame(cloumset)
        if index_func and len(df)>0:
            df.index = df.apply(index_func, axis = 1)
        return df

    # 
    # return [DataFrame, error_msg]
    # 
    def jset(self, view, fields="", filter="", format="pandas", **kwargs ) :
        if not self._handle : return None
        
        param = { }
        for kw in kwargs.items():
            param[ str(kw[0]) ] = str(kw[1])

        is_pandas = '1' if format=="pandas" else '0'

        param['_pandas'] = is_pandas
        param['fields']  = str(fields)
        param['filter']  = str(filter)

        r = _jzquant_api.jset(self._handle, view, **param)

        if not r:            return (None, "UNKNOWN ERROR")
        if not r['result'] : return (None, r['msg'])

        if is_pandas == '1':
            import pandas as pd
            return (JzQuantApi._to_dataframe(r['columns']), None)
        else:
            (r['columns'], None)


    def close(self):
        if self._handle:
            _jzquant_api.close(self._handle)
            self._handle = 0

    # 
    # return [DataFrame, error_msg]
    # 
    def jsq(self, security, fields="", format="pandas", func=None, **kwargs ) :
        if not self._handle : return None
        
        param = { }
        for kw in kwargs.items():
            param[ str(kw[0]) ] = str(kw[1])

        is_pandas = '1' if format=="pandas" else '0'

        param['_pandas']     = is_pandas
        param['fields']      = str(fields)
        if func:
            param['func']    = func

        r = _jzquant_api.jsq(self._handle, security, **param)

        if not r:            return (None, "UNKNOWN ERROR")
        if not r['result'] : return (None, r['msg'])

        #print "**********************" , r
        if func:
            return (r["securities"], None)
        else:
            if is_pandas == '1':
                df = pd.DataFrame(r['quotes'])
                df.index = df['SYMBOL']
                del df.index.name
                return (df, None)
            else:
                return (r['quotes'], None)

    # 
    # return [DataFrame, error_msg]
    # 
    def jsd(self, security, fields="", start_date="", end_date="", fill_forward="", format="pandas", **kwargs ) :
        if not self._handle : return None
        
        param = { }
        for kw in kwargs.items():
            param[ str(kw[0]) ] = str(kw[1])

        is_pandas = '1' if format=="pandas" else '0'

        param['_pandas']     = is_pandas
        param['fields']      = str(fields)
        param['start_date']  = str(start_date)
        param["end_date"]    = str(end_date)
        param["fill_forward"]= str(fill_forward)

        r = _jzquant_api.jsd(self._handle, security, **param)

        if not r:            return (None, "UNKNOWN ERROR")
        if not r['result'] : return (None, r['msg'])

        if is_pandas == '1':
            return (JzQuantApi._to_dataframe(r['columns'], JzQuantApi._to_date), None)
        else:
            return (r['columns'], None)

    # 
    # return [DataFrame, error_msg]
    # 
    def jsi(self, security, fields="", start_time="", end_time="", format="pandas", **kwargs ) :
        if not self._handle : return None
        
        param = { }
        for kw in kwargs.items():
            param[ str(kw[0]) ] = str(kw[1])

        is_pandas = '1' if format=="pandas" else '0'

        param['_pandas']     = is_pandas
        param['fields']      = str(fields)
        param['start_time']  = str(start_time)
        param["end_time"]    = str(end_time)

        r = _jzquant_api.jsi(self._handle, security, **param)

        if not r:            return (None, "UNKNOWN ERROR")
        if not r['result'] : return (None, r['msg'])

        if is_pandas == '1':
            return (JzQuantApi._to_dataframe(r['columns'], JzQuantApi._to_datetime), None)
        else:
            return (r['columns'], None)

    # 
    # return [DataFrame, error_msg]
    # 
    def jsh(self, security, fields="", date="", start_time="", end_time="", format="pandas", **kwargs ) :
        if not self._handle : return None
        
        param = { }
        for kw in kwargs.items():
            param[ str(kw[0]) ] = str(kw[1])

        is_pandas = '1' if format=="pandas" else '0'

        param['_pandas']     = is_pandas
        param['fields']      = str(fields)
        param['date']        = str(date)
        param['start_time']  = str(start_time)
        param["end_time"]    = str(end_time)

        r = _jzquant_api.jsh(self._handle, security, **param)

        if not r:            return (None, "UNKNOWN ERROR")
        if not r['result'] : return (None, r['msg'])

        if is_pandas == '1':
            return (JzQuantApi._to_dataframe(r['columns'], JzQuantApi._to_datetime), None)
        else:
            return (r['columns'], None)			

    # 
    # return [Panel]
    # 
    def jsim(self, security, fields="", start_time="", end_time="", format="pandas", **kwargs ) :
        if not self._handle : return None
        
        param = { }
        for kw in kwargs.items():
            param[ str(kw[0]) ] = str(kw[1])

        is_pandas = '1' if format=="pandas" else '0'

        param['_pandas']     = is_pandas
        param['fields']      = str(fields)
        param['start_time']  = str(start_time)
        param["end_time"]    = str(end_time)

        r = _jzquant_api.jsim(self._handle, security, **param)

        if not r:            return (None, "UNKNOWN ERROR")
        if not r['result'] : return (None, r['msg'])

        dict = {}
        values = r['values']
		
        for (k,v) in values.items():
            df = JzQuantApi._to_dataframe(v, JzQuantApi._to_datetime)
            dict[k] = df
    	
        return pd.Panel(dict)
	
    def close(self):
        if self._handle:
            _jzquant_api.close(self._handle)
            self._handle = 0

def connect(addr, user="", password=""):
    h = _jzquant_api.connect(addr, user=user, password=password)
    if not h: return None
    return JzQuantApi(h)

def create_connection(addr, user="", password=""):
    _jzquant_api.create_connection(addr, user=user, password=password)

def close_connection():
    _jzquant_api.close_connection()

# 
# return [Panel]
# 
def jsib(security, fields="", start_time="", end_time="", format="pandas", **kwargs ) :
    param = { }
    for kw in kwargs.items():
        param[ str(kw[0]) ] = str(kw[1])

    is_pandas = '1' if format=="pandas" else '0'

    param['_pandas']     = is_pandas
    param['fields']      = str(fields)
    param['start_time']  = str(start_time)
    param["end_time"]    = str(end_time)

    r = _jzquant_api.jsib(security, **param)

    if not r:            return (None, "UNKNOWN ERROR")
    if not r['result'] : return (None, r['msg'])

    dict = {}
    values = r['values']
	
    for (k,v) in values.items():
        df = JzQuantApi._to_dataframe(v, None)
        dict[k] = df

    return (pd.Panel(dict), "")
		
	
