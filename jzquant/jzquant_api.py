import _jzquant_api
import pandas as pd


def get_jzquant_api(address, user="", password=""):
    handle = _jzquant_api.connect(address, user=user, password=password)
    if not handle:
        return None
    return JzQuantApi(_jzquant_api, handle)


class JzQuantApi:
    def __init__(self, _api, handle):
        self._handle = handle
        self._api = _api

        self.API_MAP = {'jset': self._api.jset,
                        'jsd': self._api.jsd,
                        'jsi': self._api.jsi,
                        'jsh': self._api.jsh,
                        'jsq': self._api.jsq}

    def __del__(self):
        self._handle = 0
        self._close()

    def _close(self):
        if self._handle:
            self._api.close_connection()

    @staticmethod
    def _to_datetime_(row):
        """
        Convert date (and time) in a row of DataFrame to datetime.datetime object.
        Used in pd.apply method.

        Parameters
        ----------
        row : pd.Series.
            A row of pd.DataFrame. Must contain 'date'.

        Returns
        -------
        res : datetime.datetime

        """
        date = int(row['DATE'])
        res = pd.datetime(year=date/10000, month=date/100%100, day=date%100)
        if 'TIME' in row:
            time = int(row['TIME'])
            res = res.replace(hour=time/10000, minute=time/100%100, second=time%100)
        return res

    @staticmethod
    def _to_dataframe(columns_dic, index_func = None):
        df = pd.DataFrame(columns_dic)
        if index_func and len(df) > 0:
            df.index = df.apply(index_func, axis=1)
        return df

    def jz_unified(self, api_name, security, fields="", format_="pandas", **kwargs):
        """
        Wrapper for various api such as _jzquant_api.jsd.
        Usage:
            daily bar:
            jsd (security, fields="", format="pandas", start_date="", end_date="", fill_forward="", **kwargs )
            historic minute bar:
            jsh (security, fields="", format="pandas", date="", start_time="", end_time="", **kwargs )
            intraday minute bar:
            jsi (security, fields="", format="pandas", start_time="", end_time="", **kwargs )
            intraday minute bar parallel:

            jsq (security, fields="", format="pandas", func=None, **kwargs )
            jset(security, fields="", format="pandas", filter_="", **kwargs)


        Parameters
        ----------
        api_name : str
            API_MAP = {'jset': self._api.jset,
                       'jsd': self._api.jsd,
                       'jsi': self._api.jsi,
                       'jsh': self._api.jsh,
                       'jsim': self._api.jsim,
                       'jsib': self._api.jsib,
                       'jsq': self._api.jsq}
        security : str
        fields : str
            default = ""
        format_ : str 'pandas'
        kwargs : dict
            may include:
                'filter' : str
                'start_date' : str
                'end_date' : str
                'fill_forward' : str
                'start_time' : str
                'end_time' : str
                'date' : str

        Returns
        -------
        (data, err_msg)
        data : pd.DataFrame or columns
        err_msg : str

        """
        if not self._handle:
            return None

        param = {k: str(v) for k, v in kwargs.items()}
        param['_pandas'] = '1' if format_ == "pandas" else '0'
        param['fields'] = fields

        api = self.API_MAP.get(api_name, None)
        if api is None:
            raise ValueError("{:s} is not a valid api.".format(api_name))
        res = api(self._handle, security, **param)

        if not res:
            return None, "UNKNOWN ERROR"
        if not res['result']:
            return None, res['msg']
        # res['columns'] may be empty. Let users deal with empty data

        data = res['columns']
        if param['_pandas'] == '1':
            data = self._to_dataframe(data, JzQuantApi._to_datetime_)
        return data, None

    def _jset(self, security, fields="", filter_="", format_="pandas", **kwargs) :
        if not self._handle:
            return None
        
        param = dict()
        for kw in kwargs.items():
            param[str(kw[0])] = str(kw[1])

        is_pandas = '1' if format_ == "pandas" else '0'

        param['_pandas'] = is_pandas
        param['fields']  = str(fields)
        param['filter']  = str(filter_)

        r = _jzquant_api.jset(self._handle, security, **param)

        if not r:
            return None, "UNKNOWN ERROR"
        if not r['result']:
            return None, r['msg']

        if is_pandas == '1':
            return JzQuantApi._to_dataframe(r['columns']), None
        else:
            return r['columns'], None  #TODO correct?

    def _jsq(self, security, fields="", format_="pandas", func=None, **kwargs):
        if not self._handle : return None
        
        param = dict()
        for kw in kwargs.items():
            param[str(kw[0])] = str(kw[1])

        is_pandas = '1' if format_ == "pandas" else '0'

        param['_pandas'] = is_pandas
        param['fields'] = str(fields)
        if func:
            param['func'] = func

        r = _jzquant_api.jsq(self._handle, security, **param)

        if not r:
            return None, "UNKNOWN ERROR"
        if not r['result']:
            return None, r['msg']

        if func:
            return r["securities"], None
        else:
            if is_pandas == '1':
                df = pd.DataFrame(r['quotes'])
                df.index = df['SYMBOL']
                del df.index.name
                return df, None
            else:
                return r['quotes'], None

    def _jsd(self, security, fields="", start_date="", end_date="", fill_forward="", format_="pandas", **kwargs) :
        if not self._handle : return None
        
        param = dict()
        for kw in kwargs.items():
            param[str(kw[0])] = str(kw[1])

        is_pandas = '1' if format_ == "pandas" else '0'

        param['_pandas'] = is_pandas
        param['fields'] = str(fields)
        param['start_date'] = str(start_date)
        param["end_date"] = str(end_date)
        param["fill_forward"] = str(fill_forward)

        r = _jzquant_api.jsd(self._handle, security, **param)

        if not r:
            return None, "UNKNOWN ERROR"
        if not r['result']:
            return None, r['msg']

        if is_pandas == '1':
            return JzQuantApi._to_dataframe(r['columns'], JzQuantApi._to_date), None
        else:
            return r['columns'], None

    def _jsi(self, security, fields="", start_time="", end_time="", format_="pandas", **kwargs) :
        if not self._handle : return None
        
        param = { }
        for kw in kwargs.items():
            param[ str(kw[0]) ] = str(kw[1])

        is_pandas = '1' if format_ == "pandas" else '0'

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

    def _jsh(self, security, fields="", date="", start_time="", end_time="", format_="pandas", **kwargs) :
        if not self._handle : return None
        
        param = { }
        for kw in kwargs.items():
            param[ str(kw[0]) ] = str(kw[1])

        is_pandas = '1' if format_ == "pandas" else '0'

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

    def jsim(self, security, fields="", start_time="", end_time="", format_="pandas", **kwargs):
        """Return pd.Panel"""
        if not self._handle:
            return None
        
        param = { }
        for k, v in kwargs.items():
            param[k] = str(v)

        is_pandas = '1' if format_ == "pandas" else '0'

        param['_pandas'] = is_pandas
        param['fields'] = str(fields)
        param['start_time'] = str(start_time)
        param["end_time"] = str(end_time)

        r = _jzquant_api.jsim(self._handle, security, **param)

        if not r:
            return (None, "UNKNOWN ERROR")
        if not r['result']:
            return (None, r['msg'])

        dict = {}
        values = r['values']

        for (k, v) in values.items():
            df = self._to_dataframe(v, self._to_datetime)
            dict[k] = df

        return pd.Panel(dict)

    def jsib(self, security, fields="", start_time="", end_time="", format_="pandas", **kwargs):
        """Return pd.Panel"""
        param = dict()
        for k, v in kwargs.items():
            param[k] = str(v)

        is_pandas = '1' if format_ == "pandas" else '0'

        param['_pandas'] = is_pandas
        param['fields'] = str(fields)
        param['start_time'] = str(start_time)
        param["end_time"] = str(end_time)

        r = self._api.jsib(security, **param)

        if not r:
            return None, "UNKNOWN ERROR"
        if not r['result']:
            return None, r['msg']

        dict = {}
        values = r['values']

        for (k, v) in values.items():
            df = self._to_dataframe(v, None)
            dict[k] = df

        return pd.Panel(dict), ""


# def create_connection(address, user="", password=""):
#     _jzquant_api.create_connection(address, user=user, password=password)


if __name__ == "__main__":
    address = 'tcp://10.2.0.14:61616'
    usr, pwd = "TODO", "TODO"
    my_api = get_jzquant_api(address, usr, pwd)

    df, msg = my_api.jz_unified('jsd', "000001.SH", fields="",
                                start_date='2017-06-27', end_date='2017-07-27')

    df2, msg2 = my_api.jz_unified('jsh', "rb1710.SHF", fields='',
                                  date=20170707, start_time='', end_time='', bar_size='1')

    assert df.shape == (23, 12)
    assert msg is None
    assert df2.shape == (345, 10)
    assert msg2 is None

    print "Test pass."
