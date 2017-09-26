# encoding: utf-8
import pandas as pd

from framework.common import QUOTE_TYPE
from data.dataserver import BaseDataServer
from framework.jzcalendar import JzCalendar


class BaseDataView(object):
    """
    Prepare data before research / backtest. Support file I/O.
    
    Attributes
    ----------
    data_api : BaseDataServer
    security : list
    start_date : int
    end_date : int
    fields : set
    freq : int
    market_daily_fields, reference_fields : set
    dic_market, dic_ref : dict
        {security: DataFrame with date be index and fields be columns}
    multi_market, multi_ref : DataFrame
        index is date, columns is security-field MultiIndex
    data : DataFrame
        Final merged data.
        index is date, columns is security-field MultiIndex
    
    """
    # TODO only support stocks!
    def __init__(self, data_api=None):
        self.data_api = data_api

        self.security = []
        self.start_date = 0
        self.end_date = 0
        self.fields = []
        self.freq = 1

        self.meta_data = None
        self.data = None
        self.dic_market = None
        self.dic_ref = None
        self.multi_market = None
        self.multi_ref = None
        
        common_list = {'security', 'start_date', 'end_date'}
        market_bar_list = {'open', 'high', 'low', 'close', 'volume', 'turnover', 'vwap', 'oi'}
        market_tick_list = {'volume', 'oi',
                            'askprice1', 'askprice2', 'askprice3', 'askprice4', 'askprice5',
                            'bidprice1', 'bidprice1', 'bidprice1', 'bidprice1', 'bidprice1',
                            'askvolume1', 'askvolume2', 'askvolume3', 'askvolume4', 'askvolume5',
                            'bidvolume1', 'bidvolume2', 'bidvolume3', 'bidvolume4', 'bidvolume5'}
        self.market_daily_fields = {'open', 'high', 'low', 'close', 'volume', 'turnover', 'vwap', 'oi', 'trade_status'}
        self.reference_fields = {'currency', 'total_market_value', 'float_market_value',
                                 'high_52w', 'low_52w', 'high_52w_adj', 'low_52w_adj', 'close_price',
                                 'price_level', 'limit_status',
                                 'pe', 'pb', 'pe_ttm', 'pcf', 'pcf_ttm', 'ncf', 'ncf_ttm', 'ps', 'ps_ttm',
                                 'turnover_ratio', 'turnover_ratio_float', 'share_amount', 'share_float', 'price_div_dps',
                                 'share_float_free', 'nppc_ttm', 'nppc_lyr', 'net_assets',
                                 'ncfoa_ttm', 'ncfoa_lyr', 'rev_ttm', 'rev_lyr', 'nicce_ttm'}

    """
    def initialize(self, security, start_date, end_date, fields="", freq=1):
        '''Get parameters from research / backtest configs.'''
        sep = ','
        
        self.security = security.split(sep)
        self.start_date = start_date
        self.end_date = end_date
        self.fields = set(fields.split(sep))
        self.freq = freq
        
        self.meta_data = {'start_date': self.start_date, 'end_date': self.end_date,
                          'freq': self.freq, 'fields': self.fields, 'security': self.security}
    """
    def _get_data(self):
        """
        Query data using different APIs, then store them in dict.
        Keys of dict are securitites.
        
        """
        sep = ','
    
        if self.freq == 1:
            fields_reference = self.reference_fields.intersection(self.fields)
            fields_market_daily = self.market_daily_fields.intersection(self.fields)
            dic_market_daily = dict()
            dic_ref = dict()
            for sec in self.security:
                # TODO : use fields = {field: kwargs} to enable params
                df_daily, msg1 = self.data_api.daily(sec, begin_date=self.start_date, end_date=self.end_date,
                                                     adjust_mode=None, fields=sep.join(fields_market_daily))
                df_ref, msg2 = self._query_wd(sec, self.start_date, self.end_date, sep.join(fields_reference))
                if msg1 != '0,':
                    print msg1
                if msg2 != '0,':
                    print msg2
                dic_market_daily[sec] = df_daily
                dic_ref[sec] = df_ref
        else:
            raise NotImplementedError("freq = {}".format(self.freq))

        self.dic_market = dic_market_daily
        self.dic_ref = dic_ref
        
    @staticmethod
    def _date_to_dt(date):
        return pd.to_datetime(date, format="%Y%m%d")
    
    @staticmethod
    def _date_index(df):
        """Use datetime as index."""
        df.index = JzCalendar.convert_int_to_datetime(df.loc[:, 'trade_date'])
        df.index.name = 'date'
        df.drop(['trade_date', 'security'], axis=1, inplace=True)
        return df
    
    @staticmethod
    def dic_of_df_to_multi(dic):
        """
        Convert dict of DataFrame to MultiIndex DataFrame.
        Columns of result will be MultiIndex constructed using keys of dict and columns of DataFrame.
        Index of result will be the same with DataFrame.
        Different DataFrame will be aligned (outer join) using index.
        
        Parameters
        ----------
        dic : dict
            {security: DataFrame with index be datetime and columns be fields}.

        Returns
        -------
        merge : pd.DataFrame
            with MultiIndex columns.

        """
        keys = dic.keys()
        values = dic.values()
        cols = values[0].columns.values
        multi_idx = pd.MultiIndex.from_product([keys, cols], names=['security', 'field'])
        merge = pd.concat(values, axis=1, join='outer')
        merge.columns = multi_idx
        if merge.isnull().sum().sum() > 0:
            print "WARNING: Nan encountered, use latest to fill nan."
            merge.fillna(method='ffill')
        return merge
        
    def _preprocess_data(self):
        """Process index and construct MultiIndex."""
        for sec, df in self.dic_market.viewitems():
            self.dic_market[sec] = self._date_index(df)
            
        for sec, df in self.dic_ref.viewitems():
            df_mod = self._date_index(df)
            df_mod = df_mod.loc[:, self.reference_fields.intersection(self.fields)]
            if 'pe' in df_mod.columns:
                df_mod.loc[:, 'pe'] = df_mod.loc[:, 'pe'].values.astype(float)
            if 'price_level' in df_mod.columns:
                df_mod.loc[:, 'price_level'] = df_mod.loc[:, 'price_level'].values.astype(int)
            self.dic_ref[sec] = df_mod

        self.multi_market = self.dic_of_df_to_multi(self.dic_market)
        self.multi_ref = self.dic_of_df_to_multi(self.dic_ref)

    def _merge_data(self):
        """
        Merge data from different APIs into one DataFrame.

        """
        # align on date index, concatenate on columns (security and fields)
        self.data = pd.concat([self.multi_market, self.multi_ref], axis=1, join='inner')
        if self.data.isnull().sum().sum() > 0:
            Warning("nan in final merged data.")
            self.data.fillna(method='ffill', inplace=True)
        self.data.sort_index(axis=1, level=['security', 'field'], inplace=True)
        
    def prepare_data(self, props=None, file_path=None):
        """
        Do all dirty work before research / backtest.
        If props is given, then query data from data_server.
        If file_path is given then query data from local file.
        
        Parameters
        ----------
        props : dict, optional
            start_date, end_date, freq, security, fields
        file_path : str, optional
            File path of pre-stored hd5 file.
            
        """
        if file_path:
            h5 = pd.HDFStore(file_path)
            self.data = h5['data']
            self.meta_data = h5['meta_data'].to_dict()  # Series to dict
            self.__dict__.update(self.meta_data)
            h5.close()
        elif props:
            sep = ','
            self.security = props['security'].split(sep)
            self.start_date = props['start_date']
            self.end_date = props['end_date']
            self.fields = set(props['fields'].split(sep))
            self.freq = props['freq']

            self.meta_data = {'start_date': self.start_date, 'end_date': self.end_date,
                              'freq': self.freq,
                              'fields': ','.join(self.fields), 'security': ','.join(self.security)}
            
            self._get_data()
            self._preprocess_data()
            self._merge_data()
        else:
            raise ValueError("one of props or file_path must be given.")
    
    @staticmethod
    def _dic2url(d):
        """
        Convert a dict to str like 'k1=v1&k2=v2'
        
        Parameters
        ----------
        d : dict

        Returns
        -------
        str

        """
        l = ['='.join([key, str(value)]) for key, value in d.items()]
        return '&'.join(l)

    def _query_wd(self, security, start_date, end_date, fields=""):
        """Helper function to call data_api.query with 'wd.secDailyIndicator more conveniently."""
        filter_argument = self._dic2url({'security': security,
                                         'start_date': start_date,
                                         'end_date': end_date})
    
        return self.data_api.query("wd.secDailyIndicator",
                                   fields=fields,
                                   filter=filter_argument,
                                   orderby="trade_date")
    
    def get(self, security, start_date=0, end_date=999999, fields=""):
        """
        Basic API to get arbitrary data.
        
        Parameters
        ----------
        security : str
            Separated by ','
        start_date : int
        end_date : int
        fields : str
            Separated by ','

        Returns
        -------
        res : pd.DataFrame

        """
        sep = ','
        fields = fields.split(sep)
        security = security.split(sep)
        start_date = JzCalendar.convert_int_to_datetime(start_date)
        end_date = JzCalendar.convert_int_to_datetime(end_date)
        
        res = self.data.loc[pd.IndexSlice[start_date: end_date], pd.IndexSlice[security, fields]]
        #  res = self.data.sort_index(axis=1, level=['security', 'field']).loc[pd.IndexSlice[start_date: end_date], pd.IndexSlice[security, fields]]
        return res
    
    def get_snapshot(self, security, snapshot_date=0, fields=""):
        """
        
        Parameters
        ----------
        security : str
            Separated by ','
        snapshot_date : int
        fields : str
            Separated by ','

        Returns
        -------
        res : pd.DataFrame

        """
        # return single security snapshot
        res = self.get(security, start_date=snapshot_date, end_date=snapshot_date, fields=fields)
        res = res.stack(level='security')
        res.index = res.index.droplevel(level='date')
        return res

    def save(self, file_name='store.hd5', append_info=True):
        """Save data and meta_data to a single hd5 file."""
        if append_info:
            file_name = "{:d}_{:d}_freq={:d}".format(self.start_date, self.end_date, self.freq) + file_name
    
        folder = 'prepared'
        h5 = pd.HDFStore('/'.join([folder, file_name]))
        h5['data'] = self.data
        h5['meta_data'] = pd.DataFrame(index=range(len(self.meta_data)), data=self.meta_data)
        h5.close()

    """
    def get_snapshot_time(self, time, fields):
        # return single time snapshot
        res = self.get(','.join(self.security), start_date=time, end_date=time, fields=fields)
        res = res.stack(level='security')
        res.index = res.index.droplevel(level='date')
        return
    
    """


"""
    def create_dataview(self, view_name="", security, time, fields):
        pass
    
    def append_data(self, field="field_name", func="calc_func")
        pass
    
    def append_data(self, field="field_name", data=df_data):
        pass
    
    def save_dataview(self, file_name="", view_name=""):
        pass
    
    def load_dataview(self, file_name="", view_name=""):
        pass
    
    def get_snapshot_secu(self, security, fields):
        # return single security snapshot
        pass
    
    
    def get_snapshot_time(self, time, fields):
        # return single time snapshot
        pass
    
    def get_data(self, security, time, fields):
        # return dict at the moment
        pass


"""


def get_yahoo():
    import pandas as pd
    import pandas_datareader.data as wb
    
    stocks = ['AAPL', 'GOOG', 'FB']
    pan = wb.DataReader(stocks, 'yahoo', '2016-01-01')
    print type(pan), pan.shape
    print pan.axes
    
    s = pd.HDFStore('store.hd5')
    s['panel'] = pan
    
    
def test_xarray():
    import numpy as np
    import pandas as pd
    import xarray as xr
    
    a = xr.DataArray(np.random.randn(2, 3))
    
    
def test_dataview():
    from data.dataserver import JzDataServer
    
    ds = JzDataServer()
    dv = BaseDataView(data_api=ds)
    
    secs = '600030.SH,000063.SZ'
    props = {'start_date': 20170605, 'end_date': 20170701, 'security': secs,
             'fields': 'close,high,oi,pb,net_assets,ncf', 'freq': 1}
    
    # formula = ' pb * close / open'
    # dv.fill_data('mypb', furmula)
    
    dv.prepare_data(props=props)
    assert dv.multi_market.shape == (20, 6)
    assert dv.multi_ref.shape == (27, 6)
    assert dv.data.shape == (dv.multi_market.shape[0], dv.multi_ref.shape[1] + dv.multi_market.shape[1])
    
    dv.save()
    dv.save('test_store.hd5', append_info=False)
    
    snap1 = dv.get_snapshot('600030.SH,000063.SZ', 20170606, 'close,pb')
    assert snap1.shape == (2, 2)
    assert set(snap1.columns.values) == {'close', 'pb'}
    assert set(snap1.index.values) == {'600030.SH', '000063.SZ'}


def test_dataview_read():
    dv = BaseDataView()
    dv.prepare_data(props=None, file_path='prepared/test_store.hd5')
    
    assert len(dv.meta_data) == 5

    snap1 = dv.get_snapshot('600030.SH,000063.SZ', 20170606, 'close,pb')
    assert snap1.shape == (2, 2)
    assert set(snap1.columns.values) == {'close', 'pb'}
    assert set(snap1.index.values) == {'600030.SH', '000063.SZ'}
    
    
if __name__ == "__main__":
    test_dataview()
    test_dataview_read()
