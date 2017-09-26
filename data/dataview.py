# encoding: utf-8
import os
import pandas as pd

from data.dataserver import BaseDataServer
from framework.jzcalendar import JzCalendar
import util.fileio
from data.py_expression_eval import Parser


class BaseDataView(object):
    """
    Prepare data before research / backtest. Support file I/O.
    
    Attributes
    ----------
    data_api : BaseDataServer
    security : list
    start_date : int
    end_date : int
    fields : list
    freq : int
    market_daily_fields, reference_fields : list
    dic_market, dic_ref : dict
        {security: DataFrame with date be index and fields be columns}
    multi_market, multi_ref : DataFrame
        index is date, columns is security-field MultiIndex
    data : pd.DataFrame
        Final merged data.
        index is date, columns is security-field MultiIndex
    
    """
    # TODO only support stocks!
    def __init__(self):
        self.data_api = None
        
        self.security = []
        self.start_date = 0
        self.end_date = 0
        self.fields = []
        self.freq = 1

        self.meta_data_list = ['start_date', 'end_date', 'freq', 'fields', 'security']
        self.data = None
        self.data_q = None
        
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
                                 'turnover_ratio', 'turnover_ratio_float',
                                 'share_amount', 'share_float', 'price_div_dps',
                                 'share_float_free', 'nppc_ttm', 'nppc_lyr', 'net_assets',
                                 'ncfoa_ttm', 'ncfoa_lyr', 'rev_ttm', 'rev_lyr', 'nicce_ttm'}

    def _query_data(self, security, fields):
        """
        Query data using different APIs, then store them in dict.
        period, start_date and end_date are fixed.
        Keys of dict are securitites.
        
        Parameters
        ----------
        security : list of str
        fields : list of str

        Returns
        -------
        dic_market_daily : dict
            {str: DataFrame}
        dic_ref : dict
            {str: DataFrame}

        """
        sep = ','
    
        if self.freq == 1:
            fields_reference = set.intersection(set(self.reference_fields), set(fields))
            fields_market_daily = set.intersection(set(self.market_daily_fields), set(fields))
            dic_market_daily = dict()
            dic_ref = dict()
            for sec in security:
                # TODO : use fields = {field: kwargs} to enable params
                df_daily, df_ref = None, None
                
                if fields_market_daily:
                    df_daily, msg1 = self.data_api.daily(sec, start_date=self.start_date, end_date=self.end_date,
                                                         adjust_mode=None, fields=sep.join(fields_market_daily))
                    if msg1 != '0,':
                        print msg1
                    dic_market_daily[sec] = df_daily
                
                if fields_reference:
                    df_ref, msg2 = self._query_wd(sec, self.start_date, self.end_date, sep.join(fields_reference))
                    if msg2 != '0,':
                        print msg2
                    dic_ref[sec] = df_ref
                
        else:
            raise NotImplementedError("freq = {}".format(self.freq))
        
        return dic_market_daily, dic_ref
        
    @staticmethod
    def _process_index(df):
        """Use datetime as index."""
        df.index = JzCalendar.convert_int_to_datetime(df.loc[:, 'trade_date'])
        df.index.name = 'date'
        df.drop(['trade_date', 'security'], axis=1, inplace=True)
        return df
    
    @staticmethod
    def dic_of_df_to_multi_index_df(dic):
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
        
    def _preprocess_market_daily(self, dic):
        """
        Process data and construct MultiIndex.
        
        Parameters
        ----------
        dic : dict

        Returns
        -------
        res : pd.DataFrame

        """
        if not dic:
            return None
        for sec, df in dic.viewitems():
            dic[sec] = self._process_index(df)
            
        res = self.dic_of_df_to_multi_index_df(dic)
        return res
        
    def _preprocess_ref(self, dic, fields):
        """
        Process data and construct MultiIndex.
        
        Parameters
        ----------
        dic : dict

        Returns
        -------
        res : pd.DataFrame

        """
        if not dic:
            return None
        for sec, df in dic.viewitems():
            df_mod = self._process_index(df)
            df_mod = df_mod.loc[:, self.reference_fields.intersection(fields)]
            if 'pe' in df_mod.columns:
                df_mod.loc[:, 'pe'] = df_mod.loc[:, 'pe'].values.astype(float)
            if 'price_level' in df_mod.columns:
                df_mod.loc[:, 'price_level'] = df_mod.loc[:, 'price_level'].values.astype(int)
            dic[sec] = df_mod
        
        res = self.dic_of_df_to_multi_index_df(dic)
        return res

    def _merge_data(self, dfs, trade_date=True):
        """
        Merge data from different APIs into one DataFrame.
        
        Parameters
        ----------
        trade_date : bool

        Returns
        -------
        merge : pd.DataFrame
        
        """
        # align on date index, concatenate on columns (security and fields)
        dfs = [df for df in dfs if df is not None]
        if trade_date:
            join_method = 'inner'
        else:
            join_method = 'outer'
            
        merge = pd.concat(dfs, axis=1, join=join_method)
        if merge.isnull().sum().sum() > 0:
            Warning("nan in final merged data.")
            merge.fillna(method='ffill', inplace=True)
            
        merge.sort_index(axis=1, level=['security', 'field'], inplace=True)
        
        return merge
        
    def prepare_data(self, props, data_api, trade_date=True):
        """
        Query various data from data_server and automatically merge them.
        This make research / backtest easier.
        
        Parameters
        ----------
        props : dict, optional
            start_date, end_date, freq, security, fields
        data_api : BaseDataServer
        trade_date : bool, optional
            if True, merge result will only contain trading days.
        
        """
        self.data_api = data_api

        sep = ','
        self.security = props['security'].split(sep)
        self.start_date = props['start_date']
        self.end_date = props['end_date']
        self.fields = props['fields'].split(sep)
        self.freq = props['freq']
        
        # query data
        dic_market_daily, dic_ref = self._query_data(self.security, self.fields)
        
        # pre-process data
        multi_market_daily = self._preprocess_market_daily(dic_market_daily)
        multi_ref = self._preprocess_ref(dic_ref, self.fields)
        
        self.data = self._merge_data([multi_market_daily, multi_ref], trade_date=trade_date)

    def add_field(self, data_api, field_name):
        """
        Query and append new field.
        
        Parameters
        ----------
        data_api : BaseDataServer
        field_name : str
        trade_date : bool, optional
            if True, merge data only contains trading days.

        """
        self.data_api = data_api
        
        dic_market_daily, dic_ref = self._query_data(self.security, [field_name])
        multi_market_daily = self._preprocess_market_daily(dic_market_daily)
        multi_ref = self._preprocess_ref(dic_ref, [field_name])

        merge = self._merge_data([multi_market_daily, multi_ref], trade_date=False)  # contain all days
        self._append(merge, field_name)  # whether contain only trade days is decided by existing data.
    
    def add_formula(self, formula, field_name):
        parser = Parser()
        expr = parser.parse(formula)
        var_dic = dict()
        var_list = expr.variables()
        for var in var_list:
            var_dic[var] = self.get_ts(','.join(self.security), self.start_date, self.end_date, field=var)
        df_eval = expr.evaluate(var_dic)
        self._append(df_eval, field_name)
            

    @staticmethod
    def _load_h5(fp):
        """Load data and meta_data from hd5 file.
        
        Parameters
        ----------
        fp : str, optional
            File path of pre-stored hd5 file.
        
        """
        h5 = pd.HDFStore(fp)
        
        res = dict()
        for key in ['data']:
            res[key] = h5[key]
            
        h5.close()
        
        return res
        
    def load_dataview(self, folder='.'):
        """
        Load data from local file.
        
        Parameters
        ----------
        folder : str, optional
            Folder path to store hd5 file and meta data.
            
        """
        meta_data = util.fileio.read_json(os.path.join(folder, 'meta_data.json'))
        dic = self._load_h5(os.path.join(folder, 'data.hd5'))
        self.data = dic['data']
        self.__dict__.update(meta_data)  # Series to dict

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
        return res
    
    def get_snapshot(self, security, snapshot_date=0, fields=""):
        """
        Get snapshot of fields at snapshot_date.
        
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
        # result: security as index, field as columns
        res = res.stack(level='security')
        res.index = res.index.droplevel(level='date')
        return res
    
    def get_ts(self, security, start_date, end_date, field):
        """
        Get time series data of single field.
        
        Parameters
        ----------
        security : str
            Separated by ','
        start_date : int
        end_date : int
        field : str

        Returns
        -------
        res : pd.DataFrame

        """
        res = self.get(security, start_date=start_date, end_date=end_date, fields=field)
        # result: date as index, security as columns
        res.columns = res.columns.droplevel(level='field')
        return res

    def save_dataview(self, folder="."):
        """Save data and meta_data to a single hd5 file."""
        sub_folder = "{:d}_{:d}_freq={:d}D".format(self.start_date, self.end_date, self.freq)
        
        folder = os.path.join(folder, sub_folder)
        meta_path = os.path.join(folder, 'meta_data.json')
        data_path = os.path.join(folder, 'data.hd5')
    
        meta_data = {key: self.__dict__[key] for key in self.meta_data_list}
        util.fileio.save_json(meta_data, meta_path)
        
        self._save_h5(data_path, {'data': self.data})
    
    @staticmethod
    def _save_h5(fp, dic):
        """
        Save data in dic to a hd5 file.
        
        Parameters
        ----------
        fp : str
            File path.
        dic : dict

        """
        util.fileio.create_dir(fp)
        h5 = pd.HDFStore(fp)
        for key, value in dic.items():
            h5[key] = value
        h5.close()
    
    def _append(self, df, field_name):
        """
        Append DataFrame to existing multi-index DataFrame.
        
        Parameters
        ----------
        df : pd.DataFrame or pd.Series
        field_name : str

        """
        if isinstance(df, pd.DataFrame):
            pass
        elif isinstance(df, pd.Series):
            df = pd.DataFrame(df)
        else:
            raise ValueError("Data to be appended must be pandas format. But we have {}".format(type(df)))
        
        multi_idx = pd.MultiIndex.from_product([self.data.columns.levels[0], [field_name]])
        df.columns = multi_idx
        
        merge = self.data.join(df, how='left')  # left: keep index of existing data unchanged
        merge.sort_index(axis=1, level=['security', 'field'], inplace=True)
        
        self.data = merge
    
    
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


def test_xarray():
    import numpy as np
    import xarray as xr
    
    a = xr.DataArray(np.random.randn(2, 3))
    
    
def test_dv_write():
    from data.dataserver import JzDataServer
    
    ds = JzDataServer()
    dv = BaseDataView()
    
    secs = '600030.SH,000063.SZ,000001.SZ'
    props = {'start_date': 20170605, 'end_date': 20170701, 'security': secs,
             'fields': 'close,high,volume,pb,net_assets,ncf', 'freq': 1}
    
    # formula = ' pb * close / open'
    # dv.fill_data('mypb', furmula)
    
    dv.prepare_data(props=props, data_api=ds, trade_date=False)
    assert dv.data.shape == (27, 18)
    dv.prepare_data(props=props, data_api=ds, trade_date=True)
    assert dv.data.shape == (20, 18)
    
    dv.save_dataview('prepared')
    
    snap1 = dv.get_snapshot('600030.SH,000063.SZ', 20170606, 'close,pb')
    assert snap1.shape == (2, 2)
    assert set(snap1.columns.values) == {'close', 'pb'}
    assert set(snap1.index.values) == {'600030.SH', '000063.SZ'}


def test_dv_read():
    dv = BaseDataView()
    dv.load_dataview(folder='prepared/20170605_20170701_freq=1D')
    
    assert dv.start_date == 20170605 and set(dv.security) == set('000001.SZ,600030.SH,000063.SZ'.split(','))

    snap1 = dv.get_snapshot('600030.SH,000063.SZ', 20170606, 'close,pb')
    assert snap1.shape == (2, 2)
    assert set(snap1.columns.values) == {'close', 'pb'}
    assert set(snap1.index.values) == {'600030.SH', '000063.SZ'}


def _test_join():
    dv = BaseDataView()
    dv.load_dataview(folder='prepared/20170605_20170701_freq=1D')
    
    raw = dv.data
    df1 = raw.loc[:, pd.IndexSlice[:, ['pb', 'close', 'high', 'net_assets']]]
    df2 = raw.loc[:, pd.IndexSlice[:, 'volume']]
    
    df2.columns = df2.columns.droplevel(level=1)
    df2.columns = pd.MultiIndex.from_product([df2.columns, ['volume']])
    
    merge = df1.join(df2, how='outer')
    merge.sort_index(axis=1, level=['security', 'field'], inplace=True)
    
    print merge
    print raw


def test_add_field():
    dv = BaseDataView()
    dv.load_dataview(folder='prepared/20170605_20170701_freq=1D')
    assert dv.data.shape == (20, 18)
    
    from data.dataserver import JzDataServer
    ds = JzDataServer()
    dv.add_field(ds, 'share_amount')
    assert dv.data.shape == (20, 21)


def test_add_formula():
    dv = BaseDataView()
    dv.load_dataview(folder='prepared/20170605_20170701_freq=1D')
    assert dv.data.shape == (20, 18)
    
    formula = 'Delta(high - close, 1)'
    dv.add_formula(formula, 'myvar1')
    print dv.data.shape
    
    formula2 = 'myvar1 - close'
    dv.add_formula(formula2, 'myvar2')
    print dv.data.shape
    
    
if __name__ == "__main__":
    test_add_formula()
    # _test_join()
    pass
