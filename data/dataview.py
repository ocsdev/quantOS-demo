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
    market_daily_fields, reference_daily_fields : list
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
        self.reference_daily_fields = {'currency', 'total_market_value', 'float_market_value',
                                       'high_52w', 'low_52w', 'high_52w_adj', 'low_52w_adj', 'close_price',
                                       'price_level', 'limit_status',
                                       'pe', 'pb', 'pe_ttm', 'pcf', 'pcf_ttm', 'ncf', 'ncf_ttm', 'ps', 'ps_ttm',
                                       'turnover_ratio', 'turnover_ratio_float',
                                       'share_amount', 'share_float', 'price_div_dps',
                                       'share_float_free', 'nppc_ttm', 'nppc_lyr', 'net_assets',
                                       'ncfoa_ttm', 'ncfoa_lyr', 'rev_ttm', 'rev_lyr', 'nicce_ttm'}
        self.reference_quarterly_fields = {'ann_date', 'int_inc', 'net_profit_incl_min_int_inc', 'oper_exp',
                                           'oper_profit', 'oper_rev', 'report_date', 'security',
                                           'tot_oper_rev', 'tot_profit'}
        self.ANN_DATE_FIELD_NAME = 'ann_date'
        self.REPORT_DATE_FIELD_NAME = 'report_date'
    
    @staticmethod
    def _group_df_to_dict(df, by):
        gp = df.groupby(by=by)
        res = {key: value for key, value in gp}
        return res
    
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
        dic_ref_daily : dict
            {str: DataFrame}
        dic_ref_quarterly: dict
            {str: DataFrame}

        """
        sep = ','
    
        if self.freq == 1:
            fields_ref_daily = set.intersection(set(self.reference_daily_fields), set(fields))
            fields_market_daily = set.intersection(set(self.market_daily_fields), set(fields))
            fields_ref_quarterly = set.intersection(set(self.reference_quarterly_fields), set(fields))
            
            dic_market_daily = dict()
            dic_ref_daily = dict()
            dic_ref_quarterly = dict()

            # TODO : use fields = {field: kwargs} to enable params
            # TODO: read securities together
            security_str = sep.join(security)
            if fields_market_daily:
                df_daily, msg1 = self.data_api.daily(security_str, start_date=self.start_date, end_date=self.end_date,
                                                     adjust_mode=None, fields=sep.join(fields_market_daily))
                if msg1 != '0,':
                    print msg1
                dic_market_daily = self._group_df_to_dict(df_daily, 'security')

            if fields_ref_daily:
                df_ref_daily, msg2 = self._query_wd_dailyindicator(security_str, self.start_date, self.end_date,
                                                                   sep.join(fields_ref_daily))
                if msg2 != '0,':
                    print msg2
                dic_ref_daily = self._group_df_to_dict(df_ref_daily, 'security')

            if fields_ref_quarterly:
                df_ref_quarterly, msg3 = self._query_wd_income(security_str, self.start_date, self.end_date,
                                                               sep.join(fields_ref_quarterly))
                if msg3 != '0,':
                    print msg3
                dic_ref_quarterly = self._group_df_to_dict(df_ref_quarterly, 'security')
        else:
            raise NotImplementedError("freq = {}".format(self.freq))
        
        return dic_market_daily, dic_ref_daily, dic_ref_quarterly
        
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
        
    def _preprocess_ref_daily(self, dic, fields):
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
            df_mod = df_mod.loc[:, self.reference_daily_fields.intersection(fields)]
            if 'pe' in df_mod.columns:
                df_mod.loc[:, 'pe'] = df_mod.loc[:, 'pe'].values.astype(float)
            if 'price_level' in df_mod.columns:
                df_mod.loc[:, 'price_level'] = df_mod.loc[:, 'price_level'].values.astype(int)
            dic[sec] = df_mod
        
        res = self.dic_of_df_to_multi_index_df(dic)
        return res

    def _preprocess_ref_quarterly(self, dic, fields):
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
            # df_mod = self._process_index(df)
            # df_mod = df_mod.loc[:, self.reference_daily_fields.intersection(fields)]
            idx_list = [self.REPORT_DATE_FIELD_NAME, 'security']
            raw_idx = df.set_index(idx_list)
            raw_idx.sort_index(axis=0, level=idx_list, inplace=True)
            dic[sec] = raw_idx
    
        res = self.dic_of_df_to_multi_index_df(dic)
        return res
    
    @staticmethod
    def _merge_data(dfs, trade_date=True):
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
        dic_market_daily, dic_ref_daily, dic_ref_quarterly = self._query_data(self.security, self.fields)
        
        # pre-process data
        multi_market_daily = self._preprocess_market_daily(dic_market_daily)
        multi_ref_daily = self._preprocess_ref_daily(dic_ref_daily, self.fields)
        multi_ref_quarterly = self._preprocess_ref_quarterly(dic_ref_quarterly, self.fields)
        
        self.data = self._merge_data([multi_market_daily, multi_ref_daily],
                                     trade_date=trade_date)
        self.data_q = multi_ref_quarterly

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
        
        dic_market_daily, dic_ref_daily, dic_ref_quarterly = self._query_data(self.security, [field_name])
        multi_market_daily = self._preprocess_market_daily(dic_market_daily)
        multi_ref = self._preprocess_ref_daily(dic_ref_daily, [field_name])
        multi_ref_quarterly = self._preprocess_ref_quarterly(dic_ref_quarterly, self.fields)

        merge = self._merge_data([multi_market_daily, multi_ref], trade_date=False)  # contain all days
        self._append(merge, field_name)  # whether contain only trade days is decided by existing data.
    
    @staticmethod
    def _split_ann_value(df, field_name):
        df_ann = df.loc[pd.IndexSlice[:, :], self.ANN_DATE_FIELD_NAME]
        df_ann = df_ann.unstack(level=1)
    
        df_value = df.loc[pd.IndexSlice[:, :], field_name]
        df_value = df_value.unstack(level=1)
        
        return df_ann, df_value
        
    def add_formula(self, formula, field_name):
        import align
        parser = Parser()
        expr = parser.parse(formula)
        var_dic = dict()
        var_list = expr.variables()
        for var in var_list:
            df_var = self.get_ts(var)
            
            if var[-3:] == '__D':
                var = var.split('__')[0]
                
                df_ann, df_value = self._split_ann_value(self.data_q, var)
                date_arr = self.data.index.values
                
                df_var = align.align(df_ann, df_var, date_arr)
                
            var_dic[var] = df_var
        
        #TODO: send ann_date into expr.evaluate. We assume that ann_date of all fields of a security is the same
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

    def _query_wd_income(self, security, start_date, end_date, fields=""):
        """Helper function to call data_api.query with 'wd.income' more conveniently."""
        fields = set(fields.split(','))
        fields.add(self.ANN_DATE_FIELD_NAME)
        fields.add(self.REPORT_DATE_FIELD_NAME)
        fields = ','.join(list(fields))
        filter_argument = self._dic2url({'security': security,
                                         'start_date': start_date,
                                         'end_date': end_date,
                                         'statement_type': '408002000'})
        return self.data_api.query("wd.income",
                                   fields=fields,
                                   filter=filter_argument,
                                   order_by=self.ANN_DATE_FIELD_NAME)
    
    def _query_wd_dailyindicator(self, security, start_date, end_date, fields=""):
        """Helper function to call data_api.query with 'wd.secDailyIndicator' more conveniently."""
        filter_argument = self._dic2url({'security': security,
                                         'start_date': start_date,
                                         'end_date': end_date})
    
        return self.data_api.query("wd.secDailyIndicator",
                                   fields=fields,
                                   filter=filter_argument,
                                   orderby="trade_date")

    @property
    def dates(self):
        """
        Get date array of underlying data.
        
        Returns
        -------
        res : np.array
            dtype: np.datetime64

        """
        res = JzCalendar.convert_datetime_to_int(self.data.index.values)
        return res

    def get(self, security="", start_date=0, end_date=0, fields=""):
        """
        Basic API to get arbitrary data.
        
        Parameters
        ----------
        security : str, optional
            Separated by ',' default "" (all securities).
        start_date : int, optional
            Default 0 (self.start_date).
        end_date : int, optional
            Default 0 (self.start_date).
        fields : str, optional
            Separated by ',' default "" (all fields).

        Returns
        -------
        res : pd.DataFrame
            index is datetimeindex, columns are (security, fields) MultiIndex

        """
        sep = ','
        
        if not fields:
            fields = self.fields
        else:
            fields = fields.split(sep)
        
        if not security:
            security = self.security
        else:
            security = security.split(sep)
        
        if not start_date:
            start_date = self.start_date
        if not end_date:
            end_date = self.end_date
        
        start_date = JzCalendar.convert_int_to_datetime(start_date)
        end_date = JzCalendar.convert_int_to_datetime(end_date)
        
        res = self.data.loc[pd.IndexSlice[start_date: end_date], pd.IndexSlice[security, fields]]
        return res
    
    def get_snapshot(self, snapshot_date, security="", fields=""):
        """
        Get snapshot of fields at snapshot_date.
        
        Parameters
        ----------
        snapshot_date : int
            Date of snapshot.
        security : str, optional
            Separated by ',' default "" (all securities).
        fields : str, optional
            Separated by ',' default "" (all fields).

        Returns
        -------
        res : pd.DataFrame

        """
        # return single security snapshot
        res = self.get(security=security, start_date=snapshot_date, end_date=snapshot_date, fields=fields)
        # result: security as index, field as columns
        res = res.stack(level='security')
        res.index = res.index.droplevel(level='date')
        return res
    
    def get_ts(self, field, security="", start_date=0, end_date=0):
        """
        Get time series data of single field.
        
        Parameters
        ----------
        field : str
            Single field.
        security : str, optional
            Separated by ',' default "" (all securities).
        start_date : int, optional
            Default 0 (self.start_date).
        end_date : int, optional
            Default 0 (self.start_date).

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
