# encoding: utf-8
import os
import pandas as pd

from data.dataserver import JzDataServer
from framework.jzcalendar import JzCalendar
import util.fileio
from data.py_expression_eval import Parser
from data.align import align


class BaseDataView(object):
    """
    Prepare data before research / backtest. Support file I/O.
    Support: add field, add formula, save / load.
    
    Attributes
    ----------
    data_api : JzDataServer
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
        
        self.universe = ""
        self.security = []
        self.start_date = 0
        self.end_date = 0
        self.fields = []
        self.freq = 1

        self.meta_data_list = ['start_date', 'end_date', 'freq', 'fields', 'security', 'universe']
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
        self.reference_quarterly_fields = {"security", "ann_date", "start_date", "end_date",
                                           "comp_type_code", "comp_type_code", "act_ann_date", "start_actdate",
                                           "end_actdate", "report_date", "start_reportdate", "start_reportdate",
                                           "report_type", "report_type", "currency", "total_oper_rev", "oper_rev",
                                           "int_income", "net_int_income", "insur_prem_unearned",
                                           "handling_chrg_income", "net_handling_chrg_income", "net_inc_other_ops",
                                           "plus_net_inc_other_bus", "prem_income", "less_ceded_out_prem",
                                           "chg_unearned_prem_res", "incl_reinsurance_prem_inc",
                                           "net_inc_sec_trading_brok_bus", "net_inc_sec_uw_bus",
                                           "net_inc_ec_asset_mgmt_bus", "other_bus_income", "plus_net_gain_chg_fv",
                                           "plus_net_invest_inc", "incl_inc_invest_assoc_jv_entp",
                                           "plus_net_gain_fx_trans", "tot_oper_cost", "less_oper_cost", "less_int_exp",
                                           "less_handling_chrg_comm_exp", "less_taxes_surcharges_ops",
                                           "less_selling_dist_exp", "less_gerl_admin_exp", "less_fin_exp",
                                           "less_impair_loss_assets", "prepay_surr", "tot_claim_exp",
                                           "chg_insur_cont_rsrv", "dvd_exp_insured", "reinsurance_exp", "oper_exp",
                                           "less_claim_recb_reinsurer", "less_ins_rsrv_recb_reinsurer",
                                           "less_exp_recb_reinsurer", "other_bus_cost", "oper_profit",
                                           "plus_non_oper_rev", "less_non_oper_exp", "il_net_loss_disp_noncur_asset",
                                           "tot_profit", "inc_tax", "unconfirmed_invest_loss",
                                           "net_profit_incl_min_int_inc", "net_profit_excl_min_int_inc",
                                           "minority_int_inc", "other_compreh_inc", "tot_compreh_inc",
                                           "tot_compreh_inc_parent_comp", "tot_compreh_inc_min_shrhldr", "ebit",
                                           "ebitda", "net_profit_after_ded_nr_lp", "net_profit_under_intl_acc_sta",
                                           "s_fa_eps_basic", "s_fa_eps_diluted", "insurance_expense",
                                           "spe_bal_oper_profit", "tot_bal_oper_profit", "spe_bal_tot_profit",
                                           "tot_bal_tot_profit", "spe_bal_net_profit", "tot_bal_net_profit",
                                           "undistributed_profit", "adjlossgain_prevyear",
                                           "transfer_from_surplusreserve", "transfer_from_housingimprest",
                                           "transfer_from_others", "distributable_profit", "withdr_legalsurplus",
                                           "withdr_legalpubwelfunds", "workers_welfare", "withdr_buzexpwelfare",
                                           "withdr_reservefund", "distributable_profit_shrhder", "prfshare_dvd_payable",
                                           "withdr_othersurpreserve", "comshare_dvd_payable",
                                           "capitalized_comstock_div"}
        self.ANN_DATE_FIELD_NAME = 'ann_date'
        self.REPORT_DATE_FIELD_NAME = 'report_date'
        self.TRADE_STATUS_FIELD_NAME = 'trade_status'
    
    @staticmethod
    def _group_df_to_dict(df, by):
        gp = df.groupby(by=by)
        res = {key: value for key, value in gp}
        return res
    
    def _get_ref_daily_fields(self, fields):
        """fields is a list of str."""
        return list(set.intersection(set(self.reference_daily_fields), set(fields)))
    
    def _get_ref_quarterly_fields(self, fields, complement=False, append=False):
        """
        Get list of fields that are in ref_quarterly_fields.
        
        Parameters
        ----------
        fields : list of str
        complement : bool, optional
            If True, get fields that are NOT in ref_quarterly_fields.

        Returns
        -------
        list

        """
        s = set.intersection(set(self.reference_quarterly_fields), set(fields))
        if complement:
            s = set(fields) - s
        if append:
            s.add(self.ANN_DATE_FIELD_NAME)
            s.add(self.REPORT_DATE_FIELD_NAME)
        return list(s)

    def _get_market_daily_fields(self, fields, append=False):
        """fields is a list of str."""
        s = set.intersection(set(self.market_daily_fields), set(fields))
        if append:
            s.add(self.TRADE_STATUS_FIELD_NAME)
        return list(s)
    
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
            fields_ref_daily = self._get_ref_daily_fields(fields)
            fields_market_daily = self._get_market_daily_fields(fields, append=True)  # TODO: not each time we want append = True
            fields_ref_quarterly = self._get_ref_quarterly_fields(fields, append=True)
            
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
                df_ref_daily, msg2 = self.data_api.query_wd_dailyindicator(security_str, self.start_date, self.end_date,
                                                                           sep.join(fields_ref_daily))
                if msg2 != '0,':
                    print msg2
                dic_ref_daily = self._group_df_to_dict(df_ref_daily, 'security')

            if fields_ref_quarterly:
                df_ref_quarterly, msg3 = self.data_api.query_wd_income(security_str, self.start_date, self.end_date,
                                                                       sep.join(fields_ref_quarterly), extend=52)
                if msg3 != '0,':
                    print msg3
                dic_ref_quarterly = self._group_df_to_dict(df_ref_quarterly, 'security')
        else:
            raise NotImplementedError("freq = {}".format(self.freq))
        
        return dic_market_daily, dic_ref_daily, dic_ref_quarterly
        
    @staticmethod
    def _process_index(df, index_name='trade_date'):
        """Use datetime as index."""
        df.drop_duplicates(inplace= True)
        df.set_index(index_name, inplace=True)
        df.sort_index(axis=0, inplace=True)
        df.drop(['security'], axis=1, inplace=True)
        return df
    
    @staticmethod
    def _dic_of_df_to_multi_index_df(dic, levels=None):
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
        if levels is None:
            levels = ['security', 'field']
        keys = dic.keys()
        values = dic.values()
        cols = values[0].columns.values
        multi_idx = pd.MultiIndex.from_product([keys, cols], names=levels)
        # 601117.SH, 601375.SH
        # merge = pd.concat(values[222:225], axis=1, join='outer')
        # for i, (k, v) in enumerate(dic.viewitems()):
        #     if v.shape[0] < cm:
        #         print i, k
        merge = pd.concat(values, axis=1, join='outer')
        merge.columns = multi_idx
        merge.sort_index(axis=1, level=levels, inplace=True)
        if merge.isnull().sum().sum() > 0:
            print "WARNING: Nan encountered, NO fill."
            # merge.fillna(method='ffill')
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
            
        res = self._dic_of_df_to_multi_index_df(dic)
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
            df_mod = df_mod.loc[:, self._get_ref_daily_fields(fields)]
            """
            if 'pe' in df_mod.columns:
                df_mod.loc[:, 'pe'] = df_mod.loc[:, 'pe'].values.astype(float)
            if 'price_level' in df_mod.columns:
                df_mod.loc[:, 'price_level'] = df_mod.loc[:, 'price_level'].values.astype(int)
            """
            dic[sec] = df_mod
        
        res = self._dic_of_df_to_multi_index_df(dic)
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
            df_mod = self._process_index(df, self.REPORT_DATE_FIELD_NAME)
            # df_mod = df.drop_duplicates(subset=self.ANN_DATE_FIELD_NAME)  # TODO
            # df_mod.set_index(self.REPORT_DATE_FIELD_NAME, inplace=True)
            # df_mod.sort_index(axis=0, inplace=True)
            
            df_mod = df_mod.loc[:, self._get_ref_quarterly_fields(fields, append=True)]
            
            dic[sec] = df_mod
    
        res = self._dic_of_df_to_multi_index_df(dic)
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

    def _merge_data2(self, dfs):
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
        
        fields = []
        for df in dfs:
            fields.extend(df.columns.get_level_values(level='field').unique())
        col_multi = pd.MultiIndex.from_product([self.security, fields], names=['security', 'field'])
        merge = pd.DataFrame(index=dfs[0].index, columns=col_multi, data=None)
        
        for df in dfs:
            fields_df = df.columns.get_level_values(level='field')
            sec_df = df.columns.get_level_values(level='security')
            idx = df.index
            merge.loc[idx, pd.IndexSlice[sec_df, fields_df]] = df
            
        if merge.isnull().sum().sum() > 0:
            Warning("nan in final merged data.")
            merge.fillna(method='ffill', inplace=True)
    
        merge.sort_index(axis=1, level=['security', 'field'], inplace=True)
    
        return merge

    def prepare_data(self, props, data_api):
        """
        Query various data from data_server and automatically merge them.
        This make research / backtest easier.
        
        Parameters
        ----------
        props : dict, optional
            start_date, end_date, freq, security, fields
        data_api : BaseDataServer
        
        """
        self.data_api = data_api

        sep = ','
        self.start_date = props['start_date']
        self.end_date = props['end_date']
        self.fields = props['fields'].split(sep)
        self.freq = props['freq']
        self.universe = props.get('universe', "")
        if self.universe:
            self.security = data_api.get_index_comp(self.universe, self.start_date, self.end_date)
        else:
            self.security = props['security'].split(sep)
        
        # query data
        print "\nQuery data..."
        dic_market_daily, dic_ref_daily, dic_ref_quarterly = self._query_data(self.security, self.fields)
        
        # pre-process data
        print "\nPreprocess data..."
        multi_market_daily = self._preprocess_market_daily(dic_market_daily)
        multi_ref_daily = self._preprocess_ref_daily(dic_ref_daily, self.fields)
        multi_ref_quarterly = self._preprocess_ref_quarterly(dic_ref_quarterly, self.fields)
        
        print "\nStore data..."
        merge = self._merge_data([multi_market_daily, multi_ref_daily])
        trade_dates = self.data_api.get_trade_date(self.start_date, self.end_date, is_datetime=False)
        self.data = merge.loc[trade_dates, pd.IndexSlice[:, :]].copy()
        self.data.index.name = 'trade_date'
        
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
        self.fields.append(field_name)
        self.data_api = data_api

        dic_market_daily, dic_ref_daily, dic_ref_quarterly = self._query_data(self.security, [field_name])
        if field_name in self.market_daily_fields:
            df_multi = self._preprocess_market_daily(dic_market_daily)
        elif field_name in self.reference_daily_fields:
            df_multi = self._preprocess_ref_daily(dic_ref_daily, [field_name])
        elif field_name in self.reference_quarterly_fields:
            df_multi = self._preprocess_ref_quarterly(dic_ref_quarterly, self.fields)
        else:
            raise ValueError("field_name = {:s}".format(field_name))
        
        df_multi = df_multi.loc[:, pd.IndexSlice[:, field_name]]
        self._append(df_multi, field_name)  # whether contain only trade days is decided by existing data.
    
    @staticmethod
    def _split_ann_value(ann_date_field_name, df, field_name):
        df_ann = df.loc[pd.IndexSlice[:, :], ann_date_field_name]
        df_ann = df_ann.unstack(level=1)
    
        df_value = df.loc[pd.IndexSlice[:, :], field_name]
        df_value = df_value.unstack(level=1)
        
        return df_ann, df_value
        
    def add_formula(self, formula, field_name):
        self.fields.append(field_name)
        
        parser = Parser()
        expr = parser.parse(formula)  # 'Decay_exp(Decay_linear(open,2.0),0.5, 2)'
        
        var_dic = dict()
        var_list = expr.variables()
        
        df_ann = None
        for var in var_list:
            
            if var in self.reference_quarterly_fields:
                df_ann, df_var = self.get_ts_quarter(var)
            else:
                df_var = self.get_ts(var)
            
            """
            if var[-3:] == '__D':
                var = var.split('__')[0]
                
                df_ann, df_value = self._split_ann_value(self.ANN_DATE_FIELD_NAME, self.data_q, var)
                date_arr = self.data.index.values
                
                df_var = align.align(df_ann, df_var, date_arr)
                
            """
            var_dic[var] = df_var
        
        # TODO: send ann_date into expr.evaluate. We assume that ann_date of all fields of a security is the same
        # TODO: dates is array of int
        df_eval = parser.evaluate(var_dic, df_ann, self.dates)
        
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
        for key in ['data', 'data_q']:
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
        self.data_q = dic['data_q']
        self.__dict__.update(meta_data)  # Series to dict

    @property
    def dates(self):
        """
        Get date array of underlying data.
        
        Returns
        -------
        res : np.array
            dtype: np.datetime64

        """
        # res = JzCalendar.convert_datetime_to_int(self.data.index.values)
        res = self.data.index.values
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
        
        # start_date = JzCalendar.convert_int_to_datetime(start_date)
        # end_date = JzCalendar.convert_int_to_datetime(end_date)

        fields_others = self._get_ref_quarterly_fields(fields, complement=True)
        fields_ref_quarterly = self._get_ref_quarterly_fields(fields, complement=False)
        
        df_ref_expanded = None
        if fields_ref_quarterly:
            df_ref_quarterly = self.data_q.loc[:,
                                               pd.IndexSlice[security, fields_ref_quarterly]]
            df_ref_ann = self.data_q.loc[:,
                                         pd.IndexSlice[security, self.ANN_DATE_FIELD_NAME]]
            df_ref_ann.columns = df_ref_ann.columns.droplevel(level='field')
            # for field_name in df_ref_quarterly.columns.get_level_values(level='field'):
            #     df = df_ref_quarterly.loc[:, pd.IndexSlice[:, field_name]]
            dic_expanded = dict()
            for field_name, df in df_ref_quarterly.groupby(level=1, axis=1):  # by column multiindex fields
                df_expanded = align(df, df_ref_ann, self.dates)
                dic_expanded[field_name] = df_expanded
            # df_ref_expanded = self._dic_of_df_to_multi_index_df(dic_expanded, levels=['field', 'security'])
            df_ref_expanded = pd.concat(dic_expanded.values(), axis=1)
            # df_ref_expanded.index = JzCalendar.convert_int_to_datetime(df_ref_expanded.index)
            df_ref_expanded.index.name = 'trade_date'
            # df_ref_expanded = df_ref_expanded.swaplevel(axis=1)
        
        df_others = self.data.loc[pd.IndexSlice[start_date: end_date],
                                  pd.IndexSlice[security, fields_others]]
        
        df_merge = self._merge_data([df_others, df_ref_expanded])
        return df_merge
    
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
        res = res.stack(level='security', dropna=False)
        res.index = res.index.droplevel(level='trade_date')
        return res

    def get_ts_quarter(self, field, security="", start_date=0, end_date=0):
        # TODO
        sep = ','
        if not security:
            security = self.security
        else:
            security = security.split(sep)
    
        if not start_date:
            start_date = self.start_date
        if not end_date:
            end_date = self.end_date
    
        # start_date = JzCalendar.convert_int_to_datetime(start_date)
        # end_date = JzCalendar.convert_int_to_datetime(end_date)
    
        df_ref_quarterly = self.data_q.loc[:,
                                           pd.IndexSlice[security, field]]
        df_ref_quarterly.columns = df_ref_quarterly.columns.droplevel(level='field')
        
        df_ref_ann = self.data_q.loc[:, pd.IndexSlice[security, self.ANN_DATE_FIELD_NAME]]
        df_ref_ann.columns = df_ref_ann.columns.droplevel(level='field')
        
        return df_ref_ann, df_ref_quarterly
    
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
        
        self._save_h5(data_path, {'data': self.data, 'data_q': self.data_q})
    
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
        
        if field_name in self.reference_quarterly_fields:
            the_data = self.data_q
        else:
            the_data = self.data
        multi_idx = pd.MultiIndex.from_product([the_data.columns.levels[0], [field_name]])
        df.columns = multi_idx
        
        merge = the_data.join(df, how='left')  # left: keep index of existing data unchanged
        merge.sort_index(axis=1, level=['security', 'field'], inplace=True)

        if field_name in self.reference_quarterly_fields:
            self.data_q = merge
        else:
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
