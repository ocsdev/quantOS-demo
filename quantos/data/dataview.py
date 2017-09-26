# encoding: utf-8
"""
How to add custom (alternative) data:
    1. Build a DataFrame of your data, whose index is dv.dates, column is dv.security.
    2. Use dv.append_df to add your DataFrame to the DataView object.
    
If you will use this data frequently, you can add define a new method of DataServer, then get data from your DataServer.
If you want to declare your field in props, instead of append it manually, you will have to modify prepare_data function.
"""
import os

import numpy as np
import pandas as pd
from backtest.calendar import JzCalendar
from data.align import align

import quantos.util.fileio
from quantos.data.py_expression_eval import Parser


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
    data_d : pd.DataFrame
        All daily frequency data will be merged and stored here.
        index is date, columns is security-field MultiIndex
    data_q : pd.DataFrame
        All quarterly frequency data will be merged and stored here.
        index is date, columns is security-field MultiIndex
    
    """
    # TODO only support stocks!
    def __init__(self):
        self.data_api = None
        
        self.universe = ""
        self.security = []
        self.start_date = 0
        self.extended_start_date = 0
        self.end_date = 0
        self.fields = []
        self.freq = 1

        self.meta_data_list = ['start_date', 'end_date', 'freq', 'fields', 'security', 'universe']
        self.data_d = None
        self.data_q = None
        
        common_list = {'security', 'start_date', 'end_date'}
        market_bar_list = {'open', 'high', 'low', 'close', 'volume', 'turnover', 'vwap', 'oi'}
        market_tick_list = {'volume', 'oi',
                            'askprice1', 'askprice2', 'askprice3', 'askprice4', 'askprice5',
                            'bidprice1', 'bidprice1', 'bidprice1', 'bidprice1', 'bidprice1',
                            'askvolume1', 'askvolume2', 'askvolume3', 'askvolume4', 'askvolume5',
                            'bidvolume1', 'bidvolume2', 'bidvolume3', 'bidvolume4', 'bidvolume5'}
        # fields map
        self.market_daily_fields = \
            {'open', 'high', 'low', 'close', 'volume', 'turnover', 'vwap', 'oi', 'trade_status'}
        self.reference_daily_fields = \
            {'currency', 'total_market_value', 'float_market_value',
             'high_52w', 'low_52w', 'high_52w_adj', 'low_52w_adj', 'close_price',
             'price_level', 'limit_status',
             'pe', 'pb', 'pe_ttm', 'pcf', 'pcf_ttm', 'ncf', 'ncf_ttm', 'ps', 'ps_ttm',
             'turnover_ratio', 'turnover_ratio_float',
             'share_amount', 'share_float', 'price_div_dps',
             'share_float_free', 'nppc_ttm', 'nppc_lyr', 'net_assets',
             'ncfoa_ttm', 'ncfoa_lyr', 'rev_ttm', 'rev_lyr', 'nicce_ttm'}
        self.fin_stat_income = \
            {"security", "ann_date", "start_date", "end_date",
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
        self.fin_stat_balance_sheet = \
            {"monetary_cap", "tradable_assets", "notes_rcv", "acct_rcv", "other_rcv", "pre_pay",
             "dvd_rcv", "int_rcv", "inventories", "consumptive_assets", "deferred_exp",
             "noncur_assets_due_1y", "settle_rsrv", "loans_to_banks", "prem_rcv", "rcv_from_reinsurer",
             "rcv_from_ceded_insur_cont_rsrv", "red_monetary_cap_for_sale", "other_cur_assets",
             "tot_cur_assets", "fin_assets_avail_for_sale", "held_to_mty_invest", "long_term_eqy_invest",
             "invest_real_estate", "time_deposits", "other_assets", "long_term_rec", "fix_assets",
             "const_in_prog", "proj_matl", "fix_assets_disp", "productive_bio_assets",
             "oil_and_natural_gas_assets", "intang_assets", "r_and_d_costs", "goodwill",
             "long_term_deferred_exp", "deferred_tax_assets", "loans_and_adv_granted",
             "oth_non_cur_assets", "tot_non_cur_assets", "cash_deposits_central_bank",
             "asset_dep_oth_banks_fin_inst", "precious_metals", "derivative_fin_assets",
             "agency_bus_assets", "subr_rec", "rcv_ceded_unearned_prem_rsrv", "rcv_ceded_claim_rsrv",
             "rcv_ceded_life_insur_rsrv", "rcv_ceded_lt_health_insur_rsrv", "mrgn_paid",
             "insured_pledge_loan", "cap_mrgn_paid", "independent_acct_assets", "clients_cap_deposit",
             "clients_rsrv_settle", "incl_seat_fees_exchange", "rcv_invest", "tot_assets", "st_borrow",
             "borrow_central_bank", "deposit_received_ib_deposits", "loans_oth_banks", "tradable_fin_liab",
             "notes_payable", "acct_payable", "adv_from_cust", "fund_sales_fin_assets_rp",
             "handling_charges_comm_payable", "empl_ben_payable", "taxes_surcharges_payable", "int_payable",
             "dvd_payable", "other_payable", "acc_exp", "deferred_inc", "st_bonds_payable", "payable_to_reinsurer",
             "rsrv_insur_cont", "acting_trading_sec", "acting_uw_sec", "non_cur_liab_due_within_1y", "other_cur_liab",
             "tot_cur_liab", "lt_borrow", "bonds_payable", "lt_payable", "specific_item_payable", "provisions",
             "deferred_tax_liab", "deferred_inc_non_cur_liab", "other_non_cur_liab", "tot_non_cur_liab",
             "liab_dep_other_banks_inst", "derivative_fin_liab", "cust_bank_dep", "agency_bus_liab", "other_liab",
             "prem_received_adv", "deposit_received", "insured_deposit_invest", "unearned_prem_rsrv", "out_loss_rsrv",
             "life_insur_rsrv", "lt_health_insur_v", "independent_acct_liab", "incl_pledge_loan", "claims_payable",
             "dvd_payable_insured", "total_liab", "capital_stk", "capital_reser", "special_rsrv", "surplus_rsrv",
             "undistributed_profit", "less_tsy_stk", "prov_nom_risks", "cnvd_diff_foreign_curr_stat",
             "unconfirmed_invest_loss", "minority_int", "tot_shrhldr_eqy_excl_min_int", "tot_shrhldr_eqy_incl_min_int",
             "tot_liab_shrhldr_eqy", "spe_cur_assets_diff", "tot_cur_assets_diff", "spe_non_cur_assets_diff",
             "tot_non_cur_assets_diff", "spe_bal_assets_diff", "tot_bal_assets_diff", "spe_cur_liab_diff",
             "tot_cur_liab_diff", "spe_non_cur_liab_diff", "tot_non_cur_liab_diff", "spe_bal_liab_diff",
             "tot_bal_liab_diff", "spe_bal_shrhldr_eqy_diff", "tot_bal_shrhldr_eqy_diff", "spe_bal_liab_eqy_diff",
             "tot_bal_liab_eqy_diff", "lt_payroll_payable", "other_comp_income", "other_equity_tools",
             "other_equity_tools_p_shr", "lending_funds", "accounts_receivable", "st_financing_payable", "payables"}
        self.fin_stat_cash_flow = \
            {"cash_recp_sg_and_rs", "recp_tax_rends", "net_incr_dep_cob", "net_incr_loans_central_bank",
             "net_incr_fund_borr_ofi", "cash_recp_prem_orig_inco", "net_incr_insured_dep",
             "net_cash_received_reinsu_bus", "net_incr_disp_tfa", "net_incr_int_handling_chrg", "net_incr_disp_faas",
             "net_incr_loans_other_bank", "net_incr_repurch_bus_fund", "other_cash_recp_ral_oper_act",
             "stot_cash_inflows_oper_act", "cash_pay_goods_purch_serv_rec", "cash_pay_beh_empl", "pay_all_typ_tax",
             "net_incr_clients_loan_adv", "net_incr_dep_cbob", "cash_pay_claims_orig_inco", "handling_chrg_paid",
             "comm_insur_plcy_paid", "other_cash_pay_ral_oper_act", "stot_cash_outflows_oper_act",
             "net_cash_flows_oper_act", "cash_recp_disp_withdrwl_invest", "cash_recp_return_invest",
             "net_cash_recp_disp_fiolta", "net_cash_recp_disp_sobu", "other_cash_recp_ral_inv_act",
             "stot_cash_inflows_inv_act", "cash_pay_acq_const_fiolta", "cash_paid_invest", "net_cash_pay_aquis_sobu",
             "other_cash_pay_ral_inv_act", "net_incr_pledge_loan", "stot_cash_outflows_inv_act",
             "net_cash_flows_inv_act", "cash_recp_cap_contrib", "incl_cash_rec_saims", "cash_recp_borrow",
             "proc_issue_bonds", "other_cash_recp_ral_fnc_act", "stot_cash_inflows_fnc_act", "cash_prepay_amt_borr",
             "cash_pay_dist_dpcp_int_exp", "incl_dvd_profit_paid_sc_ms", "other_cash_pay_ral_fnc_act",
             "stot_cash_outflows_fnc_act", "net_cash_flows_fnc_act", "eff_fx_flu_cash", "net_incr_cash_cash_equ",
             "cash_cash_equ_beg_period", "cash_cash_equ_end_period", "net_profit", "unconfirmed_invest_loss",
             "plus_prov_depr_assets", "depr_fa_coga_dpba", "amort_intang_assets", "amort_lt_deferred_exp",
             "decr_deferred_exp", "incr_acc_exp", "loss_disp_fiolta", "loss_scr_fa", "loss_fv_chg", "fin_exp",
             "invest_loss", "decr_deferred_inc_tax_assets", "incr_deferred_inc_tax_liab", "decr_inventories",
             "decr_oper_payable", "incr_oper_payable", "others", "im_net_cash_flows_oper_act", "conv_debt_into_cap",
             "conv_corp_bonds_due_within_1y", "fa_fnc_leases", "end_bal_cash", "less_beg_bal_cash",
             "plus_end_bal_cash_equ", "less_beg_bal_cash_equ", "im_net_incr_cash_cash_equ", "free_cash_flow",
             "spe_bal_cash_inflows_oper", "tot_bal_cash_inflows_oper", "spe_bal_cash_outflows_oper",
             "tot_bal_cash_outflows_oper", "tot_bal_netcash_outflows_oper", "spe_bal_cash_inflows_inv",
             "tot_bal_cash_inflows_inv", "spe_bal_cash_outflows_inv", "tot_bal_cash_outflows_inv",
             "tot_bal_netcash_outflows_inv", "spe_bal_cash_inflows_fnc", "tot_bal_cash_inflows_fnc",
             "spe_bal_cash_outflows_fnc", "tot_bal_cash_outflows_fnc", "tot_bal_netcash_outflows_fnc",
             "spe_bal_netcash_inc", "tot_bal_netcash_inc", "spe_bal_netcash_equ_undir", "tot_bal_netcash_equ_undir",
             "spe_bal_netcash_inc_undir", "tot_bal_netcash_inc_undir"}
        # const
        self.ANN_DATE_FIELD_NAME = 'ann_date'
        self.REPORT_DATE_FIELD_NAME = 'report_date'
        self.TRADE_STATUS_FIELD_NAME = 'trade_status'
        self.TRADE_DATE_FIELD_NAME = 'trade_date'
    
    @staticmethod
    def _group_df_to_dict(df, by):
        gp = df.groupby(by=by)
        res = {key: value for key, value in gp}
        return res

    def _get_fields(self, field_type, fields, complement=False, append=False):
        """
        Get list of fields that are in ref_quarterly_fields.
        
        Parameters
        ----------
        field_type : {'market_daily', 'ref_daily', 'income', 'balance_sheet', 'cash_flow', 'daily', 'quarterly'
        fields : list of str
        complement : bool, optional
            If True, get fields that are NOT in ref_quarterly_fields.

        Returns
        -------
        list

        """
        if field_type == 'market_daily':
            pool = self.market_daily_fields
        elif field_type == 'ref_daily':
            pool = self.reference_daily_fields
        elif field_type == 'income':
            pool = self.fin_stat_income
        elif field_type == 'balance_sheet':
            pool = self.fin_stat_balance_sheet
        elif field_type == 'cash_flow':
            pool = self.fin_stat_cash_flow
        elif field_type == 'daily':
            pool = set.union(self.market_daily_fields, self.reference_daily_fields)
        elif field_type == 'quarterly':
            pool = set.union(self.fin_stat_income, self.fin_stat_balance_sheet, self.fin_stat_cash_flow)
        else:
            raise NotImplementedError("field_type = {:s}".format(field_type))
        
        s = set.intersection(set(pool), set(fields))
        if not s:
            return []
        
        if complement:
            s = set(fields) - s
        if append:
            if field_type == 'market_daily':
                s.add(self.TRADE_STATUS_FIELD_NAME)
            elif field_type == 'income' or field_type == 'balance_sheet' or field_type == 'cash_flow':
                s.add(self.ANN_DATE_FIELD_NAME)
                s.add(self.REPORT_DATE_FIELD_NAME)
        
        l = list(s)
        return l
    
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
        security_str = sep.join(security)
        
        dic_ref_daily = None
        dic_market_daily = None
        dic_balance = None
        dic_cf = None
        dic_income = None
        
        if self.freq == 1:
            # TODO : use fields = {field: kwargs} to enable params
            
            fields_market_daily = self._get_fields('market_daily', fields, append=True)  # TODO: not each time we want append = True
            if fields_market_daily:
                # TODO adjust mode
                df_daily, msg1 = self.data_api.daily(security_str, start_date=self.start_date, end_date=self.end_date,
                                                     adjust_mode='post', fields=sep.join(fields_market_daily))
                if msg1 != '0,':
                    print msg1
                dic_market_daily = self._group_df_to_dict(df_daily, 'security')

            fields_ref_daily = self._get_fields('ref_daily', fields)
            if fields_ref_daily:
                df_ref_daily, msg2 = self.data_api.query_wd_dailyindicator(security_str, self.start_date, self.end_date,
                                                                           sep.join(fields_ref_daily))
                if msg2 != '0,':
                    print msg2
                dic_ref_daily = self._group_df_to_dict(df_ref_daily, 'security')

            fields_income = self._get_fields('income', fields, append=True)
            if fields_income:
                df_income, msg3 = self.data_api.query_wd_fin_stat('income', security_str, self.extended_start_date, self.end_date,
                                                                  sep.join(fields_income))
                if msg3 != '0,':
                    print msg3
                dic_income = self._group_df_to_dict(df_income, 'security')

            fields_balance = self._get_fields('balance_sheet', fields, append=True)
            if fields_balance:
                df_balance, msg3 = self.data_api.query_wd_fin_stat('balance_sheet', security_str, self.extended_start_date, self.end_date,
                                                                   sep.join(fields_balance))
                if msg3 != '0,':
                    print msg3
                dic_balance = self._group_df_to_dict(df_balance, 'security')

            fields_cf = self._get_fields('cash_flow', fields, append=True)
            if fields_cf:
                df_cf, msg3 = self.data_api.query_wd_fin_stat('cash_flow', security_str, self.extended_start_date, self.end_date,
                                                              sep.join(fields_cf))
                if msg3 != '0,':
                    print msg3
                dic_cf = self._group_df_to_dict(df_cf, 'security')
        else:
            raise NotImplementedError("freq = {}".format(self.freq))
        
        return dic_market_daily, dic_ref_daily, dic_income, dic_balance, dic_cf
        
    @staticmethod
    def _process_index(df, index_name='trade_date'):
        """
        Drop duplicates, set and sort index.
        
        Parameters
        ----------
        df : pd.DataFrame
            index of df must be unique.
        index_name : str, optional
            label of column which will be used as index.

        Returns
        -------
        df : pd.DataFrame
            processed
        
        Notes
        -----
        We do not use inplace operations, which will be a lot slower

        """
        # df.drop_duplicates(subset=index_name, inplace=True)  # TODO not a good solution
        dup = df.duplicated(subset=index_name)
        if np.sum(dup.values) > 0:
            print "WARNING: Duplicate {:s} encountered, droped.".format(index_name)
            idx = np.logical_not(dup)
            df = df.loc[idx, :]
        
        df = df.set_index(index_name)
        df = df.sort_index(axis=0)

        if 'security' in df.columns:
            df = df.drop(['security'], axis=1)
        
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
            dic[sec] = self._process_index(df, self.TRADE_DATE_FIELD_NAME)
            
        res = self._dic_of_df_to_multi_index_df(dic, levels=['security', 'field'])
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
            df_mod = self._process_index(df, self.TRADE_DATE_FIELD_NAME)
            df_mod = df_mod.loc[:, self._get_fields('ref_daily', fields)]
            dic[sec] = df_mod
        
        res = self._dic_of_df_to_multi_index_df(dic, levels=['security', 'field'])
        return res

    def _preprocess_ref_quarterly(self, type_, dic, fields):
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
        new_dic = dict()
        for sec, df in dic.viewitems():
            df_mod = self._process_index(df, self.REPORT_DATE_FIELD_NAME)
            df_mod = df_mod.loc[:, self._get_fields(type_, fields, append=True)]
            
            new_dic[sec] = df_mod
    
        res = self._dic_of_df_to_multi_index_df(new_dic, levels=['security', 'field'])
        return res
    
    @staticmethod
    def _merge_data(dfs, index_name='trade_date'):
        """
        Merge data from different APIs into one DataFrame.
        
        Parameters
        ----------
        dfs : list of pd.DataFrame

        Returns
        -------
        merge : pd.DataFrame or None
            If dfs is empty, return None
        
        Notes
        -----
        Align on date index, concatenate on columns (security and fields)
        
        """
        dfs = [df for df in dfs if df is not None]
        if not dfs:
            return None
        
        merge = pd.concat(dfs, axis=1, join='outer')
        if merge.isnull().sum().sum() > 0:
            Warning("nan in final merged data.")
            merge.fillna(method='ffill', inplace=True)
            
        merge.sort_index(axis=1, level=['security', 'field'], inplace=True)
        merge.index.name = index_name
        
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

    def _validate_fields(self):
        """Check whether field_names in fields can be recognized."""
        for field_name in self.fields:
            flag = (field_name in self.market_daily_fields
                    or field_name in self.reference_daily_fields
                    or field_name in self.fin_stat_income
                    or field_name in self.fin_stat_balance_sheet
                    or field_name in self.fin_stat_cash_flow)
            if not flag:
                print "Field name {:s} not valid, ignore.".format(field_name)
    
    def _prepare_data(self, fields):
        # query data
        print "\nQuery data..."
        dic_market_daily, dic_ref_daily, dic_income, dic_balance_sheet, dic_cash_flow = \
            self._query_data(self.security, fields)
    
        # pre-process data
        print "\nPreprocess data..."
        multi_market_daily = self._preprocess_market_daily(dic_market_daily)
        multi_ref_daily = self._preprocess_ref_daily(dic_ref_daily, fields)
        multi_income = self._preprocess_ref_quarterly('income', dic_income, fields)
        multi_balance_sheet = self._preprocess_ref_quarterly('balance_sheet', dic_balance_sheet, fields)
        multi_cash_flow = self._preprocess_ref_quarterly('cash_flow', dic_cash_flow, fields)
    
        print "\nMerge data..."
        merge_d = self._merge_data([multi_market_daily, multi_ref_daily],
                                   index_name=self.TRADE_DATE_FIELD_NAME)
        merge_q = self._merge_data([multi_income, multi_balance_sheet, multi_cash_flow],
                                   index_name=self.REPORT_DATE_FIELD_NAME)
    
        # drop dates that are not trade date
        if merge_d is not None:
            trade_dates = self.data_api.get_trade_date(self.start_date, self.end_date, is_datetime=False)
            merge_d = merge_d.loc[trade_dates, pd.IndexSlice[:, :]].copy()
        
        return merge_d, merge_q
    
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
        
        # initialize parameters
        self.start_date = props['start_date']
        self.extended_start_date = JzCalendar.shift(self.start_date, n_weeks=-52)
        self.end_date = props['end_date']
        self.fields = props['fields'].split(sep)
        self.freq = props['freq']
        self.universe = props.get('universe', "")
        if self.universe:
            self.security = data_api.get_index_comp(self.universe, self.start_date, self.end_date)
        else:
            self.security = props['security'].split(sep)
        
        # check validity of fields
        self._validate_fields()
        
        self.data_d, self.data_q = self._prepare_data(self.fields)
        
        print "Data has been successfully prepared."

    def add_field_OLD(self, data_api, field_name):
        """
        Query and append new field.
        
        Parameters
        ----------
        data_api : BaseDataServer
        field_name : str

        """
        self.fields.append(field_name)
        self.data_api = data_api

        dic_market_daily, dic_ref_daily, dic_ref_quarterly = self._query_data(self.security, [field_name])
        if field_name in self.market_daily_fields:
            df_multi = self._preprocess_market_daily(dic_market_daily)
        elif field_name in self.reference_daily_fields:
            df_multi = self._preprocess_ref_daily(dic_ref_daily, [field_name])
        elif field_name in self.fin_stat_income:
            df_multi = self._preprocess_ref_quarterly(dic_ref_quarterly, self.fields)
        else:
            raise ValueError("field_name = {:s}".format(field_name))
        
        df_multi = df_multi.loc[:, pd.IndexSlice[:, field_name]]
        self.append_df(df_multi, field_name)  # whether contain only trade days is decided by existing data.

    def add_field(self, field_name, data_api=None):
        """
        Query and append new field.
        
        Parameters
        ----------
        data_api : BaseDataServer
        field_name : str
            Must be a known field name (which is given in documents).

        """
        self.fields.append(field_name)
        if data_api is None:
            if self.data_api is None:
                print "Add field failed. No data_api available. Please specify one in parameter."
        else:
            self.data_api = data_api
    
        merge_d, merge_q = self._prepare_data([field_name])
    
        merge = merge_d if merge_d is not None else merge_q
        merge = merge.loc[:, pd.IndexSlice[:, field_name]]
        self.append_df(merge, field_name)  # whether contain only trade days is decided by existing data.

    def add_formula(self, field_name, formula):
        """
        Add a new field, which is calculated using existing fields.
        
        Parameters
        ----------
        formula : str
            A formula contains operations and function calls.
        field_name : str
            A custom name for the new field.

        """
        if field_name in self.fields:
            print "Add formula failed: field name [{:s}] exist. Try another name.".format(field_name)
            return
        
        self.fields.append(field_name)
        
        parser = Parser()
        expr = parser.parse(formula)
        
        var_df_dic = dict()
        var_list = expr.variables()
        
        df_ann = self.get_ann_df()
        for var in var_list:
            if self._is_quarter_field(var):
                df_var = self.get_ts_quarter(var)
            else:
                df_var = self.get_ts(var)
            
            var_df_dic[var] = df_var
        
        # TODO: send ann_date into expr.evaluate. We assume that ann_date of all fields of a security is the same
        df_eval = parser.evaluate(var_df_dic, df_ann, self.dates)
        
        self.append_df(df_eval, field_name)
            
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
        for key in h5.keys():
            res[key] = h5.get(key)
            
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
        meta_data = quantos.util.fileio.read_json(os.path.join(folder, 'meta_data.json'))
        dic = self._load_h5(os.path.join(folder, 'data.hd5'))
        self.data_d = dic.get('/data_d', None)
        self.data_q = dic.get('/data_q', None)
        self.__dict__.update(meta_data)
        
        print "Dataview loaded successfully."

    @property
    def dates(self):
        """
        Get date array of the underlying data.
        
        Returns
        -------
        res : np.array
            dtype: int

        """
        if self.data_d is not None:
            res = self.data_d.index.values
        elif self.data_api is not None:
            res = self.data_api.get_trade_date(self.start_date, self.end_date, is_datetime=False)
        else:
            raise ValueError("Cannot get dates array when neither of data and data_api exists.")
            
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
        
        fields_others = self._get_fields('daily', fields)
        fields_ref_quarterly = self._get_fields('quarterly', fields)
        
        df_ref_expanded = None
        if fields_ref_quarterly:
            df_ref_quarterly = self.data_q.loc[:,
                                               pd.IndexSlice[security, fields_ref_quarterly]]
            df_ref_ann = self.data_q.loc[:,
                                         pd.IndexSlice[security, self.ANN_DATE_FIELD_NAME]]
            df_ref_ann.columns = df_ref_ann.columns.droplevel(level='field')
            
            dic_expanded = dict()
            for field_name, df in df_ref_quarterly.groupby(level=1, axis=1):  # by column multiindex fields
                df_expanded = align(df, df_ref_ann, self.dates)
                dic_expanded[field_name] = df_expanded
            # df_ref_expanded = self._dic_of_df_to_multi_index_df(dic_expanded, levels=['field', 'security'])
            df_ref_expanded = pd.concat(dic_expanded.values(), axis=1)
            # df_ref_expanded.index = JzCalendar.convert_int_to_datetime(df_ref_expanded.index)
            df_ref_expanded.index.name = self.TRADE_DATE_FIELD_NAME
            # df_ref_expanded = df_ref_expanded.swaplevel(axis=1)
        
        df_others = self.data_d.loc[pd.IndexSlice[start_date: end_date],
                                    pd.IndexSlice[security, fields_others]]
        
        df_merge = self._merge_data([df_others, df_ref_expanded], index_name=self.TRADE_DATE_FIELD_NAME)
        return df_merge
    
    def get_snapshot(self, snapshot_date, security="", fields=""):
        """
        Get snapshot of given fields and security at snapshot_date.
        
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
            security as index, field as columns

        """
        res = self.get(security=security, start_date=snapshot_date, end_date=snapshot_date, fields=fields)
        
        res = res.stack(level='security', dropna=False)
        res.index = res.index.droplevel(level=self.TRADE_DATE_FIELD_NAME)
        
        return res

    def get_ann_df(self):
        """
        Query announcement date of financial statements of all securities.
        
        Returns
        -------
        df_ann : pd.DataFrame or None
            Index is date, column is security.
            If no quarterly data available, return None.
        
        """
        if self.data_q is None:
            return None
        df_ann = self.data_q.loc[:,
                                     pd.IndexSlice[:, self.ANN_DATE_FIELD_NAME]]
        
        df_ann = df_ann.copy()
        df_ann.columns = df_ann.columns.droplevel(level='field')
    
        return df_ann
        
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
    
        df_ref_quarterly = self.data_q.loc[:,
                                           pd.IndexSlice[security, field]]
        df_ref_quarterly.columns = df_ref_quarterly.columns.droplevel(level='field')
        
        return df_ref_quarterly
    
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
            Index is int date, column is security.

        """
        res = self.get(security, start_date=start_date, end_date=end_date, fields=field)
        
        res.columns = res.columns.droplevel(level='field')
        
        return res

    def save_dataview(self, folder_path="."):
        """Save data and meta_data_to_store to a single hd5 file."""
        sub_folder = "{:d}_{:d}_freq={:d}D".format(self.start_date, self.end_date, self.freq)
        
        folder_path = os.path.join(folder_path, sub_folder)
        abs_folder = os.path.abspath(folder_path)
        meta_path = os.path.join(folder_path, 'meta_data.json')
        data_path = os.path.join(folder_path, 'data.hd5')
        
        data_to_store = {'data_d': self.data_d, 'data_q': self.data_q}
        data_to_store = {k: v for k, v in data_to_store.items() if v is not None}
        meta_data_to_store = {key: self.__dict__[key] for key in self.meta_data_list}

        print "\nStore data..."
        quantos.util.fileio.save_json(meta_data_to_store, meta_path)
        self._save_h5(data_path, data_to_store)
        
        print ("Dataview has been successfully saved to:\n"
               + abs_folder + "\n\n"
               + "You can load it with load_dataview('{:s}')".format(abs_folder))

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
        quantos.util.fileio.create_dir(fp)
        h5 = pd.HDFStore(fp)
        for key, value in dic.items():
            h5[key] = value
        h5.close()
    
    def append_df(self, df, field_name):
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
        
        is_quarterly = self._is_quarter_field(field_name)
        if is_quarterly:
            the_data = self.data_q
        else:
            the_data = self.data_d
            
        multi_idx = pd.MultiIndex.from_product([the_data.columns.levels[0], [field_name]])
        df.columns = multi_idx
        
        merge = the_data.join(df, how='left')  # left: keep index of existing data unchanged
        merge.sort_index(axis=1, level=['security', 'field'], inplace=True)

        if is_quarterly:
            self.data_q = merge
        else:
            self.data_d = merge
    
    def _is_quarter_field(self, field_name):
        res = (field_name in self.fin_stat_balance_sheet
               or field_name in self.fin_stat_cash_flow
               or field_name in self.fin_stat_income)
        return res
