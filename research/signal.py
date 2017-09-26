# encoding: utf-8

import numpy as np

from data.dataserver import JzDataServer
from data.dataview import BaseDataView


def save_dataview_new():
    # total 130 seconds
    
    ds = JzDataServer()
    dv = BaseDataView()
    
    """
    props = {'start_date': 20140609, 'end_date': 20160609, 'universe': '000300.SH',
             'fields': ('open,high,low,close,vwap,volume,turnover,'
                        + 'pb,net_assets,'
                        + 'total_oper_rev,total_oper_exp,total_profit,int_income'),
             'freq': 1}
    """
    props = {'start_date': 20170109, 'end_date': 20170909, 'universe': '000300.SH',
             'fields': ('open,high,low,close,vwap,volume,turnover,'
                        # + 'pb,net_assets,'
                        + 'total_oper_rev,oper_exp,tot_profit,int_income'),
             'freq': 1}
    
    dv.prepare_data(props=props, data_api=ds)
    dv.save_dataview(folder='../output/prepared')


def main():
    dv = BaseDataView()
    # dv.load_dataview(folder='../output/prepared/20140609_20160609_freq=1D')
    dv.load_dataview(folder='../output/prepared/20160109_20170909_freq=1D')
    print dv.fields

    vwap_formula = 'turnover / volume'
    factor_name = 'myvwap'
    dv.add_formula(vwap_formula, factor_name)
    
    factor_formula = '-1 * Rank(Ts_Max(Delta(myvwap, 7), 11))'  # GTJA
    # factor_formula = '-Delta(close, 5) / close'#  / pb'  # revert
    # factor_formula = 'Delta(tot_profit, 1) / Delay(tot_profit, 1)' # pct change
    factor_name = 'factor1'
    dv.add_formula(factor_formula, factor_name)
    
    factor = dv.get_ts(factor_name)
    trade_status = dv.get_ts('trade_status')
    close = dv.get_ts('vwap')
    
    mask_sus = trade_status != u'交易'.encode('utf-8')

    factor_data = alphalens.utils.get_clean_factor_and_forward_returns(factor, close, mask_sus=mask_sus, periods=[5, 10])
    """
    For check validity of data (avoid look ahead bias).
    import pandas as pd
    import datetime
    start = np.datetime64(datetime.date(2016, 7, 05))
    end = np.datetime64(datetime.date(2016, 7, 30))
    df_tmp = factor_data.loc[pd.IndexSlice[start: end, '600000.SH'], :]
    """
    alphalens.tears.create_full_tear_sheet(factor_data, output_format='pdf')


def test_align():
    # u'停牌'   method 2: use turnover == 0
    # trade_status.apply(lambda s: s.decode('utf-8'), inplace=True)
    # n_days, n_securities = trade_status.shape
    from data.dataserver import JzDataServer
    
    ds = JzDataServer()
    dv = BaseDataView()
    
    # TODO: start_date should be extended for some length
    secs = '600030.SH,000063.SZ,000001.SZ'
    props = {'start_date': 20150701, 'end_date': 20160701, 'security': secs,
             'fields': 'open,share_float,oper_exp,oper_rev', 'freq': 1}
    
    dv.prepare_data(props=props, data_api=ds)
    
    folder_path = '../output/prepared'
    
    dv.save_dataview(folder=folder_path)
    
    
if __name__ == "__main__":
    from util.profile import SimpleTimer
    timer = SimpleTimer()
    timer.tick('start')
    
    import alphalens
    timer.tick('import alphalens')
    # save_dataview_new()
    main()
    
    timer.tick('end')
