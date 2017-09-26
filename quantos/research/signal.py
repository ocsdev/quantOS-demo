# encoding: utf-8

import numpy as np
import pandas as pd

from quantos.data.dataview import BaseDataView
from quantos.data.dataserver import JzDataServer
from quantos.research import alphalens


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
    props = {'start_date': 20140108, 'end_date': 20170108, 'universe': '000300.SH',
             'fields': ('open,high,low,close,vwap,volume,turnover,'
                        # + 'pb,net_assets,'
                        + 'total_oper_rev,oper_exp,tot_profit,int_income'),
             'freq': 1}
    
    dv.prepare_data(props=props, data_api=ds)
    dv.save_dataview(folder_path='../output/prepared')


def main():
    dv = BaseDataView()
    # dv.load_dataview(folder='../output/prepared/20140609_20160609_freq=1D')
    import os
    fullpath = os.path.abspath('../../output/prepared/20160609_20170601_freq=1D')
    print fullpath
    dv.load_dataview(folder=fullpath)
    print dv.fields

    # vwap_formula = 'turnover / volume'
    # factor_name = 'myvwap'
    # dv.add_formula(factor_name, vwap_formula)
    
    # factor_formula = '-1 * Rank(Ts_Max(Delta(myvwap, 7), 11))'  # GTJA
    factor_formula = '-Delta(close, 5) / close'#  / pb'  # revert
    # factor_formula = 'Delta(tot_profit, 1) / Delay(tot_profit, 1)' # pct change
    factor_name = 'factor1'
    dv.add_formula(factor_name, factor_formula)
    
    factor = dv.get_ts(factor_name)
    trade_status = dv.get_ts('trade_status')
    close = dv.get_ts('close')
    
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


def _test_align():
    # u'停牌'   method 2: use turnover == 0
    # trade_status.apply(lambda s: s.decode('utf-8'), inplace=True)
    # n_days, n_securities = trade_status.shape
    from quantos.data.dataserver import JzDataServer
    
    ds = JzDataServer()
    dv = BaseDataView()
    
    # TODO: start_date should be extended for some length
    secs = '600030.SH,000063.SZ,000001.SZ'
    props = {'start_date': 20150701, 'end_date': 20160701, 'security': secs,
             'fields': 'open,share_float,oper_exp,oper_rev', 'freq': 1}
    
    dv.prepare_data(props=props, data_api=ds)
    
    folder_path = '../output/prepared'
    
    dv.save_dataview(folder_path=folder_path)
    
    
def _test_append_custom_data():
    # --------------------------------------------------------------------------------
    # get custom data
    ds = JzDataServer()
    # lb.blablabla
    df_raw, msg = ds.api.query("wd.secRestricted",
                                  fields="security,list_date,lifted_shares,lifted_ratio",
                                  filter="start_date=20170325&end_date=20170525",
                                  orderby="",
                                  data_format='pandas')
    assert msg == '0,'
    gp = df_raw.groupby(by=['list_date', 'security'])
    df_multi = gp.agg({'lifted_ratio': np.sum})
    
    df_value = df_multi.unstack(level=1)
    df_value.columns = df_value.columns.droplevel(level=0)
    
    # df_value = df_value.fillna(0.0)
    
    # --------------------------------------------------------------------------------
    # Format df_custom

    dv = BaseDataView()
    dv.load_dataview('../output/prepared/20160609_20170601_freq=1D')
    
    df_value = df_value.loc[:, dv.security]
    df_custom = pd.DataFrame(index=dv.dates, columns=dv.security, data=None)
    df_custom.loc[df_value.index, df_value.columns] = df_value
    df_custom.fillna(0.0, inplace=True)
    
    # --------------------------------------------------------------------------------
    # append DataFrame to existed DataView
    dv.append_df(df_custom + 1e-3 * np.random.rand(df_custom.shape[1]), field_name='custom')
    dv.add_formula('myfactor', 'Rank(custom)')
    
    # --------------------------------------------------------------------------------
    # test this factor
    factor = dv.get_ts('myfactor')
    trade_status = dv.get_ts('trade_status')
    close = dv.get_ts('close')

    mask_sus = trade_status != u'交易'.encode('utf-8')

    factor_data = alphalens.utils.get_clean_factor_and_forward_returns(factor, close, mask_sus=mask_sus, periods=[5])

    alphalens.tears.create_full_tear_sheet(factor_data, output_format='pdf')
    
    
if __name__ == "__main__":
    from quantos.util.profile import SimpleTimer
    timer = SimpleTimer()
    timer.tick('start')

    timer.tick('import alphalens')
    # save_dataview_new()
    main()
    # test_append_custom_data()
    
    timer.tick('end')
