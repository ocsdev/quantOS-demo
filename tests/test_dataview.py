# encoding: utf-8

import sys
sys.path.append('/home/bliu/work/myproj/quantos/trunk')
from quantos.data.dataview import DataView


def test_xarray():
    import numpy as np
    import xarray as xr
    
    a = xr.DataArray(np.random.randn(2, 3))


def test_add_formula_directly():
    from quantos.data.dataservice import RemoteDataService
    
    ds = RemoteDataService()
    dv = DataView()
    
    secs = '600030.SH,000063.SZ,000001.SZ'
    props = {'start_date': 20160601, 'end_date': 20170601, 'symbol': secs,
             'fields': 'open,close',
             'freq': 1}
    dv.init_from_config(props, data_api=ds)
    dv.prepare_data()
    
    dv.add_formula("myfactor", 'close / open')
    assert dv.data_d.shape == (242, 33)
    

def test_write():
    from quantos.data.dataservice import RemoteDataService
    
    ds = RemoteDataService()
    dv = DataView()
    
    secs = '600030.SH,000063.SZ,000001.SZ'
    props = {'start_date': 20160601, 'end_date': 20170601, 'symbol': secs,
             'fields': 'open,close,high,low,volume,pb,net_assets,ncf',
             'freq': 1}

    dv.init_from_config(props, data_api=ds)
    dv.prepare_data()
    assert dv.data_d.shape == (242, 42)
    assert dv.dates.shape == (242, )
    # TODO
    """
    PerformanceWarning:
    your performance may suffer as PyTables will pickle object types that it cannot
    map directly to c-types [inferred_type->mixed,key->block1_values] [items->[('000001.SZ', 'int_income'), ('000001.SZ', 'less_handling_chrg_comm_exp'), ('000001.SZ', 'net_int_income'), ('000001.SZ', 'oper_exp'), ('000001.SZ', 'symbol'), ('000063.SZ', 'int_income'), ('000063.SZ', 'less_handling_chrg_comm_exp'), ('000063.SZ', 'net_int_income'), ('000063.SZ', 'oper_exp'), ('000063.SZ', 'symbol'), ('600030.SH', 'int_income'), ('600030.SH', 'less_handling_chrg_comm_exp'), ('600030.SH', 'net_int_income'), ('600030.SH', 'oper_exp'), ('600030.SH', 'symbol')]]
    """
    
    folder_path = '../output/prepared'
    dv.save_dataview(folder_path=folder_path)


def test_quarterly():
    from quantos.data.dataservice import RemoteDataService
    
    ds = RemoteDataService()
    dv = DataView()
    
    secs = '600030.SH,000063.SZ,000001.SZ'
    props = {'start_date': 20160609, 'end_date': 20170601, 'universe': '000300.SH', 'symbol': secs,
             'fields': ('open,close,'
                        + 'pb,net_assets,'
                        + 'total_oper_rev,oper_exp,'
                        + 'cash_paid_invest,'
                        + 'capital_stk,'
                        + 'roe'), 'freq': 1}

    dv.init_from_config(props, data_api=ds)
    dv.prepare_data()
    folder_path = '../output/prepared'
    dv.save_dataview(folder_path=folder_path)
    
    
def test_get_quarterly():
    dv = DataView()
    folder_path = '../output/prepared/20160609_20170601_freq=1D'
    dv.load_dataview(folder=folder_path)
    res = dv.get("", 0, 0, 'total_oper_rev')


def test_add_field_quarterly():
    dv = DataView()
    folder_path = '../output/prepared/20160609_20170601_freq=1D'
    dv.load_dataview(folder=folder_path)
    nrows, ncols = dv.data_q.shape
    n_securities = len(dv.data_d.columns.levels[0])
    
    from quantos.data.dataservice import RemoteDataService
    ds = RemoteDataService()
    dv.add_field('net_inc_other_ops', ds)
    """
    dv.add_field('oper_rev', ds)
    dv.add_field('turnover', ds)
    """
    assert dv.data_q.shape == (nrows, ncols + 1 * n_securities)
    
    
def test_add_formula_quarterly():
    dv = DataView()
    folder_path = '../output/prepared/20160609_20170601_freq=1D'
    dv.load_dataview(folder=folder_path)
    nrows, ncols = dv.data_d.shape
    n_securities = len(dv.data_d.columns.levels[0])
    
    formula = 'total_oper_rev / close'
    dv.add_formula('myvar1', formula)
    df1 = dv.get_ts('myvar1')
    assert df1.shape[0] == len(dv.dates)
    
    formula2 = 'Delta(oper_exp * myvar1 - open, 3)'
    dv.add_formula('myvar2', formula2)
    df2 = dv.get_ts('myvar2')
    assert df2.shape[0] == len(dv.dates)


def test_load():
    dv = DataView()
    folder_path = '../output/prepared/20160601_20170601_freq=1D'
    dv.load_dataview(folder=folder_path)
    
    assert dv.start_date == 20160601 and set(dv.symbol) == set('000001.SZ,600030.SH,000063.SZ'.split(','))

    # test get_snapshot
    snap1 = dv.get_snapshot(20170504, symbol='600030.SH,000063.SZ', fields='close,pb')
    assert snap1.shape == (2, 2)
    assert set(snap1.columns.values) == {'close', 'pb'}
    assert set(snap1.index.values) == {'600030.SH', '000063.SZ'}
    
    # test get_ts
    ts1 = dv.get_ts('close', symbol='600030.SH,000063.SZ', start_date=20170101, end_date=20170302)
    assert ts1.shape == (38, 2)
    assert set(ts1.columns.values) == {'600030.SH', '000063.SZ'}
    assert ts1.index.values[-1] == 20170302


def test_add_field():
    dv = DataView()
    folder_path = '../output/prepared/20160601_20170601_freq=1D'
    dv.load_dataview(folder=folder_path)
    nrows, ncols = dv.data_d.shape
    n_securities = len(dv.data_d.columns.levels[0])
    
    from quantos.data.dataservice import RemoteDataService
    ds = RemoteDataService()
    dv.add_field('share_amount', ds)
    assert dv.data_d.shape == (nrows, ncols + 1 * n_securities)


def test_add_formula():
    dv = DataView()
    folder_path = '../output/prepared/20160601_20170601_freq=1D'
    dv.load_dataview(folder=folder_path)
    nrows, ncols = dv.data_d.shape
    n_securities = len(dv.data_d.columns.levels[0])
    
    formula = 'Delta(high - close, 1)'
    dv.add_formula('myvar1', formula)
    assert dv.data_d.shape == (nrows, ncols + 1 * n_securities)
    
    formula2 = 'myvar1 - close'
    dv.add_formula('myvar2', formula2)
    assert dv.data_d.shape == (nrows, ncols + 2 * n_securities)


if __name__ == "__main__":
    # test_write()
    # test_read()
    # test_xarray()
    # test_add_field()
    # test_add_formula()
    # test_add_formula_directly()
    test_quarterly()
    # test_add_field_quarterly()
    # test_add_formula_quarterly()