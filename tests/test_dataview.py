# encoding: utf-8

from data.dataview import BaseDataView


def test_xarray():
    import numpy as np
    import xarray as xr
    
    a = xr.DataArray(np.random.randn(2, 3))
    
    
def test_dv_write():
    from data.dataserver import JzDataServer
    
    ds = JzDataServer()
    dv = BaseDataView()
    
    secs = '600030.SH,000063.SZ,000001.SZ'
    props = {'start_date': 20160601, 'end_date': 20170601, 'security': secs,
             'fields': 'open,close,high,low,volume,pb,net_assets,ncf', 'freq': 1}
    
    dv.prepare_data(props=props, data_api=ds)
    assert dv.data.shape == (242, 27)
    # TODO
    """
    PerformanceWarning:
    your performance may suffer as PyTables will pickle object types that it cannot
    map directly to c-types [inferred_type->mixed,key->block1_values] [items->[('000001.SZ', 'int_income'), ('000001.SZ', 'less_handling_chrg_comm_exp'), ('000001.SZ', 'net_int_income'), ('000001.SZ', 'oper_exp'), ('000001.SZ', 'security'), ('000063.SZ', 'int_income'), ('000063.SZ', 'less_handling_chrg_comm_exp'), ('000063.SZ', 'net_int_income'), ('000063.SZ', 'oper_exp'), ('000063.SZ', 'security'), ('600030.SH', 'int_income'), ('600030.SH', 'less_handling_chrg_comm_exp'), ('600030.SH', 'net_int_income'), ('600030.SH', 'oper_exp'), ('600030.SH', 'security')]]
    """
    
    folder_path = '../output/prepared'
    dv.save_dataview(folder=folder_path)


def test_dv_quarterly():
    from data.dataserver import JzDataServer
    
    ds = JzDataServer()
    dv = BaseDataView()
    
    secs = '600030.SH,000063.SZ,000001.SZ'
    props = {'start_date': 20170309, 'end_date': 20170601, 'universe': '000300.SH', 'security': secs,
             'fields': 'open,close,pb,net_assets,total_oper_rev,total_oper_exp', 'freq': 1}
    
    dv.prepare_data(props=props, data_api=ds)
    folder_path = '../output/prepared'
    dv.save_dataview(folder=folder_path)
    
    res = dv.get("", 0, 0, 'total_oper_rev')


def test_add_field_quarterly():
    dv = BaseDataView()
    folder_path = '../output/prepared/20160609_20170601_freq=1D'
    dv.load_dataview(folder=folder_path)
    nrows, ncols = dv.data_q.shape
    n_securities = len(dv.data.columns.levels[0])
    
    from data.dataserver import JzDataServer
    ds = JzDataServer()
    dv.add_field(ds, 'net_inc_other_ops')
    assert dv.data_q.shape == (nrows, ncols + 1 * n_securities)
    
    
def test_add_formula_quarterly():
    dv = BaseDataView()
    folder_path = '../output/prepared/20160609_20170601_freq=1D'
    dv.load_dataview(folder=folder_path)
    nrows, ncols = dv.data.shape
    n_securities = len(dv.data.columns.levels[0])
    
    formula = 'total_oper_rev / close'
    dv.add_formula(formula, 'myvar1')
    df1 = dv.get_ts('myvar1')
    assert df1.shape[0] == len(dv.dates)
    
    formula2 = 'Delta(total_oper_exp * myvar1 - open, 3)'
    dv.add_formula(formula2, 'myvar2')
    df2 = dv.get_ts('myvar2')
    assert df2.shape[0] == len(dv.dates)


def test_dv_read():
    dv = BaseDataView()
    folder_path = '../output/prepared/20160601_20170601_freq=1D'
    dv.load_dataview(folder=folder_path)
    
    assert dv.start_date == 20160601 and set(dv.security) == set('000001.SZ,600030.SH,000063.SZ'.split(','))

    # test get_snapshot
    snap1 = dv.get_snapshot(20170504, security='600030.SH,000063.SZ', fields='close,pb')
    assert snap1.shape == (2, 2)
    assert set(snap1.columns.values) == {'close', 'pb'}
    assert set(snap1.index.values) == {'600030.SH', '000063.SZ'}
    
    # test get_ts
    ts1 = dv.get_ts('close', security='600030.SH,000063.SZ', start_date=20170101, end_date=20170302)
    assert ts1.shape == (38, 2)
    assert set(ts1.columns.values) == {'600030.SH', '000063.SZ'}
    from framework.jzcalendar import JzCalendar
    assert ts1.index.values[-1] == 20170302


def test_add_field():
    dv = BaseDataView()
    folder_path = '../output/prepared/20160601_20170601_freq=1D'
    dv.load_dataview(folder=folder_path)
    nrows, ncols = dv.data.shape
    n_securities = len(dv.data.columns.levels[0])
    
    from data.dataserver import JzDataServer
    ds = JzDataServer()
    dv.add_field(ds, 'share_amount')
    assert dv.data.shape == (nrows, ncols + 1 * n_securities)


def test_add_formula():
    dv = BaseDataView()
    folder_path = '../output/prepared/20160601_20170601_freq=1D'
    dv.load_dataview(folder=folder_path)
    nrows, ncols = dv.data.shape
    n_securities = len(dv.data.columns.levels[0])
    
    formula = 'Delta(high - close, 1)'
    dv.add_formula(formula, 'myvar1')
    assert dv.data.shape == (nrows, ncols + 1 * n_securities)
    
    formula2 = 'myvar1 - close'
    dv.add_formula(formula2, 'myvar2')
    assert dv.data.shape == (nrows, ncols + 2 * n_securities)


if __name__ == "__main__":
    test_dv_write()
    test_dv_read()
    # test_xarray()
    test_add_field()
    # test_add_formula()
    # test_dv_quarterly()
    # test_add_field_quarterly()
    # test_add_formula_quarterly()