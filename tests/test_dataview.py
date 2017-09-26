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
    
    dv.prepare_data(props=props, data_api=ds, trade_date=False)
    assert dv.data.shape == (366, 24)
    dv.prepare_data(props=props, data_api=ds, trade_date=True)
    assert dv.data.shape == (242, 24)
    
    folder_path = '../output/prepared'
    dv.save_dataview(folder=folder_path)
    
    
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
    assert JzCalendar.convert_datetime_to_int(ts1.index.values[-1]) == 20170302


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
