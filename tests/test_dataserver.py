# encoding: UTF-8

from data.dataserver import JzDataServer, BaseDataServer


def test_jz_data_server_daily():
    ds = JzDataServer()
    
    # test daily
    res, msg = ds.daily('rb1710.SHF,600662.SH', fields="",
                        start_date=20170828, end_date=20170831,
                        adjust_mode=None)
    rb = res.loc[res.loc[:, 'security'] == 'rb1710.SHF', :]
    stk = res.loc[res.loc[:, 'security'] == '600662.SH', :]
    assert msg == '0,'
    assert rb.shape == (4, 13)
    assert rb.loc[:, 'volume'].values[0] == 189616
    assert stk.loc[:, 'volume'].values[0] == 7174813


def test_jz_data_server_bar():
    ds = JzDataServer()
    
    # test bar
    res2, msg2 = ds.bar('rb1710.SHF,600662.SH', start_time=200000, end_time=160000, trade_date=20170831, fields="")
    rb2 = res2.loc[res2.loc[:, 'security'] == 'rb1710.SHF', :]
    stk2 = res2.loc[res2.loc[:, 'security'] == '600662.SH', :]
    assert msg2 == '0,'
    assert rb2.shape == (345, 14)
    assert stk2.shape == (240, 14)
    assert rb2.loc[:, 'volume'].values[344] == 3366
    
    
def test_jz_data_server_wd():
    ds = JzDataServer()
    
    # test wd.secDailyIndicator
    res3, msg3 = ds.query("wd.secDailyIndicator", fields="pb,pe,share_float_free,net_assets,limit_status",
                          filter="security=600030.SH&start_date=20170907&end_date=20170907",
                          orderby="trade_date")
    assert msg3 == '0,'
    assert abs(res3.loc[0, 'pb'] - 1.5135) < 1e-4
    assert abs(res3.loc[0, 'share_float_free'] - 781496.5954) < 1e-4
    assert abs(res3.loc[0, 'net_assets'] - 1.437e11) < 1e8
    assert res3.loc[0, 'limit_status'] == 0
    
    res4, msg4 = ds.query("wd.income", fields="",
                          filter="security=600000.SH&start_date=20150101&end_date=20170101&statement_type=408002000",
                          order_by="ann_date")
    assert msg4 == '0,'
    assert res4.shape == (7, 10)
    assert res4.loc[4, 'oper_rev'] == 42191000000
