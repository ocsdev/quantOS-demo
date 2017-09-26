# encoding: UTF-8

from quantos.data import JzDataServer


def test_jz_data_server_daily():
    ds = JzDataServer()
    
    # test daily
    res, msg = ds.daily('rb1710.SHF,600662.SH', fields="",
                        start_date=20170828, end_date=20170831,
                        adjust_mode=None)
    assert msg == '0,'
    
    rb = res.loc[res.loc[:, 'security'] == 'rb1710.SHF', :]
    stk = res.loc[res.loc[:, 'security'] == '600662.SH', :]
    assert rb.shape == (4, 13)
    assert rb.loc[:, 'volume'].values[0] == 189616
    assert stk.loc[:, 'volume'].values[0] == 7174813


def test_jz_data_server_daily_quited():
    ds = JzDataServer()
    
    # test daily
    res, msg = ds.daily('600832.SH', fields="",
                        start_date=20140828, end_date=20170831,
                        adjust_mode=None)
    assert msg == '0,'
    assert res.shape == (175, 13)


def test_jz_data_server_bar():
    ds = JzDataServer()
    
    # test bar
    res2, msg2 = ds.bar('rb1710.SHF,600662.SH', start_time=200000, end_time=160000, trade_date=20170831, fields="")
    assert msg2 == '0,'
    
    rb2 = res2.loc[res2.loc[:, 'security'] == 'rb1710.SHF', :]
    stk2 = res2.loc[res2.loc[:, 'security'] == '600662.SH', :]
    assert rb2.shape == (345, 14)
    assert stk2.shape == (240, 14)
    assert rb2.loc[:, 'volume'].values[344] == 3366
    
    
def test_jz_data_server_wd():
    ds = JzDataServer()
    
    # test wd.secDailyIndicator
    fields = "pb,pe,share_float_free,net_assets,limit_status"
    for res3, msg3 in [ds.query("wd.secDailyIndicator", fields=fields,
                                filter="security=600030.SH&start_date=20170907&end_date=20170907",
                                orderby="trade_date"),
                       ds.query_wd_dailyindicator('600030.SH', 20170907, 20170907, fields)]:
        assert msg3 == '0,'
        assert abs(res3.loc[0, 'pb'] - 1.5135) < 1e-4
        assert abs(res3.loc[0, 'share_float_free'] - 781496.5954) < 1e-4
        assert abs(res3.loc[0, 'net_assets'] - 1.437e11) < 1e8
        assert res3.loc[0, 'limit_status'] == 0
    
    # test wd.income
    for res4, msg4 in [ds.query("wd.income", fields="",
                                filter="security=600000.SH&start_date=20150101&end_date=20170101&report_type=408002000",
                                order_by="report_date"),
                       ds.query_wd_fin_stat('income', '600000.SH', 20150101, 20170101, fields="")]:
        assert msg4 == '0,'
        assert res4.shape == (8, 12)
        print res4.loc[4, 'oper_rev'] == 37918000000
        assert res4.loc[4, 'oper_rev'] == 37918000000


def test_jz_data_server_daily_ind_performance():
    ds = JzDataServer()
    
    hs300 = ds.get_index_comp('000300.SH', 20140101, 20170101)
    hs300_str = ','.join(hs300)
    
    fields = "pb,pe,share_float_free,net_assets,limit_status"
    res, msg = ds.query("wd.secDailyIndicator", fields=fields,
                          filter=("security=" + hs300_str
                                  + "&start_date=20160907&end_date=20170907"),
                          orderby="trade_date")
    assert msg == '0,'
    
    print


def test_jz_data_server_components():
    ds = JzDataServer()
    res = ds.get_index_comp_df(index='000300.SH', start_date=20140101, end_date=20170505)
    assert res.shape == (814, 430)
    
    arr = ds.get_index_comp(index='000300.SH', start_date=20140101, end_date=20170505)
    assert len(arr) == 430


if __name__ == "__main__":
    test_jz_data_server_daily_ind_performance()
