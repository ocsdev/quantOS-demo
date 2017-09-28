# encoding: UTF-8

from quantos.data.dataapi import DataApi


def test_data_api():
    api = DataApi("tcp://10.1.0.210:8910", use_jrpc=False)
    api.login('arbitrary_user', "123")
    
    daily, msg = api.daily(symbol="600030.SH,000002.SZ", start_date=20170103, end_date=20170708,
                           fields="open,high,low,close,volume,last,trade_date,settle")
    daily2, msg2 = api.daily(symbol="600030.SH", start_date=20170103, end_date=20170708,
                             fields="open,high,low,close,volume,last,trade_date,settle")
    # err_code, err_msg = msg.split(',')
    assert msg == '0,'
    assert msg2 == '0,'
    assert daily.shape == (248, 8)
    assert daily2.shape == (124, 8)
    
    df, msg = api.bar(symbol="600030.SH", trade_date=20170904, freq='1m', start_time=90000, end_time=150000)
    print df.columns
    assert df.shape == (240, 15)
    
    print "test passed"
    

if __name__ == "__main__":
    test_data_api()
