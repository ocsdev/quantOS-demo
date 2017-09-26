# encoding: UTF-8

from data_api import DataApi


if __name__ == "__main__":
    api = DataApi("tcp://10.1.0.210:8910", use_jrpc=False)
    api.connect()

    daily, msg = api.daily(security="600030.SH,000002.SZ", begin_date=20170103, end_date=20170708,
                           fields="open,high,low,close,volume,last,trade_date")
    daily2, msg = api.daily(security="600030.SH", begin_date=20170103, end_date=20170708,
                           fields="open,high,low,close,volume,last,trade_date")
    print type(msg)
    print "msg = {}".format(msg)
    print type(daily), len(daily)
    print daily.columns, daily.shape

    df, msg = api.bar(security="600030.SH", trade_date=20170904, cycle='1m', begin_time=90000, end_time=150000)
    print df.columns, df.shape
