# encoding: utf-8

import datetime

from quantos.backtest.calendar import Calendar


def test_jzcalendar():
    calendar = Calendar()
    date = 20170808
    # print calendar.get_last_trade_date(date)
    # print calendar.get_next_trade_date(date)
    # print calendar.get_trade_date_range(20170701, 20170723)
    assert calendar.get_next_period_day(20170831, 'day', 1) == 20170904
    assert calendar.get_next_period_day(20170831, 'week', 1) == 20170905
    assert calendar.get_next_period_day(20170831, 'month', 0) == 20170901
    
    monthly = 20170101
    while monthly < 20180301:
        monthly = calendar.get_next_period_day(monthly, 'month', 0)
        assert datetime.datetime.strptime(str(monthly), "%Y%m%d").weekday() < 5
    
