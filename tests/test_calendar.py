# encoding: utf-8

import datetime

from data.calendar import Calendar, Calendar_NEW


def _test_calendar_new():
    calendar = Calendar_NEW()
    res1 = calendar.get_trade_date_range(20121224, 20130201)
    res2 = calendar.get_next_trade_date(20170102)
    res3 = calendar.get_next_trade_date(20170104)
    print
    res11 = calendar.get_trade_date_range(20161224, 20170201)


def _test_calendar():
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


if __name__ == "__main__":
    _test_calendar_new()
