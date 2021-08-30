# -*- coding: utf-8 -*-
# @Time    : 2021-08-02 10:50 a.m.
# @Author  : cindy
# @File    : chn_trad_day.py
# @Software: PyCharm

import chinese_calendar as cal
import datetime as dt


def is_trade_day(date):
    """
    判断一个日期是不是中国的交易日
    :param date: datetime
    :return: boolean
    """
    return (not cal.is_holiday(date)) & (not cal.is_in_lieu(date)) & (date.weekday() < 5)


def get_next_trading_day(date):
    """
    求出下一个交易日日期
    :param date: datetime
    :return: datetime
    """
    next_date = date + dt.timedelta(days=1)
    while not is_trade_day(next_date):
        next_date = next_date + dt.timedelta(days=1)
    return next_date