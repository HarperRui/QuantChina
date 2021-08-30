# -*- coding: utf-8 -*-
# @Time    : 2021/8/4 11:47
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : daily_gamma_signal.py
# @Software: PyCharm

import numpy as np
import pandas as pd
import math
import datetime
from zltsql import SQLConn
from daily_iv import get_daily_iv
from HV_Percentile import get_percentile_data
from Percentile_classification import classification

#%%
def get_daily_percentile(date, code, vol_type, df_rf, df_stock, df_contract, df_hv, df_vix, eliminate_extremum=True,
                         ref_hv='HV20'):
    """
    获取当天IV和HV的分位数
    :param date: 交易日期
    :param code: 标的代码
    :param vol_type: 'IV' or 'VIX'
    :param df_rf: 含有无风险利率的DataFrame
    :param df_stock: 含有标的收盘价的DataFrame
    :param df_contract: 含有期权信息的DataFrame
    :param df_hv: 含有HV value的DataFrame
    :param df_vix: 含有VIX的DataFrame
    :param eliminate_extremum: 在计算分位点时是否剔除极值，默认剔除
    :param ref_hv: 计算iv分位数时的对应HV，默认为HV20
    :return: DataFrame
        iv  HV5  HV10  HV20  HV40  HV60
    0   80  70   70    60    90    90
    """

    # step1: get daily volatility
    if vol_type.upper() == 'IV':
        iv = get_daily_iv(code[0:6], date, df_rf, df_contract, df_stock, expire_threshold=10) * 100
    elif vol_type.upper() == 'VIX':
        iv = df_vix.loc[df_vix['日期'] == date, 'iVIX'].values[0]
    else:
        print('Wrong volatility type')
        return

    # step2: get HV percentiles
    percentile_lst = list(np.arange(0, 110, 10))
    result_hv_percentile = pd.DataFrame()
    for kind in ['HV5', 'HV10', 'HV20', 'HV40', 'HV60']:
        temp = get_percentile_data(df_hv.loc[(df_hv['Date'] <= date), kind].copy().dropna(), percentile_lst,
                                   eliminate_extremum=eliminate_extremum)
        for i in range(len(percentile_lst)):
            result_hv_percentile.at[0, '%s_%i' % (kind, percentile_lst[i])] = temp[i]

    # step3: get percentile classification
    df_percentile = pd.DataFrame()
    for i in ['iv', 'HV5', 'HV10', 'HV20', 'HV40', 'HV60']:
        if i == 'iv':
            colname = [ref_hv + '_' + str(x) for x in percentile_lst]
            pct_values = result_hv_percentile.loc[0, colname]
            raw_values = pd.concat([pd.Series({'IV': iv}), pct_values])
        else:
            colname = [i + '_' + str(x) for x in percentile_lst]
            hv_value = df_hv.loc[df_hv['Date'] == date, i].values[0]
            pct_values = result_hv_percentile.loc[0, colname]
            raw_values = pd.concat([pd.Series({'HV': hv_value}), pct_values])
        df_percentile.at[0, i] = int(classification(raw_values))

    return df_percentile


def get_large_move_signal(date, code, vol_type, df_index, df_rf, df_stock, df_contract, df_vix):
    """
    判断当天市场的成交量、IV和标的价格有没有发生大幅改变
    :param date: 交易日期
    :param code: 标的代码
    :param vol_type: 'IV' or 'VIX'
    :param df_index: 含有指数行情数据的DataFrame
    :param df_rf: 含有无风险利率的DataFrame
    :param df_stock: 含有标的收盘价的DataFrame
    :param df_contract: 含有期权信息的DataFrame
    :param df_vix: 含有VIX的DataFrame
    :return: 0~3之间的整数，表示三个指标中总共有几个发生大幅改变
    """

    today_idx = df_vix[df_vix['日期'] == date].index[0]

    if today_idx == 0:
        # 期权上市的第一天
        return 0

    date = df_vix.at[today_idx, '日期']
    pre_date = df_vix.at[today_idx-1, '日期']  # 上一个交易日

    if vol_type.upper() == 'IV':
        iv = get_daily_iv(code[0:6], date, df_rf, df_contract, df_stock, expire_threshold=10) * 100
        pre_iv = get_daily_iv(code[0:6], pre_date, df_rf, df_contract, df_stock, expire_threshold=10) * 100
    elif vol_type.upper() == 'VIX':
        iv = df_vix.at[today_idx, 'iVIX']
        pre_iv = df_vix.at[today_idx-1, 'iVIX']
    else:
        print('Wrong volatility type')
        return

    volume = df_index.loc[df_index['TRADE_DT'] == date, 'S_DQ_VOLUME'].values[0]
    pre_volume = df_index.loc[df_index['TRADE_DT'] == pre_date, 'S_DQ_VOLUME'].values[0]
    close = df_index.loc[df_index['TRADE_DT'] == date, 'S_DQ_CLOSE'].values[0]
    pre_close = df_index.loc[df_index['TRADE_DT'] == pre_date, 'S_DQ_CLOSE'].values[0]

    threshold_volume_weekday = 0.1
    threshold_volume_weekend = 0.15
    threshold_iv = 0.0
    days = (date - pre_date).days  # 距离上一个交易日的天数
    weekday = date.weekday()  # 当前是星期几
    if (days > 3) or ((1 <= weekday <= 4) & days > 1):
        # 当天为节假日后的第一个交易日
        threshold_volume = threshold_volume_weekend
    elif days > 2:
        # 当天为周一
        threshold_volume = threshold_volume_weekend
    else:
        threshold_volume = threshold_volume_weekday

    # 成交量大幅变动
    volume_change = 1 if volume / pre_volume - 1 > threshold_volume else 0

    # IV大幅变动
    iv_change = 1 if iv / pre_iv - 1 > threshold_iv else 0

    # 标的大幅变动
    stock_change = 1 if abs(close / pre_close - 1) > pre_iv / (100 * math.sqrt(365)) else 0

    return volume_change + iv_change + stock_change


def get_gamma_signal(date, code, vol_type, df_rf, df_stock, df_contract, df_hv, df_vix, df_index,
                     bound_low=30, bound_mid=60, HVtype_short='HV5', HVtype_long='HV20',
                     eliminate_extremum=True, smooth=True, prev_position=None, get_iv_position=False):
    """
    gamma signal
    :param date: 日期 datetime
    :param code: 标的代码
    :param vol_type: 'IV' or 'VIX'
    :param df_rf: 含有无风险收益率的DataFrame
    :param df_stock: 含有标的资产收盘价的DataFrame
    :param df_contract: 含有期权合约信息的DataFrame
    :param df_hv: 含有HV values的DataFrame
    :param df_vix: 含有vix的DataFrame
    :param df_index: 含有标的资产指数行情的DataFrame
    :param bound_low: 低档位IV范围的最大值, 默认30
    :param bound_mid: 中档位IV范围的最大值, 默认60
    :param HVtype_short: 短期HV，默认HV5
    :param HVtype_long: 长期HV，默认HV20
    :param eliminate_extremum: 在计算HV分位数时是否剔除极值，默认剔除
    :param smooth: 是否做平滑处理 (相对前一天最多变动0.5)
    :param prev_position: 前一天的信号
    :param get_iv_position: 是否获取当天IV处于的高、中、低状态
    :return: gamma signal 和当天IV所处的状态(high, mid, low)(if get_iv_position is True)
    """

    df_percentile = get_daily_percentile(date, code, vol_type, df_rf, df_stock, df_contract, df_hv, df_vix,
                                         eliminate_extremum=eliminate_extremum)

    iv = df_percentile.at[0, 'iv']
    hv_short = df_percentile.at[0, HVtype_short]

    # 按照位置将iv分类至高中低三挡
    if iv <= bound_low:
        iv_position = 'low'
    elif iv <= bound_mid:
        iv_position = 'mid'
    else:
        iv_position = 'high'

    # 判断短期hv是否高于长期hv
    # 对于hv_short = hv_long的情况，暂时的处理方法是用前一天两者的大小关系替代，如果前一天仍相等则用前前一天，以此类推
    idx = df_vix[df_vix['日期'] == date].index[0]
    short_above_long = None
    while idx >= 0:
        new_date = df_vix.at[idx, '日期']
        df_percentile = get_daily_percentile(new_date, code, vol_type, df_rf, df_stock, df_contract, df_hv, df_vix,
                                             eliminate_extremum=eliminate_extremum)
        new_hv_short = df_percentile.at[0, HVtype_short]
        new_hv_long = df_percentile.at[0, HVtype_long]
        if new_hv_short > new_hv_long:
            short_above_long = True
            break
        elif new_hv_short < new_hv_long:
            short_above_long = False
            break
        else:
            idx -= 1
    # 如果从今天开始一直到测试的第一天，hv_short都等于hv_long，则返回通常的仓位：-0.5
    if short_above_long is None:
        signal = -0.5
        if get_iv_position:
            return signal, iv_position
        else:
            return signal

    # 判断iv是否高于短期hv
    # 对于iv = hv_short的情况，暂时视作iv > hv_short
    iv_above_hv = True if iv >= hv_short else False

    if iv_above_hv:
        if short_above_long:
            signal = -0.35 if iv_position == 'high' else -0.5 if iv_position == 'mid' else -0.35
        else:
            signal = -0.7 if iv_position == 'high' else -1 if iv_position == 'mid' else -0.7
    else:
        if short_above_long:
            signal = 0.175 if iv_position == 'high' else 0.175 if iv_position == 'mid' else 0.175
        else:
            signal = -0.175 if iv_position == 'high' else -0.25 if iv_position == 'mid' else -0.175

    if prev_position is None:
        # 如果今天是第一天，无需平滑处理
        if get_iv_position:
            return signal, iv_position
        else:
            return signal

    # 如果今天的gamma相比昨天的向正方向变动，但是成交量、IV、标的都没有提示发生大幅变动，则今天的gamma敞口不变
    # 如果今天的gamma相比昨天的向负方向变动，但是成交量、IV、标的有两个及以上提示发生大幅变动，则今天的gamma敞口不变
    large_change = get_large_move_signal(date, code, vol_type, df_index, df_rf, df_stock, df_contract, df_vix)
    if (signal > prev_position) & (large_change == 0):
        signal = prev_position
    elif (signal < prev_position) & (large_change >= 2):
        signal = prev_position

    # 如果今天是周四或周五，信号相比昨天向负方向变动，则gamma敞口不变
    # 如果今天是周一或周二，信号相比昨天向正方向变动，则gamma敞口不变 （暂时不用）
    if (date.weekday() in [3, 4]) & (signal < prev_position):
        signal = prev_position
    # elif (date.weekday() in [0, 1]) & (signal > prev_position):
    #     signal = prev_position

    if smooth:
        if abs(signal - prev_position) <= 0.5:
            if get_iv_position:
                return signal, iv_position
            else:
                return signal
        else:
            new_signal = prev_position + 0.5 if signal > prev_position else prev_position - 0.5
            if get_iv_position:
                return new_signal, iv_position
            else:
                return new_signal
    else:
        if get_iv_position:
            return signal, iv_position
        else:
            return signal


#%%
if __name__ == '__main__':

    stock_code = '510050.SH'

    SQ = SQLConn()
    df_HVpercentile = SQ.GetData('HV_percentile')

    df_contract_all = SQ.GetData('contract_info_daily')
    df_contract_all = df_contract_all[df_contract_all['交易代码'].apply(lambda x: x.find('M') >= 0)]
    df_contract = df_contract_all[df_contract_all['期权标的'] == stock_code].reset_index(drop=True)  # 保留50或300的期权信息

    df_rf = SQ.GetData('rf')  # 无风险利率

    df_stock = SQ.GetData('etf_50')

    df_vix = SQ.GetData('df_vol_50etf')

    SQ.CloseSql()

    df_index = '这里写从数据库取50指数的行情数据 :p'
    # 测试单日的函数调用
    get_gamma_signal(date=datetime.datetime(2021, 4, 1, 0, 0), code=stock_code, vol_type='VIX', df_rf=df_rf,
                     df_stock=df_stock, df_contract=df_contract, df_hv=df_HVpercentile, df_vix=df_vix,
                     df_index=df_index, bound_low=30, bound_mid=60, HVtype_short='HV5', HVtype_long='HV20',
                     eliminate_extremum=True, smooth=True, prev_position=None, get_iv_position=False)

    # 测试2015年期权上市以来每日的数据
    date_list = df_contract['日期'].apply(lambda x: datetime.datetime.strftime(x, '%Y%m%d')).unique()
    df_gamma = pd.DataFrame({'Date': date_list})
    now_positon = None
    for i in range(len(df_gamma)):
        date_today = df_gamma.at[i, 'Date']
        print(date_today)
        now_positon = get_gamma_signal(date=date_today, code=stock_code, vol_type='VIX', df_rf=df_rf, df_stock=df_stock,
                                       df_contract=df_contract, df_hv=df_HVpercentile, df_vix=df_vix, df_index=df_index,
                                       bound_low=30, bound_mid=60, HVtype_short='HV5', HVtype_long='HV20',
                                       eliminate_extremum=True, smooth=True, prev_position=now_positon, get_iv_position=False)
        df_gamma.at[i, 'gamma_signal'] = now_positon

