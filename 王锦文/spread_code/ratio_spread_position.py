# -*- coding: utf-8 -*-
# @Time    : 2021/7/13 16:29
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : ratio_spread_position.py
# @Software: PyCharm

import math
import numpy as np
import pandas as pd
import datetime
from zltsql import SQLConn
from daily_iv import get_daily_iv, get_daily_iv_delta


#%%
def get_ratio_spread_positon(date, code, ratio, df_rf, df_contract, df_stock, df_hv, df_vix, vol_type='IV'):
    """
    获取每天构建认购正比例、认购反比例、认沽正比例、认沽反比例价差的期权信息
    :param date: 日期，例如'20210401'
    :param code: 标的资产的代码，例如 '510050.SH'
    :param ratio:  比例价差的比例（大于1的数字）
    :param df_rf: 含有无风险利率的DataFrame
    :param df_contract: 期权的contract_daily_info信息，DataFrame
    :param df_stock: 含有标的资产每天收盘价的DataFrame
    :param df_hv: 含有每日HV的DataFrame
    :param df_vix: 含有每日VIX的DataFrame
    :param vol_type: 'IV' or 'VIX' or 'HV**', 如果是HV的话需要指定多少日的HV，比如'HV20'
    :return: 依次为认购正比例、认购反比例、认沽正比例、认沽反比例价差的DataFrame
    """

    stock_prc = df_stock.loc[df_stock['日期'] == date, 'close'].values[0]  # 当天标的资产的收盘价

    # 每天的波动率（日度化）
    if vol_type == 'IV':
        vol = get_daily_iv(code[0:6], date, df_rf, df_contract, df_stock, expire_threshold=10) / math.sqrt(365)
    elif vol_type[0:2] == 'HV':
        vol = df_hv.loc[(df_hv['Date'] == date) & (df_hv['Code'] == code), vol_type].values[0] / 100 / math.sqrt(252)
    elif vol_type == 'VIX':
        vol = df_vix.loc[df_stock['日期'] == date, 'iVIX'].values[0] / 100 / math.sqrt(365)
    else:
        print('Wrong volatility type')
        return

    # 包含当天所有期权行权价、callput、iv、delta、ttm的DataFrame
    df_option = get_daily_iv_delta(code[0:6], date, df_rf, df_contract, df_stock)

    ttm_list = np.sort(df_option['ttm'].unique())
    ttm_l1, ttm_l2 = ttm_list[0], ttm_list[1]  # 近月和远月合约的到期时间（年化）

    def get_option_pool(option_data, ttm, callput, daily_vol, spot_prc):
        """
        筛选出行权价在 ‘0.5delta行权价’ 和 ‘当前标的价格一倍标准差范围’ 之间的期权合约
        :param option_data: 含有期权行权价、callput、iv、delta、ttm的DataFrame
        :param ttm: 期权的到期时间（年化）
        :param callput: 'c' or 'p'
        :param daily_vol: 日度化的波动率
        :param spot_prc: 标的资产当前的价格
        :return: 行权价符合标准的期权合约及其信息，DataFrame
        """
        if callput == 'c':
            # max_strike = spot_prc * (1 + daily_vol * math.sqrt(ttm*365))  # discrete
            max_strike = spot_prc * math.exp(daily_vol * math.sqrt(ttm * 365))  # continuous
            pool = option_data[(option_data['callput'] == 'c') &
                               (option_data['delta'] < 0.5) &
                               (option_data['strike_prc'] < max_strike) &
                               (option_data['ttm'] == ttm)].reset_index(drop=True)
        else:
            # min_strike = spot_prc * (1 - daily_vol * math.sqrt(ttm*365))  # discrete
            min_strike = spot_prc * math.exp(-daily_vol * math.sqrt(ttm * 365))  # continuous
            pool = option_data[(option_data['callput'] == 'p') &
                               (option_data['delta'] > -0.5) &
                               (option_data['strike_prc'] > min_strike) &
                               (option_data['ttm'] == ttm)].reset_index(drop=True)
        return pool

    def get_position(option_data, ratio, callput, direction):
        """
        根据比例和认购/认沽，选出组成的头寸中delta最接近0的组合
        :param option_data: 含有期权行权价、callput、iv、delta、ttm的DataFrame（已经筛选过的）
        :param ratio: 比例价差的比例（大于1的数字）
        :param callput: 'c' or 'p'
        :param direction: 'direct' 表示正比例，'inverse' 表示反比例
        :return: 组成最接近 0 delta头寸的期权信息, DataFrame

        ratio > 0, 表示正比例价差，即 1 long和 2 short
            long    short   delta   ratio   callput
            3.6     3.7     0.02    2       c

        ratio < 0, 表示反比例价差，即 2 long和 1 short
            long    short   delta   ratio   callput
            3.7     3.6     -0.02   -2      c
        """

        if len(option_data) < 2:
            # 有可能在筛选过后只有一个或者没有符合条件的合约，无法构成比例价差
            return None

        if callput == 'c':
            option_data.sort_values(by='strike_prc', ignore_index=True, inplace=True)
        else:
            option_data.sort_values(by='strike_prc', ignore_index=True, inplace=True, ascending=False)

        outcomes = []  # 记录不同行权价的组合
        deltas = []  # 记录每种组合的delta
        for i in range(len(option_data) - 1):
            higher_delta_strike = option_data.at[i, 'strike_prc']
            delta_higher = option_data.at[i, 'delta']
            for j in range(i+1, len(option_data)):
                lower_delta_strike = option_data.at[j, 'strike_prc']
                delta_lower = option_data.at[j, 'delta']
                outcomes.append((higher_delta_strike, lower_delta_strike))
                deltas.append(delta_higher - ratio * delta_lower)

        result = pd.DataFrame()
        minimum_delta_index = np.abs(deltas).argmin()
        if direction == 'direct':
            result.at[0, 'long'] = outcomes[minimum_delta_index][0]
            result.at[0, 'short'] = outcomes[minimum_delta_index][1]
            result.at[0, 'delta'] = deltas[minimum_delta_index]
            result.at[0, 'ratio'] = ratio
        else:
            result.at[0, 'long'] = outcomes[minimum_delta_index][1]
            result.at[0, 'short'] = outcomes[minimum_delta_index][0]
            result.at[0, 'delta'] = deltas[minimum_delta_index] * -1
            result.at[0, 'ratio'] = ratio * -1
        result.at[0, 'callput'] = callput
        return result

    def get_minimum_delta_position(option_data, ttm_short, ttm_long, callput, daily_vol, spot_prc, ratio, direction):
        """
        比较用近月和次月合约构成的比例价差的delta，选择delta最接近0的一组
        :param option_data: 含有期权行权价、callput、iv、delta、ttm的DataFrame
        :param ttm_short: 近月合约的到期时间（年化）
        :param ttm_long: 次月合约的到期时间（年化）
        :param callput: 'c' or 'p'
        :param daily_vol: 日度化的波动率
        :param spot_prc: 标的资产的价格
        :param ratio: 比例价差的比例（大于1的数字）
        :param direction: 'direct' 表示正比例，'inverse' 表示反比例
        :return: 拥有最接近0 delta的组合的期权信息
        """

        if ttm_short < 10 / 365:
            # 若近月合约到期时间小于10天，则只考虑使用次月合约构建价差
            option_pool = get_option_pool(option_data, ttm_long, callput, daily_vol, spot_prc)
            result = get_position(option_pool.copy(), ratio, callput, direction)
            if result is None:
                return None
            else:
                result.at[0, 'maturity'] = 'L2'
                return result
        else:
            option_pool1 = get_option_pool(option_data, ttm_short, callput, daily_vol, spot_prc)
            option_pool2 = get_option_pool(option_data, ttm_long, callput, daily_vol, spot_prc)
            result1 = get_position(option_pool1.copy(), ratio, callput, direction)
            result2 = get_position(option_pool2.copy(), ratio, callput, direction)
            if result1 is None:
                # 可能没有足够符合条件的期权合约
                if result2 is not None:
                    result2.at[0, 'maturity'] = 'L2'
                    return result2
                else:
                    return None
            elif abs(result1.at[0, 'delta']) < abs(result2.at[0, 'delta']):
                result1.at[0, 'maturity'] = 'L1'
                return result1
            else:
                result2.at[0, 'maturity'] = 'L2'
                return result2

    position_call_direct = get_minimum_delta_position(df_option.copy(), ttm_l1, ttm_l2, 'c', vol, stock_prc, ratio,
                                                      'direct')
    position_call_inverse = get_minimum_delta_position(df_option.copy(), ttm_l1, ttm_l2, 'c', vol, stock_prc, ratio,
                                                       'inverse')
    position_put_direct = get_minimum_delta_position(df_option.copy(), ttm_l1, ttm_l2, 'p', vol, stock_prc, ratio,
                                                     'direct')
    position_put_inverse = get_minimum_delta_position(df_option.copy(), ttm_l1, ttm_l2, 'p', vol, stock_prc, ratio,
                                                      'direct')
    
    position = pd.concat([position_call_direct, position_call_inverse, position_put_direct, position_put_inverse],
                         ignore_index=True)
    return position


#%%
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

# get_ratio_spread_positon(date='20210420', code='510050.SH', ratio=2, df_rf=df_rf, df_contract=df_contract,
#                          df_stock=df_stock, df_hv=df_HVpercentile, df_vix=df_vix, vol_type='IV')

# eg
date_list = df_contract['日期'].apply(lambda x: datetime.datetime.strftime(x, '%Y%m%d')).unique()
date_list = [x for x in date_list if x[0:6] == '202104']

for date in date_list:
    print(date)
    now_positon = get_ratio_spread_positon(date=date, code=stock_code, ratio=2, df_rf=df_rf, df_contract=df_contract,
                                           df_stock=df_stock, df_hv=df_HVpercentile, df_vix=df_vix, vol_type='IV')
    print(now_positon)
