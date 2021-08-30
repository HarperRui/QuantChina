# -*- coding: utf-8 -*-
# @Time    : 2021/7/10 23:02
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : daily_iv.py
# @Software: PyCharm

import math
import numpy as np
import pandas as pd
import datetime
from py_vollib_vectorized import vectorized_implied_volatility
from scipy.special import ndtr
from scipy import interpolate
import warnings
warnings.filterwarnings('ignore')

#%% 计算dividend
def get_synthetic_futures(code, date, rf, stockprice, df_contract):
    """
    计算合成期货价格
    :param code: 标的资产代码，'510050' or '510300' or '000300'
    :param date: 处理当天的日期，格式为 '20210521'
    :param rf: 年化的无风险利率
    :param stockprice: 标的资产收盘价
    :param df_contract: 包含每日期权信息的 Dataframe
    :return: 包含当天收盘时的分红率的Dataframe
    """

    daily_options = df_contract[df_contract['日期'] == date].reset_index(drop=True)

    df_futures = pd.DataFrame()  # 储存合成期货数据
    strike_prc_list = np.sort(daily_options['行权价'].unique())  # 包含当天期权行权价的array

    if stockprice >= 0:
        closest_index = np.argmin(np.abs(strike_prc_list - stockprice))  # 最接近标的价格的行权价
        if stockprice < strike_prc_list[0]:  # 标的价格小于最低行权价
            k1 = strike_prc_list[0]
            k2 = k1
            k3 = k1
        elif stockprice > strike_prc_list[-1]:  # 标的价格大于最高行权价
            k1 = strike_prc_list[-1]
            k2 = k1
            k3 = k1
        elif closest_index == 0:  # 最接近标的价格的是最低的行权价，且标的价格高于最低的行权价
            # 取最低和次低的行权价
            k1 = strike_prc_list[closest_index]
            k2 = strike_prc_list[closest_index + 1]
            k3 = k2
        elif closest_index == len(strike_prc_list) - 1:  # 最接近标的价格的是最高的行权价，且标的价格低于最高的行权价
            # 取最高和次高的行权价
            k1 = strike_prc_list[closest_index]
            k2 = strike_prc_list[closest_index - 1]
            k3 = k2
        else:
            k1 = strike_prc_list[closest_index]
            k2 = strike_prc_list[closest_index + 1]
            k3 = strike_prc_list[closest_index - 1]
        k_list = list({k1, k2, k3})

        monthlist = np.sort(pd.Series([x[7: 11] for x in daily_options['交易代码']]).unique())  # 当天存续合约的四个到期月份

        min_price = pd.DataFrame(index=k_list, columns=monthlist)  # 储存一分钟的不同到期日、不同行权价计算出的合成期货价格

        maturities = {}  # 储存当前分钟距离四个到期日的时间（年化）

        for k in k_list:
            for month in min_price.columns:
                tradecode_call = code + 'C%sM' % month + '0%i' % int(k * 1000)  # 看涨期权交易代码
                tradecode_put = code + 'P' + tradecode_call[7:]  # 看跌期权交易代码

                call_index = (daily_options['交易代码'] == tradecode_call).argmax()
                put_index = (daily_options['交易代码'] == tradecode_put).argmax()
                if 'T_%s' % month not in maturities.keys():
                    expire_time = str(daily_options.at[call_index, '到期日'])[0:10] + ' 15:00:00'
                    expire_time = datetime.datetime.strptime(expire_time, '%Y-%m-%d %H:%M:%S')  # 期权到期时间
                    now_time = str(daily_options.at[call_index, '日期'])[0:10] + ' 15:00:00'
                    now_time = datetime.datetime.strptime(now_time, '%Y-%m-%d %H:%M:%S')
                    T = (expire_time - now_time).total_seconds() / 60 / (365 * 24 * 60)  # 距离到期的时间（年化）
                    maturities['T_%s' % month] = T

                C = daily_options.at[call_index, '收盘价']
                P = daily_options.at[put_index, '收盘价']
                t = maturities['T_%s' % month]
                q = -np.log((C + k * np.exp(-rf * t) - P) / stockprice) / t  # 分红率
                min_price.at[k, month] = stockprice * np.exp((rf - q) * t)
        # 取三个行权价合成期货价格的均值填入sys_future

        for month in min_price.columns:
            t = maturities['T_%s' % month]
            df_futures.at[0, 'q_%s' % month] = rf - np.log(min_price[month].mean() / stockprice) / t

    return df_futures

# get_synthetic_futures('510050', '20210401', 0.025, 3.567, df_contract_info)

#%% 计算iv与delta
def Delta(s, k, t, r, q, sigma, callput):
    """
    Calculate the delta of an option
    :param s: 标的资产价格
    :param k: 行权价
    :param t: 距离到期日时间（年化）
    :param r: 无风险利率
    :param q: 分红率
    :param sigma: 波动率
    :param callput: 认购认沽，'c' or 'p'
    :return: float
    """
    if sigma == 0:
        return 0
    d1 = (np.log(s / k) + (r - q + 0.5 * sigma ** 2) * t) / (sigma * np.sqrt(t))
    if callput == 'c':
        return np.exp(-q * t) * ndtr(d1)
    else:
        return np.exp(-q * t) * (ndtr(d1) - 1)


def get_iv_greeks(date, rf, stock_prc, df_contract, df_dividend):
    """
    在minbar数据中添加期权基本信息以及bid,ask,mid,close四个价格对应的IV和Greeks
    :param date: 处理当天的日期，格式为 '20210521'
    :param rf: 年化的无风险利率
    :param stock_prc: 标的资产收盘价
    :param df_contract: 包含每日期权信息的 Dataframe(只包含当前标的的合约)
    :param df_dividend: 包含每个到期日dividend的DataFrame
    :return: 将处理好的minbar数据保存
    """

    daily_contract_code = df_contract[df_contract['日期'] == date].reset_index(drop=True)  # 当天的期权合约信息(只包含50、300中的一种)

    prc_list = []
    strike_list = []
    callput_list = []
    iv_list = []
    delta_list = []
    maturity_list = []
    ttm_list = []
    for i in range(len(daily_contract_code)):
        option_prc = daily_contract_code.at[i, '收盘价']
        prc_list.append(option_prc)
        callput = 'c' if daily_contract_code.at[i, '认购认沽'] == '认购' else 'p'
        callput_list.append(callput)  # 添加认购认沽
        strike_prc = daily_contract_code.at[i, '行权价']
        strike_list.append(strike_prc)  # 添加行权价
        expire_time = str(daily_contract_code.at[i, '到期日'])[0:10] + ' 15:00:00'
        expire_time = datetime.datetime.strptime(expire_time, '%Y-%m-%d %H:%M:%S')  # 期权到期时间
        now_time = str(daily_contract_code.at[i, '日期'])[0:10] + ' 15:00:00'
        now_time = datetime.datetime.strptime(now_time, '%Y-%m-%d %H:%M:%S')
        ttm = (expire_time - now_time).total_seconds() / (60*365*24*60)  # 距离到期的分钟数
        ttm_list.append(ttm)
        option_monthdate = daily_contract_code.at[i, '交易代码'][7:11]  # 期权合约到期月
        maturity_list.append(option_monthdate)
        q = df_dividend.at[0, 'q_%s' % option_monthdate]  # 分红率

        # IV, Greeks
        try:
            iv = vectorized_implied_volatility(option_prc, stock_prc, strike_prc, ttm, rf, callput, q=q,
                                               model='black_scholes_merton', return_as='numpy')[0]

            delta = Delta(stock_prc, strike_prc, ttm, rf, q, iv, callput)
        except ZeroDivisionError:
            iv = 0
            delta = 0
            
        iv_list.append(iv)
        delta_list.append(delta)
    
    df_iv = pd.DataFrame({'strike_prc': strike_list, 'callput': callput_list, 'maturity': maturity_list,
                          'ttm': ttm_list, 'iv': iv_list, 'delta': delta_list})

    return df_iv.fillna(0)

# get_iv_greeks('20210401', 0.025, 3.567, df_contract_info, df_q)

#%% 计算0.5delta的iv
def get_available_options_data(df_iv, ym, option_type):
    """
    对于指定的分钟以及交割日期，获取符合条件所有行权价下期权的隐含波动率及delta
    :param df_iv: 包含期权iv, delta等信息的DataFrame
    :param ym: 到期日，例如'2104'
    :param option_type: 期权的类型，c or p
    :return: 包含不同行权价、IV及距离交割时间（年化）的 DataFrame
            +----------------------------+
            |   | delta |       IV      |
            | 0 |  0.4  |     0.182     |
            | 1 |  0.6  |     0.181     |
            +----------------------------+
    """

    result = df_iv.loc[(df_iv['callput'] == option_type) & (df_iv['maturity'] == ym),
                       ['iv', 'delta']].reset_index(drop=True)
    result['delta'] = np.abs(result['delta'])
    result.sort_values('delta', inplace=True, ignore_index=True)
    result.drop_duplicates(inplace=True, ignore_index=True)

    if len(result) < 4:
        # print('####### Not enough effective delta samples #######')
        return result

    # 取0.25~0.75 delta附近的样本
    low_idx = 0
    high_idx = len(result) - 1
    for i in range(len(result) - 1):
        if result.at[i, 'delta'] <= 0.25 < result.at[i + 1, 'delta']:
            low_idx = i
        if result.at[i, 'delta'] < 0.75 <= result.at[i + 1, 'delta']:
            high_idx = i + 1

    while high_idx - low_idx < 3:
        if low_idx != 0:
            low_idx -= 1
        elif high_idx != len(result) - 1:
            high_idx += 1
        else:
            break
    filtered_result = result.loc[low_idx:high_idx].reset_index(drop=True)

    return filtered_result


def get_inter_imvol_cubic(delta_list, iv_list, option_type):
    """
    三次样条插值拟合波动率
    :param delta_list: 期权的delta列表
    :param iv_list: IV列表，与delta_list一一对应
    :param option_type: 期权类型，'C' or 'P'
    :return: 0.25,0.5,0.75delta的隐含波动率, ndarray
    """
    target_delta = 0.5
    if option_type.upper() == 'P':
        delta_list *= -1
        target_delta *= -1
    try:
        f = interpolate.interp1d(delta_list, iv_list, kind='cubic')
        return f([target_delta])[0]
    except ValueError:
        f = interpolate.interp1d(delta_list, iv_list, kind='cubic', fill_value="extrapolate")
        return f([target_delta])[0]


def get_inter_imvol_simple(data):
    """
    简单线性插值计算波动率
    :param data: 格式如get_available_options_data函数输出结果的DataFrame
    :return: 0.5delta的隐含波动率
    """

    def get_simple_inter(low_point, high_point, target_point, low_value, high_value):
        return low_value + (high_value - low_value) * (target_point - low_point) / (high_point - low_point)

    low_idx_5 = high_idx_5 = 0
    for i in range(len(data) - 1):
        if data.at[i, 'delta'] < 0.5 <= data.at[i + 1, 'delta']:
            low_idx_5 = i
            high_idx_5 = i + 1
            break
    if low_idx_5 == high_idx_5:
        if data.at[0, 'delta'] >= 0.5:
            low_idx_5 = 0
            high_idx_5 = 1
        elif data.at[len(data) - 1, 'delta'] <= 0.5:
            low_idx_5 = len(data) - 2
            high_idx_5 = low_idx_5 + 1

    iv_5 = get_simple_inter(data.at[low_idx_5, 'delta'],
                            data.at[high_idx_5, 'delta'],
                            0.50,
                            data.at[low_idx_5, 'iv'],
                            data.at[high_idx_5, 'iv'])

    return iv_5


def get_inter_parameters(df_iv, ym, option_type):
    """
    获取指定到期日、指定分钟的0.25,0.5,0.75delta波动率
    :param df_iv: 包含期权iv, delta等信息的DataFrame
    :param ym: 交割日期，例如‘2104’
    :param option_type: 期权类型，'c' or 'p'
    :return: 0.5delta波动率，依次为三次样条和简单线性插值
    """
    df_params = get_available_options_data(df_iv, ym, option_type)
    if len(df_params) < 4:
        # 近月合约在交割当天的delta可能都为0
        return 0, 0
    result_cubic = get_inter_imvol_cubic(df_params['delta'].copy(), df_params['iv'].copy(), option_type)
    result_simple = get_inter_imvol_simple(df_params.copy())
    return result_cubic, result_simple


def get_termstructure(df_iv, maturities, option_type):
    """
    计算指定日期的0.25,0.5,0.75delta 隐含波动率差值结果并保存至输出路径
    :param df_iv: 包含期权iv, delta等信息的DataFrame
    :param maturities: 到期日的list
    :param option_type: 'c' or 'p'
    :return: 0.5delta波动率
    """
    termstructure_result = pd.DataFrame()  # 储存输出结果

    for yearmonth in maturities:
        cubic, simple = get_inter_parameters(df_iv, yearmonth, option_type)
        termstructure_result.at[0, '%s_cubic' % yearmonth] = cubic
        termstructure_result.at[0, '%s_simple' % yearmonth] = simple

    return termstructure_result


#%% 计算远期波动率
def get_forward_iv(data_iv, data_termstructure, expire_threshold=10):
    """
    forward volatility
    :param data_iv: 含有到期月份和到期时间的DataFrame
    :param data_termstructure: 含有各个到期时间0.5delta波动率的DataFrame
    :param expire_threshold: 近月合约距离交割小于一定天数时返回次月合约的0.5delta波动率，默认10天
    :return: 远期波动率
    """
    maturity = np.sort(data_iv['maturity'].unique())
    ttm = np.sort(data_iv['ttm'].unique())
    if ttm[0] <= expire_threshold / 365:
        return data_termstructure.at[0, '%s_simple' % maturity[1]]
    else:
        sigma1 = data_termstructure.at[0, '%s_simple' % maturity[0]]
        sigma2 = data_termstructure.at[0, '%s_simple' % maturity[1]]
        t1 = ttm[0]
        t2 = ttm[1]
        return math.sqrt((sigma2**2 * t2 - sigma1**2 * t1) / (t2 - t1))

#%%
def get_daily_iv(code, date, df_rf, df_contract, df_stock, expire_threshold=10):
    """
    计算每天的远期波动率
    :param code: 标的代码（不带'.SH')
    :param date: 日期
    :param df_rf: 包含无风险利率的DataFrame
    :param df_contract: 包含每日期权信息的 Dataframe
    :param df_stock: 标的资产的ohlc数据 DataFrame
    :param expire_threshold: 近月合约距离交割小于一定天数时返回次月合约的0.5delta波动率，默认10天
    :return: 远期波动率
    """
    rf = np.log(1 + df_rf.loc[df_rf['日期'] == date, '中债国债到期收益率：1年'].values[0] / 100)
    stock_prc = df_stock.loc[df_stock['日期'] == date, 'close'].values[0]
    df_q = get_synthetic_futures(code, date, rf, stock_prc, df_contract)
    df_iv = get_iv_greeks(date, rf, stock_prc, df_contract, df_q)
    maturity = np.sort(df_iv['maturity'].unique())
    df_ts = get_termstructure(df_iv, maturity, 'c')
    return get_forward_iv(df_iv, df_ts, expire_threshold)

#%%
if __name__== '__main__':
    from zltsql import SQLConn

    stock_code = '510050.SH'
    SQ = SQLConn()
    df_HVpercentile = SQ.GetData('HV_percentile')

    df_contract_all = SQ.GetData('contract_info_daily')
    df_contract_all = df_contract_all[df_contract_all['交易代码'].apply(lambda x: x.find('M') >= 0)]
    df_contract = df_contract_all[df_contract_all['期权标的'] == stock_code].reset_index(drop=True)  # 保留50或300的期权信息

    df_rf = SQ.GetData('rf')  # 无风险利率

    df_stock = SQ.GetData('etf_50')

    SQ.CloseSql()

    #date_list = df_contract['日期'].apply(lambda x: datetime.datetime.strftime(x, '%Y%m%d')).unique()
    date_list = ['20160104']
    df_iv_insert = pd.DataFrame({'Date': date_list})
    for i in range(len(df_iv_insert)):
        date_today = df_iv_insert.at[i, 'Date']
        print(date_today)
        df_iv_insert.at[i, 'iv_insert'] = get_daily_iv(stock_code[0:6], date_today, df_rf, df_contract, df_stock, expire_threshold=10) * 100
    #df_iv_insert.to_excel('D:/Harper/实习文件整理_张依依/HV_percentile/iv_insert_50etf_0728.xlsx',index=False)