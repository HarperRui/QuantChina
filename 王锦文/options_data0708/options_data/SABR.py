# -*- coding: utf-8 -*-
# @Time    : 2021/5/31 16:40
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : SABR.py
# @Software: PyCharm

import numpy as np
import pandas as pd
import QuantLib as ql
import math
from scipy import interpolate, optimize
import os
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
sns.set_style('whitegrid')
warnings.filterwarnings("ignore")


#%%

def sabr_implied_volatility(alpha, beta, nu, rho, f, K, T):
    """
    计算SABR模型的波动率
    :param alpha: 参数alpha
    :param beta: 参数beta
    :param nu: 参数nu
    :param rho: 参数rho
    :param f: 标的合成期货价格
    :param K: 行权价
    :param T: 距离到期时间（年化）
    :return: 波动率，float
    """
    try:
        z = nu/alpha * (f*K) ** ((1-beta)/2) * math.log(f/K)
        x_z = math.log((math.sqrt(1-2*rho*z+z**2) + z - rho) / (1-rho))
        numerator = alpha * (1 + ((1-beta)**2/24*alpha**2/(f*K)**(1-beta) + alpha*beta*rho*nu/4/(f*K)**((1-beta)/2) + (2-3*rho**2)/24*nu**2) * T)
        denominator = (f*K)**((1-beta)/2) * (1 + (1-beta)**2/24*math.log(f/K)**2 + (1-beta)**4/1920*math.log(f/K)**4)
        iv = numerator / denominator * z / x_z
    except ValueError:
        return 0
    return iv


def sabr_implied_volatility_np(alpha, beta, nu, rho, f, K, T):
    """
    计算SABR模型的波动率（math函数替换为numpy的函数，可以向量化运算，画曲线的时候使用）
    :param alpha: 参数alpha
    :param beta: 参数beta
    :param nu: 参数nu
    :param rho: 参数rho
    :param f: 标的合成期货价格
    :param K: 行权价
    :param T: 距离到期时间（年化）
    :return: 波动率，float
    """
    z = nu/alpha * (f*K) ** ((1-beta)/2) * np.log(f/K)
    x_z = np.log((np.sqrt(1-2*rho*z+z**2) + z - rho) / (1-rho))
    numerator = alpha * (1 + ((1-beta)**2/24*alpha**2/(f*K)**(1-beta) + alpha*beta*rho*nu/4/(f*K)**((1-beta)/2) + (2-3*rho**2)/24*nu**2) * T)
    denominator = (f*K)**((1-beta)/2) * (1 + (1-beta)**2/24*np.log(f/K)**2 + (1-beta)**4/1920*np.log(f/K)**4)
    iv = numerator / denominator * z / x_z
    return iv


def get_all_options(code, df_daily_info, path_daily_options):
    """
    读取当天的所有期权合约数据,储存在一个dict中，keys为交易代码，values为包括期权合约信息的DataFrame
    :param code: '510050' or '510300' or '000300'
    :param df_daily_info: 当天的所有合约信息，DataFrame
    :param path_daily_options: 当天期权合约文件的路径
    :return: dict
    """
    result = {}
    if code[0:3] == '510':
        for i in range(len(df_daily_info)):
            filepath = os.path.join(path_daily_options, '%i.parquet' % df_daily_info.at[i, '期权代码'])
            if os.path.exists(filepath):
                _tradecode = df_daily_info.at[i, '交易代码']
                result[_tradecode] = pd.read_parquet(filepath)
            else:
                print("%s not in minbar files" % df_daily_info.at[i, '交易代码'])
    else:
        for i in range(len(df_daily_info)):
            filepath = os.path.join(path_daily_options, '%s.parquet' % df_daily_info.at[i, '期权代码'])
            if os.path.exists(filepath):
                _tradecode = df_daily_info.at[i, '交易代码']
                result[_tradecode] = pd.read_parquet(filepath)
            else:
                print("%s not in minbar files" % df_daily_info.at[i, '交易代码'])
    return result


def get_available_options_data(code, idx, ym, _all_options, stock_price, prc_type, get_all=False):
    """
    对于指定的分钟以及交割日期，获取符合条件所有行权价下期权的隐含波动率及距离到期时间，默认剔除深度虚值样本
    :param code: '510050' or '510300' or '000300'
    :param idx: 当前分钟所在的index
    :param ym: 期权的交割日期，格式为 '2104'
    :param _all_options: 当天所有的期权合约, dict
    :param stock_price: 标的价格
    :param prc_type: 期权价格的类型，bid, ask, mid, close
    :param get_all: 是否获取所有行权价合约的信息
    :return: 包含不同行权价、IV及距离交割时间（年化）的 DataFrame
            +----------------------------------------+
            |   | strike_prc | IV_(prc_type) |  T   |
            | 0 |    3.1     |     0.182     | 0.32 |
            | 1 |    3.2     |     0.181     | 0.32 |
            +----------------------------------------+
    """

    def find_nearest_value(values, target, _num):
        """
        找到一组数中最接近target值的num个数
        :param values: 一组数字，array
        :param target: 目标数字
        :param _num: 最接近的个数
        :return: list
        """
        if len(values) <= _num:
            return values
        res = []
        while len(res) < _num:
            new_idx = np.abs(values - target).argmin()
            res.append(values[new_idx])
            values[new_idx] = 1e6
        res.sort()
        return res

    result = {}
    if code[0:3] == '510':
        stock_price *= 1000
        option_list = [x for x in _all_options.keys() if x[7:11] == ym]
        for tradecode in option_list:
            strike_price = int(tradecode[-4:])
            if tradecode[6] == 'C' and strike_price >= stock_price:
                # 加入虚值状态的看涨期权
                result[strike_price] = _all_options[tradecode].at[idx, 'IV_%s' % prc_type]
            elif tradecode[6] == 'P' and strike_price < stock_price:
                # 加入虚值状态的看跌期权
                result[strike_price] = _all_options[tradecode].at[idx, 'IV_%s' % prc_type]
        result = pd.DataFrame(result, index=['IV_%s' % prc_type]).T.reset_index().rename(columns={'index': 'strike_prc'})
        result['strike_prc'] /= 1000
        stock_price /= 1000
    else:
        option_list = [x for x in _all_options.keys() if x[2:6] == ym]
        for tradecode in option_list:
            strike_price = int(tradecode[-4:])
            if tradecode[7] == 'C' and strike_price >= stock_price:
                # 加入虚值状态的看涨期权
                result[strike_price] = _all_options[tradecode].at[idx, 'IV_%s' % prc_type]
            elif tradecode[7] == 'P' and strike_price < stock_price:
                # 加入虚值状态的看跌期权
                result[strike_price] = _all_options[tradecode].at[idx, 'IV_%s' % prc_type]
        result = pd.DataFrame(result, index=['IV_%s' % prc_type]).T.reset_index().rename(columns={'index': 'strike_prc'})

    result.sort_values('strike_prc', inplace=True, ignore_index=True)
    result['T'] = _all_options[tradecode].at[idx, 'expire_mins'] / (365*24*60)  # 添加距离到期的时间

    if not get_all:
        # 剔除深度虚值的合约
        num = 6 if code[0:3] == '510' else 12
        selected_strikes = find_nearest_value(np.array(result['strike_prc'].copy()), stock_price, num)
        threshold_low = selected_strikes[0]
        threshold_high = selected_strikes[-1]
        result = result[(result['strike_prc'] >= threshold_low) & (result['strike_prc'] <= threshold_high)]
        result.reset_index(inplace=True, drop=True)

    return result


def get_atm_imvol_cubic(stock_price, strike_list, iv_list):
    """
    三次样条插值拟合平值期权隐含波动率
    :param stock_price: 标的价格
    :param strike_list: 行权价列表
    :param iv_list: IV列表，与strike_list一一对应
    :return: 平值期权隐含波动率, ndarray
    """
    f = interpolate.interp1d(strike_list, iv_list, kind='cubic')
    return f(stock_price)


def calibrate_sabr(data, prc_type):
    """
    最优化拟合SABR的参数
    :param data: 包含行权价、隐含波动率、alpha、beta、标的价格、到期时间的DataFrame
    :param prc_type: 价格类型，bid/ask/mid/close
    :return: 拟合后的参数nu, rho
    """
    def func(p):
        """
        需要最小化的函数，SABR模型波动率与隐含波动率的残差平方和
        :param p: array-like
        :return: 残差平方和
        """
        alpha, beta, nu, rho = p
        sabr_vol = data.apply(lambda x: ql.sabrVolatility(x['strike_prc'],
                                                          x['stock_prc'],
                                                          x['T'],
                                                          alpha,
                                                          beta,
                                                          nu,
                                                          rho), axis=1)
        return ((data['IV_%s' % prc_type] - sabr_vol) ** 2).sum()
    x0 = np.array([0.3, 0.5, 1.0, -0.1])
    # minimize_result = optimize.minimize(func, x0=x0, method='nelder-mead')
    bnds = ((0.00001, None), (0.0, 1.0), (0.00001, None), (-0.99999, 0.99999))
    minimize_result = optimize.minimize(func, x0=x0, bounds=bnds, method='L-BFGS-B')
    return minimize_result.x[0], minimize_result.x[1], minimize_result.x[2], minimize_result.x[3]


def get_sabr_parameters(code, idx, ym, _all_options, stock_price, prc_type):
    """
    获取指定到期日、指定分钟的四个SABR模型参数
    :param code: '510050' or '510300' or '000300'
    :param idx: 当前分钟所在的index
    :param ym: 交割日期，例如‘2104’
    :param _all_options: 当天所有的期权合约, dict
    :param stock_price: 标的价格
    :param prc_type: 价格类型，bid/ask/mid/close
    :return: 参数alpha, beta, nu, rho
    """
    if stock_price == 0:
        return 0, 0, 0, 0
    df_params = get_available_options_data(code, idx, ym, _all_options, stock_price, prc_type)
    df_params['stock_prc'] = stock_price  # 添加标的价格列
    alpha, beta, nu, rho = calibrate_sabr(df_params, prc_type)
    return alpha, beta, nu, rho


def get_minbar_sabr_result(code, ym, future_data, options_data, _all_options, prc_type):
    """
    获取当日指定交割日期的所有SABR分钟级别参数
    :param code: '510050' or '510300' or '000300'
    :param ym: 交割日期，例如‘2104’
    :param future_data: 包含当天合成期货信息的DataFrame
    :param options_data: 包含当天所有合约信息的DataFrame
    :param _all_options: 当天所有的期权合约, dict
    :param prc_type: 价格类型，bid/ask/mid/close
    :return: 包含每分钟SABR参数的Series，元素为（alpha, beta, nu, rho)的tuple
    """
    df_result = pd.DataFrame(future_data[['datetime', ym]]).fillna(0)
    df_result['idx'] = np.arange(0, len(df_result))
    if code[0:3] == '510':
        df_contract_daily_ym = options_data[options_data['交易代码'].apply(lambda x: x[7:11].find(ym) >= 0)]  # 在一个到期日的所有合约
    else:
        df_contract_daily_ym = options_data[options_data['交易代码'].apply(lambda x: x[2:6].find(ym) >= 0)]
    df_contract_daily_ym.reset_index(inplace=True, drop=True)

    params_result = df_result.apply(lambda x: get_sabr_parameters(code,
                                                                  x['idx'],
                                                                  ym,
                                                                  _all_options,
                                                                  x[ym],
                                                                  prc_type), axis=1)
    return params_result


def get_sabr_plot(code, ymd, idx, ym, future_path, sabr_path, df_contract_info, options_path, get_all_points=True):
    """
    绘制SABR模型拟合的波动率曲线
    :param code: '510050' or '510300' or '000300'
    :param ymd: 日期，格式为 '20210401'
    :param idx: 指定分钟所在的index
    :param ym: 合约的到期日，格式为 '2104'
    :param future_path: 合成期货数据路径
    :param sabr_path: sabr参数数据路径
    :param df_contract_info: 储存期权合约信息的DataFrame
    :param options_path: 期权IV的minbar路径
    :param get_all_points: 是否绘制所有行权价的点
    :return: plot
    """
    options_path = os.path.join(options_path, ymd)
    df_contract_info = df_contract_info[df_contract_info['日期'] == ymd].reset_index(drop=True)
    _all_options = get_all_options(code, df_contract_info, options_path)

    future_path = os.path.join(future_path, '%s.csv' % ymd)
    sabr_path = os.path.join(sabr_path, '%s.csv' % ymd)
    df_future = pd.read_csv(future_path)
    df_sabr = pd.read_csv(sabr_path)

    _alpha = df_sabr.at[idx, 'alpha_%s' % ym]
    _beta = df_sabr.at[idx, 'beta_%s' % ym]
    _nu = df_sabr.at[idx, 'nu_%s' % ym]
    _rho = df_sabr.at[idx, 'rho_%s' % ym]
    _f = df_future.at[idx, ym]

    df_k = get_available_options_data(code, idx, ym, _all_options, _f, 'mid', get_all=get_all_points)
    _T = df_k.at[0, 'T']

    k = np.arange(df_k.at[0, 'strike_prc']-0.1, df_k.at[len(df_k)-1, 'strike_prc']+0.1, (df_k.at[len(df_k)-1, 'strike_prc'] - df_k.at[0, 'strike_prc'])/100)
    sabr_vol = sabr_implied_volatility_np(_alpha, _beta, _nu, _rho, _f, k, _T)

    fig = plt.figure(figsize=(10, 7))
    # plt.plot(k, sabr_vol, label='SABR')
    # plt.scatter(df_k['strike_prc'], df_k['IV_mid'], label='Real')
    ax = fig.add_subplot()
    sns.lineplot(x=k, y=sabr_vol, label='SABR')
    sns.scatterplot(x='strike_prc', y='IV_mid', data=df_k, label='Real')
    ax.set_xlabel('Strike price')
    ax.set_ylabel('Volatility')
    plt.legend()
    plt.title(ymd + '--' + ym)
    plt.show()


def get_sabr(code, date, input_iv_path, input_future_path, output_path, df_contract):
    """
    计算指定日期的SABR参数并将结果储存在本地
    :param code: '510050' or '510300' or '000300'
    :param date: 日期，格式为 '20210401'
    :param input_iv_path: 储存期权合约IV数据的路径
    :param input_future_path: 储存合成期货数据的路径
    :param output_path: sabr结果的输出路径
    :param df_contract: 储存期权合约信息的DataFrame
    :return: 将参数拟合结果保存至输出路径下
    """
    print('Calculating SABR parameters of %s' % date)
    future_daily_path = os.path.join(input_future_path, '%s.csv' % date)
    df_future_daily = pd.read_csv(future_daily_path, index_col=0)  # 当天合成期货minbar数据
    options_daily_path = os.path.join(input_iv_path, date)
    df_contract_daily = df_contract[df_contract['日期'] == date].reset_index(drop=True)  # 当天所有的合约（剔除掉除权合约）

    maturities = [x for x in df_future_daily.columns if len(x) == 4]  # 四个到期时间
    sabr_result = pd.DataFrame(df_future_daily['datetime'])  # 储存sabr输出结果

    all_options = get_all_options(code, df_contract_daily, options_daily_path)  # 当天所有的期权合约data

    for yearmonth in maturities:
        sabr_params = get_minbar_sabr_result(code, yearmonth, df_future_daily, df_contract_daily, all_options, 'mid')
        sabr_result['alpha_%s' % yearmonth] = pd.Series([x[0] for x in sabr_params])
        sabr_result['beta_%s' % yearmonth] = pd.Series([x[1] for x in sabr_params])
        sabr_result['nu_%s' % yearmonth] = pd.Series([x[2] for x in sabr_params])
        sabr_result['rho_%s' % yearmonth] = pd.Series([x[3] for x in sabr_params])
        del sabr_params

    outfile = os.path.join(output_path, '%s.csv' % date)
    sabr_result.to_csv(outfile)


#%%
# input_future_path = 'C:\\Users\\lenovo\\OneDrive\\桌面\\output\\synthetic_futures'  # 合成期货数据路径
# input_iv_path = 'C:\\Users\\lenovo\\OneDrive\\桌面\\output'  # IV路径
# sabr_outpath = 'C:\\Users\\lenovo\\OneDrive\\桌面\\output\\SABR'  # SABR结果输出路径
# if not os.path.exists(sabr_outpath):
#     os.makedirs(sabr_outpath)

#%% 获取contract_info数据
# code = '510050'
# SQ = SQLConn()
# df_contract_all = SQ.GetData('contract_info_daily')
# df_contract_code = df_contract_all[df_contract_all['期权标的'] == '%s.SH' % code]  # 只包含50 or 300
# df_contract_M = df_contract_code[df_contract_code['交易代码'].apply(lambda x: x.find('M') >= 0)]  # 剔除除权合约
# SQ.CloseSql()
#
# #%%
# date_range = [pd.to_datetime(x).strftime('%Y%m%d') for x in df_contract_code['日期'].unique()]
# date_range = [x for x in date_range if x[0:6] == '202104']  # 2021年4月
#
# for date in date_range[8:9]:
#     print('Start to process the data of %s' % date)
#     options_daily_path = os.path.join(input_iv_path, date)
#     future_daily_path = os.path.join(input_future_path, '%s.csv' % date)
#     df_future_daily = pd.read_csv(future_daily_path, index_col=0)  # 当天合成期货minbar数据
#     df_contract_daily = df_contract_M[df_contract_M['日期'] == date].reset_index(drop=True)  # 当天所有的合约（剔除掉除权合约）
#
#     maturities = [x for x in df_future_daily.columns if len(x) == 4]  # 四个到期时间
#     sabr_result = pd.DataFrame(df_future_daily['datetime'])  # 储存sabr输出结果
#
#     all_options = get_all_options(df_contract_daily, options_daily_path)  # 当天所有的期权合约data
#
#     for yearmonth in maturities:
#         sabr_params = get_minbar_sabr_result(yearmonth, df_future_daily, df_contract_daily, all_options, 'mid')
#         sabr_result['alpha_%s' % yearmonth] = pd.Series([x[0] for x in sabr_params])
#         sabr_result['beta_%s' % yearmonth] = pd.Series([x[1] for x in sabr_params])
#         sabr_result['nu_%s' % yearmonth] = pd.Series([x[2] for x in sabr_params])
#         sabr_result['rho_%s' % yearmonth] = pd.Series([x[3] for x in sabr_params])
#         del sabr_params
#
#     outfile = os.path.join(sabr_outpath, '%s.csv' % date)
#     sabr_result.to_csv(outfile)


#%%
# get_sabr_plot('20210420', 56, '2104', input_future_path, sabr_outpath, df_contract_M, input_iv_path, get_all_points=True)



































