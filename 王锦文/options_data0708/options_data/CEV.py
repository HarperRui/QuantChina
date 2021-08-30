# -*- coding: utf-8 -*-
# @Time    : 2021/6/28 9:48
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : CEV.py
# @Software: PyCharm

import numpy as np
import pandas as pd
import QuantLib as ql
from scipy import optimize
import os
from py_vollib_vectorized import vectorized_implied_volatility
import warnings
warnings.filterwarnings("ignore")

day_count = ql.Actual365Fixed()
calendar = ql.China()

#%%
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
                tradecode = df_daily_info.at[i, '交易代码']
                result[tradecode] = pd.read_parquet(filepath)
            else:
                print("%s not in minbar files" % df_daily_info.at[i, '交易代码'])
    else:
        for i in range(len(df_daily_info)):
            filepath = os.path.join(path_daily_options, '%s.parquet' % df_daily_info.at[i, '期权代码'])
            if os.path.exists(filepath):
                tradecode = df_daily_info.at[i, '交易代码']
                result[tradecode] = pd.read_parquet(filepath)
            else:
                print("%s not in minbar files" % df_daily_info.at[i, '交易代码'])
    return result


def get_minbar_cev_params(code, date, expiration_date, rf, minbar_idx, df_future, df_stock, strike_prcs, option_dict, get_all=False):
    """
    根据交易日期、期权到期时间及分钟bar所在的index计算对应的CEV model parameters
    :param code: 标的ETF的代码，'510050' or '510300'
    :param date: 交易日期，格式为'20210401'
    :param expiration_date: 期权的到期日，格式为'20210428'
    :param rf: 无风险利率（年化）
    :param minbar_idx: 分钟所在的index
    :param df_future: 含有交易当天合成期货价格和分红率的DataFrame
    :param df_stock: 当天标的资产的minbar数据
    :param strike_prcs: 行权价，ndarray
    :param option_dict: 当天所有的期权合约, dict
    :param get_all: 是否剔除掉深度虚值的数据，默认剔除
    :return: beta_call, sigma_call, beta_put, sigma_put
    """

    if date == expiration_date:
        return 0, 0, 0, 0

    def find_nearest_value(values, target, _num):
        """
        找到一组数中最接近target值的num个数值
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

    def get_option_data(strike, _code, option_type, data_type):
        if _code[0:3] == '510':
            if option_type == 'c':
                trading_code = _code + 'C' + ym_maturities + 'M0' + str(int(strike * 1000))
            else:
                trading_code = _code + 'P' + ym_maturities + 'M0' + str(int(strike * 1000))
        else:
            if option_type == 'c':
                trading_code = 'IO%s-C-%i' % (ym_maturities, strike)
            else:
                trading_code = 'IO%s-P-%i' % (ym_maturities, strike)
        return option_dict[trading_code].at[minbar_idx, data_type]

    calculation_date = ql.Date(int(date[-2:]), int(date[4:6]), int(date[0:4]))
    ql.Settings.instance().evaluationDate = calculation_date
    maturity_date = ql.Date(int(expiration_date[-2:]), int(expiration_date[4:6]), int(expiration_date[0:4]))
    eu_exercise = ql.EuropeanExercise(maturity_date)
    yield_ts = ql.YieldTermStructureHandle(ql.FlatForward(calculation_date, rf, day_count))

    ym_maturities = expiration_date[2:6]
    future_prc = df_future.at[minbar_idx, ym_maturities]  # QuantLib里的CEV model需要input future price
    spot_prc = df_stock.at[minbar_idx, 'open']
    dividend = df_future.at[minbar_idx, 'q_%s' % ym_maturities]

    if not get_all:
        num = 6 if code[0:3] == '510' else 12
        selected_strikes = find_nearest_value(strike_prcs.copy(), future_prc, num)
        threshold_low = selected_strikes[0]
        threshold_high = selected_strikes[-1]
        strike_prcs = strike_prcs[(strike_prcs >= threshold_low) & (strike_prcs <= threshold_high)]

    call_strikes = strike_prcs[strike_prcs >= spot_prc]
    put_strikes = strike_prcs[strike_prcs < spot_prc]
    data_call = pd.DataFrame(call_strikes, columns=['strike_prc'])
    data_put = pd.DataFrame(put_strikes, columns=['strike_prc'])
    data_call['option_prc'] = data_call['strike_prc'].apply(lambda x: get_option_data(x, code, 'c', 'first_mid_prc1'))
    data_put['option_prc'] = data_put['strike_prc'].apply(lambda x: get_option_data(x, code, 'p', 'first_mid_prc1'))
    for tradingcode in option_dict.keys():
        if code[0:3] == '510':
            maturity = tradingcode[7:11]
        else:
            maturity = tradingcode[2:6]
        if maturity == ym_maturities:
            t = option_dict[tradingcode].at[minbar_idx, 'expire_mins'] / (365 * 24 * 60)
            break
        else:
            continue
    data_call['T'] = t
    data_put['T'] = t

    def calibrate_cev(data, option_type):

        ql_option_type = ql.Option.Call if option_type == 'c' else ql.Option.Put

        data['market_vols'] = vectorized_implied_volatility(data['option_prc'],
                                                            spot_prc,
                                                            data['strike_prc'],
                                                            data['T'],
                                                            rf,
                                                            option_type,
                                                            q=dividend,
                                                            return_as='series').fillna(0)

        def quantlib_cev_price(strike_prc, sigma, beta):
            payoff = ql.PlainVanillaPayoff(ql_option_type, strike_prc)
            european_option = ql.VanillaOption(payoff, eu_exercise)
            cev_engine = ql.AnalyticCEVEngine(future_prc, sigma, beta, yield_ts)
            european_option.setPricingEngine(cev_engine)
            try:
                return max(european_option.NPV(), 0)
            except RuntimeError:
                return 0

        def func(p):
            """
            需要最小化的函数，CEV模型IV与BSM IV的残差平方和
            :param p: array-like
            :return: 残差平方和
            """
            beta, sigma = p
            data['cev_prc'] = data.apply(lambda x: quantlib_cev_price(x['strike_prc'],
                                                                      sigma,
                                                                      beta), axis=1)
            data['model_vols'] = vectorized_implied_volatility(data['cev_prc'],
                                                               spot_prc,
                                                               data['strike_prc'],
                                                               data['T'],
                                                               rf,
                                                               option_type,
                                                               q=dividend,
                                                               return_as='series').fillna(0)
            return ((data['model_vols'] - data['market_vols']) ** 2).sum()

        if option_type == 'c':
            x0 = np.array([2, 0.1])
            bnds = ((1.01, None), (0.00, None))
        else:
            x0 = np.array([0.5, 0.1])
            bnds = ((0.01, 0.99), (0.00, None))

        minimize_result = optimize.minimize(func, x0=x0, bounds=bnds, method='L-BFGS-B')
        return minimize_result.x[0], minimize_result.x[1]

    beta_call, sigma_call = calibrate_cev(data_call.copy(), 'c')
    beta_put, sigma_put = calibrate_cev(data_put.copy(), 'p')
    return beta_call, sigma_call, beta_put, sigma_put


def get_cev(code, date, rf, df_contract, path_future, path_minbar, path_iv, output_path):
    """
    计算指定日期的CEV参数并将结果储存在本地
    :param code: '510050' or '510300' or '000300'
    :param date: 日期，格式为 '20210401'
    :param rf: 无风险利率（年化）
    :param df_contract: 储存期权合约信息的DataFrame
    :param path_future: 储存合成期货数据的路径
    :param path_minbar: minbar数据路径
    :param path_iv: 储存期权合约IV数据的路径
    :param output_path: CEV结果的输出路径
    :return: 将参数拟合结果保存至输出路径下
    """
    print('Calculating CEV parameters of %s' % date)

    path_future = os.path.join(path_future, '%s.csv' % date)
    df_future = pd.read_csv(path_future, index_col=0)
    path_minbar = os.path.join(path_minbar, date, '1min')
    path_stock = os.path.join(path_minbar, 'sh_%s_%s_1min.parquet' % (code, date))
    df_stock = pd.read_parquet(path_stock)
    df_stock = df_stock.loc[df_stock['datetime'].apply(lambda x:
                                                       x[-8:] not in ['14:57:00', '14:58:00', '14:59:00'])
                            ].reset_index(drop=True)
    path_iv = os.path.join(path_iv, date)
    df_contract = df_contract[df_contract['日期'] == date].reset_index(drop=True)
    df_result = pd.DataFrame(df_future['datetime'])
    df_result['idx'] = np.arange(0, len(df_result))

    option_dict = get_all_options(code, df_contract, path_iv)

    expirations = np.sort(df_contract['到期日'].apply(lambda x: x.strftime('%Y%m%d')).unique())
    for expiration_date in expirations:
        df_contract_exp = df_contract[df_contract['到期日'] == expiration_date].reset_index(drop=True)
        strike_prcs = np.sort(df_contract_exp['行权价'].unique())
        temp = df_result.apply(lambda x: get_minbar_cev_params(code,
                                                               date,
                                                               expiration_date,
                                                               rf,
                                                               x['idx'],
                                                               df_future,
                                                               df_stock,
                                                               strike_prcs,
                                                               option_dict,
                                                               get_all=True), axis=1)
        ym_maturities = expiration_date[2:6]
        df_result['beta_call_%s' % ym_maturities] = pd.Series([x[0] for x in temp])
        df_result['sigma_call_%s' % ym_maturities] = pd.Series([x[1] for x in temp])
        df_result['beta_put_%s' % ym_maturities] = pd.Series([x[2] for x in temp])
        df_result['sigma_put_%s' % ym_maturities] = pd.Series([x[3] for x in temp])

    del df_result['idx']
    outfile = os.path.join(output_path, '%s.csv' % date)
    df_result.to_csv(outfile)


