# -*- coding: utf-8 -*-
# @Time    : 2021/6/18 14:46
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : Heston.py
# @Software: PyCharm

import numpy as np
import pandas as pd
import QuantLib as ql
from scipy import optimize
import os
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
sns.set_style('whitegrid')
warnings.filterwarnings("ignore")


#%%
day_count = ql.Actual365Fixed()
calendar = ql.China()


class Heston(ql.HestonModel):
    def __init__(self, start_date, spot_prc, rf, dividend, expiration_date, strike_prcs, vols, init_params=(0.05,1,1,-0.1,0.03)):
        self.start_date = start_date
        self.spot_prc = spot_prc
        self.expiration_date = expiration_date
        ql.Settings.instance().evaluationDate = self.start_date
        self.strike_prcs = strike_prcs.astype('float')
        self.vols = vols
        self.init_params = init_params
        theta, kappa, sigma, rho, v0 = self.init_params
        self.yield_ts = ql.YieldTermStructureHandle(ql.FlatForward(start_date, rf, day_count))
        self.dividend_ts = ql.YieldTermStructureHandle(ql.FlatForward(start_date, dividend, day_count))
        process = ql.HestonProcess(self.yield_ts, self.dividend_ts, ql.QuoteHandle(ql.SimpleQuote(spot_prc)),
                                   v0, kappa, theta, sigma, rho)
        super().__init__(process)
        self.vol_surf = ql.HestonBlackVolSurface(ql.HestonModelHandle(self))

    def calibrate_params(self):
        def func(p):
            """
            需要最小化的函数，Heston模型波动率与隐含波动率的残差平方和
            :param p: array-like
            :return: 残差平方和
            """
            theta, kappa, sigma, rho, v0 = p
            self.setParams(ql.Array([theta, kappa, sigma, rho, v0]))
            model_vol = np.array([self.vol_surf.blackVol((self.expiration_date - self.start_date)/365, k) for k in self.strike_prcs])
            real_vol = np.array(self.vols)
            return ((model_vol - real_vol) ** 2).sum()

        x0 = self.init_params
        bnds = ((0, None), (0.01, None), (0.01, None), (-1, 1), (0, None))
        minimize_result = optimize.minimize(func, x0=x0, bounds=bnds, method='L-BFGS-B')

    def get_plot(self):
        strf_expdate = self.expiration_date.to_date().strftime('%Y%m')[-4:]
        strf_startdate = self.start_date.to_date().strftime('%Y%m%d')
        t = (self.expiration_date - self.start_date) / 365
        X = np.arange(3.0, 4.5, 0.01)
        Y = [self.vol_surf.blackVol(t, k) for k in X]
        fig = plt.figure(figsize=(10, 7))
        sns.lineplot(X, Y, label='Heston')
        sns.scatterplot(self.strike_prcs, self.vols, color='#980002', label='BSM IV')
        plt.xlabel('Strike price')
        plt.ylabel('Implied volatility')
        plt.title('Heston calibration result of %s_%s' % (strf_startdate, strf_expdate))
        plt.show()


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


def get_minbar_heston_params(code, date, expiration_date, rf, minbar_idx, path_minbar, df_future, strike_prcs, option_dict, get_all=False):
    """
    根据交易日期、期权到期时间及分钟bar所在的index计算对应的Heston model parameters
    :param code: 标的ETF的代码，'510050' or '510300'
    :param date: 交易日期，格式为'20210401'
    :param expiration_date: 期权的到期日，格式为'20210428'
    :param rf: 无风险利率（年化）
    :param minbar_idx: 分钟所在的index
    :param path_minbar: 交易当天期权合约和标的ETF minbar data所在的路径
    :param df_future: 含有交易当天合成期货价格和分红率的DataFrame
    :param strike_prcs: 行权价，ndarray
    :param option_dict: 当天所有的期权合约, dict
    :param get_all: 是否使用所有行权价的样本进行拟合
    :return: theta, kappa, sigma, rho, v0
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

    if date == expiration_date:
        return 0, 0, 0, 0, 0

    path_etf = os.path.join(path_minbar, 'sh_%s_%s_1min.parquet' % (code, date))
    df_etf = pd.read_parquet(path_etf)
    """剔除掉集合竞价多余的分钟数据"""
    df_etf = df_etf.loc[df_etf['datetime'].apply(lambda x:
                                                 x[-8:] not in ['14:57:00', '14:58:00', '14:59:00'])
                        ].reset_index(drop=True)
    spot_prc = df_etf.at[minbar_idx, 'open']
    ym_maturities = expiration_date[2:6]
    dividend = df_future.at[minbar_idx, 'q_%s' % ym_maturities]
    start_date = ql.Date(int(date[-2:]), int(date[4:6]), int(date[0:4]))
    expiration_date = ql.Date(int(expiration_date[-2:]), int(expiration_date[4:6]), int(expiration_date[0:4]))

    if not get_all:
        num = 6 if code[0:3] == '510' else 12
        selected_strikes = find_nearest_value(strike_prcs.copy(), spot_prc, num)
        threshold_low = selected_strikes[0]
        threshold_high = selected_strikes[-1]
        strike_prcs = strike_prcs[(strike_prcs >= threshold_low) & (strike_prcs <= threshold_high)]
    vols = []

    if code[0:3] == '510':
        for k in strike_prcs:
            if k >= spot_prc:
                trading_code = code + 'C' + ym_maturities + 'M0' + str(int(k * 1000))
            else:
                trading_code = code + 'P' + ym_maturities + 'M0' + str(int(k * 1000))
            vols.append(option_dict[trading_code].at[minbar_idx, 'IV_mid'])
    else:
        for k in strike_prcs:
            if k >= spot_prc:
                trading_code = 'IO%s-C-%i' % (ym_maturities, k)
            else:
                trading_code = 'IO%s-P-%i' % (ym_maturities, k)
            vols.append(option_dict[trading_code].at[minbar_idx, 'IV_mid'])

    h = Heston(start_date, spot_prc, rf, dividend, expiration_date, strike_prcs, vols)
    h.calibrate_params()
    theta, kappa, sigma, rho, v0 = h.params()
    return theta, kappa, sigma, rho, v0


def get_heston(code, date, rf, df_contract, path_future, path_minbar, path_iv, output_path):
    """
    计算指定日期的Heston参数并将结果储存在本地
    :param code: '510050' or '510300' or '000300'
    :param date: 日期，格式为 '20210401'
    :param rf: 无风险利率（年化）
    :param df_contract: 储存期权合约信息的DataFrame
    :param path_future: 储存合成期货数据的路径
    :param path_minbar: minbar数据路径
    :param path_iv: 储存期权合约IV数据的路径
    :param output_path: Heston结果的输出路径
    :return: 将参数拟合结果保存至输出路径下
    """
    print('Calculating Heston parameters of %s' % date)
    path_future = os.path.join(path_future, '%s.csv' % date)
    df_future = pd.read_csv(path_future, index_col=0)
    path_minbar = os.path.join(path_minbar, date, '1min')
    path_iv = os.path.join(path_iv, date)
    df_contract = df_contract[df_contract['日期'] == date].reset_index(drop=True)
    df_result = pd.DataFrame(df_future['datetime'])
    df_result['idx'] = np.arange(0, len(df_result))

    option_dict = get_all_options(code, df_contract, path_iv)

    expirations = np.sort(df_contract['到期日'].apply(lambda x: x.strftime('%Y%m%d')).unique())
    for expiration_date in expirations:
        df_contract_exp = df_contract[df_contract['到期日'] == expiration_date].reset_index(drop=True)
        strike_prcs = np.sort(df_contract_exp['行权价'].unique())
        ym_maturities = expiration_date[2:6]

        # 逐行
        # for j in range(len(df_result)):
        #     print(j)
        #     temp = get_minbar_heston_params(code, date, expiration_date, rf, df_result.at[j, 'idx'], path_minbar,
        #                                     df_future, strike_prcs, option_dict)
        #     df_result.at[j, 'theta_%s' % ym_maturities] = temp[0]
        #     df_result.at[j, 'kappa_%s' % ym_maturities] = temp[1]
        #     df_result.at[j, 'sigma_%s' % ym_maturities] = temp[2]
        #     df_result.at[j, 'rho_%s' % ym_maturities] = temp[3]
        #     df_result.at[j, 'v0_%s' % ym_maturities] = temp[4]

        # apply运行
        temp = df_result.apply(lambda x: get_minbar_heston_params(code,
                                                                  date,
                                                                  expiration_date,
                                                                  rf,
                                                                  x['idx'],
                                                                  path_minbar,
                                                                  df_future,
                                                                  strike_prcs,
                                                                  option_dict), axis=1)
        df_result['theta_%s' % ym_maturities] = pd.Series([x[0] for x in temp])
        df_result['kappa_%s' % ym_maturities] = pd.Series([x[1] for x in temp])
        df_result['sigma_%s' % ym_maturities] = pd.Series([x[2] for x in temp])
        df_result['rho_%s' % ym_maturities] = pd.Series([x[3] for x in temp])
        df_result['v0_%s' % ym_maturities] = pd.Series([x[4] for x in temp])

    del df_result['idx']
    outfile = os.path.join(output_path, '%s.csv' % date)
    df_result.to_csv(outfile)
    

#%%
# code = '510050'
# df_contract_info = pd.read_parquet('C:/Users/lenovo/OneDrive/桌面/contract_info.parquet')
# df_rf = pd.read_parquet('C:/Users/lenovo/OneDrive/桌面/rf.parquet')
#
# #%%
# date = '20210401'
# future_path = 'E:/工作/ZLT/Data/output_510050/synthetic_futures'
# minbar_path = 'E:/工作/ZLT/Data/minbar/stock'
# iv_path = 'E:/工作/ZLT/Data/output_510050/IV_Greeks'
# output_path = 'C:/Users/lenovo/OneDrive/桌面'
# rf = np.log(1 + df_rf.loc[df_rf['日期'] == date, '中债国债到期收益率：1年'].values[0] / 100)
# get_heston(code=code, date=date, rf=rf, df_contract=df_contract_info,
#            path_future=future_path, path_minbar=minbar_path, path_iv=iv_path, output_path=output_path)






