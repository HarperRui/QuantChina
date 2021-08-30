# -*- coding: utf-8 -*-
# @Time    : 2021/6/25 9:19
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : VolSurfPlot.py
# @Software: PyCharm

import numpy as np
import pandas as pd
import QuantLib as ql
import matplotlib.pyplot as plt
import seaborn as sns
import math
import os
import warnings
from zltsql import SQLConn
from scipy.stats import ncx2
from py_vollib_vectorized import vectorized_implied_volatility
sns.set_style('whitegrid')
warnings.filterwarnings('ignore')

#%%
# stock_code = '000300'
data_path = 'C:\\Users\\lenovo\\OneDrive\\桌面'
day_count = ql.Actual365Fixed()
calendar = ql.China()


def cev_value(S, K, T, sigma, beta, r, q, option_type):
    if beta == 1:
        return 0
    v = sigma*sigma/2/(r-q)/(beta-1) * (math.exp(2 * (r-q) * (beta-1) * T) - 1)
    a = (K * math.exp(-(r - q) * T)) ** (2 * (1 - beta)) / (1 - beta) ** 2 / v
    b = 1/(1-beta)
    c = S ** (2 * (1 - beta)) / (1 - beta) ** 2 / v

    if 0 < beta < 1:
        if option_type == 'c':
            values = S * math.exp(-q * T) * (1 - ncx2.cdf(a, b + 2, c)) - K * math.exp(-r * T) * ncx2.cdf(c, b, a)
        elif option_type == 'p':
            values = K * math.exp(-r * T) * (1 - ncx2.cdf(c, b, a)) - S * math.exp(-q * T) * ncx2.cdf(a, b + 2, c)
        else:
            print('Wrong option type')
            return
    elif beta > 1:
        if option_type == 'c':
            values = S * math.exp(-q * T) * (1 - ncx2.cdf(c, -b, a)) - K * math.exp(-r * T) * ncx2.cdf(a, 2 - b, c)
        elif option_type == 'p':
            values = K * math.exp(-r * T) * (1 - ncx2.cdf(a, 2 - b, c)) - S * math.exp(-q * T) * ncx2.cdf(c, -b, a)
        else:
            print('Wrong option type')
            return
    else:
        return 0

    return values


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


#%%
class Volsurf:
    def __init__(self, date, code, model_type, maturity=None):
        self.data_path = data_path
        self.date = date
        self.code = code
        self.model_type = model_type
        self.maturity = maturity
        self.contract_all = pd.DataFrame()
        self.contract = pd.DataFrame()
        self.options = {}
        self.strikes = []
        self.vols = []
        self.stock_prc = 0.0
        self.dividend = 0.0
        self.rf = 0.0
        self.model_strikes = []
        self.model_vols = []
        SQ = SQLConn()
        self.df_rf = SQ.GetData('rf')
        SQ.CloseSql()

        self.update_info(date, code, maturity)

    def update_info(self, new_date, new_code, new_maturity):
        self.set_contract_info(new_date, new_code)
        self.date = new_date
        self.code = new_code
        self.maturity = new_maturity
        self.set_options_info()

    def set_contract_info(self, new_date, new_code):
        if len(self.contract) == 0 or new_code != self.code:
            SQ = SQLConn()
            if new_code[0:3] == '510':  # ETF期权
                df_contract_all = SQ.GetData('contract_info_daily')
                df_contract_all = df_contract_all[df_contract_all['交易代码'].apply(lambda x: x.find('M') >= 0)]
                self.contract_all = df_contract_all[df_contract_all['期权标的'] == '%s.SH' % new_code].reset_index(drop=True)  # 保留50或300的期权信息
                self.contract = self.contract_all[self.contract_all['日期'] == new_date].reset_index(drop=True)
            elif new_code[0:3] == '000':  # 指数期权
                df_contract_info = SQ.GetData('contract_info_300index')  # contract_info
                df_contract_info.drop(['交易代码', '期权简称', '行权价'], axis=1, inplace=True)
                df_daily = SQ.GetData('日行情_300index')  # 日行情
                self.contract_all = pd.merge(df_daily, df_contract_info, on='期权代码')
                self.contract = self.contract_all[self.contract_all['日期'] == new_date].reset_index(drop=True)
            else:
                print('Wrong input stock_code')
                SQ.CloseSql()
                return
            SQ.CloseSql()
        elif new_date != self.date:
            self.contract = self.contract_all[self.contract_all['日期'] == new_date].reset_index(drop=True)

    def set_options_info(self):
        filepath = os.path.join(self.data_path, 'output_%s' % self.code, 'IV_Greeks', self.date)
        self.options = get_all_options(self.code, self.contract, filepath)
    
    def set_minbar_data(self, minbar_index):
        synthetic_future_path = os.path.join(self.data_path, 'output_%s' % self.code,
                                             'synthetic_futures', '%s.csv' % self.date)
        df_future = pd.read_csv(synthetic_future_path)
        self.dividend = df_future.at[minbar_index, 'q_%s' % self.maturity]

        if self.model_type.upper() == 'HESTON' or 'CEV':
            stock_minbar_path = os.path.join(self.data_path, 'minbar', 'stock', self.date, '1min',
                                             'sh_%s_%s_1min.parquet' % (self.code, self.date))
            df_stock = pd.read_parquet(stock_minbar_path)
            self.stock_prc = df_stock.at[minbar_index, 'open']
        elif self.model_type.upper() == 'SABR':
            self.stock_prc = df_future.at[minbar_index, self.maturity]
        else:
            print('Wrong model type')
            return

        self.rf = np.log(1 + self.df_rf.loc[self.df_rf['日期'] == self.date, '中债国债到期收益率：1年'].values[0] / 100)

    def get_plot(self, minbar_index):
        self.set_minbar_data(minbar_index)

        # srikes
        if self.code[0:3] == '510':
            contract_maturity = self.contract[self.contract['交易代码'].apply(lambda x: x[7:11].find(self.maturity) >= 0)]  # 在一个到期日的所有合约
        else:
            contract_maturity = self.contract[self.contract['交易代码'].apply(lambda x: x[2:6].find(self.maturity) >= 0)]
        contract_maturity.reset_index(inplace=True, drop=True)
        strikes = np.sort(contract_maturity['行权价'].unique()).astype('float')
        self.strikes = strikes

        # vols
        vols = []
        for k in self.strikes:
            callput = '认购' if k >= self.stock_prc else '认沽'
            option_code = contract_maturity.loc[(contract_maturity['行权价'] == k) &
                                                (contract_maturity['认购认沽'] == callput), '交易代码'].values[0]
            vols.append(self.options[option_code].at[minbar_index, 'IV_mid'])
        self.vols = vols

        if self.model_type.upper() == 'HESTON':
            start_date = ql.Date(int(self.date[-2:]), int(self.date[4:6]), int(self.date[0:4]))
            expiration_date = contract_maturity.at[0, '到期日'].strftime('%Y%m%d')
            expiration_date = ql.Date(int(expiration_date[-2:]), int(expiration_date[4:6]), int(expiration_date[0:4]))
            ql.Settings.instance().evaluationDate = start_date
            yield_ts = ql.YieldTermStructureHandle(ql.FlatForward(start_date, self.rf, day_count))
            dividend_ts = ql.YieldTermStructureHandle(ql.FlatForward(start_date, self.dividend, day_count))
            
            heston_param_path = os.path.join(self.data_path, 'output_%s' % self.code, 'Heston', '%s.csv' % self.date)
            df_heston = pd.read_csv(heston_param_path, index_col=0)
            theta, kappa, sigma, rho, v0 = df_heston.loc[minbar_index, [x for x in df_heston.columns
                                                                        if x[-4:] == self.maturity]].values
            process = ql.HestonProcess(yield_ts, dividend_ts, ql.QuoteHandle(ql.SimpleQuote(self.stock_prc)),
                                       v0, kappa, theta, sigma, rho)
            hestonModel = ql.HestonModel(process)
            hestonHandle = ql.HestonModelHandle(hestonModel)
            vol_surf = ql.HestonBlackVolSurface(hestonHandle)

            model_strikes = np.arange(self.strikes[0], self.strikes[-1], (self.strikes[-1] - self.strikes[0]) / 200)
            t = (expiration_date - start_date) / 365
            model_vols = [vol_surf.blackVol(t, x) for x in model_strikes]
        elif self.model_type.upper() == 'SABR':
            for tradingcode in self.options.keys():
                if self.code[0:3] == '510':
                    maturity = tradingcode[7:11]
                else:
                    maturity = tradingcode[2:6]
                if maturity == self.maturity:
                    t = self.options[tradingcode].at[minbar_index, 'expire_mins'] / (365*24*60)
                    break
                else:
                    continue
            sabr_param_path = os.path.join(self.data_path, 'output_%s' % self.code, 'SABR', '%s.csv' % self.date)
            df_sabr = pd.read_csv(sabr_param_path, index_col=0)
            alpha, beta, nu, rho = df_sabr.loc[minbar_index, [x for x in df_sabr.columns
                                                              if x[-4:] == self.maturity]].values
            model_strikes = np.arange(self.strikes[0], self.strikes[-1], (self.strikes[-1] - self.strikes[0]) / 200)
            model_vols = [ql.sabrVolatility(x, self.stock_prc, t, alpha, beta, nu, rho) for x in model_strikes]
        elif self.model_type.upper() == 'CEV':
            for tradingcode in self.options.keys():
                if self.code[0:3] == '510':
                    maturity = tradingcode[7:11]
                else:
                    maturity = tradingcode[2:6]
                if maturity == self.maturity:
                    t = self.options[tradingcode].at[minbar_index, 'expire_mins'] / (365*24*60)
                    break
                else:
                    continue
            cev_param_path = os.path.join(self.data_path, 'output_%s' % self.code, 'CEV', '%s.csv' % self.date)
            df_cev = pd.read_csv(cev_param_path, index_col=0)
            beta_call, sigma_call, beta_put, sigma_put = df_cev.loc[minbar_index, [x for x in df_cev.columns
                                                                                   if x[-4:] == self.maturity]]
            # split call and put
            strikes_call = strikes[strikes >= self.stock_prc]
            strikes_put = strikes[strikes < self.stock_prc]
            vols_put = vols[0: len(strikes_put)]
            vols_call = vols[len(strikes_put):]
            model_strikes_call = np.arange(strikes_call[0], strikes_call[-1],
                                           (strikes_call[-1] - strikes_call[0]) / 100)
            model_strikes_put = np.arange(strikes_put[0], strikes_put[-1],
                                          (strikes_put[-1] - strikes_put[0]) / 100)
            df_call = pd.DataFrame(model_strikes_call, columns=['strike_prc'])
            df_put = pd.DataFrame(model_strikes_put, columns=['strike_prc'])

            df_call['option_prc'] = df_call['strike_prc'].apply(lambda x: cev_value(self.stock_prc, x, t,
                                                                                    sigma_call, beta_call, self.rf,
                                                                                    self.dividend, 'c'))
            df_put['option_prc'] = df_put['strike_prc'].apply(lambda x: cev_value(self.stock_prc, x, t,
                                                                                  sigma_put, beta_put, self.rf,
                                                                                  self.dividend, 'p'))
            df_call['model_vols'] = df_call.apply(lambda x: vectorized_implied_volatility(x['option_prc'],
                                                                                          self.stock_prc,
                                                                                          x['strike_prc'],
                                                                                          t, self.rf, 'c',
                                                                                          q=self.dividend,
                                                                                          model='black_scholes_merton',
                                                                                          return_as='numpy')[0], axis=1)
            df_put['model_vols'] = df_put.apply(lambda x: vectorized_implied_volatility(x['option_prc'],
                                                                                        self.stock_prc,
                                                                                        x['strike_prc'],
                                                                                        t, self.rf, 'p',
                                                                                        q=self.dividend,
                                                                                        model='black_scholes_merton',
                                                                                        return_as='numpy')[0], axis=1)

            fig = plt.figure(figsize=(20, 7))
            fig.add_subplot(1, 2, 1)
            sns.lineplot('strike_prc', 'model_vols', data=df_call, label='CEV model')
            sns.scatterplot(strikes_call, vols_call, color='#980002', label='BSM IV')
            plt.xlabel('Strike price')
            plt.ylabel('Implied volatility')
            plt.title('CEV call calibration result of %s_%s' % (self.date, self.maturity))
            fig.add_subplot(1, 2, 2)
            sns.lineplot('strike_prc', 'model_vols', data=df_put, label='CEV model')
            sns.scatterplot(strikes_put, vols_put, color='#980002', label='BSM IV')
            plt.xlabel('Strike price')
            plt.ylabel('Implied volatility')
            plt.title('CEV put calibration result of %s_%s' % (self.date, self.maturity))
            plt.show()
            return

            # prcs = []
            # for k in self.strikes:
            #     callput = '认购' if k >= self.stock_prc else '认沽'
            #     option_code = contract_maturity.loc[(contract_maturity['行权价'] == k) &
            #                                         (contract_maturity['认购认沽'] == callput), '交易代码'].values[0]
            #     prcs.append(self.options[option_code].at[minbar_index, 'first_mid_prc1'])
            # prcs_put = prcs[0: len(strikes_put)]
            # prcs_call = prcs[len(strikes_put):]
            #
            # fig = plt.figure(figsize=(20, 7))
            # fig.add_subplot(1, 2, 1)
            # sns.lineplot('strike_prc', 'option_prc', data=df_call, label='CEV model')
            # sns.scatterplot(strikes_call, prcs_call, color='#980002', label='Market price')
            # plt.xlabel('Strike price')
            # plt.ylabel('Option price')
            # plt.title('CEV call calibration result of %s_%s' % (self.date, self.maturity))
            # fig.add_subplot(1, 2, 2)
            # sns.lineplot('strike_prc', 'option_prc', data=df_put, label='CEV model')
            # sns.scatterplot(strikes_put, prcs_put, color='#980002', label='Market price')
            # plt.xlabel('Strike price')
            # plt.ylabel('Option price')
            # plt.title('CEV put calibration result of %s_%s' % (self.date, self.maturity))
            # plt.show()
            # return

        else:
            print('Wrong model type')
            return

        self.model_strikes = model_strikes
        self.model_vols = model_vols
        # plot result
        fig = plt.figure(figsize=(10, 7))
        sns.lineplot(model_strikes, model_vols, label='%s model' % self.model_type.upper())
        sns.scatterplot(strikes, vols, color='#980002', label='BSM IV')
        plt.xlabel('Strike price')
        plt.ylabel('Implied volatility')
        plt.title('%s calibration result of %s_%s' % (self.model_type.upper(), self.date, self.maturity))
        plt.show()
        return


#%% test
# model = Volsurf('20210401', '000300', 'Heston', '2104')
# model.get_plot(211)
# # change model type
# model.model_type = 'sabr'
# model.get_plot(211)
# # test 'update_info' method
# model.update_info('20210412', '510050', '2105')
# model.get_plot(211)
model = Volsurf('20210401', '510050', 'SABR', '2104')
model.get_plot(211)










