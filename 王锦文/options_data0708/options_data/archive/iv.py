# -*- coding: utf-8 -*-
# @Time    : 2021/5/25 15:01
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : iv.py
# @Software: PyCharm

import numpy as np
import pandas as pd
import datetime
import os
import glob
from py_vollib_vectorized import vectorized_implied_volatility
from greeks import *


# %%

def get_mid_prc(pre_settle_prc, settle_prc, bid, ask, vol):
    """
    计算 mid price
    :param pre_settle_prc: 前结算价
    :param settle_prc: 成交价
    :param bid: 买价
    :param ask: 卖价
    :param vol: 成交量
    :return: 中间价，float
    """
    if vol:
        # 该分钟有成交
        if bid and ask:
            # 存在买卖报价
            return settle_prc if bid <= settle_prc <= ask else (bid + ask) / 2  # 最新成交价处于买卖报价之间，取最新成交价，否则用中间价
        elif bid:
            # 仅存在买价
            return max(settle_prc, bid)  # 成交价和买价中的最大值
        elif ask:
            # 仅存在卖价
            return min(settle_prc, ask)  # 成交价和卖价的最小值
        else:
            # 不存在买价和卖价
            return settle_prc  # 返回成交价
    else:
        # 该分钟没有成交
        if bid and ask:
            # 存在买卖报价
            return (bid + ask) / 2  # 取中间价
        elif bid:
            # 仅存在买价
            return max(pre_settle_prc, bid)  # 前结算价和买价的最大值
        elif ask:
            # 仅存在卖价
            return min(pre_settle_prc, ask)  # 前结算价和卖价的最小值
        else:
            # 不存在买卖报价
            return pre_settle_prc  # 返回前结算价


def get_iv_greeks(code, date, rf, df_contract_all, df_contract_code, input_stock_path, input_future_path, synthetic_future_path, output_path):
    """
    在minbar数据中添加期权基本信息以及bid,ask,mid,close四个价格对应的IV和Greeks
    :param code: 标的资产代码，'510050' or '510300'
    :param date: 处理当天的日期，格式为 '20210521'
    :param rf: 年化的无风险利率
    :param df_contract_all: 包含每日期权信息的 Dataframe（包含所有50和300合约）
    :param df_contract_code: 包含每日期权信息的 Dataframe(只包含当前标的的合约)
    :param input_stock_path: 输入的minbar股票数据路径
    :param input_future_path: minbar期货数据路径
    :param output_path: 输出结果的路径
    :param synthetic_future_path: 合成期货的路径
    :return: 将处理好的minbar数据保存
    """
    future_daily_path = os.path.join(synthetic_future_path, '%s.csv' % date)
    df_future_daily = pd.read_csv(future_daily_path, index_col=0)  # 当天合成期货minbar数据
    daily_contract_code = df_contract_code[df_contract_code['日期'] == date].reset_index(drop=True)  # 当天的期权合约信息(只包含50、300中的一种)
    daily_contract_all = df_contract_all[df_contract_all['日期'] == date].reset_index(drop=True)  # 当天的期权合约信息汇总(包括50和300）
    stock_minbar_path = os.path.join(input_stock_path, date, '1min')
    future_minbar_path = os.path.join(input_future_path, date, '1min')
    outfile_path = os.path.join(output_path, date)

    if not os.path.exists(outfile_path):
        os.makedirs(outfile_path)
    stockpath = os.path.join(stock_minbar_path, 'sh_%s_%s_1min.parquet' % (code, date))  # 标的 minbar路径
    if os.path.exists(stockpath):
        df_stock = pd.read_parquet(stockpath)  # 标的 minbar数据
    else:
        print("========== No %s file in %s ==========" % (code, date))
        return
    
    if code[0:3] == '510':
        option_list = glob.glob(os.path.join(stock_minbar_path, 'sh_**_%s_1min.parquet' % date))
        option_list = [x for x in option_list if x[-31] == '_']  # 包含当天所有期权合约文件名称的list
    else:
        option_list = glob.glob(os.path.join(future_minbar_path, 'cfe_io**_%s_1min.parquet' % date))
    missing_data = []
    for option in range(len(option_list)):
        print('Start to process option %i, %i left' % (option, len(option_list) - option - 1))
        df_option = pd.read_parquet(option_list[option])
        # symbol = int(df_option['symbol'][0][:-3])
        symbol = df_option['symbol'][0].split('.')[0]
        symbol = int(symbol) if symbol[0] != 'I' else symbol
        if (daily_contract_code['期权代码'] == symbol).sum() != 0:
            # 如果指定标的的'contract_info_daily'文件中有该合约

            """剔除掉集合竞价多余的分钟数据"""
            df_option = df_option.loc[df_option['datetime'].apply(lambda x:
                                                                  x[-8:] not in ['14:57:00', '14:58:00', '14:59:00'])
                                      ].reset_index(drop=True)

            """############# 处理ask和bid的异常值 #############"""
            # 第一档挂单为0但其他档位挂单不为0, 价格填充0
            if code[0:3] == '510':
                df_option['first_ask_prc1'] = df_option.apply(lambda x: 0 if ((x['first_ask_vol1'] == 0 and
                                                                              (x['first_ask_vol2'] +
                                                                               x['first_ask_vol3'] +
                                                                               x['first_ask_vol4'] +
                                                                               x['first_ask_vol5']) != 0) or
                                                                              (x['first_ask_vol1'] +
                                                                               x['first_ask_vol2'] +
                                                                               x['first_ask_vol3'] +
                                                                               x['first_ask_vol4'] +
                                                                               x['first_ask_vol5'] == 0)
                                                                              ) else x['first_ask_prc1'], axis=1)
                df_option['first_ask_prc1'] = df_option['first_ask_prc1'].fillna(0)

                df_option['first_bid_prc1'] = df_option.apply(lambda x: 0 if ((x['first_bid_vol1'] == 0 and
                                                                              (x['first_bid_vol2'] +
                                                                               x['first_bid_vol3'] +
                                                                               x['first_bid_vol4'] +
                                                                               x['first_bid_vol5']) != 0) or
                                                                              (x['first_bid_vol1'] +
                                                                               x['first_bid_vol2'] +
                                                                               x['first_bid_vol3'] +
                                                                               x['first_bid_vol4'] +
                                                                               x['first_bid_vol5'] == 0)
                                                                              ) else x['first_bid_prc1'], axis=1)
                df_option['first_bid_prc1'] = df_option['first_bid_prc1'].fillna(0)
            """############# 处理ask和bid的异常值 #############"""
            option_index = (daily_contract_code['期权代码'] == symbol).argmax()

            # 如果是中信的数据，根据合约单位对price进行调整
            minbar_close = df_option['close'].max()
            real_close = daily_contract_code.at[option_index, '收盘价']
            if minbar_close / real_close > 1000:
                unit = daily_contract_code.at[option_index, '合约单位']
                price_names = ['open', 'high', 'low', 'close'] + [x for x in df_option.columns if 'prc' in x]
                for price_name in price_names:
                    df_option[price_name] /= unit

            df_option['pre_close_prc'] = df_option['close'].shift(1).fillna(method='bfill')  # 前结算价
            df_option['first_mid_prc1'] = df_option.apply(lambda x: get_mid_prc(x['pre_close_prc'],
                                                                                x['open'],
                                                                                x['first_bid_prc1'],
                                                                                x['first_ask_prc1'],
                                                                                x['volume']), axis=1)  # 添加 mid price

            df_option['first_close_prc1'] = df_option.apply(lambda x: x['close'] if x['volume'] != 0 else x['first_mid_prc1'], axis=1)
            df_option['trading_code'] = daily_contract_code.at[option_index, '交易代码']  # 添加交易代码
            df_option['asset_code'] = daily_contract_code.at[option_index, '期权标的']  # 添加标的代码
            df_option['callput'] = 'c' if daily_contract_code.at[option_index, '认购认沽'] == '认购' else 'p'  # 添加认购认沽
            df_option['strike_prc'] = daily_contract_code.at[option_index, '行权价']  # 添加行权价
            expire_time = str(daily_contract_code.at[option_index, '到期日'])[0:10] + ' 15:00:00'
            expire_time = datetime.datetime.strptime(expire_time, '%Y-%m-%d %H:%M:%S')  # 期权到期时间
            df_option['expire_mins'] = df_option['datetime'].apply(lambda x: (expire_time - datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')).total_seconds() / 60)  # 距离到期的分钟数
            option_monthdate = df_option['trading_code'][0][7:11] if code[0:3] == '510' else df_option['trading_code'][0][2:6]  # 期权合约到期月
            df_option['stock_prc'] = df_stock['open']  # 标的资产价格
            df_option['q'] = df_future_daily['q_%s' % option_monthdate]  # 分红率
            option_type = df_option['callput'][0]

            # IV, Greeks
            for price in ['bid', 'ask', 'mid', 'close']:
                try:
                    df_option['IV_%s' % price] = vectorized_implied_volatility(df_option['first_%s_prc1' % price],
                                                                               df_option['stock_prc'],
                                                                               df_option['strike_prc'],
                                                                               df_option['expire_mins'] / (365 * 24 * 60),
                                                                               rf,
                                                                               option_type,
                                                                               q=df_option['q'],
                                                                               model='black_scholes_merton',
                                                                               return_as='series').fillna(0)

                    df_option['Delta_%s' % price] = df_option.apply(lambda x: Delta(x['stock_prc'],
                                                                                    x['strike_prc'],
                                                                                    x['expire_mins'] / (365 * 24 * 60),
                                                                                    rf,
                                                                                    x['q'],
                                                                                    x['IV_%s' % price],
                                                                                    option_type), axis=1)
                    df_option['Gamma_%s' % price] = df_option.apply(lambda x: Gamma(x['stock_prc'],
                                                                                    x['strike_prc'],
                                                                                    x['expire_mins'] / (365 * 24 * 60),
                                                                                    rf,
                                                                                    x['q'],
                                                                                    x['IV_%s' % price]), axis=1)
                    df_option['Vega_%s' % price] = df_option.apply(lambda x: Vega(x['stock_prc'],
                                                                                  x['strike_prc'],
                                                                                  x['expire_mins'] / (365 * 24 * 60),
                                                                                  rf,
                                                                                  x['q'],
                                                                                  x['IV_%s' % price]), axis=1)
                    df_option['Theta_%s' % price] = df_option.apply(lambda x: Theta(x['stock_prc'],
                                                                                    x['strike_prc'],
                                                                                    x['expire_mins'] / (365 * 24 * 60),
                                                                                    rf,
                                                                                    x['q'],
                                                                                    x['IV_%s' % price],
                                                                                    option_type), axis=1)
                    df_option['Rho_%s' % price] = df_option.apply(lambda x: Rho(x['stock_prc'],
                                                                                x['strike_prc'],
                                                                                x['expire_mins'] / (365 * 24 * 60),
                                                                                rf,
                                                                                x['q'],
                                                                                x['IV_%s' % price],
                                                                                option_type), axis=1)

                except ZeroDivisionError:
                    df_option['IV_%s' % price] = 0
                    df_option['Delta_%s' % price] = 0
                    df_option['Gamma_%s' % price] = 0
                    df_option['Vega_%s' % price] = 0
                    df_option['Theta_%s' % price] = 0
                    df_option['Rho_%s' % price] = 0
            # 储存文件
            if code[0:3] == '510':
                outfile = os.path.join(outfile_path, '%i.parquet' % symbol)
            else:
                outfile = os.path.join(outfile_path, '%s.parquet' % symbol)
            df_option.to_parquet(outfile)

        elif (daily_contract_all['期权代码'] == symbol).sum() == 0:
            # 如果'contract_info_daily'中缺少该期权合约
            missing_data.append(symbol)

    if missing_data:
        print("========= 'contract_info_daily' miss the following options in %s: =========" % date)
        for i in missing_data:
            print(i)
        print("=================================================================================")
