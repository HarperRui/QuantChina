# -*- coding: utf-8 -*-
# @Time    : 2021/6/9 14:28
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : key_point.py
# @Software: PyCharm

import numpy as np
import pandas as pd
from scipy import interpolate
import os

target_30 = 30 / 365
target_60 = 60 / 365
target_90 = 90 / 365


#%%
def get_keypoint_cubic(t1, t2, t3, t4, iv1, iv2, iv3, iv4):
    """
    三次样条插值计算30,60,90天波动率
    :param t1: 当月合约的TTM（年化）
    :param t2: 次月合约的TTM（年化）
    :param t3: 当季月合约的TTM（年化）
    :param t4: 下季月合约的TTM（年化）
    :param iv1: 当月合约的IV
    :param iv2: 次月合约的IV
    :param iv3: 当季月合约的IV
    :param iv4: 下季月合约的IV
    :return: 30,60,90天的波动率，list
    """
    time_list = [t1, t2, t3, t4]
    iv_list = [iv1, iv2, iv3, iv4]
    try:
        f = interpolate.interp1d(time_list, iv_list, kind='cubic')
        return f([target_30, target_60, target_90])
    except ValueError:
        f = interpolate.interp1d(time_list, iv_list, kind='cubic', fill_value="extrapolate")
        return f([target_30, target_60, target_90])


def get_keypoint_simple(t1, t2, t3, t4, iv1, iv2, iv3, iv4):
    """
    简单线性插值计算30,60,90天波动率
    :param t1: 当月合约的TTM（年化）
    :param t2: 次月合约的TTM（年化）
    :param t3: 当季月合约的TTM（年化）
    :param t4: 下季月合约的TTM（年化）
    :param iv1: 当月合约的IV
    :param iv2: 次月合约的IV
    :param iv3: 当季月合约的IV
    :param iv4: 下季月合约的IV
    :return: 30,60,90天的波动率，list
    """
    def get_simple_inter(low_point, high_point, target_point, low_value, high_value):
        return low_value + (high_value-low_value) * (target_point - low_point) / (high_point - low_point)

    if target_30 <= t2:
        result_30 = get_simple_inter(t1, t2, target_30, iv1, iv2)
    else:
        result_30 = get_simple_inter(t2, t3, target_30, iv2, iv3)
    if iv1 == 0:  # 当天为当月合约的到期日
        result_30 = get_simple_inter(t2, t3, target_30, iv2, iv3)
    
    if target_60 <= t3:
        result_60 = get_simple_inter(t2, t3, target_60, iv2, iv3)
    else:
        result_60 = get_simple_inter(t3, t4, target_60, iv3, iv4)

    if target_90 <= t3:
        result_90 = get_simple_inter(t2, t3, target_90, iv2, iv3)
    else:
        result_90 = get_simple_inter(t3, t4, target_90, iv3, iv4)

    return [result_30, result_60, result_90]


def get_keypoint(code, date, input_iv_path, input_termstructure_path, output_path, df_contract):
    """
    计算给定日期的30,60,90天波动率并将结果储存至输出路径
    :param code: '510050' or '510300' or '000300'
    :param date: 日期，格式为 '20210401'
    :param input_iv_path: 储存期权合约IV数据的路径
    :param input_termstructure_path: 储存期限结构数据的路径
    :param output_path: 结果输出路径
    :param df_contract: 储存期权合约信息的DataFrame
    :return: 将插值结果保存至输出路径下
    """
    print('Calculating key point of %s' % date)
    options_daily_path = os.path.join(input_iv_path, date)
    termstructure_daily_path = os.path.join(input_termstructure_path, '%s_1min.csv' % date)
    df_termstructure_daily = pd.read_csv(termstructure_daily_path, index_col=0)  # 当天期限结构minbar数据
    df_contract_daily = df_contract[df_contract['日期'] == date].reset_index(drop=True)  # 当天所有的合约（剔除掉除权合约）

    maturities = pd.Series([x[0:4] for x in df_termstructure_daily.columns[1:]]).unique()  # 四个到期时间
    keypoint_result = pd.DataFrame(df_termstructure_daily['datetime'])  # 储存输出结果

    """对于每个到期日，添加距离到期的时间（年化）"""
    for yearmonth in maturities:
        if code[0:3] == '510':
            df_contract_ym = df_contract_daily[df_contract_daily['交易代码'].apply(lambda x: x.find(yearmonth) == 7)].reset_index(drop=True)
        else:
            df_contract_ym = df_contract_daily[
                df_contract_daily['交易代码'].apply(lambda x: x.find(yearmonth) == 2)].reset_index(drop=True)
        i = 0
        while i <= len(df_contract_ym) - 1:
            option_code = df_contract_ym.at[i, '期权代码']
            option_path = os.path.join(options_daily_path, '%s.parquet' % option_code)
            if os.path.exists(option_path):
                df_option = pd.read_parquet(option_path)
                df_termstructure_daily['T_%s' % yearmonth] = df_option['expire_mins'].values / (365 * 24 * 60)
                break
            else:
                print('%s not in minbar data' % option_code)
                i += 1

    cubic_call = df_termstructure_daily.apply(lambda x: get_keypoint_cubic(x['T_%s' % maturities[0]],
                                                                           x['T_%s' % maturities[1]],
                                                                           x['T_%s' % maturities[2]],
                                                                           x['T_%s' % maturities[3]],
                                                                           x['%s_0.5_call_cubic' % maturities[0]],
                                                                           x['%s_0.5_call_cubic' % maturities[1]],
                                                                           x['%s_0.5_call_cubic' % maturities[2]],
                                                                           x['%s_0.5_call_cubic' % maturities[3]]),
                                              axis=1)
    cubic_put = df_termstructure_daily.apply(lambda x: get_keypoint_cubic(x['T_%s' % maturities[0]],
                                                                          x['T_%s' % maturities[1]],
                                                                          x['T_%s' % maturities[2]],
                                                                          x['T_%s' % maturities[3]],
                                                                          x['%s_0.5_put_cubic' % maturities[0]],
                                                                          x['%s_0.5_put_cubic' % maturities[1]],
                                                                          x['%s_0.5_put_cubic' % maturities[2]],
                                                                          x['%s_0.5_put_cubic' % maturities[3]]),
                                             axis=1)
    simple_call = df_termstructure_daily.apply(lambda x: get_keypoint_simple(x['T_%s' % maturities[0]],
                                                                             x['T_%s' % maturities[1]],
                                                                             x['T_%s' % maturities[2]],
                                                                             x['T_%s' % maturities[3]],
                                                                             x['%s_0.5_call_simple' % maturities[0]],
                                                                             x['%s_0.5_call_simple' % maturities[1]],
                                                                             x['%s_0.5_call_simple' % maturities[2]],
                                                                             x['%s_0.5_call_simple' % maturities[3]]),
                                               axis=1)
    simple_put = df_termstructure_daily.apply(lambda x: get_keypoint_simple(x['T_%s' % maturities[0]],
                                                                            x['T_%s' % maturities[1]],
                                                                            x['T_%s' % maturities[2]],
                                                                            x['T_%s' % maturities[3]],
                                                                            x['%s_0.5_put_simple' % maturities[0]],
                                                                            x['%s_0.5_put_simple' % maturities[1]],
                                                                            x['%s_0.5_put_simple' % maturities[2]],
                                                                            x['%s_0.5_put_simple' % maturities[3]]),
                                              axis=1)

    keypoint_result['30_call_cubic'] = pd.Series([x[0] for x in cubic_call])
    keypoint_result['60_call_cubic'] = pd.Series([x[1] for x in cubic_call])
    keypoint_result['90_call_cubic'] = pd.Series([x[2] for x in cubic_call])
    keypoint_result['30_call_simple'] = pd.Series([x[0] for x in simple_call])
    keypoint_result['60_call_simple'] = pd.Series([x[1] for x in simple_call])
    keypoint_result['90_call_simple'] = pd.Series([x[2] for x in simple_call])

    keypoint_result['30_put_cubic'] = pd.Series([x[0] for x in cubic_put])
    keypoint_result['60_put_cubic'] = pd.Series([x[1] for x in cubic_put])
    keypoint_result['90_put_cubic'] = pd.Series([x[2] for x in cubic_put])
    keypoint_result['30_put_simple'] = pd.Series([x[0] for x in simple_put])
    keypoint_result['60_put_simple'] = pd.Series([x[1] for x in simple_put])
    keypoint_result['90_put_simple'] = pd.Series([x[2] for x in simple_put])

    outfile = os.path.join(output_path, '%s_1min.csv' % date)
    keypoint_result.to_csv(outfile)

    # 取5min的平均值
    outfile_5min = os.path.join(output_path, '%s_5min.csv' % date)
    keypoint_result['datetime'] = pd.to_datetime(keypoint_result['datetime'])
    result_5min = keypoint_result.set_index('datetime').resample('5T').mean().dropna().reset_index()
    result_5min.to_csv(outfile_5min)

#%%
# input_term_structure_path = 'C:\\Users\\lenovo\\OneDrive\\桌面\\output\\term_structure'  # term_structure数据路径
# input_iv_path = 'C:\\Users\\lenovo\\OneDrive\\桌面\\output'  # IV路径
# key_point_outpath = 'C:\\Users\\lenovo\\OneDrive\\桌面\\output\\key_point'  # 期限插值结果输出路径
# if not os.path.exists(key_point_outpath):
#     os.makedirs(key_point_outpath)

#%% 获取contract_info数据
# code = '510050'
# SQ = SQLConn()
# df_contract_all = SQ.GetData('contract_info_daily')
# df_contract_code = df_contract_all[df_contract_all['期权标的'] == '%s.SH' % code]  # 只包含50 or 300
# df_contract_M = df_contract_code[df_contract_code['交易代码'].apply(lambda x: x.find('M') >= 0)]  # 剔除除权合约
# SQ.CloseSql()

#%%
# date_range = [pd.to_datetime(x).strftime('%Y%m%d') for x in df_contract_code['日期'].unique()]
# date_range = [x for x in date_range if x[0:6] == '202104']  # 2021年4月
#
# target_30 = 30 / 365
# target_60 = 60 / 365
# target_90 = 90 / 365
# for date in date_range:
#     print('Start to process the data of %s' % date)
#     options_daily_path = os.path.join(input_iv_path, date)
#     termstructure_daily_path = os.path.join(input_term_structure_path, '%s_1min.csv' % date)
#     df_termstructure_daily = pd.read_csv(termstructure_daily_path, index_col=0)  # 当天合成期货minbar数据
#     df_contract_daily = df_contract_M[df_contract_M['日期'] == date].reset_index(drop=True)  # 当天所有的合约（剔除掉除权合约）
#
#     maturities = pd.Series([x[0:4] for x in df_termstructure_daily.columns[1:]]).unique()  # 四个到期时间
#     inter_result = pd.DataFrame(df_termstructure_daily['datetime'])  # 储存输出结果
#
#     """对于每个到期日，添加距离到期的时间（年化）"""
#     for yearmonth in maturities:
#         df_contract_ym = df_contract_daily[df_contract_daily['交易代码'].apply(lambda x: x.find(yearmonth) == 7)].reset_index(drop=True)
#         i = 0
#         while i <= len(df_contract_ym) - 1:
#             option_code = df_contract_ym.at[i, '期权代码']
#             option_path = os.path.join(options_daily_path, '%s.parquet' % option_code)
#             if os.path.exists(option_path):
#                 df_option = pd.read_parquet(option_path)
#                 df_termstructure_daily['T_%s' % yearmonth] = df_option['expire_mins'].values / (365 * 24 * 60)
#                 break
#             else:
#                 print('%s not in minbar data' % option_code)
#                 i += 1
#
#     cubic_call = df_termstructure_daily.apply(lambda x: get_keypoint_cubic(x['T_%s' % maturities[0]],
#                                                                            x['T_%s' % maturities[1]],
#                                                                            x['T_%s' % maturities[2]],
#                                                                            x['T_%s' % maturities[3]],
#                                                                            x['%s_0.5_call_cubic' % maturities[0]],
#                                                                            x['%s_0.5_call_cubic' % maturities[1]],
#                                                                            x['%s_0.5_call_cubic' % maturities[2]],
#                                                                            x['%s_0.5_call_cubic' % maturities[3]]),
#                                               axis=1)
#     cubic_put = df_termstructure_daily.apply(lambda x: get_keypoint_cubic(x['T_%s' % maturities[0]],
#                                                                           x['T_%s' % maturities[1]],
#                                                                           x['T_%s' % maturities[2]],
#                                                                           x['T_%s' % maturities[3]],
#                                                                           x['%s_0.5_put_cubic' % maturities[0]],
#                                                                           x['%s_0.5_put_cubic' % maturities[1]],
#                                                                           x['%s_0.5_put_cubic' % maturities[2]],
#                                                                           x['%s_0.5_put_cubic' % maturities[3]]),
#                                              axis=1)
#     simple_call = df_termstructure_daily.apply(lambda x: get_keypoint_simple(x['T_%s' % maturities[0]],
#                                                                              x['T_%s' % maturities[1]],
#                                                                              x['T_%s' % maturities[2]],
#                                                                              x['T_%s' % maturities[3]],
#                                                                              x['%s_0.5_call_simple' % maturities[0]],
#                                                                              x['%s_0.5_call_simple' % maturities[1]],
#                                                                              x['%s_0.5_call_simple' % maturities[2]],
#                                                                              x['%s_0.5_call_simple' % maturities[3]]),
#                                                axis=1)
#     simple_put = df_termstructure_daily.apply(lambda x: get_keypoint_simple(x['T_%s' % maturities[0]],
#                                                                             x['T_%s' % maturities[1]],
#                                                                             x['T_%s' % maturities[2]],
#                                                                             x['T_%s' % maturities[3]],
#                                                                             x['%s_0.5_put_simple' % maturities[0]],
#                                                                             x['%s_0.5_put_simple' % maturities[1]],
#                                                                             x['%s_0.5_put_simple' % maturities[2]],
#                                                                             x['%s_0.5_put_simple' % maturities[3]]),
#                                               axis=1)
#
#     inter_result['30_call_cubic'] = pd.Series([x[0] for x in cubic_call])
#     inter_result['60_call_cubic'] = pd.Series([x[1] for x in cubic_call])
#     inter_result['90_call_cubic'] = pd.Series([x[2] for x in cubic_call])
#     inter_result['30_call_simple'] = pd.Series([x[0] for x in simple_call])
#     inter_result['60_call_simple'] = pd.Series([x[1] for x in simple_call])
#     inter_result['90_call_simple'] = pd.Series([x[2] for x in simple_call])
#
#     inter_result['30_put_cubic'] = pd.Series([x[0] for x in cubic_put])
#     inter_result['60_put_cubic'] = pd.Series([x[1] for x in cubic_put])
#     inter_result['90_put_cubic'] = pd.Series([x[2] for x in cubic_put])
#     inter_result['30_put_simple'] = pd.Series([x[0] for x in simple_put])
#     inter_result['60_put_simple'] = pd.Series([x[1] for x in simple_put])
#     inter_result['90_put_simple'] = pd.Series([x[2] for x in simple_put])
#
#     outfile = os.path.join(key_point_outpath, '%s_1min.csv' % date)
#     inter_result.to_csv(outfile)
#
#     # 取5min的平均值
#     outfile_5min = os.path.join(key_point_outpath, '%s_5min.csv' % date)
#     inter_result['datetime'] = pd.to_datetime(inter_result['datetime'])
#     inter_result_5min = inter_result.set_index('datetime').resample('5T').mean().dropna().reset_index()
#     inter_result_5min.to_csv(outfile_5min)
