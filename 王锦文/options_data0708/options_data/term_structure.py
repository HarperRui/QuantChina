# -*- coding: utf-8 -*-
# @Time    : 2021/6/4 11:19
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : term_structure.py
# @Software: PyCharm

import numpy as np
import pandas as pd
from scipy import interpolate
import os


#%%
def get_all_options(code, df_daily_info, path_daily_options, option_type):
    """
    读取当天的所有期权合约数据,储存在一个dict中，keys为交易代码，values为包括期权合约信息的DataFrame
    :param code: '510050' or '510300' or '000300'
    :param df_daily_info: 当天的所有合约信息，DataFrame
    :param path_daily_options: 当天期权合约文件的路径
    :param option_type: 期权类型，'C' or 'P'
    :return: dict
    """
    result = {}
    if code[0:3] == '510':
        for i in range(len(df_daily_info)):
            filepath = os.path.join(path_daily_options, '%i.parquet' % df_daily_info.at[i, '期权代码'])
            if os.path.exists(filepath):
                _tradecode = df_daily_info.at[i, '交易代码']
                if option_type in _tradecode:
                    result[_tradecode] = pd.read_parquet(filepath)
            else:
                print("%s not in minbar files" % df_daily_info.at[i, '交易代码'])
    else:
        for i in range(len(df_daily_info)):
            filepath = os.path.join(path_daily_options, '%s.parquet' % df_daily_info.at[i, '期权代码'])
            if os.path.exists(filepath):
                _tradecode = df_daily_info.at[i, '交易代码']
                if option_type in _tradecode:
                    result[_tradecode] = pd.read_parquet(filepath)
            else:
                print("%s not in minbar files" % df_daily_info.at[i, '交易代码'])
    return result


def get_available_options_data(code, idx, ym, _all_options, prc_type):
    """
    对于指定的分钟以及交割日期，获取符合条件所有行权价下期权的隐含波动率及delta
    :param code: '510050' or '510300' or '000300'
    :param idx: 当前分钟所在的index
    :param ym: 期权的交割日期，格式为 '2104'
    :param _all_options: 当天所有的期权合约, dict
    :param prc_type: 期权价格的类型，bid, ask, mid, close
    :return: 包含不同行权价、IV及距离交割时间（年化）的 DataFrame
            +----------------------------+
            |   | delta | IV_(prc_type) |
            | 0 |  0.4  |     0.182     |
            | 1 |  0.6  |     0.181     |
            +----------------------------+
    """
    result = {}
    if code[0:3] == '510':
        option_list = [x for x in _all_options.keys() if x[7:11] == ym]
    else:
        option_list = [x for x in _all_options.keys() if x[2:6] == ym]
    for tradecode in option_list:
        _delta = abs(_all_options[tradecode].at[idx, 'Delta_%s' % prc_type])
        result[_delta] = _all_options[tradecode].at[idx, 'IV_%s' % prc_type]

    result = pd.DataFrame(result, index=['IV_%s' % prc_type]).T.reset_index().rename(columns={'index': 'delta'})
    result.sort_values('delta', inplace=True, ignore_index=True)

    if len(result) < 4:
        print('####### Not enough effective delta samples #######')
        return result

    # 取0.25~0.75 delta附近的样本
    low_idx = 0
    high_idx = len(result) - 1
    for i in range(len(result)-1):
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
    delta1 = 0.25
    delta2 = 0.5
    delta3 = 0.75
    if option_type == 'P':
        delta_list *= -1
        delta1 *= -1
        delta2 *= -1
        delta3 *= -1
    try:
        f = interpolate.interp1d(delta_list, iv_list, kind='cubic')
        return f([delta1, delta2, delta3])
    except ValueError:
        f = interpolate.interp1d(delta_list, iv_list, kind='cubic', fill_value="extrapolate")
        return f([delta1, delta2, delta3])


def get_inter_imvol_simple(data, prc_type):
    """
    简单线性插值计算波动率
    :param data: 格式如get_available_options_data函数输出结果的DataFrame
    :param prc_type: 价格类型
    :return: 0.25,0.5,0.75delta的隐含波动率, list
    """
    def get_simple_inter(low_point, high_point, target_point, low_value, high_value):
        return low_value + (high_value-low_value) * (target_point - low_point) / (high_point - low_point)

    low_idx_25 = 0
    high_idx_25 = low_idx_25 + 1
    low_idx_75 = len(data) - 2
    high_idx_75 = low_idx_75 + 1
    
    if data.at[1, 'delta'] < 0.25 <= data.at[2, 'delta']:
        low_idx_25 += 1
        high_idx_25 += 1
    if data.at[low_idx_75 - 1, 'delta'] < 0.75 <= data.at[low_idx_75, 'delta']:
        low_idx_75 -= 1
        high_idx_75 -= 1

    low_idx_5 = high_idx_5 = 0
    for i in range(len(data) - 1):
        if data.at[i, 'delta'] < 0.5 <= data.at[i+1, 'delta']:
            low_idx_5 = i
            high_idx_5 = i + 1
            break
    if low_idx_5 == high_idx_5:
        if data.at[0, 'delta'] >= 0.5:
            low_idx_5 = 0
            high_idx_5 = 1
        elif data.at[len(data)-1, 'delta'] <= 0.5:
            low_idx_5 = len(data) - 2
            high_idx_5 = low_idx_5 + 1
    
    iv_25 = get_simple_inter(data.at[low_idx_25, 'delta'],
                             data.at[high_idx_25, 'delta'],
                             0.25,
                             data.at[low_idx_25, 'IV_%s' % prc_type],
                             data.at[high_idx_25, 'IV_%s' % prc_type])
    iv_5 = get_simple_inter(data.at[low_idx_5, 'delta'],
                            data.at[high_idx_5, 'delta'],
                            0.50,
                            data.at[low_idx_5, 'IV_%s' % prc_type],
                            data.at[high_idx_5, 'IV_%s' % prc_type])
    iv_75 = get_simple_inter(data.at[low_idx_75, 'delta'],
                             data.at[high_idx_75, 'delta'],
                             0.75,
                             data.at[low_idx_75, 'IV_%s' % prc_type],
                             data.at[high_idx_75, 'IV_%s' % prc_type])
    return [iv_25, iv_5, iv_75]


def get_inter_parameters(code, idx, ym, _all_options, prc_type, option_type):
    """
    获取指定到期日、指定分钟的0.25,0.5,0.75delta波动率
    :param code: '510050' or '510300' or '000300'
    :param idx: 当前分钟所在的index
    :param ym: 交割日期，例如‘2104’
    :param _all_options: 当天所有的期权合约, dict
    :param prc_type: 价格类型，bid/ask/mid/close
    :param option_type: 期权类型，'C' or 'P'
    :return: 0.25,0.5,0.75delta波动率，依次为三次样条和简单线性插值
    """
    df_params = get_available_options_data(code, idx, ym, _all_options, prc_type)
    if len(df_params) < 4:
        # 近月合约在交割当天的delta可能都为0
        return 0, 0, 0, 0, 0, 0
    result_cubic = get_inter_imvol_cubic(df_params['delta'].copy(), df_params['IV_%s' % prc_type].copy(), option_type)
    result_simple = get_inter_imvol_simple(df_params, prc_type)
    return result_cubic[0], result_cubic[1], result_cubic[2], result_simple[0], result_simple[1], result_simple[2]


def get_minbar_inter_result(code, ym, future_data, _all_options, prc_type, option_type):
    """
    获取当日指定交割日期的所有0.25,0.5,0.75delta波动率
    :param code: '510050' or '510300' or '000300'
    :param ym: 交割日期，例如‘2104’
    :param future_data: 包含当天合成期货信息的DataFrame
    :param _all_options: 当天所有的期权合约, dict
    :param prc_type: 价格类型，bid/ask/mid/close
    :param option_type: 期权类型，'C' or 'P'
    :return: 包含每分钟0.25,0.5,0.75delta波动率的tuple
    """
    df_result = pd.DataFrame(future_data[['datetime']])
    df_result['idx'] = np.arange(0, len(df_result))

    params_result = df_result.apply(lambda x: get_inter_parameters(code,
                                                                   x['idx'],
                                                                   ym,
                                                                   _all_options,
                                                                   prc_type,
                                                                   option_type), axis=1)
    return params_result


def get_termstructure(code, date, input_iv_path, input_future_path, output_path, df_contract):
    """
    计算指定日期的0.25,0.5,0.75delta 隐含波动率差值结果并保存至输出路径
    :param code: '510050' or '510300' or '000300'
    :param date: 日期，格式为 '20210401'
    :param input_iv_path: 储存期权合约IV数据的路径
    :param input_future_path: 储存合成期货数据的路径
    :param output_path: 期限结构结果的输出路径
    :param df_contract: 储存期权合约信息的DataFrame
    :return: 将插值结果保存至输出路径下
    """
    print('Calculating term structure of %s' % date)
    options_daily_path = os.path.join(input_iv_path, date)
    future_daily_path = os.path.join(input_future_path, '%s.csv' % date)
    df_future_daily = pd.read_csv(future_daily_path, index_col=0)  # 当天合成期货minbar数据
    df_contract_daily = df_contract[df_contract['日期'] == date].reset_index(drop=True)  # 当天所有的合约（剔除掉除权合约）

    maturities = [x for x in df_future_daily.columns if len(x) == 4]  # 四个到期时间
    termstructure_result = pd.DataFrame(df_future_daily['datetime'])  # 储存输出结果

    for yearmonth in maturities:
        all_call_options = get_all_options(code, df_contract_daily, options_daily_path, 'C')
        all_put_options = get_all_options(code, df_contract_daily, options_daily_path, 'P')
        result_call = get_minbar_inter_result(code, yearmonth, df_future_daily, all_call_options, 'mid', 'C')
        result_put = get_minbar_inter_result(code, yearmonth, df_future_daily, all_put_options, 'mid', 'P')

        termstructure_result['%s_0.25_call_cubic' % yearmonth] = pd.Series([x[0] for x in result_call])
        termstructure_result['%s_0.5_call_cubic' % yearmonth] = pd.Series([x[1] for x in result_call])
        termstructure_result['%s_0.75_call_cubic' % yearmonth] = pd.Series([x[2] for x in result_call])
        termstructure_result['%s_0.25_call_simple' % yearmonth] = pd.Series([x[3] for x in result_call])
        termstructure_result['%s_0.5_call_simple' % yearmonth] = pd.Series([x[4] for x in result_call])
        termstructure_result['%s_0.75_call_simple' % yearmonth] = pd.Series([x[5] for x in result_call])

        termstructure_result['%s_0.25_put_cubic' % yearmonth] = pd.Series([x[0] for x in result_put])
        termstructure_result['%s_0.5_put_cubic' % yearmonth] = pd.Series([x[1] for x in result_put])
        termstructure_result['%s_0.75_put_cubic' % yearmonth] = pd.Series([x[2] for x in result_put])
        termstructure_result['%s_0.25_put_simple' % yearmonth] = pd.Series([x[3] for x in result_put])
        termstructure_result['%s_0.5_put_simple' % yearmonth] = pd.Series([x[4] for x in result_put])
        termstructure_result['%s_0.75_put_simple' % yearmonth] = pd.Series([x[5] for x in result_put])

    outfile = os.path.join(output_path, '%s_1min.csv' % date)
    termstructure_result.to_csv(outfile)

    # 取5min的平均值
    outfile_5min = os.path.join(output_path, '%s_5min.csv' % date)
    termstructure_result['datetime'] = pd.to_datetime(termstructure_result['datetime'])
    result_5min = termstructure_result.set_index('datetime').resample('5T').mean().dropna().reset_index()
    result_5min.to_csv(outfile_5min)


#%%
# input_future_path = 'C:\\Users\\lenovo\\OneDrive\\桌面\\output\\synthetic_futures'  # 合成期货数据路径
# input_iv_path = 'C:\\Users\\lenovo\\OneDrive\\桌面\\output'  # IV路径
# termstructure_outpath = 'C:\\Users\\lenovo\\OneDrive\\桌面\\output\\term_structure'  # 期限结构结果输出路径
# if not os.path.exists(termstructure_outpath):
#     os.makedirs(termstructure_outpath)

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
# for date in date_range:
#     print('Start to process the data of %s' % date)
#     options_daily_path = os.path.join(input_iv_path, date)
#     future_daily_path = os.path.join(input_future_path, '%s.csv' % date)
#     df_future_daily = pd.read_csv(future_daily_path, index_col=0)  # 当天合成期货minbar数据
#     df_contract_daily = df_contract_M[df_contract_M['日期'] == date].reset_index(drop=True)  # 当天所有的合约（剔除掉除权合约）
#
#     maturities = [x for x in df_future_daily.columns if len(x) == 4]  # 四个到期时间
#     inter_result = pd.DataFrame(df_future_daily['datetime'])  # 储存输出结果
#
#     for yearmonth in maturities:
#         all_call_options = get_all_options(df_contract_daily, options_daily_path, 'C')
#         all_put_options = get_all_options(df_contract_daily, options_daily_path, 'P')
#         result_call = get_minbar_inter_result(yearmonth, df_future_daily, all_call_options, 'mid', 'C')
#         result_put = get_minbar_inter_result(yearmonth, df_future_daily, all_put_options, 'mid', 'P')
#
#         inter_result['%s_0.25_call_cubic' % yearmonth] = pd.Series([x[0] for x in result_call])
#         inter_result['%s_0.5_call_cubic' % yearmonth] = pd.Series([x[1] for x in result_call])
#         inter_result['%s_0.75_call_cubic' % yearmonth] = pd.Series([x[2] for x in result_call])
#         inter_result['%s_0.25_call_simple' % yearmonth] = pd.Series([x[3] for x in result_call])
#         inter_result['%s_0.5_call_simple' % yearmonth] = pd.Series([x[4] for x in result_call])
#         inter_result['%s_0.75_call_simple' % yearmonth] = pd.Series([x[5] for x in result_call])
#
#         inter_result['%s_0.25_put_cubic' % yearmonth] = pd.Series([x[0] for x in result_put])
#         inter_result['%s_0.5_put_cubic' % yearmonth] = pd.Series([x[1] for x in result_put])
#         inter_result['%s_0.75_put_cubic' % yearmonth] = pd.Series([x[2] for x in result_put])
#         inter_result['%s_0.25_put_simple' % yearmonth] = pd.Series([x[3] for x in result_put])
#         inter_result['%s_0.5_put_simple' % yearmonth] = pd.Series([x[4] for x in result_put])
#         inter_result['%s_0.75_put_simple' % yearmonth] = pd.Series([x[5] for x in result_put])
#
#     outfile = os.path.join(termstructure_outpath, '%s_1min.csv' % date)
#     inter_result.to_csv(outfile)
#
#     # 取5min的平均值
#     outfile_5min = os.path.join(termstructure_outpath, '%s_5min.csv' % date)
#     inter_result['datetime'] = pd.to_datetime(inter_result['datetime'])
#     inter_result_5min = inter_result.set_index('datetime').resample('5T').mean().dropna().reset_index()
#     inter_result_5min.to_csv(outfile_5min)




