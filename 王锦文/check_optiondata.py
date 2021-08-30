# -*- coding: utf-8 -*-
# @Time    : 2021/7/13 9:24
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : check_optiondata.py
# @Software: PyCharm

import numpy as np
import pandas as pd
import os
import glob
from zltsql import SQLConn

#%%
SQ = SQLConn()
df_contract_info = SQ.GetData('contract_info_daily')
SQ.CloseSql()

#output_path = 'C:\\Users\\lenovo\\OneDrive\\桌面\\minbar_checking_result'\
output_path = 'D:\\Harper\\intern\\jinwen\\minbar_checking_result'
if not os.path.exists(output_path):
    os.makedirs(output_path)

minbar_path = '\\\\datahouse.quantchina.info\\share\\Roy\\CITIC\\minbar'
daily_minbar_list = glob.glob(os.path.join(minbar_path, '**'))
daily_minbar_list.sort()

for path in daily_minbar_list:
    date = path[-8:]
    daily_output_path = os.path.join(output_path, date)
    if not os.path.exists(daily_output_path):
        os.makedirs(daily_output_path)
    print('=============== Checking minbar data of %s ===============' % date)
    df_contract_daily = df_contract_info[df_contract_info['日期'] == date].reset_index(drop=True)

    missing_in_contract = {}
    missing_price = {}
    vol_error = {}

    option_list = glob.glob(os.path.join(path, '1min', '**.parquet'))
    for option_path in option_list:
        df_option = pd.read_parquet(option_path)
        code = int(df_option.at[0, 'symbol'][:-3])

        # 检查contract_info_daily中是否有该合约
        if code not in df_contract_daily['期权代码'].values:
            missing_in_contract[code] = 1
            print('%i not in contract_info' % code)
        else:
            missing_in_contract[code] = 0

        # 检查是否有多个分钟的ask/bid为0（以20为threshold）
        threshold = 20
        try:
            ask_missing_nums = df_option['first_ask_prc1'].fillna(0).value_counts()[0]
        except KeyError:
            ask_missing_nums = 0

        try:
            bid_missing_nums = df_option['first_bid_prc1'].fillna(0).value_counts()[0]
        except KeyError:
            bid_missing_nums = 0

        if (ask_missing_nums > threshold) or (bid_missing_nums > threshold):
            print('More than %i empty data points in %i' % (threshold, code))
        missing_price[code] = [ask_missing_nums, bid_missing_nums]

        # 检查是否first_ask/bid_vol1 == 0 & first_ask/bid_vol2+3+4+5... != 0
        df_option['ask_vol_error'] = df_option.apply(lambda x: 1 if ((x['first_ask_vol1'] == 0) &
                                                                     (x['first_ask_vol2'] +
                                                                      x['first_ask_vol3'] +
                                                                      x['first_ask_vol4'] +
                                                                      x['first_ask_vol5']) != 0) else 0, axis=1)
        df_option['bid_vol_error'] = df_option.apply(lambda x: 1 if ((x['first_bid_vol1'] == 0) &
                                                                     (x['first_bid_vol2'] +
                                                                      x['first_bid_vol3'] +
                                                                      x['first_bid_vol4'] +
                                                                      x['first_bid_vol5']) != 0) else 0, axis=1)
        ask_vol_errors = df_option['ask_vol_error'].sum()
        bid_vol_errors = df_option['bid_vol_error'].sum()
        if ask_vol_errors + bid_vol_errors != 0:
            print('Volume data errors in %i' % code)
        vol_error[code] = [ask_vol_errors, bid_vol_errors]

    df_missing_in_contract = pd.DataFrame(missing_in_contract, index=[0]).T.rename(columns={0: 'isMissing'})
    df_missing_price = pd.DataFrame(missing_price).T.rename(columns={0: 'ask1_missing_nums', 1: 'bid1_missing_nums'})
    df_vol_error = pd.DataFrame(vol_error).T.rename(columns={0: 'ask1_vol_errors', 1: 'bid1_vol_errors'})

    df_missing_in_contract.to_csv(os.path.join(daily_output_path, 'missing_in_contract_%s.csv' % date))
    df_missing_price.to_csv(os.path.join(daily_output_path, 'missing_price_%s.csv' % date))
    df_vol_error.to_csv(os.path.join(daily_output_path, 'volume_error_%s.csv' % date))

    del df_missing_in_contract, df_missing_price, df_vol_error




