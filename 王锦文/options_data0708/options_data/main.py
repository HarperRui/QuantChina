# -*- coding: utf-8 -*-
# @Time    : 2021/5/25 15:17
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : main.py
# @Software: PyCharm

import numpy as np
import pandas as pd
import os
from zltsql import SQLConn
from future import get_synthetic_futures
from iv import get_iv_greeks
from SABR import get_sabr
from term_structure import get_termstructure
from key_point import get_keypoint
from Heston import get_heston
from CEV import get_cev
import time
import warnings
warnings.filterwarnings("ignore")


def get_options_data(code, input_minbar_path, output_path, specify_date=None):
    """
    计算50或300合约的合成期货、IV Greeks、SABR、0.25 0.5 0.75delta、30 60 90天波动率数据并储存本地
    :param code: '510050' or '510300' or '000300'
    :param input_minbar_path: 期权合约的minbar路径
    :param output_path: 输出结果的储存路径
    :param specify_date: 可以指定只计算某个月的数据
    :return: 结果保存至本地
    """
    SQ = SQLConn()
    if code[0:3] == '510':  # ETF期权
        df_contract_all = SQ.GetData('contract_info_daily')
        df_contract_code = df_contract_all[df_contract_all['期权标的'] == '%s.SH' % code]  # 保留50或300的期权信息
        df_contract_M = df_contract_code[df_contract_code['交易代码'].apply(lambda x: x.find('M') >= 0)]  # 剔除除权合约
    elif code[0:3] == '000':  # 指数期权
        df_contract_info = SQ.GetData('contract_info_300index')  # contract_info
        df_contract_info.drop(['交易代码', '期权简称', '行权价'], axis=1, inplace=True)
        df_daily = SQ.GetData('日行情_300index')  # 日行情
        df_contract_all = pd.merge(df_daily, df_contract_info, on='期权代码')
        df_contract_code = df_contract_all.copy()
        df_contract_M = df_contract_all.copy()
    else:
        print('Wrong input code')
        SQ.CloseSql()
        return
    df_rf = SQ.GetData('rf')  # 无风险利率
    SQ.CloseSql()

    futures_outpath = os.path.join(output_path, 'synthetic_futures')  # 合成期货结果输出路径
    iv_outpath = os.path.join(output_path, 'IV_Greeks')  # IV,Greeks结果输出路径
    sabr_outpath = os.path.join(output_path, 'SABR')  # SABR结果输出路径
    termstructure_outpath = os.path.join(output_path, 'term_structure')  # 期限结构结果输出路径
    keypoint_outpath = os.path.join(output_path, 'key_point')  # 期限插值结果输出路径
    # heston_outpath = os.path.join(output_path, 'Heston')  # Heston结果输出路径
    # cev_outpath = os.path.join(output_path, 'CEV')  # CEV结果输出路径

    if not os.path.exists(futures_outpath):
        os.makedirs(futures_outpath)
    if not os.path.exists(iv_outpath):
        os.makedirs(iv_outpath)
    if not os.path.exists(sabr_outpath):
        os.makedirs(sabr_outpath)
    if not os.path.exists(termstructure_outpath):
        os.makedirs(termstructure_outpath)
    if not os.path.exists(keypoint_outpath):
        os.makedirs(keypoint_outpath)
    # if not os.path.exists(heston_outpath):
    #     os.makedirs(heston_outpath)
    # if not os.path.exists(cev_outpath):
    #     os.makedirs(cev_outpath)

    date_range = [pd.to_datetime(x).strftime('%Y%m%d') for x in df_contract_code['日期'].unique()]
    if specify_date:
        date_range = [x for x in date_range if x[0:6] == specify_date]  # 如果指定了特定月份
    start = time.time()

    input_minbar_stock_path = os.path.join(input_minbar_path, 'stock')
    input_minbar_future_path = os.path.join(input_minbar_path, 'future')
    for date in date_range:
        rf = np.log(1 + df_rf.loc[df_rf['日期'] == date, '中债国债到期收益率：1年'].values[0] / 100)
        get_synthetic_futures(code, date, rf, df_contract_M, input_minbar_stock_path, input_minbar_future_path, futures_outpath)
        get_iv_greeks(code, date, rf, df_contract_all, df_contract_code, input_minbar_stock_path, input_minbar_future_path, futures_outpath, iv_outpath)
        get_sabr(code, date, iv_outpath, futures_outpath, sabr_outpath, df_contract_M)
        
        get_termstructure(code, date, iv_outpath, futures_outpath, termstructure_outpath, df_contract_M)
        get_keypoint(code, date, iv_outpath, termstructure_outpath, keypoint_outpath, df_contract_M)
        # get_heston(code, date, rf, df_contract_M, futures_outpath, input_minbar_stock_path, iv_outpath, heston_outpath)
        # get_cev(code, date, rf, df_contract_M, futures_outpath, input_minbar_stock_path, iv_outpath, cev_outpath)
        
    end = time.time()
    print('Running time:', end - start)


#%% Main
stock_code = '510050'
daily_minbar_path = '\\\\datahouse.quantchina.info\\data\\minbar'  # minbar路径
# daily_minbar_path = 'C:\\Users\\lenovo\\OneDrive\\桌面\\minbar'  # minbar路径
#result_outpath = 'C:\\Users\\lenovo\\OneDrive\\桌面\\output_%s' % stock_code
result_outpath = '//192.168.8.90/投研部/Harper/期权策略/options_data0708/output_%s' % stock_code
if __name__ == '__main__':
    specify_date = ['201502','201503','201504','201505','201506','201507','201508','201509','201510','201511','201512']
    #specify_date = ['202001','202002','202003','202004','202005','202006','202007','202008','202009','202010','202011','202012']
    #specify_date = ['201901','201902','201903','201904','201905','201906','201907','201908','201909','201910','201911','201912']
    #specify_date = ['201801','201802','201803','201804','201805','201806','201807','201808','201809','201810','201811','201812']
    #specify_date = ['201701','201702','201703','201704','201705','201706','201707','201708','201709','201710','201711','201712']
    #specify_date = ['201601','201602','201603','201604','201605','201606','201607','201608','201609','201610','201611','201612']
    for i in specify_date:
        get_options_data(stock_code, daily_minbar_path, result_outpath, specify_date=i)
    #get_options_data(stock_code, daily_minbar_path, result_outpath, specify_date=None)


