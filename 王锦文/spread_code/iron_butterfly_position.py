# -*- coding: utf-8 -*-
# @Time    : 2021/7/14 15:49
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : iron_butterfly_position.py
# @Software: PyCharm

import math
import numpy as np
import pandas as pd
import datetime
# from zltsql import SQLConn
from daily_iv import get_daily_iv, get_daily_iv_delta
import sys
sys.path.append('D:/Harper/Class')
from Get_Price import *

#%%
def get_iron_butterfly_positon(date, code, df_rf, df_contract, df_stock, df_vol, vol_type='IV',threshold = 7,expire_threshold=10):
    """
    获取每天构建认购正比例、认购反比例、认沽正比例、认沽反比例价差的期权信息
    :param date: 日期，例如'20210401'
    :param code: 标的资产的代码，例如 '510050.SH'
    :param df_rf: 含有无风险利率的DataFrame
    :param df_contract: 期权的contract_daily_info信息，DataFrame
    :param df_stock: 含有标的资产每天收盘价的DataFrame
    :param df_vol: 用到的波动率的DataFrame， IV / VIX / HV
    :param vol_type: 'IV' or 'VIX' or 'HV**', 如果是HV的话需要指定多少日的HV，比如'HV20'
    :param threshold: 到期日小于？的时候就 不用近月合约了
    :param expire_threshold: 计算到期日小于？的插值iv
    :return: 构建铁蝶式价差
    """

    stock_prc = df_stock.loc[df_stock['日期'] == date, 'close'].values[0]  # 当天标的资产的收盘价

    # 每天的波动率（日度化）
    if vol_type == 'IV':
        #vol = get_daily_iv(code[0:6], date, df_rf, df_contract, df_stock, expire_threshold) / math.sqrt(365)
        vol = df_vol.loc[(df_vol['Date'] == date) & (df_vol['Code'] == code), 'iv_insert'].values[0] / 100 / math.sqrt(365)
    elif vol_type[0:2] == 'HV':
        vol = df_vol.loc[(df_vol['Date'] == date) & (df_vol['Code'] == code), vol_type].values[0] / 100 / math.sqrt(252)
    elif vol_type == 'VIX':
        vol = df_vol.loc[df_vol['日期'] == date, 'iVIX'].values[0] / 100 / math.sqrt(365)
    else:
        print('Wrong volatility type')
        return

    # 包含当天所有期权行权价、callput、iv、delta、ttm的DataFrame
    df_option = get_daily_iv_delta(code[0:6], date, df_rf, df_contract, df_stock)


    ttm_list = np.sort(df_option['ttm'].unique())
    ttm_l1, ttm_l2 = ttm_list[0], ttm_list[1]  # 近月和远月合约的到期时间（年化）

    def get_available_options(data, ttm, daily_vol, spot_prc,ratio_condition = 0.1):
        """
        确定构建铁蝶式期权的四个期权合约，short call and short put为最接近0.5 delta的合约，long call and long put
        为行权价最接近标的价格一倍标准差的合约。（如果没有足够的期权合约则 'isbuild' 为0。
        假设由call构建的垂直价差的delta为delta_call, 数量为m, 由put构建的为delta_put, 数量为n, ratio=n/m且在0.9-1.1之间，
        调整ratio使得总的头寸delta最接近0。
        :param data: 含有期权行权价、callput、iv、delta、ttm的DataFrame
        :param ttm: 合约的到期时间（年化）
        :param daily_vol: 日度的波动率
        :param spot_prc: 标的资产的价格
        :param ratio_condtion: call, put 价差 做delta对冲时的配比限制，超出配比范围 就不做蝶式
        :return: DataFrame
        假如有四只不同的期权来构建铁蝶式价差
            put_long    call_short  put_short   call_long   isbuild   ratio   delta   maturity
            3.4         3.5         3.5         3.6         1         0.98    0.002   L1

        假如不够四只期权合约
            put_long    call_short  put_short   call_long   isbuild
            3.5         3.5         3.5         3.6         0
        """
        result = pd.DataFrame()
        option_data = data[data['ttm'] == ttm].reset_index(drop=True)
        option_call = option_data[option_data['callput'] == 'c'].reset_index(drop=True)
        option_put = option_data[option_data['callput'] == 'p'].reset_index(drop=True)

        # 最接近标的价格1倍标准差的call and put
        # strike_higher = spot_prc * (1 + daily_vol * math.sqrt(ttm*365))  # discrete
        # strike_lower = spot_prc * (1 - daily_vol * math.sqrt(ttm*365))  # discrete
        strike_higher = spot_prc * math.exp(daily_vol * math.sqrt(ttm * 365))  # continuous
        strike_lower = spot_prc * math.exp(-daily_vol * math.sqrt(ttm * 365))  # continuous

        put_long_index = np.abs(option_put['strike_prc'].copy() - strike_lower).argmin()
        call_long_index = np.abs(option_call['strike_prc'].copy() - strike_higher).argmin()
        put_long = option_put.at[put_long_index, 'strike_prc']
        call_long = option_call.at[call_long_index, 'strike_prc']

        # 最接近0.5 delta的call and put
        call_short_index = np.abs(option_call['delta'].copy() - 0.5).argmin()
        put_short_index = np.abs(option_put['delta'].copy() + 0.5).argmin()
        call_short = option_call.at[call_short_index, 'strike_prc']
        put_short = option_put.at[put_short_index, 'strike_prc']

        result.at[0, 'put_long'] = put_long
        result.at[0, 'call_short'] = call_short
        result.at[0, 'put_short'] = put_short
        result.at[0, 'call_long'] = call_long

        #期权合约的代码
        result.at[0, 'put_long_code'] = option_put.at[put_long_index, 'code']
        result.at[0, 'call_short_code'] = option_call.at[call_short_index, 'code']
        result.at[0, 'put_short_code'] = option_put.at[put_short_index, 'code']
        result.at[0, 'call_long_code'] = option_call.at[call_long_index, 'code']



        if (call_long == call_short) or (put_long == put_short):
            result.at[0, 'isbuild'] = 0
            return result
        else:
            result.at[0, 'isbuild'] = 1

        # 确定call组合(认购垂直价差) and short组合(认沽垂直价差)的比例，call组合与put组合的比例为 1 : ratio
        call_delta = option_call.at[call_long_index, 'delta'] * option_call.at[call_long_index, 'contract_unit'] - option_call.at[call_short_index, 'delta'] * option_call.at[call_short_index, 'contract_unit']
        put_delta = -option_put.at[put_short_index, 'delta'] * option_put.at[put_short_index, 'contract_unit'] + option_put.at[put_long_index, 'delta'] * option_put.at[put_long_index, 'contract_unit']

        ratio = -call_delta / put_delta
        '''
        锦文原代码
        if ratio > 1.1:
            ratio = 1.1
        elif ratio < 0.9:
            ratio = 0.9
        '''
        #如果超出配比就不做蝶式
        if (ratio > 1 + ratio_condition) or (ratio < 1 - ratio_condition):
            result.at[0, 'isbuild'] = 0


        result.at[0, 'ratio(c/p)'] = ratio
        result.at[0, 'butterfly_delta'] = call_delta + put_delta * ratio
        return result


    options_l1 = get_available_options(data=df_option, ttm=ttm_l1, daily_vol=vol, spot_prc=stock_prc)
    options_l2 = get_available_options(data=df_option, ttm=ttm_l2, daily_vol=vol, spot_prc=stock_prc)
    if ttm_l1 >= threshold / 365:
        if options_l1.at[0, 'isbuild'] == 1:
            options_l1.at[0, 'maturity'] = 'L1'
            return options_l1
        else:
            options_l2.at[0, 'maturity'] = 'L2'
            return options_l2
    else:
        options_l2.at[0, 'maturity'] = 'L2'
        return options_l2


#%%
if __name__ == '__main__':


    stock_code = '510050.SH'

    # SQ = SQLConn()
    # df_HVpercentile = SQ.GetData('HV_percentile')
    #
    # df_contract_all = SQ.GetData('contract_info_daily')
    # df_contract_all = df_contract_all[df_contract_all['交易代码'].apply(lambda x: x.find('M') >= 0)]
    # df_contract = df_contract_all[df_contract_all['期权标的'] == stock_code].reset_index(drop=True)  # 保留50或300的期权信息
    #
    # df_rf = SQ.GetData('rf')  # 无风险利率
    #
    # df_stock = SQ.GetData('etf_50')
    #
    # df_vix = SQ.GetData('df_vol_50etf')
    #
    # SQ.CloseSql()



    SQL =  SQL_conn('85')
    df_contract_all, df_rf, df_stock,df_vix = Get_price._85_database(SQL,['contract_info_daily','rf','etf_50','df_vol_50etf'])
    SQL.close_conn()

    df_contract_all = df_contract_all[df_contract_all['交易代码'].apply(lambda x: x.find('M') >= 0)]
    df_contract = df_contract_all[df_contract_all['期权标的'] == stock_code].reset_index(drop=True)  # 保留50或300的期权信息

    # get_iron_butterfly_positon(date='20210429', code='510050.SH', df_rf=df_rf, df_contract=df_contract,
    #                            df_stock=df_stock, df_hv=df_HVpercentile, df_vix=df_vix, vol_type='IV')

    date_list = df_contract['日期'].apply(lambda x: datetime.datetime.strftime(x, '%Y%m%d')).unique()
    #date_list = [x for x in date_list if x[0:6] == '202101']

    position = None

    for date in date_list:
        print(date)
        now_positon = get_iron_butterfly_positon(date=date, code='510050.SH', df_rf=df_rf, df_contract=df_contract,
                                                 df_stock=df_stock, df_vol=df_vix, vol_type='VIX')
        if now_positon is not None:
            now_positon['Date'] = date
        if position is None:
            position = now_positon.copy()
        else:
            position = pd.concat([position, now_positon], ignore_index=True)
    position.to_excel('D:/Harper/option_strategy/butterfly.xlsx')

