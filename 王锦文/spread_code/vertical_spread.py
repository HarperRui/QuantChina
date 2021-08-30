# -*- coding: utf-8 -*-
# @Time    : 2021/7/15 14:17
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : vertical_spread.py
# @Software: PyCharm

import math
import numpy as np
import pandas as pd
import datetime
from zltsql import SQLConn
from daily_iv import get_daily_iv, get_daily_iv_delta


# %%
def get_vertical_positon(date, code, df_rf, df_contract, df_stock, df_hv, df_vix,
                         spread_type, callput, maturity, vol_type='IV'):
    """
    根据选择的垂直价差类型(bull or bear)、期权种类(c or p)和近月/远月确定应当使用的两个期权，一个为最接近0.5delta，
    另一个为行权价最接近标的价格一倍标准差。如果两个期权为同一种，则返回None。
    :param date: 日期，例如'20210401'
    :param code: 标的资产的代码，例如 '510050.SH'
    :param df_rf: 含有无风险利率的DataFrame
    :param df_contract: 期权的contract_daily_info信息，DataFrame
    :param df_stock: 含有标的资产每天收盘价的DataFrame
    :param df_hv: 含有每日HV的DataFrame
    :param df_vix: 含有每日VIX的DataFrame
    :param spread_type: 'bull' or 'baer'
    :param callput: 'c' or 'p'
    :param maturity: 'L1'表示近月合约，'L2'表示远月合约
    :param vol_type: 'IV' or 'VIX' or 'HV**', 如果是HV的话需要指定多少日的HV，比如'HV20'
    :return: DataFrame
        spread_type     long    short   delta   maturity
        bull_call       3.6     3.7     0.3     L1
    """

    stock_prc = df_stock.loc[df_stock['日期'] == date, 'close'].values[0]  # 当天标的资产的收盘价

    # 每天的波动率（日度化）
    if vol_type == 'IV':
        vol = get_daily_iv(code[0:6], date, df_rf, df_contract, df_stock, expire_threshold=10) / math.sqrt(365)
    elif vol_type[0:2] == 'HV':
        vol = df_hv.loc[(df_hv['Date'] == date) & (df_hv['Code'] == code), vol_type].values[0] / 100 / math.sqrt(252)
    elif vol_type == 'VIX':
        vol = df_vix.loc[df_stock['日期'] == date, 'iVIX'].values[0] / 100 / math.sqrt(365)
    else:
        print('Wrong volatility type')
        return

    # 包含当天所有期权行权价、callput、iv、delta、ttm的DataFrame
    df_option = get_daily_iv_delta(code[0:6], date, df_rf, df_contract, df_stock)

    ttm_list = np.sort(df_option['ttm'].unique())
    ttm_target = ttm_list[0] if maturity == 'L1' else ttm_list[1]  # 近月或远月合约的到期时间（年化）

    result = pd.DataFrame()
    option_data = df_option[(df_option['ttm'] == ttm_target) & (df_option['callput'] == callput)].reset_index(drop=True)

    if callput == 'c':
        # 最接近标的价格1倍标准差的call
        # strike_higher = stock_prc * (1 + vol * math.sqrt(ttm_target*365))  # discrete
        strike_higher = stock_prc * math.exp(vol * math.sqrt(ttm_target * 365))  # continuous
        higher_strike_index = np.abs(option_data['strike_prc'].copy() - strike_higher).argmin()

        # 最接近0.5delta的call
        lower_strike_index = np.abs(option_data['delta'].copy() - 0.5).argmin()

        if higher_strike_index == lower_strike_index:
            # 无法构建价差
            return None
        elif spread_type == 'bull':
            result.at[0, 'spread_type'] = 'bull_call'
            result.at[0, 'long'] = option_data.at[lower_strike_index, 'strike_prc']
            result.at[0, 'short'] = option_data.at[higher_strike_index, 'strike_prc']
            result.at[0, 'delta'] = option_data.at[lower_strike_index, 'delta'] - option_data.at[higher_strike_index,
                                                                                                 'delta']
        elif spread_type == 'bear':
            result.at[0, 'spread_type'] = 'bear_call'
            result.at[0, 'long'] = option_data.at[higher_strike_index, 'strike_prc']
            result.at[0, 'short'] = option_data.at[lower_strike_index, 'strike_prc']
            result.at[0, 'delta'] = option_data.at[higher_strike_index, 'delta'] - option_data.at[lower_strike_index,
                                                                                                  'delta']
        else:
            print('Wrong spread type')
            return

        result.at[0, 'maturity'] = maturity
        return result

    elif callput == 'p':
        # 最接近标的价格1倍标准差的put
        # strike_lower = stock_prc * (1 - vol * math.sqrt(ttm_target*365))  # discrete
        strike_lower = stock_prc * math.exp(-vol * math.sqrt(ttm_target * 365))  # continuous
        lower_strike_index = np.abs(option_data['strike_prc'].copy() - strike_lower).argmin()

        # 最接近0.5delta的put
        higher_strike_index = np.abs(option_data['delta'].copy() + 0.5).argmin()

        if higher_strike_index == lower_strike_index:
            # 无法构建价差
            return None
        elif spread_type == 'bull':
            result.at[0, 'spread_type'] = 'bull_put'
            result.at[0, 'long'] = option_data.at[lower_strike_index, 'strike_prc']
            result.at[0, 'short'] = option_data.at[higher_strike_index, 'strike_prc']
            result.at[0, 'delta'] = option_data.at[lower_strike_index, 'delta'] - option_data.at[higher_strike_index,
                                                                                                 'delta']
        elif spread_type == 'bear':
            result.at[0, 'spread_type'] = 'bear_put'
            result.at[0, 'long'] = option_data.at[higher_strike_index, 'strike_prc']
            result.at[0, 'short'] = option_data.at[lower_strike_index, 'strike_prc']
            result.at[0, 'delta'] = option_data.at[higher_strike_index, 'delta'] - option_data.at[lower_strike_index,
                                                                                                  'delta']
        else:
            print('Wrong spread type')
            return

        result.at[0, 'maturity'] = maturity
        return result
    else:
        print('Wrong option type')
        return


#%%
stock_code = '510050.SH'

SQ = SQLConn()
df_HVpercentile = SQ.GetData('HV_percentile')

df_contract_all = SQ.GetData('contract_info_daily')
df_contract_all = df_contract_all[df_contract_all['交易代码'].apply(lambda x: x.find('M') >= 0)]
df_contract = df_contract_all[df_contract_all['期权标的'] == stock_code].reset_index(drop=True)  # 保留50或300的期权信息

df_rf = SQ.GetData('rf')  # 无风险利率

df_stock = SQ.GetData('etf_50')

df_vix = SQ.GetData('df_vol_50etf')

SQ.CloseSql()


# eg
get_vertical_positon(date='20210401', code='510050.SH', df_rf=df_rf, df_contract=df_contract, df_stock=df_stock,
                     df_hv=df_HVpercentile, df_vix=df_vix, spread_type='bull', callput='c', maturity='L1', vol_type='IV')

date_list = df_contract['日期'].apply(lambda x: datetime.datetime.strftime(x, '%Y%m%d')).unique()
date_list = [x for x in date_list if x[0:6] == '202104']

position = None
for date in date_list:
    print(date)
    for maturity in ['L1', 'L2']:
        for callput in ['c', 'p']:
            for bullbear in ['bull', 'bear']:
                now_positon = get_vertical_positon(date=date, code=stock_code, df_rf=df_rf, df_contract=df_contract,
                                                   df_stock=df_stock, df_hv=df_HVpercentile, df_vix=df_vix,
                                                   spread_type=bullbear, callput=callput, maturity=maturity,
                                                   vol_type='IV')
                if now_positon is not None:
                    now_positon.at[0, 'Date'] = date
                if position is None:
                    position = now_positon.copy()
                else:
                    position = pd.concat([position, now_positon], ignore_index=True)
