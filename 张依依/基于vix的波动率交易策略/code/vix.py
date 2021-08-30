'''
计算iVIX数据，与官方数据比较，2015-2016年有细微偏差，后期基本一致
总体趋势拟合程度非常高
'''

import numpy as np
import pandas as pd
import datetime

#1.计算iVIX
#1.1 volatility function
def volatility(df, T, r):
    # 求F
    dfd = df.sort_values(by='价格差', ascending=True).reset_index(drop=True)
    F = dfd['行权价'].values[0] + (dfd['认购期权价格'].values[0] - dfd['认沽期权价格'].values[0]) * np.e ** (r * T)
    #求K0
    k0 = 0
    dfs = df.sort_values(by='行权价', ascending=True).reset_index(drop=True)
    for i in range(len(dfs['行权价'].values)):
        if dfs['行权价'].values[i] >= F:
            k0 = dfs['行权价'].values[i - 1]
            break
    #此处对原版有所修改
    if k0 == 0:
        k0 = dfs['行权价'].values[-1]
    
    #求sum
    sum = 0
    for i in range(len(dfs['行权价'].values)):
        if dfs['行权价'].values[i] < k0:
            P = dfs['认沽期权价格'].values[i]
        elif dfs['行权价'].values[i] > k0:
            P = dfs['认购期权价格'].values[i]
        else:
            P = (dfs['认购期权价格'].values[i] + dfs['认沽期权价格'].values[i]) / 2

        if i == 0:
            sum = sum + ((dfs['行权价'].values[i + 1] - dfs['行权价'].values[i]) / dfs['行权价'].values[i] ** 2) * np.e ** (
                        r * T) * P
        elif i == len(dfs['行权价'].values) - 1:
            sum = sum + ((dfs['行权价'].values[i] - dfs['行权价'].values[i - 1]) / dfs['行权价'].values[i] ** 2) * np.e ** (
                        r * T) * P
        else:
            sum = sum + (((dfs['行权价'].values[i + 1] - dfs['行权价'].values[i - 1]) / 2) / dfs['行权价'].values[
                i] ** 2) * np.e ** (r * T) * P

    #求Volatility
    volatility = (2 / T) * sum - (1 / T) * (F / k0 - 1) ** 2
    return volatility

#1.2 maturity function
def option_maturity(option_data, date, option_code):
    option = df_contract[df_contract['期权代码'] == int(option_code)]
    return option['到期日'] - date


#1.3 df_vix存储计算出的vix数据
df_daily = pd.read_excel('../数据/日行情_new.xlsx')
df_contract = pd.read_excel('../数据/contract_info_new.xlsx')
df_rf = pd.read_excel('../数据/rf_new.xlsx')
df_rf['指标名称'] = pd.to_datetime(df_rf['指标名称'])
df_vix = pd.DataFrame(columns=['日期', 'iVIX'])
day = df_daily['日期'].drop_duplicates().reset_index(drop=True)
df_vix.日期 = day

for i in range(0, len(day)-2):
    print(day[i])
    r = df_rf[df_rf['指标名称'] == day[i]]['中债国债到期收益率:1年'].values[0] / 100
    df_date = df_daily[df_daily.日期 == day[i]]  #当日所有合约信息
    for option_code in df_daily[df_daily.日期 == day[i]]['期权代码']:
        df_date.loc[df_date.期权代码 == option_code, 'maturity'] = option_maturity(df_contract, day[i], option_code).values[0]

    test_time1 = datetime.timedelta(days=7, hours=0)
    maturity = df_date[df_date['maturity'] > test_time1]['maturity'].drop_duplicates().sort_values(
        ascending=True).reset_index(drop=True)
    df_date1 = df_date[df_date['maturity'] == maturity[0]] #得到近月期权
    df_date2 = df_date[df_date['maturity'] == maturity[1]] #得到次近月期权
    NT1 = maturity[0].days
    T1 = NT1 / 365
    NT2 = maturity[1].days
    T2 = NT2 / 365
    

    # df1整理出近月期权的价格差
    df1 = pd.DataFrame(columns=['行权价', '认购期权价格', '认沽期权价格', '价格差'])
    df1.行权价 = df_date1['行权价'].drop_duplicates().sort_values(ascending=True).reset_index(drop=True)
    for s in range(len(df1['行权价'])):
        tp = df_date1[df_date1['行权价'] == df1['行权价'].values[s]]
        if '购' in tp['期权简称'].values[0]:
            df1.认购期权价格[s] = tp['收盘价'].values[0]
            df1.认沽期权价格[s] = tp['收盘价'].values[1]
        else:
            df1.认购期权价格[s] = tp['收盘价'].values[1]
            df1.认沽期权价格[s] = tp['收盘价'].values[0]
        df1.价格差[s] = abs(df1.认购期权价格[s] - df1.认沽期权价格[s])
    

    # df2整理出次近月期权的价格差
    df2 = pd.DataFrame(columns=['行权价', '认购期权价格', '认沽期权价格', '价格差'])
    df2.行权价 = df_date2['行权价'].drop_duplicates().sort_values(ascending=True).reset_index(drop=True)
    for s in range(len(df2['行权价'])):
        tp = df_date2[df_date2['行权价'] == df2['行权价'].values[s]]
        if '购' in tp['期权简称'].values[0]:
            df2.认购期权价格[s] = tp['收盘价'].values[0]
            df2.认沽期权价格[s] = tp['收盘价'].values[1]
        else:
            df2.认购期权价格[s] = tp['收盘价'].values[1]
            df2.认沽期权价格[s] = tp['收盘价'].values[0]
        df2.价格差[s] = df2.认购期权价格[s] - df2.认沽期权价格[s]
    test_time2 = datetime.timedelta(days=30, hours=0)
    #计算iVIX
    if maturity[0] >= test_time2:
        vol1_s = volatility(df1, T1, r)
        df_vix.loc[df_vix.日期 == day[i], 'iVIX'] = 100 * np.sqrt(T1*vol1_s*(365/30))
    else:
        vol1_s = volatility(df1, T1, r)
        vol2_s = volatility(df2, T2, r)
        df_vix.loc[df_vix.日期 == day[i], 'iVIX'] = 100 * np.sqrt(
            (T1 * vol1_s * ((NT2 - 30) / (NT2 - NT1)) + T2 * vol2_s * ((30 - NT1) / (NT2 - NT1))) * (365 / 30))
df_vix= df_vix.reset_index(drop=True)
df_vix.to_excel('../数据/df_vix_new.xlsx')




