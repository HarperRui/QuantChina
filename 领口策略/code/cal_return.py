# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 10:08:59 2021

@author: Xuan
"""


import pandas as pd
import empyrical as ep
import numpy as np




pathlist = [ 'D:/Harper/Collar/300etf_collar_0611_85_105.xlsx',
             'D:/Harper/Collar/300etf_collar_0611_85_110.xlsx',
             'D:/Harper/Collar/300etf_collar_0611_90_110.xlsx',
             'D:/Harper/Collar/300etf_collar_0611_95_115.xlsx',
             'D:/Harper/Collar/300etf_collar_0615_95_120.xlsx',
             'D:/Harper/Collar/300etf_collar_0615_95_115_chgday8.xlsx',
             'D:/Harper/Collar/fut_300etf_collar_0611_85_105_ps_itm_switch.xlsx',
             'D:/Harper/Collar/fut_300etf_collar_0615_95_115_ps_itm_switch.xlsx',
             'D:/Harper/Collar/fut_300etf_collar0cost_0611_85_105_105.xlsx',
             'D:/Harper/Collar/fut_300etf_collar0cost_0615_95_115_115.xlsx']
df_result_all = pd.DataFrame([])

for j in range(len(pathlist)):
    path = pathlist[j]   
    df = pd.read_excel(path)
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.loc[df['Date']>=pd.to_datetime('2015-04-16')]
    df['etf_r'] = df['etf'].pct_change()
    name = path[27:len(path)-5]
    all_year = df['Date'].dt.year.drop_duplicates().reset_index(drop=True)
    if j == 0:
        df_result = pd.DataFrame(columns = ['年份','年化收益率_etf','年化波动率_etf','最大回撤_etf','Sharpe_etf','年化收益率_%s'%name,'年化波动率_%s'%name,'最大回撤_%s'%name,'Sharpe_%s'%name])
    else:
        df_result = pd.DataFrame(columns = ['年份','年化收益率_%s'%name,'年化波动率_%s'%name,'最大回撤_%s'%name,'Sharpe_%s'%name])
    df_result['年份'] = df['Date'].dt.year.drop_duplicates().reset_index(drop=True)
    
    
    for i in range(len(df_result)):
        start = pd.to_datetime(str(df_result.年份[i]) + '0101')
        print('---',start,'---')
        if i < len(df_result)-1:
            end =  pd.to_datetime(str(df_result.年份[i+1]) + '0101')
            df_year = df.loc[(df['Date'] >= start) & (df['Date'] < end)]
        else:
            df_year = df.loc[(df['Date'] >= start)]
        if j == 0:
            df_result.iloc[i,1] = ep.annual_return(df_year['etf_r'])
            df_result.iloc[i,2] = ep.annual_volatility(df_year['etf_r'])
            df_result.iloc[i,3] = ep.max_drawdown(df_year['etf_r'])
            df_result.iloc[i,4] = ep.annual_return(df_year['etf_r'])/ep.annual_volatility(df_year['etf_r']) 
            df_result.iloc[i,5] = ep.annual_return(df_year['PnL%'])
            df_result.iloc[i,6] = ep.annual_volatility(df_year['PnL%'])
            df_result.iloc[i,7] = ep.max_drawdown(pd.to_numeric(df_year['PnL%']))
            df_result.iloc[i,8] = ep.annual_return(df_year['PnL%'])/ep.annual_volatility(df_year['PnL%']) 
        else:
            df_result.iloc[i,1] = ep.annual_return(df_year['PnL%'])
            df_result.iloc[i,2] = ep.annual_volatility(df_year['PnL%'])
            df_result.iloc[i,3] = ep.max_drawdown(pd.to_numeric(df_year['PnL%']))
            df_result.iloc[i,4] = ep.annual_return(df_year['PnL%'])/ep.annual_volatility(df_year['PnL%']) 
            
    if len(df_result_all) == 0:
        df_result_all = df_result
    else:
        df_result_all = pd.merge(df_result_all,df_result, on = '年份')
df_result_all.to_excel('./return_collar1.xlsx',index=False)