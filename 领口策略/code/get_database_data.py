# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 10:35:20 2021

@author: Xuan
"""


import pandas as pd
import sys,datetime
import current_price as cp
import sql_get_save as sql_data

date = pd.to_datetime(datetime.date.today())

def read_database(kind, choice='intraday'): #kind:50etf/300etf/300index
    
    sql="SELECT * FROM rf order by 日期"
    print("Processing Rf")
    df_rf = sql_data.sql_select(sql)
    df_rf.columns = ['日期','中债国债到期收益率:1年']
    
    if kind =='300etf':
        if choice =='intraday':
            sql="SELECT * FROM option_list_300etf"
            print("Processing 300ETF Option list")
            option_list = sql_data.sql_select(sql)
            option_list.columns = ['日期','期权代码','交易代码','行权价','期权简称','前结算价']
        else:
            sql="SELECT * FROM 日行情_300etf"
            print("Processing 300ETF 日行情")
            option_list = sql_data.sql_select(sql)
            option_list.columns = ['日期','期权代码','交易代码','行权价','期权简称','前结算价','收盘价','结算价']
        
        sql="SELECT * FROM etf_300 order by 日期"
        print("Processing 300ETF daily price")
        df_etf = sql_data.sql_select(sql)
        df_etf.columns = ['日期','close','lnt']
        
        sql="SELECT * FROM contract_info_300etf"
        print("Processing 300ETF contract_info")
        df_contract = sql_data.sql_select(sql)
        df_contract.columns = ['期权代码', '交易代码', '期权简称', '期权标的', '认购认沽', '行权价', '合约单位','上市日', '到期日']
       
        sql="SELECT * FROM fut_IF order by Date"
        print("Processing IF data")
        df_fut = sql_data.sql_select(sql)
        df_fut.columns = ['Date','close','contract','index','price_t','price_t1']
                
        
    elif kind =='50etf':   
        if choice =='intraday':
            sql="SELECT * FROM option_list_50etf"
            print("Processing 50ETF Option list")
            option_list = sql_data.sql_select(sql)
            option_list.columns = ['日期','期权代码','交易代码','行权价','期权简称','前结算价']
        else:
            sql="SELECT * FROM 日行情_50etf"
            print("Processing 50ETF 日行情")
            option_list = sql_data.sql_select(sql)
            option_list.columns = ['日期','期权代码','交易代码','行权价','期权简称','前结算价','收盘价','结算价']
        
        
        sql="SELECT * FROM etf_50 order by 日期"
        print("Processing 50ETF daily price")
        df_etf = sql_data.sql_select(sql)
        df_etf.columns = ['日期','close','lnt']
        
        sql="SELECT * FROM contract_info_50etf"
        print("Processing 50ETF contract_info")
        df_contract = sql_data.sql_select(sql)
        df_contract.columns = ['期权代码', '交易代码', '期权简称', '期权标的', '认购认沽', '行权价', '合约单位','上市日', '到期日']
        
        sql="SELECT * FROM fut_IH order by Date"
        print("Processing IH data")
        df_fut = sql_data.sql_select(sql)
        df_fut.columns = ['Date','close','contract','index','price_t','price_t1']
    
    elif kind == '300index':
        if choice =='intraday':
            sql="SELECT * FROM option_list_300index"
            print("Processing 300index Option list")
            option_list = sql_data.sql_select(sql)
            option_list.columns = ['日期','期权代码','交易代码','行权价','期权简称','前结算价']
        else:
            sql="SELECT * FROM 日行情_300index"
            print("Processing 300index 日行情")
            option_list = sql_data.sql_select(sql)
            option_list.columns = ['日期','期权代码','交易代码','行权价','期权简称','前结算价','收盘价','结算价']
        
        sql="SELECT * FROM etf_300 order by 日期"
        print("Processing 300ETF daily price")
        df_etf = sql_data.sql_select(sql)
        df_etf.columns = ['日期','close','lnt']
        
        sql="SELECT * FROM contract_info_300index"
        print("Processing 300index contract_info")
        df_contract = sql_data.sql_select(sql)
        df_contract.columns = ['期权代码', '交易代码', '期权简称', '期权标的', '认购认沽', '行权价', '合约单位','上市日', '到期日']
       
        sql="SELECT * FROM fut_IF order by Date"
        print("Processing IF data")
        df_fut = sql_data.sql_select(sql)
        df_fut.columns = ['Date','close','contract','index','price_t','price_t1']        
        
    else:
        print('cannot process data')
    
    return df_rf,option_list,df_etf,df_contract,df_fut

def option_price(code,date,kind = '510300.SH'):
    if kind == '510300.SH':
        sql="SELECT 收盘价 FROM 日行情_300etf where 日期 ='%s' and 期权代码 = '%s'"%(date.strftime('%Y-%m-%d'),code)
    elif kind == '510050.SH':
        sql="SELECT 收盘价 FROM 日行情_50etf where 日期 ='%s' and 期权代码 = '%s'"%(date.strftime('%Y-%m-%d'),code)
    elif kind == '000300.SH':
        sql="SELECT 收盘价 FROM 日行情_300index where 日期 ='%s' and 期权代码 = '%s'"%(date.strftime('%Y-%m-%d'),code)
    df = sql_data.sql_select(sql)
    return df.iloc[0,0]




def trading_cal(start_date,end_date = date,exchange = 'SSE'): #exchange:'SSE'/'SZSE'
    sql = "SELECT TRADE_DAYS FROM ASHARECALENDAR where S_INFO_EXCHMARKET = '%s' and TRADE_DAYS > '%s' and TRADE_DAYS <= '%s' order by TRADE_DAYS"%(exchange,start_date.strftime('%Y%m%d'),end_date.strftime('%Y%m%d'))
    df = sql_data.sql_select_wind(sql)
    if len(df) > 0:
        df.columns = ['date']
        return df
    else:
        print("Error to get trading calendays")
    


'''
fut_code300 = cp.active_future_code('IF.CFE',date.strftime('%Y-%m-%d'))['contract'].values[0]
fut_code50 = cp.active_future_code('IH.CFE',date.strftime('%Y-%m-%d'))['contract'].values[0]
'''

'''
fut_code300 = 'IF2104.CFE'
fut_code50 = 'IH2104.CFE'

option_price_300 = cp.get_price(option_list_300etf['期权代码'],'last_price')
option_price_50 = cp.get_price(option_list_50etf['期权代码'],'last_price')
fut_300 = cp.get_price(fut_code300,'last_price','future')
etf_300 = cp.get_price('510300.SH','last_price','future')

fut_50 = cp.get_price(fut_code50,'last_price','future')
etf_50 = cp.get_price('510050.SH','last_price','future')

'''







