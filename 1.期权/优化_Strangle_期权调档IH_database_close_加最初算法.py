# -*- coding: utf-8 -*-
"""
Created on Tue Mar 30 13:39:42 2021

@author: Xuan
"""


import pandas as pd
import numpy as np
import sys,datetime
import sql_get_save,get_database_data 
from scipy import stats

def run_close_position_50etf():
    #%%从数据库读历史数据
    now = datetime.date.today()
    today = pd.to_datetime(now)
    # trading calendar
    trade_cal = get_database_data.trading_cal(today+datetime.timedelta(days = -200),today)
    trade_cal['date'] = pd.to_datetime(trade_cal['date'])
    if trade_cal.iloc[-1,0] == today:
       trade_cal = trade_cal[:len(trade_cal)-1] 
    
    #df_volatility
    sql = "Select * from df_vol_50etf order by 日期 ASC"
    print("Processing df_vol_50ETF")
    df_volatility = sql_get_save.sql_select(sql)
    df_volatility.columns = ['日期', 'iVIX', 'HV', 'Spread', 'MA', 'EMA']
    
    # Results from database
    sql = "Select * from strangle_50etf_5mn order by Date ASC"
    print("Processing df_records 50ETF")
    df_records = sql_get_save.sql_select(sql)
    
    df_records.columns = ['Date', 'Vix', 'flag', 'Signal', 'Action', 'Type', 'Holdings',
           'Strike_c', 'Code_c', 'Num_c', 'No_c', 'Strike_p', 'Code_p', 'Num_p',
           'No_p', 'Code_IH', 'Num_IH', 'Delta', 'Gamma', 'Vega', 'Theta',
           'Margin', 'Option', 'Cost', 'PnL_option', 'PnL_IH', 'PnL', 'PnL_cul',
           '净值']
    
    df_records['Date'] = pd.to_datetime(df_records['Date'])
    
    df_rf, df_options, df_underlying, df_contract,IH = get_database_data.read_database('50etf','close')
    df_options['日期'] = pd.to_datetime(df_options['日期'])
    df_underlying['日期'] = pd.to_datetime(df_underlying['日期'])
    IH['Date'] = pd.to_datetime(IH['Date'])
    last_date_records = df_records['Date'].drop_duplicates().reset_index(drop=True).iloc[-1]
    #last_date_records = pd.to_datetime('2021-03-24')
    datelist = trade_cal[trade_cal['date']>last_date_records].reset_index(drop=True)
    if len(datelist) == 0:
        print("不用更新 df_records_50etf(收盘后)")
        return
    #%% 判断需不需要update df_vol, 日行情
    if len(datelist[datelist['date'].isin(df_volatility['日期'])]) < len(datelist):
        print("Need to update df_vol_50etf")
        return
    if len(datelist[datelist['date'].isin(df_options['日期'])]) < len(datelist):
        print("Need to update 日行情_50etf") 
        return
    
    #%%开始update df_records
    
    def test(rank, percent, col):
        rank = df_volatility[df_volatility['日期']==datelist[rank]].index[0]
        df_test = df_volatility.loc[rank-window:rank]
        test = np.percentile(df_test[col],percent)
        return test
    
    def openshort(rank):
        opens = 0
        rank1 = df_volatility[df_volatility['日期']==datelist[rank]].index[0]
        if (test(rank, short3, 'iVIX') < df_volatility.iVIX[rank1]) & (
                test(rank, short3, 'Spread') < df_volatility.Spread[rank1]):
            opens = 3
        elif (test(rank, short2, 'iVIX') <df_volatility.iVIX[rank1]) & (
                test(rank, short2, 'Spread') < df_volatility.Spread[rank1]):
            opens = 2
        elif (test(rank, short1, 'iVIX') < df_volatility.iVIX[rank1]) & (
                test(rank, short1, 'Spread') < df_volatility.Spread[rank1]):
            opens = 1
        
        #底仓
        elif df_volatility.HV[rank1] < df_volatility.iVIX[rank1]:
            opens = 0.5
        return opens
    
    #绝对条件首先满足
    def new_short(rank):
        rank1 = df_volatility[df_volatility['日期']==datelist[rank]].index[0]
        if df_volatility.iVIX[rank1] > 35:
            hs = 4
        elif df_volatility.iVIX[rank1] > 30:
            hs = 3
        elif df_volatility.iVIX[rank1] > 25:
            hs = 2
        elif df_volatility.iVIX[rank1] < 10:
            hs = 1
        else:
            hs = 0
        return hs
    
    def openlong(rank):
        openl = 0
        rank1 = df_volatility[df_volatility['日期']==datelist[rank]].index[0]
        if (df_volatility.iVIX[rank1] < df_volatility.HV[rank1]):
            if (test(rank,long3,'iVIX') > df_volatility.iVIX[rank1]) & (test(rank,long3,'Spread') > df_volatility.Spread[rank1]):
                openl = 3
            elif (test(rank,long2,'iVIX') > df_volatility.iVIX[rank1]) & (test(rank,long2,'Spread') > df_volatility.Spread[rank1]):
                openl = 2
            elif (test(rank,long1,'iVIX') > df_volatility.iVIX[rank1]) & (test(rank,long1,'Spread') > df_volatility.Spread[rank1]):
                openl = 1
            
            #底仓
            else:
                openl = 0.5
        return openl
    
    def closelong(rank):
        closel = 0
        rank1 = df_volatility[df_volatility['日期']==datelist[rank]].index[0]
        if (test(rank, close_l3, 'iVIX') < df_volatility.iVIX[rank1]) & (
                test(rank, close_l3, 'Spread') < df_volatility.Spread[rank1]):
            closel = 3
        elif (test(rank, close_l2, 'iVIX') < df_volatility.iVIX[rank1]) & (
                test(rank, close_l2, 'Spread') < df_volatility.Spread[rank1]):
            closel = 2
        elif (test(rank, close_l1, 'iVIX') < df_volatility.iVIX[rank1]) & (
                test(rank, close_l1, 'Spread') < df_volatility.Spread[rank1]):
            closel = 1
        return closel
    
    def closeshort(rank):
        closes = 0
        rank1 = df_volatility[df_volatility['日期']==datelist[rank]].index[0]
        if (test(rank, close_s3, 'iVIX') > df_volatility.iVIX[rank1]) & (
                test(rank, close_s3, 'Spread') > df_volatility.Spread[rank1]):
            closes = 3
        elif (test(rank, close_s2, 'iVIX') > df_volatility.iVIX[rank1]) & (
                test(rank, close_s2, 'Spread') > df_volatility.Spread[rank1]):
            closes = 2
        elif (test(rank, close_s1, 'iVIX') > df_volatility.iVIX[rank1]) & (
                test(rank, close_s1,'Spread') > df_volatility.Spread[rank1]):
            closes = 1
        return closes
    
    
    def getinfor(i,product): # product: 'd', 'g'
        j=i
        df_tradedate =df_options[df_options['日期'] == datelist[i]]
        df_tradedate = df_tradedate[~df_tradedate['交易代码'].str.contains('A')] #除权除息的合约一般都不是整数档（剔除）
        df_tradedate.loc[:,'期权代码'] = df_tradedate['期权代码'].astype('str')
        etf = df_underlying[df_underlying['日期'] == datelist[i]].close.values[0]
        # d1是第二天所有交易的期权的交易代码,df_tradedate是建仓日的日行情
        d1 = list(df_tradedate['期权代码'].astype('str').reset_index(drop=True))
        # df_tmrpool是建仓日必要信息，找到购买期权的到期日
        df_tmrpool = pd.DataFrame({'Code': d1, 'maturity': None, 'Strike': None, 'Price': None}).reset_index(
            drop=True)
        for i in range(len(d1)):
            #判断交易代码是否有信息（因除息除权改名）
            df_tmrpool['maturity'].values[i] = df_contract[df_contract['期权代码'] == d1[i]].到期日.values[0]
            df_tmrpool['Strike'].values[i] = df_tradedate[df_tradedate['期权代码'] == d1[i]].行权价.values[0] #从日行情数据中获取strike
        
        df_tmrpool = df_tmrpool.reset_index(drop=True)
        # 计算到期时间不小于10天的对应合约
        first = df_tmrpool['maturity'].drop_duplicates().sort_values(ascending=True).reset_index(drop=True).values[0]
    
        if (first-pd.to_datetime(datelist[j].strftime('%Y-%m-%d'))).to_timedelta64().astype('timedelta64[D]').astype(int) < 10:
            secondMaturity = df_tmrpool['maturity'].drop_duplicates().sort_values(ascending=True).reset_index(drop=True).values[1]
        else:
            secondMaturity = df_tmrpool['maturity'].drop_duplicates().sort_values(ascending=True).reset_index(drop=True).values[0]
        
        #算strike
        df1 = df_tmrpool[df_tmrpool['maturity'] == secondMaturity]
        strike = 100
    
        for k in range(len(df1)):
            a = df1.Strike.values[k]
            if abs(a - etf) < abs(strike - etf):
                strike = a
        #交易期权的code
        df2 = df1[df1['Strike']==strike]
        call = None
        put = None
        for j in range(len(df2)):
            if 'C' in df_tradedate.loc[df_tradedate['期权代码']==df2.Code.values[j],'交易代码'].values[0]:
                call = df2.Code.values[j]
            else:
                put = df2.Code.values[j]
        
        strike_c = strike
        strike_p = strike
        
        if product == 'g': #strangle
            #交易期权的code
            if etf >= strike:
                try:
                    strike_c =  df1[df1['Strike']>strike]['Strike'].drop_duplicates().sort_values(ascending=True).values[0]
                    df2 = df1[df1['Strike']==strike_c]
                    call = None
                    for j in range(len(df2)):
                        if 'C' in df_tradedate.loc[df_tradedate['期权代码']==df2.Code.values[j],'交易代码'].values[0]:
                            call = df2.Code.values[j]
                except IndexError:
                    print('击穿行权价（涨）')
            else:
                try:
                    strike_p =  df1[df1['Strike']<strike]['Strike'].drop_duplicates().sort_values(ascending=False).values[0]
                    df2 = df1[df1['Strike']==strike_p]
                    put = None
                    for j in range(len(df2)):
                        if 'P' in df_tradedate.loc[df_tradedate['期权代码']==df2.Code.values[j],'交易代码'].values[0]:
                            put = df2.Code.values[j]
                except IndexError:
                    print('击穿行权价（跌）')
        return (secondMaturity,strike_c,strike_p,call,put)
    
    
    def get_data(i,k,code,n=1):
    
        if n == 1: #call information
            delta = Greeks(i,1,k,code)
            vega = Greeks(i,4,k,code)
        else: #put information
            delta = Greeks(i,1,k,code,n =-1)
            vega = Greeks(i,4,k,code,n =-1)
        
        lot_option = lot(code)
        
        return(delta,vega,lot_option)
            
            
    
    def compare_delta(i):
        (_,k_cd,k_pd,code_cd,code_pd) = getinfor(i,'d') #straddle information
        (_,k_cg,k_pg,code_cg,code_pg) = getinfor(i,'g') #strangle information
        (delta_cd,vega_cd,lot_cd) = get_data(i,k_cd,code_cd)
        (delta_pd,vega_pd,lot_pd) = get_data(i,k_pd,code_pd,-1)
        (delta_cg,vega_cg,lot_cg) = get_data(i,k_cg,code_cg)
        (delta_pg,vega_pg,lot_pg) = get_data(i,k_pg,code_pg,-1)
        
        # Straddle's delta
        num_cd = abs(lot_cd * round(targetvega / (vega_cd * lot_cd + vega_pd * lot_pd)))
        num_pd = abs(lot_pd * round(targetvega / (vega_cd * lot_cd + vega_pd * lot_pd)))   
        no_cd = num_cd/lot_cd
        no_pd = num_pd/lot_pd
        delta_d = num_cd * delta_cd + num_pd * delta_pd 
        vega_d = num_cd * vega_cd + num_pd * vega_pd 
        
        # Strangle's delta
        num_cg = abs(lot_cg * round(targetvega / (vega_cg * lot_cg + vega_pg * lot_pg)))
        num_pg = abs(lot_pg * round(targetvega / (vega_cg * lot_cg + vega_pg * lot_pg)))    
        no_cg = num_cg/lot_cg
        no_pg = num_pg/lot_pg
        delta_g = num_cg * delta_cg + num_pg * delta_pg
        vega_g = num_cg * vega_cg + num_pg * vega_pg
        
        if abs(delta_d) > abs(delta_g):
            kind = 'Strangle'
            return (k_cg,k_pg,code_cg,code_pg,num_cg,num_pg,no_cg,no_pg,delta_g,vega_g,kind)
        else:
            kind = 'Straddle'
            return (k_cd,k_pd,code_cd,code_pd,num_cd,num_pd,no_cd,no_pd,delta_d,vega_d,kind)
        '''
        kind = 'Straddle'
        return (k_cd,k_pd,code_cd,code_pd,num_cd,num_pd,no_cd,no_pd,delta_d,vega_d,kind)
        '''
    
    def cal_position_new(i,choice, df_tradedate):  #i是交易当天的日期
        (strike_c,strike_p,code_c,code_p,num_c,num_p,no_c,no_p,delta,vega,product_type) = compare_delta(i)
        (code_IH, price_IH) = get_IH(i)   
        delta_IH = IH.loc[IH['Date']==date,'index'].values[0]*300/etf_close # 1张IH的delta
        delta = choice * delta
        
        num_IH = -1 * delta / delta_IH
        delta += delta_IH * num_IH
        
        gamma = choice * (num_c * Greeks(i,2,strike_c,code_c) + num_p * Greeks(i,2,strike_p,code_p,n=-1))
        vega = choice * vega
        theta = choice * (num_c * Greeks(i,3,strike_c,code_c) + num_p * Greeks(i,3,strike_p,code_p,n=-1))
        
        p_c = df_tradedate[df_tradedate['期权代码'].astype('str')==code_c].收盘价.values[0]
        p_p = df_tradedate[df_tradedate['期权代码'].astype('str')==code_p].收盘价.values[0]
        if choice == 1:
            action= 'Long'
            test_option = num_c * p_c + num_p * p_p
            res = 0.8 #option权利金占initial的最大比例
            if test_option/initial > res:
                print('long所买入的期权总金额 > ',int(res*100),'% initial')
                (strike_c,code_c,num_c,no_c,strike_p,code_p,num_p, no_p, num_IH, delta,gamma,vega,theta) = adjust_num(i,res,choice,strike_c,strike_p,code_c,code_p,num_c,num_p,no_c,no_p,delta_IH)
            printRecord(i,signal,action,product_type,strike_c,code_c,num_c,no_c,strike_p,code_p,num_p,no_p,code_IH, num_IH, delta,gamma,vega,theta)
        elif choice == -1:
            action = 'Short' 
            test_margin = calmargin(i,strike_c,strike_p,code_c,code_p,-num_c,-num_p,num_IH, date)
            test_option = num_c * p_c + num_p * p_p
            res = 0.8 #margin占initial的最大比例
            if (test_margin-test_option)/initial > res:
                print('short所需保证金 > ',int(res*100),'% initial')
                (strike_c,code_c,num_c,no_c,strike_p,code_p,num_p, no_p, num_IH, delta,gamma,vega,theta) = adjust_num(i,res,choice,strike_c,strike_p,code_c,code_p,num_c,num_p,no_c,no_p,delta_IH)
                
            printRecord(i,signal,action,product_type,strike_c,code_c,-num_c,-no_c,strike_p,code_p,-num_p,-no_p,code_IH, num_IH, delta,gamma,vega,theta)
    
    def cal_position(i,choice, df_tradedate): #i是交易当天的日期,old method: not use future, only use options to do delta hedging
    
        code_c_old = str(df_records[df_records['Date']==yesdate]['Code_c'].values[0])[:8]
        k_c_old = get_k(code_c_old, df_tradedate)
        
        '''
        code_c_old ='10003284'
        k_c_old = 5.25
        '''
        
        code_p_old = str(df_records[df_records['Date']==yesdate]['Code_p'].values[0])[:8]
        k_p_old = get_k(code_p_old, df_tradedate)
        df_tradedate.loc[:,'期权代码'] = df_tradedate['期权代码'].astype('str')
        df_contract.loc[:,'期权代码'] = df_contract['期权代码'].astype('str')
        df_tradedate = pd.merge(df_tradedate,df_contract[['期权代码','到期日']], on='期权代码',how='left')
        #优化换档
        if (atm(k_c_old)) and (maturity(datelist[i], code_c_old) >= change_day): 
            if atm(k_p_old):
                (k_cd,k_pd,code_cd,code_pd) = (k_c_old,k_p_old,code_c_old,code_p_old)
            else:
                time_to_maturity = df_tradedate[df_tradedate['期权代码']==code_c_old]['到期日'].values[0]
                code_p_new = df_tradedate[(df_tradedate['行权价']==k_c_old)&(df_tradedate['到期日']==time_to_maturity)&(df_tradedate['交易代码'].str.contains('P'))]['期权代码'].values[0]
                (k_cd,k_pd,code_cd,code_pd) = (k_c_old,k_c_old,code_c_old,code_p_new)
        elif (atm(k_p_old)) and (maturity(datelist[i], code_p_old) >= change_day):
            if atm(k_c_old):
                (k_cd,k_pd,code_cd,code_pd) = (k_c_old,k_p_old,code_c_old,code_p_old)
            else:
                time_to_maturity = df_tradedate[df_tradedate['期权代码']==code_p_old]['到期日'].values[0]
                code_c_new = df_tradedate[(df_tradedate['行权价']==k_p_old)&(df_tradedate['到期日']==time_to_maturity)&(df_tradedate['交易代码'].str.contains('C'))]['期权代码'].values[0]
                (k_cd,k_pd,code_cd,code_pd) = (k_p_old,k_p_old,code_c_new,code_p_old)
    
        else:
            (_,k_cd,k_pd,code_cd,code_pd) = getinfor(i,'d') #straddle information
            
        (delta_cd,vega_cd,lot_cd) = get_data(i,k_cd,code_cd)
        (delta_pd,vega_pd,lot_pd) = get_data(i,k_pd,code_pd,-1)
        ratio = abs(delta_cd / delta_pd)
        num_cd = abs(lot_cd * round(targetvega / (vega_cd * lot_cd + vega_pd * ratio * lot_pd)))
        num_pd = abs(lot_pd * round(targetvega * ratio /  (vega_cd * lot_cd + vega_pd * ratio * lot_pd)))
        no_cd = num_cd/lot_cd 
        no_pd = num_pd/lot_pd
        
        p_c = df_tradedate[df_tradedate['期权代码'].astype('str')==code_cd].收盘价.values[0]
        p_p = df_tradedate[df_tradedate['期权代码'].astype('str')==code_pd].收盘价.values[0]
        (code_IH, price_IH) = get_IH(i)
        if choice == 1:
            action= 'Long'
            test_option = num_cd * p_c + num_pd * p_p
            res = 0.8 #option权利金占initial的最大比例
            if test_option/initial > res:
                print('换到new method--long所买入的期权总金额 > ',int(res*100),'% initial')
                cal_position_new(i,choice, df_tradedate)
            else:
                delta = choice * (num_cd *delta_cd + num_pd *delta_pd) 
                gamma = choice * (num_cd * Greeks(i,2,k_cd,code_cd) + num_pd * Greeks(i,2,k_pd,code_pd,n=-1))
                vega = choice * (num_cd *vega_cd + num_pd * vega_pd) 
                theta = choice * (num_cd * Greeks(i,3,k_cd,code_cd) + num_pd * Greeks(i,3,k_pd,code_pd,n=-1))
                printRecord(i,signal,action,'straddle - old method',k_cd,code_cd,num_cd,no_cd,k_pd,code_pd,num_pd,no_pd,code_IH, 0, delta,gamma,vega,theta)
       
        elif choice == -1:
            action = 'Short' 
            test_margin = calmargin(i,k_cd,k_pd,code_cd,code_pd,-num_cd,-num_pd,0, date,'old')
            test_option = num_cd * p_c + num_pd * p_p
            res = 0.8 #margin占initial的最大比例
            if (test_margin-test_option)/initial > res:
                print('换到new method--short所需保证金 > ',int(res*100),'% initial')
                cal_position_new(i,choice, df_tradedate)
            else:    
                delta = choice * (num_cd *delta_cd + num_pd *delta_pd) 
                gamma = choice * (num_cd * Greeks(i,2,k_cd,code_cd) + num_pd * Greeks(i,2,k_pd,code_pd,n=-1))
                vega = choice * (num_cd *vega_cd + num_pd * vega_pd) 
                theta = choice * (num_cd * Greeks(i,3,k_cd,code_cd) + num_pd * Greeks(i,3,k_pd,code_pd,n=-1))
                printRecord(i,signal,action,'straddle - old method',k_cd,code_cd,-num_cd,-no_cd,k_pd,code_pd,-num_pd,-no_pd,code_IH, 0, delta,gamma,vega,theta)
    
    def atm(k_old):
        
        if etf_close <= 3:
            tick = 0.05
        elif etf_close > 3 and etf_close <= 5:
            tick = 0.1
        elif etf_close > 5 and etf_close <= 10:
            tick = 0.25
        elif etf_close > 10 and etf_close <= 20:
            tick = 0.5
        elif etf_close > 20 and etf_close <= 50:
            tick = 1
        elif etf_close > 50 and etf_close <= 100:
            tick = 2.5
        elif etf_close > 100:
            tick = 5
    
        
        if abs(etf_close - k_old) <= tick:
            return True
        else:
            return False  
    
    
    
    def adjust_num(i,restriction,choice,k_c,k_p,code_c,code_p,num_c,num_p,no_c,no_p,delta_IH): #所需金额超过一定限制之后进行调整
        no_c = int(no_c * restriction)
        no_p = int(no_p * restriction)
        (delta_c,vega_c,lot_c) = get_data(i,k_c,code_c)
        (delta_p,vega_p,lot_p) = get_data(i,k_p,code_p,-1)
        num_c = no_c * lot_c
        num_p = no_p * lot_p
        delta = num_c * delta_c + num_p * delta_p 
        vega = num_c * vega_c + num_p * vega_p
        delta = choice * delta
        
        num_IH = -1 * delta / delta_IH
        delta += delta_IH * num_IH
        
        gamma = choice * (num_c * Greeks(i,2,k_c,code_c) + num_p * Greeks(i,2,k_p,code_p,n=-1))
        vega = choice * vega
        theta = choice * (num_c * Greeks(i,3,k_c,code_c) + num_p * Greeks(i,3,k_p,code_p,n=-1))
        
        return (k_c,code_c,num_c,no_c,k_p,code_p,num_p,no_p, num_IH, delta,gamma,vega,theta)
        
    
    
    def close(i, choice, df_tradedate):
        #choice1 is to close long, choice2 is to close short
        if choice ==1:
            action = 'Close Long'
        else:
            action = 'Close Short'
        printRecord(i,signal,action,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)
        
    def get_k (code,df_tradedate):
        k = df_tradedate.loc[df_tradedate['期权代码'].astype('str')==code,'行权价'].values[0] #除权除息期权的行权价会变
        return k
    
    
    def maturity(date, option_code):
        option = df_contract[df_contract['期权代码'].astype('str') == option_code]
        return (option['到期日'] - date).astype('timedelta64[D]').astype(int).values[0]
    
    def printRecord(i, Signal,Action,Type,Strike_c, Code_c, Num_c, No_c, Strike_p, Code_p, Num_p, No_p, Code_IH, Num_IH, Delta, Gamma, Vega, Theta):
        df_records.at[i,'Signal'] = Signal
        df_records.at[i,'Action'] = Action
        df_records.at[i,'Type'] = Type
        df_records.at[i,'Strike_c'] = Strike_c
        df_records.at[i,'Code_c'] = Code_c
        df_records.at[i,'Num_c'] = Num_c
        df_records.at[i,'Num_p'] = Num_p
        df_records.at[i,'No_c'] = No_c
        df_records.at[i,'No_p'] = No_p
        df_records.at[i,'Strike_p'] = Strike_p
        df_records.at[i,'Code_p'] = Code_p
        df_records.at[i,'Code_IH'] = Code_IH
        df_records.at[i,'Num_IH'] = Num_IH
        df_records.at[i,'Delta'] = Delta
        df_records.at[i,'Gamma'] = Gamma
        df_records.at[i,'Vega'] = Vega
        df_records.at[i,'Theta'] = Theta
    
    
    def Greeks(i,choice, strike, code, price=0, n=1): #n默认为1看涨期权的delta, n为-1为看跌期权的delta，price用来算implied vol
        etf = df_underlying[df_underlying['日期'] == datelist[i]].close.values[0]
        hv = df_volatility[df_volatility['日期']==datelist[i]].HV.values[0]/100
        rf = np.log(1+df_rf[df_rf['日期']==datelist[i]]['中债国债到期收益率:1年'].values[0] / 100)
        t = maturity(datelist[i],code)/365
        d1 = (np.log(etf / strike) + (rf + 1 / 2 * hv**2) * t) / (hv * np.sqrt(t))
        d2 = d1 - hv * np.sqrt(t)
        delta = n * stats.norm.cdf(n * d1)
        gamma = stats.norm.pdf(d1) / (etf * hv * np.sqrt(t))
        theta = -1 * (etf * stats.norm.pdf(d1) * hv)/(2 * np.sqrt(t)) - n * rf * strike * np.exp(-rf * t) * stats.norm.cdf(n*d2)
        vega = etf * np.sqrt(t) * stats.norm.pdf(d1)
       #计算的都是出现信号的收盘当天的greeks
        if choice == 1:
            return delta
        elif choice ==2:
            return gamma
        elif choice ==3:
            return theta
        elif choice ==4:
            return vega
        else:
            upper = 1
            lower = float(0)
            sigma = 0.5*(upper+lower)
            test = 0
            iteration = 0
    
            while (abs(test - price) > 1e-4) and (iteration < 100):
                d1_sigma = (np.log(etf / strike) + (rf + sigma ** 2 / 2) * t) / (sigma * np.sqrt(t))
                d2_sigma = d1_sigma - sigma * np.sqrt(t)
                if n == 1:
                    test = etf * stats.norm.cdf(d1_sigma,0.,1.) - strike * np.exp(-rf * t) * stats.norm.cdf(d2_sigma,0.,1.)
                else:
                    test = strike * np.exp(-rf * t) * stats.norm.cdf(-d2_sigma,0.,1.) - etf * stats.norm.cdf(-d1_sigma,0.,1.)
                if test - price > 0:
                    upper = sigma
                    sigma = (sigma + lower) / 2
                else:
                    lower = sigma
                    sigma = (sigma + upper) / 2
                iteration += 1
                
                if sigma < 0.01:
                    sigma = df_volatility.HV[i]
                    break
            return sigma
    
    def lot(code):
        k = df_date[df_date['期权代码'].astype('str') == code].行权价.values[0]
        trading_code = df_contract[df_contract['期权代码'].astype('str') == code].交易代码.values[0]       
        if 'A' in trading_code: #合约单位会因为除息除权而变化
            if  int(trading_code[len(trading_code)-4:len(trading_code)])/1000 == k:
                lot = 10000
            else:
                lot = df_contract[df_contract['期权代码'].astype('str') == code].合约单位.values[0]
        else:
            lot = 10000
            
        return lot  
    
    
    
    def get_IH(i):
        code_IH = IH.loc[IH['Date']==date,'contract'].values[0]
        price_IH = IH.loc[IH['Date']==date,'close'].values[0]
        return (code_IH,price_IH)    
    
    
    
    def calmargin(i,strike_c,strike_p,code_c,code_p, num_c, num_p,num_ih, date, method = 'new'): #开仓保证金
        settle_cyes = df_date[df_date['期权代码']==code_c].前结算价.values[0]
        settle_pyes = df_date[df_date['期权代码']==code_p].前结算价.values[0]
        margin = 0
        
        if method == 'new':
            if num_c < 0:
                margin_c = (settle_cyes+max(0.12*etf_yes-max(strike_c-etf_close,0), 0.07*etf_yes))*abs(num_c)
                margin_p = (min(settle_pyes+max(0.12*etf_yes-max(etf_close-strike_p,0), 0.07*strike_p),strike_p))*abs(num_p) 
                if margin_c <= margin_p:
                    margin = margin_p + settle_cyes * abs(num_c)
                elif margin_c > margin_p:
                    margin = margin_c + settle_pyes * abs(num_p)
            margin = margin * 1.07        
            margin += abs(num_ih) * price_IH * 0.1 * 300 
        else:
            if num_c < 0:
                if abs (num_c) > abs(num_p):
                    num_diff =abs(abs(num_c)-abs(num_p))
                    margin_c = (settle_cyes+max(0.12*etf_yes-max(strike_c-etf_close,0), 0.07*etf_yes))*abs(num_p)
                    margin_p = (min(settle_pyes+max(0.12*etf_yes-max(etf_close-strike_p,0), 0.07*strike_p),strike_p))*abs(num_p) 
                    if margin_c <= margin_p:
                        margin = margin_p + settle_cyes * abs(num_p)
                    elif margin_c > margin_p:
                        margin = margin_c + settle_pyes * abs(num_p)
                    
                    margin_cplus = (settle_cyes+max(0.12*etf_yes-max(strike_c-etf_close,0), 0.07*etf_yes))*abs(num_diff)
                    margin = (margin+margin_cplus) * 1.07  
                elif abs (num_c) <= abs(num_p):
                    num_diff = abs(abs(num_p)-abs(num_c))
                    margin_c = (settle_cyes+max(0.12*etf_yes-max(strike_c-etf_close,0), 0.07*etf_yes))*abs(num_c)
                    margin_p = (min(settle_pyes+max(0.12*etf_yes-max(etf_close-strike_p,0), 0.07*strike_p),strike_p))*abs(num_c) 
                    if margin_c <= margin_p:
                        margin = margin_p + settle_cyes * abs(num_c)
                    elif margin_c > margin_p:
                        margin = margin_c + settle_pyes * abs(num_c)
                    
                    margin_pplus = (min(settle_pyes+max(0.12*etf_yes-max(etf_close-strike_p,0), 0.07*strike_p),strike_p))*abs(num_diff) 
                    margin = (margin+margin_pplus) * 1.07  
        return margin
    
    
    def cal_cost(i):
        cost = 0
        if i > 0:
            '''
            if i ==1:
                #IH
                p_IH = IH.loc[IH['Date']==datelist[i],'close'].values[0]
                cost = abs(df_records.Num_IF[i])*300* p_IH * cost_IH
                if df_records.No_c[i] > 0:
                    cost += abs(df_records.No_c[i]+df_records.No_p[i]) * cost_o
            else:
            '''
            #IH
            p_IH = IH.loc[IH['Date']==datelist[i],'close'].values[0]
            p_IHy = IH.loc[IH['Date']==datelist[i-1],'price_t1'].values[0]
            if df_records.Code_IH[i] == df_records.Code_IH[i-1]:
                cost = abs(df_records.Num_IH[i]-df_records.Num_IH[i-1])*300* p_IH * cost_IH
            else:
                cost = abs(df_records.Num_IH[i])*300* p_IH * cost_IH + abs(df_records.Num_IH[i-1])*300* p_IHy * cost_IH
        
            # call
            if df_records.Code_c[i] == str(df_records.Code_c[i-1])[:8]:
                cost += abs(df_records.No_c[i]-df_records.No_c[i-1]) * cost_o
            else:
                if df_records.No_c[i] < 0: #卖开不要钱
                    cost += abs(df_records.No_c[i-1]) * cost_o
                else:
                    cost += (abs(df_records.No_c[i])+abs(df_records.No_c[i-1])) * cost_o
            #put
            if df_records.Code_p[i] == str(df_records.Code_p[i-1])[:8]:
                cost += abs(df_records.No_p[i]-df_records.No_p[i-1]) * cost_o
            else:
                if df_records.No_p[i] < 0: #卖开不要钱
                    cost += abs(df_records.No_p[i-1]) * cost_o
                else:
                    cost += (abs(df_records.No_p[i])+abs(df_records.No_p[i-1])) * cost_o     
            
        return cost
    
    
    #%% Main calculation
    version = '5mn'  
    cost_o = 2.5
    cost_IH =0.000023 #成交金额的万分之零点二三，其中平今仓手续费为成交金额的万分之三点四五
    
    
    #删去异常日期
    df_volatility = df_volatility[~df_volatility['日期'].isin([pd.DatetimeIndex(['2015-08-25'])[0], pd.DatetimeIndex(['2020-02-03'])[0]])]
    df_volatility.index=range(0,len(df_volatility))
    
    
    change_day = 10
    window = 30 #回看周期
    cost = 2.5
    cost_IH =0.000023 #成交金额的万分之零点二三，其中平今仓手续费为成交金额的万分之三点四五 
    short1 = 70   #short1,2,3;long1,2,3分别表示分步建仓信号的分位数
    short2 = 80
    short3 = 90
    long1 = 30
    long2 = 20
    long3 = 10
    #holding =0     #holding 用于记录目前持有份数，[-3,3]，负数表示short,正数表示long.
    #flag = 0       #开底仓标志(0:未开，1:多，2:空)
    
    flag = df_records.flag[len(df_records)-1]    
    holding = df_records.Holdings[len(df_records)-1]     #holding 用于记录目前持有份数，[-3,3]，负数表示short,正数表示long.
    close_s1 = 50       #平仓信号
    close_s2 = 40
    close_s3 = 35
    close_l1 = 50
    close_l2 = 60
    close_l3 = 65
    #initial = 5000000  #初始资金
    initial = int(float(version.split('m', 1)[0])*1000000)
    targetvega = initial/30  #一次建仓的vega值
    targetvega0 = initial/30
    vegaratio = 10 *targetvega0
    targetvega = holding*vegaratio
    
    
    # add new date and vix to df_records
    df_new = df_volatility[df_volatility['日期'].isin(datelist['date'])].reset_index(drop=True)[['日期','iVIX']]
    df_new.columns = ['Date','Vix']
    df_records = df_records.append(df_new,ignore_index=True)
    datelist = datelist.append({'date':last_date_records},ignore_index=True).sort_values(by='date').reset_index(drop=True)
    datelist = datelist['date']
    df_records = df_records[df_records['Date'].isin(datelist)].reset_index(drop=True)
    for i in range(1,len(datelist)):  
        print(i)
        print('----',datelist[i],'----')
        
        etf_close = df_underlying[df_underlying['日期'] == datelist[i]].close.values[0]
        etf_yes = df_underlying[df_underlying['日期'] == datelist[i-1]].close.values[0]
        date = datelist[i]
        yesdate = datelist[i-1]
        df_date = df_options[df_options['日期'] == date]
        df_yesdate = df_options[df_options['日期'] == datelist[i-1]]
        
        if new_short(i) <= 1:
            if holding == 0:
                if (openlong(i) != 0) & (openlong(i) > openshort(i)):
                    signal = 'long'+str(openlong(i))
                    holding = holding + 1
                    targetvega = holding*vegaratio
                    cal_position(i, 1, df_date)
                    if openlong(i) == 0.5:
                        flag = 1
                elif (openshort(i) != 0) & (openshort(i) > openlong(i)):
                    signal = 'short'+str(openshort(i))
                    holding = holding - 1
                    targetvega = holding * vegaratio
                    cal_position(i, -1, df_date)
                    if openshort(i) == 0.5:
                        flag = 2
                else:
                    df_records.Cash[i] = df_records.Cash[i-1]
                    df_records.Total[i ] = df_records.Total[i-1]
        
            elif (holding == 1) & (flag == 1):
                if openlong(i) > holding:
                    signal = 'long'+str(openlong(i))
                    holding = holding + 1
                    targetvega = holding*vegaratio
                    cal_position(i,1,df_date)
                    df_records.Action[i] = "Add Long"
                    flag = 0
                elif (openlong(i) >= 0.5) & (openlong(i) > openshort(i)):
                    signal = 'long'+str(openlong(i))
                    if (openlong(i) == 0.5):
                        cal_position(i,1,df_date)
                    elif (openlong(i) == 1):  #区分是为了平价期权和原来的处理方式一致
                        cal_position(i,1,df_date)
                        if (df_records.Code_c[i-1] == df_records.Code_c[i]) & (df_records.Code_p[i-1] == df_records.Code_p[i]):
                            df_records.Action[i] = "hold"
                        flag = 0
                elif (openshort(i) >= 0.5) & (openshort(i) > openlong(i)):
                    signal = 'short'+str(openshort(i))
                    holding = -1
                    targetvega = holding*vegaratio
                    cal_position(i,-1,df_date)
                    df_records.Action[i] = "Close Long & Open Short"
                    if openshort(i) == 0.5:
                        flag = 2
                    else:
                        flag = 0
        
            elif (holding > 1) or ((holding == 1) & (flag == 0)):
                if openlong(i) > holding:
                    signal = 'long'+str(openlong(i))
                    holding = holding + 1
                    targetvega = holding*vegaratio
                    cal_position(i,1,df_date)
                    df_records.Action[i] = "Add Long"
                elif closelong(i)!=0:
                    if (closelong(i) >= holding) & (openshort(i) > 0):
                        signal = 'closeL'+str(closelong(i))+' & '+'short'+str(openshort(i))
                        holding = -1
                        targetvega = holding*vegaratio
                        cal_position(i,-1,df_date)
                        df_records.Action[i] = "Close Long & Open Short"
                        if (openshort(i) == 0.5):
                            flag = 2
                        else:
                            flag = 0
                    elif (closelong(i) >= holding) & (openlong(i)==0.5):
                        signal = 'closeL'+str(closelong(i))+' & '+'long'+str(openlong(i))
                        holding = 1
                        targetvega = holding*vegaratio
                        cal_position(i, 1, df_date)
                        df_records.Action[i] = "Close Long_P"
                        flag = 1
                    else:
                        signal = 'closeL'+str(closelong(i))
                        holding = holding - closelong(i)
                        targetvega = holding*vegaratio
                        cal_position(i, 1, df_date)
                        df_records.Action[i] = "Close Long_P"
                elif (holding - openlong(i)) >= 1.5:
                    signal = 'long'+str(openlong(i)) + ' & '+'closeL'
                    holding = holding - 1
                    targetvega = holding*vegaratio
                    cal_position(i, 1, df_date)
                    df_records.Action[i] = "Close Long_P"
                else:
                    signal = 'long'+str(openlong(i))
                    cal_position(i, 1, df_date)
        
            elif (holding < -1) or ((holding == -1) & (flag == 0)):
                if openshort(i) > abs(holding):
                    signal = 'short'+str(openshort(i))
                    holding = holding - 1
                    if ((new_short(i) == 1) & (holding < -1)):
                        holding = -1
                        signal = 'short' + str(new_short(i))
                    targetvega = holding*vegaratio
                    cal_position(i,-1,df_date)
                    if df_records.Holdings[i-1] < holding:
                        df_records.Action[i] = "Close Short_P"
                    elif df_records.Holdings[i-1] > holding:
                        df_records.Action[i] = 'Add Short'
                elif closeshort(i)!=0:
                    if (closeshort(i) >= abs(holding)) & (openlong(i)>0):
                        signal = 'closeS'+str(closeshort(i))+' & '+'long'+str(openlong(i))
                        holding = 1
                        targetvega = holding*vegaratio
                        cal_position(i,1,df_date)
                        df_records.Action[i] = "Close Short & Open Long"
                        if (openlong(i)) == 0.5:
                            flag = 1
                        else:
                            flag = 0
                    elif (closeshort(i) >= abs(holding)) & (openshort(i)==0.5):
                        signal = 'closeS'+str(closeshort(i))
                        holding = -1
                        targetvega = holding*vegaratio
                        cal_position(i, -1, df_date)
                        df_records.Action[i] = "Close Short_P"
                        flag = 2
                    else:
                        signal = 'closeS'+str(closeshort(i))
                        holding = holding + closeshort(i)
                        targetvega = holding*vegaratio
                        cal_position(i, -1, df_date)
                        df_records.Action[i] = "Close Short_P" 
                elif (abs(holding) - openshort(i)) >= 1.5:
                    signal = 'short'+str(openshort(i)) + ' & '+'closeS'
                    holding = holding + 1
                    targetvega = holding*vegaratio
                    cal_position(i, -1, df_date)
                    df_records.Action[i] = "Close Short_P"
                    if openshort(i)==0.5:
                        flag = 2
                    else:
                        flag = 0
                else:
                    signal = 'short'+str(openshort(i))
                    cal_position(i, -1, df_date)
        
            elif (holding == -1) & (flag == 2):
                if openshort(i) > abs(holding):
                    signal = 'short'+str(openshort(i))
                    holding = holding - 1
                    if ((new_short(i) == 1) & (holding < -1)):
                        holding = -1
                        signal = 'short' + str(new_short(i))
                    targetvega = holding*vegaratio
                    cal_position(i,-1,df_date)
                    if df_records.Holdings[i-1] > holding:
                        df_records.Action[i] = 'Add Short'
                    flag = 0
                elif (openshort(i) >= 0.5) & (openshort(i) >= openlong(i)):
                    signal = 'short'+str(openshort(i))
                    if (openshort(i) == 0.5):
                        cal_position(i,-1,df_date)
                    elif (openshort(i) == 1):  #区分是为了平价期权和原来的处理方式一致
                        cal_position(i,-1,df_date)
                        if (df_records.Code_c[i-1] == df_records.Code_c[i]) & (df_records.Code_p[i-1] == df_records.Code_p[i]):
                            df_records.Action[i] = "hold"
                        flag = 0
                elif (openlong(i) >= 0.5) & (openlong(i) > openshort(i)):
                    signal = 'long'+str(openlong(i))
                    holding = 1
                    targetvega = holding*vegaratio
                    cal_position(i,1,df_date)
                    df_records.Action[i] = "Close Short & Open Long"
                    if openlong(i) == 0.5:
                        flag = 1
                    else:
                        flag = 0
                        
        else:
            signal = 'short' + str(new_short(i))
            flag = 0
            if holding == 0:
                holding = -new_short(i)
                targetvega = holding*vegaratio
                cal_position(i, -1, df_date)
            elif holding > 0:
                holding = -new_short(i)
                targetvega = holding*vegaratio
                cal_position(i,-1,df_date)
                df_records.Action[i] = "Close Long & Open Short"
            elif holding > -new_short(i):
                holding = -new_short(i)
                targetvega = holding*vegaratio
                cal_position(i,-1,df_date)
                df_records.Action[i] = "Add Short"
            elif holding < -new_short(i):
                holding = -new_short(i)
                targetvega = holding*vegaratio
                cal_position(i, -1, df_date)
                df_records.Action[i] = "Close Short_P"
            else:
                cal_position(i, -1, df_date)
        
        
        df_records.loc[i,'Holdings'] = holding
        #df_records.Vix[i] = df_volatility[df_volatility['日期']==date]['iVIX'].values[0]
        df_records.loc[i,'flag'] = flag
        
        
        #Calculate PnL
        df_date.loc[:,'期权代码'] = df_date['期权代码'].astype('str')
        df_yesdate.loc[:,'期权代码'] = df_yesdate['期权代码'].astype('str')
        
        if 'old method' in df_records.Type[i]:
            df_records.loc[i,'Margin'] = calmargin(i,df_records.Strike_c[i],df_records.Strike_p[i],str(df_records.Code_c[i])[:8],str(df_records.Code_p[i])[:8], df_records.Num_c[i], df_records.Num_p[i],df_records.Num_IH[i],date,'old')
        else:
            df_records.loc[i,'Margin'] = calmargin(i,df_records.Strike_c[i],df_records.Strike_p[i],str(df_records.Code_c[i])[:8],str(df_records.Code_p[i])[:8], df_records.Num_c[i], df_records.Num_p[i],df_records.Num_IH[i],date)
        df_records.loc[i,'Option'] = df_date.loc[df_date['期权代码'] == str(df_records.Code_c[i])[:8],'收盘价'].values[0] * df_records.Num_c[i] + df_date.loc[df_date['期权代码'] == str(df_records.Code_p[i])[:8],'收盘价'].values[0] * df_records.Num_p[i]
        df_records.loc[i,'Cost'] = -cal_cost(i)
        
        #t-1 postion
        code_c_yes = str(df_records.Code_c[i-1])[:8]
        code_p_yes = str(df_records.Code_p[i-1])[:8]
        
         
        price_c =  df_date.loc[df_date['期权代码'] == code_c_yes,'收盘价'].values[0]
        price_cyes = df_yesdate.loc[df_yesdate['期权代码'] == code_c_yes,'收盘价'].values[0]
        
        price_p =  df_date.loc[df_date['期权代码'] == code_p_yes,'收盘价'].values[0]
        price_pyes = df_yesdate.loc[df_yesdate['期权代码'] == code_p_yes,'收盘价'].values[0]
        
        price_IH =  IH.loc[IH['Date']==datelist[i-1],'price_t1'].values[0]
        
        price_IHyes = IH.loc[IH['Date']==datelist[i-1],'close'].values[0]
        
        df_records.loc[i,'PnL_option'] = (price_c - price_cyes) * df_records.Num_c[i-1] + (price_p - price_pyes) * df_records.Num_p[i-1]
        df_records.loc[i,'PnL_IH'] = (price_IH - price_IHyes) * df_records.Num_IH[i-1]*300
        
        df_records.loc[i,'PnL'] = df_records['PnL_option'][i] + df_records['PnL_IH'][i] + df_records['Cost'][i]
        #df_records['PnL_cul'][i] = df_records['PnL'][:i+1].sum()
        df_records.loc[i,'PnL_cul'] = df_records['PnL'][i] + df_records['PnL_cul'][i-1]
        df_records.loc[i,'净值'] = (initial + df_records['PnL_cul'][i])/initial
        
    
    print("Update df_records 50ETF(收盘后)")
    df_records[1:].to_sql('strangle_50etf_5mn',con = sql_get_save.sql_save(),if_exists = 'append',index = False)    

#%% Main
if __name__ ==  "__main__":
    run_close_position_50etf()
    
    
#EoF





