# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 09:18:04 2021

@author: Xuan
"""


import pandas as pd
import numpy as np
import sys,datetime
import sql_get_save,get_database_data 
from scipy import stats
import empyrical as ep

etf_kind = '300etf'
ps_maintain, ps_open = 95, 95
cl_maintain, cl_open = 115,115
change_day = 10
no_fut = 1
margin_fut_ratio = 0.12 #保证金

#def run_close_position(etf_kind,kind,set_numsl): #'50etf'/'300etf',kind:'call'.'put'
#%%从数据库读历史数据
now = datetime.date.today()
today = pd.to_datetime(now)
# trading calendar
trade_cal = get_database_data.trading_cal(today+datetime.timedelta(days = -20000),today)
trade_cal['date'] = pd.to_datetime(trade_cal['date'])
if trade_cal.iloc[-1,0] == today:
   trade_cal = trade_cal[:len(trade_cal)-1] 
   

# Create df_records
df_records = pd.DataFrame(columns = ['Date', 'Signal','Remark', 'etf','price_fut','Code_fut','No_fut', 
       'Strike_ps', 'Code_ps', 'Num_ps', 'No_ps', 'Lot_ps','T_ps',
       'Strike_cl', 'Code_cl', 'Num_cl','No_cl', 'Lot_cl','T_cl',
       'Margin','Margin_o', 'Margin_f','Option','Futures', 'Cost', 'Cash','PnL_option','PnL_ps','PnL_cl', 'PnL_fut', 'PnL', 'PnL%','净值'])

df_rf, df_options, df_underlying, df_contract,df_fut = get_database_data.read_database(etf_kind,'close')
df_options['日期'] = pd.to_datetime(df_options['日期'])
df_options['期权代码'] = df_options['期权代码'].astype('str')
df_contract['期权代码'] = df_contract['期权代码'].astype('str')
df_underlying['日期'] = pd.to_datetime(df_underlying['日期'])
df_fut['Date'] = pd.to_datetime(df_fut['Date'])
'''
if etf_kind =='50etf':
     last_date_records = pd.to_datetime('2016-01-04')
else:    
    last_date_records = df_volatility['日期'][30]
'''    
#期权日行情最早Date
last_date_records = df_options['日期'].min()
datelist = trade_cal[trade_cal['date']>last_date_records].reset_index(drop=True)

#%% 判断需不需要update df_vol, 日行情
if len(datelist[datelist['date'].isin(df_options['日期'])]) < len(datelist):
    print("Need to update 日行情_%s"%etf_kind)


#%%开始update df_records

def getinfor(date,df_tradedate, etf, return_type,ps_open = ps_open, cl_open = cl_open, change_day = change_day):  #return_type:'atm'/'small','large' 
    df_tradedate = df_tradedate[~df_tradedate['交易代码'].str.contains('A')] #除权除息的合约一般都不是整数档（剔除）
    #计算到期时间不小于10天的对应合约（<15天加仓加到后面，10天近月平仓）
    maturity_list = df_tradedate.loc[df_tradedate['maturity_days']>=change_day,['到期日','maturity_days']].drop_duplicates().sort_values(by='到期日',ascending=True).reset_index(drop=True)
    firstMaturity =  maturity_list['到期日'].drop_duplicates().sort_values(ascending=True).reset_index(drop=True).values[0]

    #算strike
    df1 = df_tradedate.loc[df_tradedate['到期日'] == firstMaturity]
    all_k = df1['行权价'].drop_duplicates().sort_values().reset_index(drop=True) 
    strike = all_k.at[np.argmin(np.abs(all_k-etf))] 
    
    #交易期权的code(first maturity)
    all_put = df1[df1['交易代码'].str.contains('P')]
    all_call = df1[df1['交易代码'].str.contains('C')]

    t =  maturity_list.loc[maturity_list['到期日']==firstMaturity, 'maturity_days'].values[0]
    
    (range_min,range_max) = k_range(etf,'open')
    remark = ''
    if return_type == 'ps':
        strike_small = all_k.at[np.argmin(np.abs(all_k-range_min))]
        if strike_small >= etf: #有可能击穿
            remark += 'small_pk取最小'
            strike_small = all_k[0]
        put_small = df1.loc[(df1['行权价']==strike_small)&(df1['期权代码'].isin(all_put['期权代码'])),'期权代码'].values[0]
        return (t,firstMaturity,strike_small,put_small,remark)
    
    elif return_type == 'cl' :
        strike_large = all_k.at[np.argmin(np.abs(all_k-range_max))]
        if strike_large <= etf: #有可能击穿
            remark += 'large_cl取最大'
            strike_large = all_k.max()
        call_large = df1.loc[(df1['行权价']==strike_large)&(df1['期权代码'].isin(all_call['期权代码'])),'期权代码'].values[0]
        return (t,firstMaturity,strike_large,call_large,remark)

#找price最接近price_ps的contract---to do 0 cost
def getinfor_cl(code_ps,df_tradedate,kind=''):
    t = df_tradedate.loc[df_tradedate['期权代码']==code_ps,'maturity_days'].values[0]
    price_select = df_tradedate.loc[df_tradedate['期权代码']==code_ps,'收盘价'].values[0]
    if kind == 'M' or kind == 'A':
        df = df_tradedate.loc[(df_tradedate['maturity_days']==t)&(df_tradedate['交易代码'].str.contains(kind))&(df_tradedate['交易代码'].str.contains('C')),:].reset_index(drop=True) 
    else:    
        df = df_tradedate.loc[(df_tradedate['maturity_days']==t)&(df_tradedate['交易代码'].str.contains('C')),:].reset_index(drop=True) 
        
    result = df.iloc[(df['收盘价']-price_select).abs().argsort()[0]]
    return (result['maturity_days'],result['到期日'],result['行权价'],result['期权代码'],'')


def cal_position(yesterday, date, etf_yes, etf, df_tradedate,df_contract,df_fut_date): # buy S + buy put, sell call
    #kind: 'call'/'put'
    remark_ps = ''
    remark_cl = ''
    if pd.isnull(df_records[df_records['Date']==yesterday]['Code_ps'].values[0]):  
        (t_ps,_,k_ps,code_ps,remark_ps) = getinfor(date,df_tradedate, etf, 'ps')
        (t_cl,_,k_cl,code_cl,remark_cl) = getinfor_cl(code_ps,df_tradedate,'M')
  
    else:
        #近月
        code_ps_old = str(df_records[df_records['Date']==yesterday]['Code_ps'].values[0])[:8]
        k_ps_old = get_k(code_ps_old, df_tradedate)
        
        code_cl_old = str(df_records[df_records['Date']==yesterday]['Code_cl'].values[0])[:8]
        k_cl_old = get_k(code_cl_old, df_tradedate)
        
    
        t_ps_old = df_tradedate.loc[df_tradedate['期权代码']==code_ps_old,'maturity_days'].values[0]
        t_cl_old = df_tradedate.loc[df_tradedate['期权代码']==code_cl_old,'maturity_days'].values[0]
         
        
        if switch_flag(etf,k_ps_old,t_ps_old,'ps') == False:
            (t_ps,k_ps,code_ps) = (t_ps_old,k_ps_old,code_ps_old)
       
        else:
            (t_ps,_,k_ps,code_ps,remark_ps) = getinfor(date,df_tradedate, etf, 'ps')
    
        
        '''
        if switch_flag(etf,k_cl_old,t_cl_old,'cl') == False:

            (t_cl,k_cl,code_cl) = (t_cl_old,k_cl_old,code_cl_old)
       
        else:
            (t_cl,_,k_cl,code_cl,remark_ps) = getinfor(date,df_tradedate, etf, 'cl')
    
       '''
        #check call/put lot要相等    
        lot_ps = lot(code_ps,df_tradedate,df_contract)
        
        
        if lot_ps != 10000:
            (t_cl,_,k_cl,code_cl,remark_cl) = getinfor_cl(code_ps,df_tradedate,'A')
        else:
            (t_cl,_,k_cl,code_cl,remark_cl) = getinfor_cl(code_ps,df_tradedate,'M')
            
    
        lot_cl = lot(code_cl,df_tradedate,df_contract)
        '''
        if lot_ps != lot_cl:
            if lot_ps != 10000:
                (t_ps,_,k_ps,code_ps,remark_ps) = getinfor(date,df_tradedate, etf, 'ps')
            else:
                (t_cl,_,k_cl,code_cl,remark_ps) = getinfor(date,df_tradedate, etf, 'cl')
    '''            
        
    
    #check call/put lot要相等    
    lot_ps = lot(code_ps,df_tradedate,df_contract)
    lot_cl = lot(code_cl,df_tradedate,df_contract)
    
   
    
    code_fut = df_fut_date.loc[0,'contract']
    price_fut = df_fut_date.loc[0,'price_t']
    
    no_ps = round(price_fut * 300 * no_fut / etf / lot_ps)
    no_cl = -no_ps
    #no_ps,no_cl = 1,-1
    num_ps, num_cl = no_ps * lot_ps, no_cl * lot_cl

        
    printRecord(i,remark_ps+' '+remark_cl, etf, code_fut, no_fut,
                k_ps, code_ps, num_ps, no_ps, lot_ps, t_ps,
                k_cl, code_cl, num_cl, no_cl, lot_cl,t_cl)



def get_data(df,date,kind,df_contract = ''): 
    """
    Parameters
    ----------
    df : dataframe, df_option or df_underlying
    date : today or yesterday
    kind : 'etf'/'option'
    df_contract: optional, only 'option' need
    Returns: etf_price or df_option on specific date
    -------
    """
    if kind == 'option':
        df_date = df[df['日期'] == date]
        df_date = pd.merge(df_date,df_contract.loc[:,['期权代码','到期日']],on = '期权代码', how = 'left')
        df_date['maturity_days'] = (pd.to_datetime(df_date['到期日'])-date).astype('timedelta64[D]').astype(int)
        return df_date
    elif kind == 'etf':
        etf_close = df[df['日期'] == date].close.values[0]
        return etf_close
    elif kind == 'fut':
        df_date = df[df['Date'] == date].reset_index(drop=True)
        return df_date
        
        
        
def k_range(s,kind): #优化的两边尾部期权的行权价选择,t: remaining days
    
    if kind == 'open':
        range_min = s * ps_open/100
        range_max = s * cl_open/100
    elif kind == 'hold':
        range_min = s * ps_maintain/100 
        range_max = s * cl_maintain/100
    else:
        print('error')
    
    return (range_min,range_max)


def get_k (code,df_tradedate):
    k = df_tradedate.loc[df_tradedate['期权代码'].astype('str')==code,'行权价'].values[0] #除权除息期权的行权价会变
    return k           


def lot(code,df_tradedate,df_contract):
    k = df_tradedate[df_tradedate['期权代码'].astype('str') == code].行权价.values[0]
    trading_code = df_contract[df_contract['期权代码'].astype('str') == code].交易代码.values[0]       
    if 'A' in trading_code: #合约单位会因为除息除权而变化
        if  int(trading_code[len(trading_code)-4:len(trading_code)])/1000 == k:
            lot = 10000
        else:
            lot = df_contract[df_contract['期权代码'].astype('str') == code].合约单位.values[0]
    else:
        lot = 10000      
    return lot  


def margin_single_option(settle_yes,strike,num,etf_yes,etf_close,kind): #kind:'call'/'put'
    if num < 0:
        if kind == 'call':
            margin = (settle_yes+max(0.12*etf_yes-max(strike-etf_close,0), 0.07*etf_yes))*abs(num)
        else:
            margin = (min(settle_yes+max(0.12*etf_yes-max(etf_close-strike,0), 0.07*strike),strike))*abs(num) 
    else:
        margin = 0
    return margin

def margin_fut(fut_price,num_fut,ratio_fut = margin_fut_ratio):
    margin = abs(fut_price * 300 * num_fut * ratio_fut)
    return margin

def printRecord(i,Remark, etf, Code_fut, No_fut,
                Strike_ps, Code_ps, Num_ps, No_ps, lot_ps, t_ps,
                Strike_cl, Code_cl, Num_cl, No_cl, lot_cl,t_cl):
    
    df_records.loc[i,'Remark'] = Remark
    
    df_records.loc[i,'etf'] = etf
    df_records.loc[i,'Code_fut'] = Code_fut
    df_records.loc[i,'No_fut'] = No_fut
    
    
    
    df_records.loc[i,'Strike_ps'] = Strike_ps
    df_records.loc[i,'Code_ps'] = Code_ps
    df_records.loc[i,'Num_ps'] = Num_ps
    df_records.loc[i,'No_ps'] = No_ps
    df_records.loc[i,'Lot_ps'] = lot_ps
    df_records.loc[i,'T_ps'] = t_ps

    
    df_records.loc[i,'Strike_cl'] = Strike_cl     
    df_records.loc[i,'Code_cl'] = Code_cl
    df_records.loc[i,'Num_cl'] = Num_cl
    df_records.loc[i,'No_cl'] = No_cl
    df_records.loc[i,'Lot_cl'] = lot_cl
    df_records.loc[i,'T_cl'] = t_cl

    
    #df_records.loc[i,'Code_fut'] = Code_IH
    #df_records.loc[i,'Num_fut'] = Num_IH


def switch_flag(etf,k_old,t_old,option_type): #option_type: 'ps'/'cl'
    # Check if need to change the K
    (range_min,range_max) = k_range(etf,'hold')

    if option_type == 'ps':
        if  range_min - k_old >= 0:
            flag_atm = True
        else:
            flag_atm = False  
     
    if option_type == 'cl':
        if  k_old - range_max <= 0:
            flag_atm = True
        else:
            flag_atm = False 
    
    #change if need to change the T       
    if (t_old >= change_day):
        flag_ismaturity = True
    else:
        flag_ismaturity = False

    # Check if need to change the contract
    if flag_atm and flag_ismaturity:
        return False
    else:
        return True
 
def cal_cost(i,df_fut_yes):
    cost = 0
    if i > 0:
        
        #交易手续费
        if i ==1:
            #IH
            cost = abs(df_records.No_fut[i] * df_records.price_fut[i]*300) * cost_fut
            
            cost += abs(df_records.No_ps[i]) * cost_t
            cost_impact = (abs(df_records.No_ps[i])+abs(df_records.No_cl[i])) * cost_i
            cost += cost_impact
            
        else:
            if df_records.Code_fut[i] == df_records.Code_fut[i-1]:
                cost = abs(df_records.No_fut[i]-df_records.No_fut[i-1]) * df_records.price_fut[i]* 300 *cost_fut
            else:  
                cost = abs(df_records.No_fut[i])* df_records.price_fut[i] * 300 * cost_fut + abs(df_records.No_fut[i-1])*300* df_fut_yes.price_t1[0] * cost_fut
        
            columns = ['ps','cl']
            code_yes = []
            no_yes = []
            code_t = []
            no_t = []
            for code in columns:
                code_yes.append(str(df_records.at[i-1,'Code_'+code])[:8])
                no_yes.append(df_records.at[i-1,'No_'+code])
                code_t.append(str(df_records.at[i,'Code_'+code])[:8])
                no_t.append(df_records.at[i,'No_'+code])
            
            df_pos_yes = pd.DataFrame({'code':code_yes,'t-1':no_yes})
            df_pos_t = pd.DataFrame({'code':code_t,'t':no_t})
            df_pos = pd.merge(df_pos_yes,df_pos_t,on='code',how='outer')
            df_pos = df_pos.fillna(0)
            df_pos['change'] =  df_pos['t'] - df_pos['t-1']
            df_pos['open_short'] = 0
            df_pos.loc[(df_pos['t-1']==0)&(df_pos['t']<0),'open_short'] = 1
            df_pos['cost_impact'] = abs(df_pos['change']) * cost_i
            df_pos['cost_trading'] = np.where(df_pos['open_short'].apply(lambda x: x) == 1, 0, abs(df_pos['change'])*cost_t)
        
    
            cost += df_pos['cost_impact'].sum() + df_pos['cost_trading'].sum()      
                
    return cost

def print_signal(i):
    signal = ''
    if i == 1:
        signal = 'open'
    else:
        if (df_records.Code_ps[i] == df_records.Code_ps[i-1]) and (df_records.Code_cl[i] == df_records.Code_cl[i-1]):
            signal = 'hold'
        else:
            if df_records.Code_ps[i] != df_records.Code_ps[i-1]:
                signal += 'switch ps'
            else:
                signal += 'hold ps'
            if df_records.Code_cl[i] != df_records.Code_cl[i-1]:
                signal += '+switch cl'
            else:
                signal += '+hold cl'
    return signal
        
#%% Main calculation

cost_t = 1.7 #交易手续费
cost_i = 0.5 #冲击成本
cost_fut = 0.000023 #成交金额的万分之零点二三，其中平今仓手续费为成交金额的万分之三点四五
cost_etf = 0.00003



#删去异常日期

#initial = 5000000  #初始资金


# add new date and vix to df_records

datelist = datelist.append({'date':last_date_records},ignore_index=True).sort_values(by='date').reset_index(drop=True)
datelist = datelist[datelist['date'] >= pd.to_datetime('2015-04-16')].reset_index(drop=True) #IH 2015-4-16才开始上市
df_new = df_underlying[df_underlying['日期'].isin(datelist['date'])].reset_index(drop=True)[['日期','close']]
df_new.columns = ['Date','etf']
df_records = df_records.append(df_new,ignore_index=True)
datelist = datelist['date']
df_records = df_records[df_records['Date'].isin(datelist)].reset_index(drop=True)
df_records.loc[0,'净值'] = 1    
df_records.loc[0,'PnL%'] = 0 
for i in range(1,len(datelist)): 
    print(i,'/',len(datelist),'----',datelist[i],'----')
    date = datelist[i]
    yesterday = datelist[i-1]
    # Get etf, option price (today,yesterday)
    etf_close = get_data(df_underlying, date, 'etf')
    etf_yes = get_data(df_underlying, yesterday, 'etf')
    df_date = get_data(df_options, date, 'option', df_contract)
    df_yesterday = get_data(df_options, yesterday, 'option', df_contract)
    df_fut_date =  get_data(df_fut, date, 'fut')
    df_fut_yes =  get_data(df_fut, yesterday, 'fut')
    #Calculate position
    cal_position(yesterday, date, etf_yes, etf_close, df_date,df_contract,df_fut_date)
    df_records.loc[i,'Signal'] = print_signal(i)
    df_records.loc[i,'price_fut'] = df_fut_date.loc[0,'price_t'] 
    
    #Calculate PnL
    try:
        settle_yes = df_date[df_date['期权代码']==df_records.Code_cl[i]].前结算价.values[0]
    except:
        settle_yes = df_date[df_date['期权代码']==df_records.Code_cl[i]].收盘价.values[0]

    df_records.loc[i,'Margin_o'] = margin_single_option(settle_yes,df_records.Strike_cl[i],df_records.Num_cl[i],etf_yes,etf_close,'call')
                                    
    df_records.loc[i,'Margin_f'] =  margin_fut(df_records.price_fut[i],df_records.No_fut[i])
    df_records.loc[i,'Margin'] = df_records.loc[i,'Margin_o'] + df_records.loc[i,'Margin_f']
    
    df_records.loc[i,'Option'] = df_date.loc[df_date['期权代码'] == df_records.Code_ps[i],'收盘价'].values[0] * df_records.Num_ps[i]\
                                + df_date.loc[df_date['期权代码'] == df_records.Code_cl[i],'收盘价'].values[0] * df_records.Num_cl[i]\

    df_records.loc[i,'Futures'] =  df_records.price_fut[i] * df_records.No_fut[i] * 300
    
    df_records.loc[i,'Cost'] = -cal_cost(i,df_fut_yes)
    
    df_records.loc[i,'Cash'] = df_records.loc[i,'Margin'] + df_records.loc[i,'Option'] + df_records.loc[i,'Futures'] + abs(df_records.loc[i,'Cost'])


    #t-1 postion
    if pd.isnull(df_records.Code_ps[i-1]) == False:
        code_ps_yes = df_records.Code_ps[i-1]
        code_cl_yes = df_records.Code_cl[i-1]
       
        code_yes = [code_ps_yes,code_cl_yes]
        num_yes = [df_records.Num_ps[i-1],df_records.Num_cl[i-1]]
        price_yes = [0]*len(code_yes)
        price_today = [0]*len(code_yes)
        for code in range(len(code_yes)):
            price_yes[code] = df_yesterday.loc[df_yesterday['期权代码'] == code_yes[code],'收盘价'].values[0]
            price_today[code] = df_date.loc[df_date['期权代码'] == code_yes[code],'收盘价'].values[0]
        
        price_diff = [price_today[z] - price_yes[z] for z in range(len(code_yes))]
        
   
        df_records.at[i,'PnL_option'] = sum(x*y for x,y in zip(price_diff,num_yes)) 
        df_records.at[i,'PnL_ps'] =  price_diff[0] * num_yes[0]
        df_records.at[i,'PnL_cl'] =  price_diff[1] * num_yes[1]   
        df_records.at[i,'PnL_fut'] = (df_fut_yes.price_t1[0]- df_fut_yes.price_t[0]) * df_records.No_fut[i-1] * 300
        
        df_records.loc[i,'PnL'] = df_records['PnL_option'][i] + df_records['PnL_fut'][i] + df_records['Cost'][i]
        df_records.loc[i,'PnL%'] = df_records.loc[i,'PnL'] / df_records.loc[i-1,'Cash']
        #df_records.loc[i,'净值'] = df_records['PnL%'][i] + df_records['净值'][i-1]
    else:
        df_records.loc[i,'PnL'] = df_records.loc[i,'Cost']
        df_records.loc[i,'PnL%'] = df_records.loc[i,'Cost']/df_records.loc[i,'Cash']
        #df_records.loc[i,'净值'] = df_records['PnL%'][i] + df_records['净值'][i-1]
        
df_records['etf_chg'] = df_records['etf'] / df_records['etf'][0]
df_records.loc[:,'净值'] = (df_records.loc[:,'PnL%']+1).cumprod()

print("Update df_records %s(收盘后)"%etf_kind)
df_records.to_excel('./fut_%s_collar0cost_%s_%s_%s_%s.xlsx'%(etf_kind,datetime.datetime.now().strftime('%m%d'),ps_open,cl_open,cl_maintain), index = False) 

  
