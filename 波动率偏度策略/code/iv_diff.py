import numpy as np
import pandas as pd
from pathlib import Path
import scipy.stats as si
import time

'''
整合计算所有使用到的数据，可不运行，直接读取cal_dt.xlsx

option_price = pd.read_excel("../数据/option_price.xlsx",dtype={"期权代码": str})
contract = pd.read_excel("../数据/contract_info.xlsx")[:-2]
etf = pd.read_excel("../数据/50ETF.xlsx")
rf = pd.read_excel("../数据/rf.xlsx")
rf.rename(columns={"指标名称": "日期", "中债国债到期收益率:1年": "r"},inplace=True)
rf = rf[['日期','r']]
#rf = pd.read_excel("../数据/implied_r_30day.xlsx")

#计算iv与greeks
def get_sigma(price, window=60):
    #计算50etf的波动率(前60个交易日的)
    pctChange = price.pct_change().dropna()
    logReturns = np.log(1+pctChange)
    sigma = logReturns.rolling(window+1).std()
    annualized_sigma = sigma * np.sqrt(252)
    return annualized_sigma

def d(s,k,T,r,sigma):
    d1 = (np.log(s / k) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return (d1,d2)

def option_value(s,k,T,r,sigma,n):
    # 计算期权理论价格
    (d1,d2) = d(s,k,T,r,sigma)
    value = n * s * si.norm.cdf(n * d1) - n * k * np.exp(-r * T) * si.norm.cdf(n * d2)
    return value

def vega(s,k,T,r,sigma):
    d1 = d(s,k,T,r,sigma)[0]
    v = s * si.norm.pdf(d1) * np.sqrt(T)
    return v

def iv(s,k,T,r,price,n,iteration=100):
    # 牛顿法
    iv_est = np.sqrt(2 * abs((np.log(s / k) + r * T) / T)) #粗估计
    if T < 4/365:
        return np.nan
    for i in range(iteration):
        diff = option_value(s,k,T,r,iv_est,n) - price
        if (abs(diff) < 1.0e-5):
            return iv_est
        v = vega(s,k,T,r,iv_est)
        if v < 1.0e-30:
            iv_est =  np.nan
            break
        else:
            iv_est -= (diff / v)
    return iv_est

cal_dt = option_price[['日期','期权代码','Delta','Gamma','Vega','Theta','Rho','行权价','收盘价']]
cal_dt = pd.merge(cal_dt, contract[['期权代码','到期日','认购/认沽']], on='期权代码')
cal_dt['T'] = (cal_dt['到期日'] - cal_dt['日期']) / np.timedelta64(365, 'D')
cal_dt['认购/认沽'].replace(['认购','认沽'],[1,-1],inplace=True)
cal_dt = pd.merge(cal_dt, rf, on='日期')
cal_dt['r'] = np.log(1 + cal_dt['r'] / 100)
#cal_dt['r'] = cal_dt['r'] / 100
etf['sigma'] = get_sigma(etf[['close']])
etf.rename(columns={"Date":"日期"},inplace=True)
cal_dt = pd.merge(cal_dt, etf[['日期','close','sigma']], on='日期')
cal_dt.rename(columns={"行权价":"K", '收盘价':"price", '认购/认沽':'n', 'close':'S'},inplace=True)

cal_dt['d1'] = (np.log(cal_dt['S'] / cal_dt['K']) + (cal_dt['r'] + 0.5 * cal_dt['sigma'] ** 2) * cal_dt['T']) / (cal_dt['sigma'] * np.sqrt(cal_dt['T']))
cal_dt['d2'] = cal_dt['d1'] - cal_dt['sigma'] * np.sqrt(cal_dt['T'])
cal_dt['Delta_cal'] = cal_dt['n'] * si.norm.cdf(cal_dt['n'] * cal_dt['d1'])
cal_dt['Vega_cal'] = cal_dt['S'] * si.norm.pdf(cal_dt['d1']) * np.sqrt(cal_dt['T'])
cal_dt['Gamma_cal'] = si.norm.pdf(cal_dt['d1']) / (cal_dt['S'] * cal_dt['sigma'] * np.sqrt(cal_dt['T']))
cal_dt['Theta_cal'] = -1 * (cal_dt['S'] * si.norm.pdf(cal_dt['d1']) * cal_dt['sigma']) / (2 * np.sqrt(cal_dt['T'])) - cal_dt['n'] * cal_dt['r'] * cal_dt['K'] * np.exp(- cal_dt['r'] * cal_dt['T']) * si.norm.cdf(cal_dt['n'] * cal_dt['d2'])
cal_dt['iv'] = cal_dt.apply(lambda row: iv(row['S'], row['K'], row['T'], row['r'], row['price'], row['n']), axis=1)

cal_dt = cal_dt[['日期','期权代码','K','T','n','price','r','S','sigma','Delta', 'Gamma', 'Vega', 'Theta', 'Rho','Delta_cal', 'Vega_cal',
       'Gamma_cal', 'Theta_cal', 'iv']]
cal_dt.to_excel('../数据/cal_dt.xlsx', index=False)
'''


cal_dt = pd.read_excel('../数据/cal_dt.xlsx',dtype={"期权代码": str})
cal_dt.set_index('日期',inplace=True)
date_list = cal_dt.index.unique()
iv_diff_call = pd.DataFrame(index=date_list[61:], columns=['diff','50code','25code'])
iv_diff_put = pd.DataFrame(index=date_list[61:], columns=['diff','50code','25code'])


#找到特定delta值的期权iv值
def get_iv(df, delta, k=10):
    #k为取最接近delta值的前k个
    df_delta = list(abs(df['Delta_cal'] - delta).sort_values().index)[:k]
    df_iv = df.loc[df_delta, 'iv']
    for code in df_delta:
        iv_delta = df_iv[code]
        if not np.isnan(iv_delta):
            break
    return (iv_delta,code)

#找到特定(k/s)值的期权delta值与iv值
def get_iv2(df, k_s, k=10):
    df_k_s = list(abs((df['K'] / df['S']) - k_s).sort_values().index)[:k]
    df_iv = df.loc[df_k_s, 'iv']
    for code in df_k_s:
        iv_k_s = df_iv[code]
        if not np.isnan(iv_k_s):
            break
    return (iv_k_s,code)

#找到行权价固定差两档的期权对
def get_iv3(df, type):
    if type  == 'call':
        d = 2
    else:
        d = -2
    sort_df = df.sort_values(by=['K'])
    code50 = list(abs(sort_df['K'] - sort_df['S']).sort_values().index)[0]
    index50 = list(sort_df.index).index(code50)
    if (index50 + d) < 0:
        return 100
    try:
        code25 = sort_df.iloc[index50+d].name
        iv_d = sort_df.loc[code50, 'iv'] - sort_df.loc[code25, 'iv']
        return (iv_d, code50, code25)
    except IndexError:
        return 100

for date in date_list[61:]:
    print('----',date,'----')
    today_dt = cal_dt.loc[date]
    #选取近月期权
    all_t = np.sort(today_dt['T'].unique())
    near_t = all_t[1]
    if all_t[0] >= 10/365:
        near_t =  all_t[0]
    today_dt_t =  today_dt[today_dt['T']==near_t]
    #删去行权价被击穿的天
    max_k = np.max(today_dt_t.K)
    min_k = np.min(today_dt_t.K)
    if (today_dt_t.S[0] > max_k) or (today_dt_t.S[0] < min_k):
        iv_diff_call.drop(date,inplace=True)
        iv_diff_put.drop(date,inplace=True)
        continue
    
    today_dt_c = today_dt_t[today_dt_t['n']==1].reset_index().set_index('期权代码')
    today_dt_p = today_dt_t[today_dt_t['n']==-1].reset_index().set_index('期权代码')
    
    iv_diff_call.loc[date,'diff'] = (get_iv(today_dt_c, 0.5)[0] - get_iv(today_dt_c, 0.25)[0])
    iv_diff_call.loc[date,'50code'] = get_iv(today_dt_c, 0.5)[1]
    iv_diff_call.loc[date,'25code'] = get_iv(today_dt_c, 0.25)[1]
    iv_diff_put.loc[date,'diff'] = (get_iv(today_dt_p, -0.5)[0] - get_iv(today_dt_p, -0.25)[0])
    iv_diff_put.loc[date,'50code'] = get_iv(today_dt_p, -0.5)[1]
    iv_diff_put.loc[date,'25code'] = get_iv(today_dt_p, -0.25)[1]
    
    '''
    #删去没有两档行权价的天
    if get_iv3(today_dt_c, 'call') == 100:
        iv_diff_call.drop(date,inplace=True)
    else:
        iv_diff_call.loc[date,'diff'] = get_iv3(today_dt_c, 'call')[0]
        iv_diff_call.loc[date,'50code'] = get_iv3(today_dt_c, 'call')[1]
        iv_diff_call.loc[date,'25code'] = get_iv3(today_dt_c, 'call')[2]
    
    if get_iv3(today_dt_p, 'put') == 100:
        iv_diff_put.drop(date,inplace=True)
    else:
        iv_diff_put.loc[date,'diff'] = get_iv3(today_dt_p, 'put')[0]
        iv_diff_put.loc[date,'50code'] = get_iv3(today_dt_p, 'put')[1]
        iv_diff_put.loc[date,'25code'] = get_iv3(today_dt_p, 'put')[2]
    '''

iv_diff_call.dropna(inplace=True)
iv_diff_put.dropna(inplace=True)
iv_diff_call.to_excel('../数据/iv_diff_call.xlsx')
iv_diff_put.to_excel('../数据/iv_diff_put.xlsx')

