import numpy as np
import pandas as pd

cal_dt = pd.read_excel('../数据/cal_dt.xlsx',dtype={"期权代码": str})
cal_dt.set_index('日期',inplace=True)
date_list = cal_dt.index.unique()

def getinfor(df_tradedate, s):
    #得到c,p,k,t,s
    #month=0,当月期权；month=1,次月期权
    df_tmrpool = df_tradedate[['期权代码','K','T','n','price']]
    # 计算当/次月合约对应的到期日
    '''
    t = np.sort(df_tmrpool['T'].unique())[0]
    if t < 10/365:
        t = np.sort(df_tmrpool['T'].unique())[1]
    '''
    t0 = np.sort(df_tmrpool['T'].unique())[0]
    t1 = np.sort(df_tmrpool['T'].unique())[1]
    #选到期日最接近30天的期权
    if abs(t0-30/365) < abs(t1-30/365):
        t = t0
    else:
        t = t1
    #算strike(找到与etf价格最接近的strike)
    df_t = df_tmrpool[df_tmrpool['T'] == t]
    k = 100
    for i in range(len(df_t)):
        a = df_t['K'].values[i]
        if abs(a - s) < abs(k - s):
            k = a
    c = df_t[(df_t['K']==k) & (df_t['n']==1)].price.values[0]
    p = df_t[(df_t['K']==k) & (df_t['n']==-1)].price.values[0]
    return (c,p,k,t)

df = pd.DataFrame(index=date_list, columns=['c','p','k','t','s'])
for date in date_list:
    df_tradedate = cal_dt.loc[date]
    s = df_tradedate['S'][0]
    df.loc[date,'s'] = s
    df.loc[date,['c','p','k','t']] = getinfor(df_tradedate, s)
implied_r = (- np.log(((df['p'] - df['c'] + df['s']) / df['k']).astype('float64')) / df['t']) * 100


implied_r.replace(np.inf, np.nan, inplace=True)
implied_r.replace(-np.inf, np.nan, inplace=True)
implied_r.fillna(method='ffill',inplace=True)
implied_r.to_excel('../数据/implied_r1.xlsx')

