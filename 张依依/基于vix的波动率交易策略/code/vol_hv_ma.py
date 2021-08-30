import numpy as np
import pandas as pd

def hv(date, df):
    #20日历史波动率
    a = df[df['日期']==date].index
    hv = np.std(df.loc[a[0]-20:a[0],'diff'],ddof = 1)*100*np.sqrt(252)
    return hv

#优化ghost effect:
#所有涨跌幅超过4%的交易日数据只会在计算n天内的realized vol时用到
def hv_ghost(date, df, n=5):
    a = df[df['日期']==date].index
    dt_candidate = df.loc[a[0]-20:a[0]]
    dt_check = dt_candidate[:-n]
    dt_check['diff'][dt_check['change'].abs() > 0.04] = np.nan
    hv = np.std(dt_candidate['diff'],ddof = 1)*100*np.sqrt(252)
    return hv

#Main Body
df_volatility = pd.read_excel('../数据/df_vix_new.xlsx')
df_etf_dr = pd.read_excel('../数据/etf_new.xlsx')
df_etf_dr['change'] = df_etf_dr['close'].pct_change()
#1.Calculate HV, Spread, MA, EMA
N = 10 #10日平均
alpha = 1/(1+N)
dayseries = df_volatility.日期
df_volatility['HV']  = None
df_volatility['Spread']  = None #波动率差
df_volatility['MA']  = None
df_volatility['EMA']  = None
for i in range(len(dayseries)):
    print(i)
    #df_volatility.HV[i] = hv(dayseries[i],df_etf_dr)
    df_volatility.HV[i] = hv_ghost(dayseries[i],df_etf_dr)
    df_volatility.Spread[i]=df_volatility.iVIX[i]-df_volatility.HV[i]
    if i >= N-1:
        df_volatility.MA[i] = np.average(df_volatility.loc[i-N+1:i,'iVIX'])
        if i >= N:
            df_volatility.EMA[i] = alpha*df_volatility.iVIX[i]+(1-alpha)*df_volatility.EMA[i-1]
        else:
            df_volatility.EMA[i] = df_volatility.iVIX[i]


df_volatility.to_excel('../数据/df_volatility_new.xlsx')

