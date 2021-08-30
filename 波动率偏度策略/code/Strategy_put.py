import numpy as np
import pandas as pd
from scipy import stats

cal_dt = pd.read_excel('../数据/cal_dt.xlsx',dtype={"期权代码": str})
cal_dt.set_index('日期',inplace=True)
diff_df = pd.read_excel('../数据/iv_diff_put.xlsx', dtype={'50code': str, '25code':str})
diff_df.set_index('日期',inplace=True)

def test(df, rank, percent):
    df_test = df.iloc[rank-window:rank]
    test = np.percentile(df_test,percent)
    return test

def openshort(df, rank, percent):
    opens = False
    if test(df,rank,percent) < df.iloc[rank]:
        opens = True
    return opens

#检查是否需要换仓
#若前一交易日的0.5delta期权与0.25delta期权在今交易日都为otm期权且到期日仍不小于10天则不需要换仓以减小交易成本
def test_otm_maturity(df_tradedate, type):
    flag = False
    s = df_tradedate['S'][0]
    k_50 = df_tradedate[df_tradedate['期权代码'] == df_records.Code_50[i-1]]['K'].values[0]
    k_25 = df_tradedate[df_tradedate['期权代码'] == df_records.Code_25[i-1]]['K'].values[0]
    t_50 = df_tradedate[df_tradedate['期权代码'] == df_records.Code_50[i-1]]['T'].values[0]
    t_25 = df_tradedate[df_tradedate['期权代码'] == df_records.Code_25[i-1]]['T'].values[0]
    if type == 'call':
        if ((k_50 >= s) & (k_25 >= s)) & ((t_50 >= 10/365) & (t_25 >= 10/365)):
            flag = True
    else:
        if ((k_50 <= s) & (k_25 <= s)) & ((t_50 >= 10/365) & (t_25 >= 10/365)):
            flag = True
    return flag
    
def openlong(df, rank,  percent):
    openl = False
    if (test(df,rank,percent) > df.iloc[rank]) & (df.iloc[rank] < 0):
        openl = True
    return openl

def closelong(df, rank, percent):
    closel = False
    #if ((test(df,rank,percent) < df.iloc[rank])) or (df.iloc[rank] > 0):
    if test(df,rank,percent) < df.iloc[rank]:
        closel = True
    return closel

def closeshort(df, rank, percent):
    closes = False
    #if (test(df,rank,percent) > df.iloc[rank]) or (df.iloc[rank] < 0):
    if test(df,rank,percent) > df.iloc[rank]:
        closes = True
    return closes

def getinfor(df_tradedate,code):
    infor = df_tradedate[df_tradedate['期权代码'] == code][['price','Delta_cal','Vega_cal','Gamma_cal','Theta_cal']].values
    price = infor[0][0]
    delta = infor[0][1]
    vega = infor[0][2]
    gamma = infor[0][3]
    theta = infor[0][4]
    etf_price = df_tradedate['S'][0]
    return (price,delta,vega,gamma,theta,etf_price)

def open(i,df_tradedate,code_50,code_25,choice):
    #choice=1:long skew; choice=-1:short skew
    (price_50,delta_50,vega_50,gamma_50,theta_50,etf_price) = getinfor(df_tradedate,code_50)
    (price_25,delta_25,vega_25,gamma_25,theta_25,etf_price) = getinfor(df_tradedate,code_25)
    ratio = abs(vega_50 / vega_25)
    num_50 = numk * lot
    num_25 = round(numk * ratio) * lot
    delta_option = choice * (num_50 * delta_50 - num_25 * delta_25)
    num_etf = round( -1 * delta_option / etf_lot) * etf_lot
    delta = delta_option + num_etf
    gamma = choice * (num_50 * gamma_50 - num_25 * gamma_25)
    vega = choice * (num_50 * vega_50 - num_25 * vega_25)
    theta = choice * (num_50 * theta_50 - num_25 * theta_25)
    portfolio = choice * (num_50 * price_50 - num_25 * price_25) + num_etf * etf_price
    cost_p =  cost * (num_50 + num_25) / lot + cost_etf * abs(num_etf) * etf_price
    cash = df_records.Cash[i-1] - portfolio - cost_p
    if choice == 1:
        action= 'Open Long'
        printRecord(i,action,cash,portfolio,-cost_p,code_50,price_50,num_50,code_25,price_25,-num_25,etf_price,num_etf,delta,gamma,vega,theta)
    elif choice == -1:
        action = 'Open Short'
        printRecord(i,action,cash,portfolio,-cost_p,code_50,price_50,-num_50,code_25,price_25,num_25,etf_price,num_etf,delta,gamma,vega,theta)
    df_records.PnL_Others[i] = -cost_p

def close(i,df_tradedate,choice):
    code_50 = df_records.Code_50[i-1]
    code_25 = df_records.Code_25[i-1]
    price_50 = df_tradedate[df_tradedate['期权代码'] == code_50].price.values[0]
    price_25 = df_tradedate[df_tradedate['期权代码'] == code_25].price.values[0]
    etf_price = df_tradedate['S'][0]
    portfolio = price_50 * df_records.Num_50[i-1] + price_25 * df_records.Num_25[i-1] + etf_price * df_records.Num_etf[i-1]
    cost_o = cost * (abs(df_records.Num_50[i-1]) + abs(df_records.Num_25[i-1])) / lot
    cost_e = abs(df_records.Num_etf[i-1]) * etf_price * cost_etf
    cost_p = cost_o + cost_e
    cash = df_records.Cash[i-1] + portfolio - cost_p
    if choice ==1:
        action = 'Close Long'
    else:
        action = 'Close Short'
    printRecord(i,action,cash,0,-cost_p,0,0,0,0,0,0,etf_price,0,0,0,0,0)
    print_pnl(i)
    
def hold(i,df_tradedate,choice):
    action = 'Hold'
    code_50 = df_records.Code_50[i-1]
    code_25 = df_records.Code_25[i-1]
    (price_50,delta_50,vega_50,gamma_50,theta_50,etf_price) = getinfor(df_tradedate,code_50)
    (price_25,delta_25,vega_25,gamma_25,theta_25,etf_price) = getinfor(df_tradedate,code_25)
    ratio = abs(vega_50 / vega_25)
    num_50 = numk * lot
    num_25 = round(numk * ratio) * lot
    delta_option = choice * (num_50 * delta_50 - num_25 * delta_25)
    num_etf = round( -1 * delta_option / etf_lot) * etf_lot
    delta = delta_option + num_etf
    gamma = choice * (num_50 * gamma_50 - num_25 * gamma_25)
    vega = choice * (num_50 * vega_50 - num_25 * vega_25)
    theta = choice * (num_50 * theta_50 - num_25 * theta_25)
    portfolio = choice * (num_50 * price_50 - num_25 * price_25) + num_etf * etf_price
    portfolio_yes = df_records.Num_50[i-1] * price_50 + df_records.Num_25[i-1] * price_25 + df_records.Num_etf[i-1] * etf_price
    cost_o = (abs(num_50 - abs(df_records.Num_50[i-1])) + abs(num_25 - abs(df_records.Num_25[i-1]))) * cost / lot
    cost_e = abs(num_etf - df_records.Num_etf[i-1]) * etf_price * cost_etf
    cost_p = cost_o + cost_e
    cash = df_records.Cash[i-1] - cost_p + portfolio_yes - portfolio
    if choice == 1:
        printRecord(i,action,cash,portfolio,-cost_p,code_50,price_50,num_50,code_25,price_25,-num_25,etf_price,num_etf,delta,gamma,vega,theta)
    else:
        printRecord(i,action,cash,portfolio,-cost_p,code_50,price_50,-num_50,code_25,price_25,num_25,etf_price,num_etf,delta,gamma,vega,theta)
    print_pnl(i)
    
    
def switch(i,df_tradedate,code_50,code_25,choice,kind):
    #choice=1, long; choice=-1, short
    #kind=1, 换50； kind=2, 换25； kind=3, 全换
    (price_50,delta_50,vega_50,gamma_50,theta_50,etf_price) = getinfor(df_tradedate,code_50)
    (price_25,delta_25,vega_25,gamma_25,theta_25,etf_price) = getinfor(df_tradedate,code_25)
    price_50yes = df_tradedate[df_tradedate['期权代码'] == df_records.Code_50[i-1]].price.values[0]
    price_25yes = df_tradedate[df_tradedate['期权代码'] == df_records.Code_25[i-1]].price.values[0]
    ratio = abs(vega_50 / vega_25)
    num_50 = numk * lot
    num_25 = round(numk * ratio) * lot
    delta_option = choice * (num_50 * delta_50 - num_25 * delta_25)
    num_etf = round( -1 * delta_option / etf_lot) * etf_lot
    delta = delta_option + num_etf
    gamma = choice * (num_50 * gamma_50 - num_25 * gamma_25)
    vega = choice * (num_50 * vega_50 - num_25 * vega_25)
    theta = choice * (num_50 * theta_50 - num_25 * theta_25)
    portfolio = choice * (num_50 * price_50 - num_25 * price_25) + num_etf * etf_price
    portfolio_yes = df_records.Num_50[i-1] * price_50yes + df_records.Num_25[i-1] * price_25yes + df_records.Num_etf[i-1] * etf_price
    cost_e = abs(num_etf - df_records.Num_etf[i-1]) * etf_price * cost_etf
    action = 'Switch'
    if kind == 1:
        cost_o = (abs(df_records.Num_50[i-1]) + num_50 + abs(num_25 - abs(df_records.Num_25[i-1]))) * cost / lot
    elif kind == 2:
        cost_o = (abs(df_records.Num_25[i-1]) + num_25 + abs(num_50 - abs(df_records.Num_50[i-1]))) * cost / lot
    else:
        cost_o = (abs(df_records.Num_25[i-1]) + abs(df_records.Num_50[i-1]) + num_25 + num_50) * cost / lot
    cost_p = cost_o + cost_e
    cash = df_records.Cash[i-1] - cost_p + portfolio_yes - portfolio
    if choice == 1:
        printRecord(i,action,cash,portfolio,-cost_p,code_50,price_50,num_50,code_25,price_25,-num_25,etf_price,num_etf,delta,gamma,vega,theta)
    else:
        printRecord(i,action,cash,portfolio,-cost_p,code_50,price_50,-num_50,code_25,price_25,num_25,etf_price,num_etf,delta,gamma,vega,theta)
    print_pnl(i)

def printRecord(i, Action, Cash, Portfolio,Cost_p,Code_50, Price_50, Num_50, Code_25, Price_25, Num_25, Price_etf, Num_etf, Delta, Gamma, Vega, Theta):
    df_records.Action[i] = Action
    df_records.Cash[i] = Cash
    df_records.Portfolio[i] = Portfolio
    df_records.Cost[i] = Cost_p
    df_records.Total[i] = Cash + Portfolio
    if df_records.Total[i] > 0:
        df_records.Return[i] = df_records.Total[i] / df_records.Total[i-1] - 1
    df_records.Code_50[i] = Code_50
    df_records.Price_50[i] = Price_50
    df_records.Num_50[i] = Num_50
    df_records.Num_25[i] = Num_25
    df_records.Code_25[i] = Code_25
    df_records.Price_25[i] = Price_25
    df_records.Price_etf[i] = Price_etf
    df_records.Num_etf[i] = Num_etf
    df_records.Delta[i] = Delta
    df_records.Gamma[i] = Gamma
    df_records.Vega[i] = Vega
    df_records.Theta[i] = Theta
    df_records.PnL_Total[i]= df_records.Total[i] - df_records.Total[i-1]

def print_pnl(i):
    etf_change = cal_dt.loc[date, 'S'][0] - cal_dt.loc[datelist[i-1], 'S'][0]
    df_records.PnL_Total[i] = df_records.Total[i] - df_records.Total[i-1]
    df_records.PnL_Delta[i] = df_records.Delta[i-1] * etf_change
    df_records.PnL_Gamma[i] = 0.5 * ((etf_change) ** 2) * df_records.Gamma[i-1]
    df_records.PnL_Theta[i] = df_records.Theta[i-1] / 365
    
    dfiv_last = cal_dt.loc[datelist[i-1], ['期权代码', 'Vega_cal', 'iv']]
    dfiv = cal_dt.loc[date, ['期权代码', 'iv']]
    div50 = dfiv[dfiv['期权代码'] == df_records.Code_50[i-1]]['iv'].values[0] - dfiv_last[dfiv_last['期权代码'] == df_records.Code_50[i-1]]['iv'].values[0]
    div25 = dfiv[dfiv['期权代码'] == df_records.Code_25[i-1]]['iv'].values[0] - dfiv_last[dfiv_last['期权代码'] == df_records.Code_25[i-1]]['iv'].values[0]
    vega50 = dfiv_last[dfiv_last['期权代码'] == df_records.Code_50[i-1]]['Vega_cal'].values[0]
    vega25 = dfiv_last[dfiv_last['期权代码'] == df_records.Code_25[i-1]]['Vega_cal'].values[0]
    df_records.PnL_Vega[i] = vega50 * df_records.Num_50[i-1] * div50 + vega25 * df_records.Num_25[i-1] * div25
    
    df_records.PnL_Others[i] = df_records.PnL_Total[i] - df_records.PnL_Delta[i] - df_records.PnL_Gamma[i] - df_records.PnL_Theta[i] - df_records.PnL_Vega[i]


type = 'put'
window = 20
cost = 2 #期权交易费用
short = 95 #short开仓分位数
long = 15 #long开仓分位数
holding = 0 #long skew holding=1，short skew holding=-1
close_s = 55 #short平仓分位数
close_l = 50 #long平仓分位数
numk = 500  #买卖系数
initial = 10000000 #初始资金
lot = 10000 #合约乘数
etf_lot = 100 #etf乘数
cost_etf = 0.00015 #etf交易费用
datelist = diff_df.index
df_records = pd.DataFrame(index=datelist, columns=('Action','Holdings','Cash','Portfolio','Cost','Total','Return','Code_50','Price_50','Num_50','Code_25','Price_25','Num_25','Price_etf','Num_etf','Delta','Gamma','Vega','Theta','PnL_Total','PnL_Delta','PnL_Gamma','PnL_Theta','PnL_Vega','PnL_Others'))
df_records.iloc[window]['Cash'] = initial
df_records.iloc[window]['End'] = initial
df_records.iloc[window]['Total'] = initial
for i in range(window+1, len(datelist)):
    date = datelist[i]
    print(i)
    print('----',date,'----')
    df_tradedate = cal_dt.loc[date]
    code_50 = diff_df.loc[date,'50code']
    code_25 = diff_df.loc[date,'25code']
    if holding == 0:
        if openlong(diff_df['diff'], i, long):
            holding = holding + 1
            open(i,df_tradedate,code_50,code_25,1)
        elif openshort(diff_df['diff'], i, short):
            holding = holding - 1
            open(i,df_tradedate,code_50,code_25,-1)
        else:
            df_records.Cash[i] = df_records.Cash[i-1]
            df_records.Total[i] = df_records.Total[i-1] 
    
    elif holding > 0:
        if closelong(diff_df['diff'], i, close_l):
            holding = holding - 1
            close(i, df_tradedate, 1)
        elif ((code_50 == df_records.Code_50[i-1]) & (code_25 == df_records.Code_25[i-1])) or (test_otm_maturity(df_tradedate, type)):
        #elif ((code_50 == df_records.Code_50[i-1]) & (code_25 == df_records.Code_25[i-1])):  
            hold(i,df_tradedate,1)
        elif (code_50 != df_records.Code_50[i-1]) & (code_25 == df_records.Code_25[i-1]):
            switch(i,df_tradedate,code_50,code_25,1,1)
        elif (code_50 == df_records.Code_50[i-1]) & (code_25 != df_records.Code_25[i-1]):
            switch(i,df_tradedate,code_50,code_25,1,2)
        elif (code_50 != df_records.Code_50[i-1]) & (code_25 != df_records.Code_25[i-1]):
            switch(i,df_tradedate,code_50,code_25,1,3)
    
    else:
        if closeshort(diff_df['diff'], i, close_s):
            close(i, df_tradedate, -1)
            holding = holding + 1
        elif ((code_50 == df_records.Code_50[i-1]) & (code_25 == df_records.Code_25[i-1])) or (test_otm_maturity(df_tradedate, type)):
        #elif ((code_50 == df_records.Code_50[i-1]) & (code_25 == df_records.Code_25[i-1])):
            hold(i,df_tradedate,-1)
        elif (code_50 != df_records.Code_50[i-1]) & (code_25 == df_records.Code_25[i-1]):
            switch(i,df_tradedate,code_50,code_25,-1,1)
        elif (code_50 == df_records.Code_50[i-1]) & (code_25 != df_records.Code_25[i-1]):
            switch(i,df_tradedate,code_50,code_25,-1,2)
        elif (code_50 != df_records.Code_50[i-1]) & (code_25 != df_records.Code_25[i-1]):
            switch(i,df_tradedate,code_50,code_25,-1,3)
            
    df_records.Holdings[i] = holding

df_records = df_records.iloc[window:]
df_records.to_excel('../put.xlsx')





