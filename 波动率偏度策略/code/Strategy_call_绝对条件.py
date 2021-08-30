import numpy as np
import pandas as pd
from scipy import stats

cal_dt = pd.read_excel('../数据/cal_dt.xlsx',dtype={"期权代码": str})
cal_dt.set_index('日期',inplace=True)
diff_df = pd.read_excel('../数据/iv_diff_call.xlsx', dtype={'50code': str, '25code':str})
diff_df.set_index('日期',inplace=True)

def openshort(df, rank):
    opens = False
    #>10意味着不做short
    if df.iloc[rank] > 10:
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
    
def openlong(df, rank):
    openl = False
    if df.iloc[rank] < ls:
        openl = True
    return openl

def closelong(df, rank):
    closel = False
    if df.iloc[rank] > csl:
        closel = True
    return closel

def closeshort(df, rank):
    closes = False
    if css > df.iloc[rank]:
        closes = True
    return closes

def getinfor(df_tradedate,code):
    infor = df_tradedate[df_tradedate['期权代码'] == code][['price','Delta_cal','Vega_cal','Gamma_cal','Theta_cal']].values
    price = infor[0][0]
    delta = infor[0][1]
    vega = infor[0][2]
    gamma = infor[0][3]
    theta = infor[0][4]
    return (price,delta,vega,gamma,theta)

def open(i,df_tradedate,code_50,code_25,choice):
    #choice=1:long skew; choice=-1:short skew
    (price_50,delta_50,vega_50,gamma_50,theta_50) = getinfor(df_tradedate,code_50)
    (price_25,delta_25,vega_25,gamma_25,theta_25) = getinfor(df_tradedate,code_25)
    ratio = abs(delta_50 / delta_25)
    num_50 = numk * lot
    num_25 = round(numk * ratio) * lot
    delta = choice * (num_50 * delta_50 - num_25 * delta_25)
    gamma = choice * (num_50 * gamma_50 - num_25 * gamma_25)
    vega = choice * (num_50 * vega_50 - num_25 * vega_25)
    theta = choice * (num_50 * theta_50 - num_25 * theta_25)
    option = choice * (num_50 * price_50 - num_25 * price_25)
    cost_o = cost * (num_50 + num_25) / lot
    cash = df_records.Cash[i-1] - option - cost_o
    if choice == 1:
        action= 'Open Long'
        printRecord(i,action,cash,option,-cost_o,code_50,price_50,num_50,code_25,price_25,-num_25,delta,gamma,vega,theta)
    elif choice == -1:
        action = 'Open Short'
        printRecord(i,action,cash,option,-cost_o,code_50,price_50,-num_50,code_25,price_25,num_25,delta,gamma,vega,theta)
    df_records.PnL_Others[i] = -cost_o

def close(i,df_tradedate,choice):
    code_50 = df_records.Code_50[i-1]
    code_25 = df_records.Code_25[i-1]
    price_50 = df_tradedate[df_tradedate['期权代码'] == code_50].price.values[0]
    price_25 = df_tradedate[df_tradedate['期权代码'] == code_25].price.values[0]
    option = price_50 * df_records.Num_50[i-1] + price_25 * df_records.Num_25[i-1]
    cost_o = cost * (abs(df_records.Num_50[i-1]) + abs(df_records.Num_25[i-1])) / lot
    cash = df_records.Cash[i-1] + option - cost_o
    if choice ==1:
        action = 'Close Long'
    else:
        action = 'Close Short'
    printRecord(i,action,cash,0,-cost_o,0,0,0,0,0,0,0,0,0,0)
    print_pnl(i)
    
def hold(i,df_tradedate,choice):
    action = 'Hold'
    code_50 = df_records.Code_50[i-1]
    code_25 = df_records.Code_25[i-1]
    (price_50,delta_50,vega_50,gamma_50,theta_50) = getinfor(df_tradedate,code_50)
    (price_25,delta_25,vega_25,gamma_25,theta_25) = getinfor(df_tradedate,code_25)
    ratio = abs(delta_50 / delta_25)
    num_50 = numk * lot
    num_25 = round(numk * ratio) * lot
    delta = choice * (num_50 * delta_50 - num_25 * delta_25)
    gamma = choice * (num_50 * gamma_50 - num_25 * gamma_25)
    vega = choice * (num_50 * vega_50 - num_25 * vega_25)
    theta = choice * (num_50 * theta_50 - num_25 * theta_25)
    option = choice * (num_50 * price_50 - num_25 * price_25)
    option_yes = df_records.Num_50[i-1] * price_50 + df_records.Num_25[i-1] * price_25
    cost_o = (abs(num_50 - abs(df_records.Num_50[i-1])) + abs(num_25 - abs(df_records.Num_25[i-1]))) * cost / lot
    cash = df_records.Cash[i-1] - cost_o + option_yes - option
    if choice == 1:
        printRecord(i,action,cash,option,-cost_o,code_50,price_50,num_50,code_25,price_25,-num_25,delta,gamma,vega,theta)
    else:
        printRecord(i,action,cash,option,-cost_o,code_50,price_50,-num_50,code_25,price_25,num_25,delta,gamma,vega,theta)
    print_pnl(i)
    
    
def switch(i,df_tradedate,code_50,code_25,choice,kind):
    #choice=1, long; choice=-1, short
    #kind=1, 换50； kind=2, 换25； kind=3, 全换
    (price_50,delta_50,vega_50,gamma_50,theta_50) = getinfor(df_tradedate,code_50)
    (price_25,delta_25,vega_25,gamma_25,theta_25) = getinfor(df_tradedate,code_25)
    price_50yes = df_tradedate[df_tradedate['期权代码'] == df_records.Code_50[i-1]].price.values[0]
    price_25yes = df_tradedate[df_tradedate['期权代码'] == df_records.Code_25[i-1]].price.values[0]
    ratio = abs(delta_50 / delta_25)
    num_50 = numk * lot
    num_25 = round(numk * ratio) * lot
    delta = choice * (num_50 * delta_50 - num_25 * delta_25)
    gamma = choice * (num_50 * gamma_50 - num_25 * gamma_25)
    vega = choice * (num_50 * vega_50 - num_25 * vega_25)
    theta = choice * (num_50 * theta_50 - num_25 * theta_25)
    option = choice * (num_50 * price_50 - num_25 * price_25)
    option_yes = df_records.Num_50[i-1] * price_50yes + df_records.Num_25[i-1] * price_25yes
    if kind == 1:
        cost_o = (abs(df_records.Num_50[i-1]) + num_50 + abs(num_25 - abs(df_records.Num_25[i-1]))) * cost / lot
        action = 'Switch'
    elif kind == 2:
        cost_o = (abs(df_records.Num_25[i-1]) + num_25 + abs(num_50 - abs(df_records.Num_50[i-1]))) * cost / lot
        action = 'Switch'
    else:
        cost_o = (abs(df_records.Num_25[i-1]) + abs(df_records.Num_50[i-1]) + num_25 + num_50) * cost / lot
        action = 'Switch'
    cash = df_records.Cash[i-1] - cost_o + option_yes - option
    if choice == 1:
        printRecord(i,action,cash,option,-cost_o,code_50,price_50,num_50,code_25,price_25,-num_25,delta,gamma,vega,theta)
    else:
        printRecord(i,action,cash,option,-cost_o,code_50,price_50,-num_50,code_25,price_25,num_25,delta,gamma,vega,theta)
    print_pnl(i)

def printRecord(i, Action, Cash, Option,Cost_o,Code_50, Price_50, Num_50, Code_25, Price_25, Num_25, Delta, Gamma, Vega, Theta):
    df_records.Action[i] = Action
    df_records.Cash[i] = Cash
    df_records.Option[i] = Option
    df_records.Cost[i] = Cost_o
    df_records.Total[i] = Cash + Option
    if df_records.Total[i] > 0:
        df_records.Return[i] = df_records.Total[i] / df_records.Total[i-1] - 1
    df_records.Code_50[i] = Code_50
    df_records.Price_50[i] = Price_50
    df_records.Num_50[i] = Num_50
    df_records.Num_25[i] = Num_25
    df_records.Code_25[i] = Code_25
    df_records.Price_25[i] = Price_25
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
    
    df_records.PnL_Others[i] = df_records.PnL_Total[i] - df_records.PnL_Delta[i] - df_records.PnL_Gamma[i] -  df_records.PnL_Theta[i] - df_records.PnL_Vega[i]

type = 'call'
window = 20
cost = 2
ls = diff_df['diff'].quantile(0.3)
csl = diff_df['diff'].quantile(0.5)
css = diff_df['diff'].quantile(0.5) #忽略
holding = 0
numk = 500
initial = 10000000
lot = 10000
datelist = diff_df.index
df_records = pd.DataFrame(index=datelist, columns=('Action','Holdings','Cash','Option','Cost','Total','Return','Code_50','Price_50','Num_50','Code_25','Price_25','Num_25','Delta','Gamma','Vega','Theta','PnL_Total','PnL_Delta','PnL_Gamma','PnL_Theta','PnL_Vega','PnL_Others'))
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
        if openlong(diff_df['diff'], i):
            holding = holding + 1
            open(i,df_tradedate,code_50,code_25,1)
        elif openshort(diff_df['diff'], i):
            holding = holding - 1
            open(i,df_tradedate,code_50,code_25,-1)
        else:
            df_records.Cash[i] = df_records.Cash[i-1]
            df_records.Total[i] = df_records.Total[i-1] 
    
    elif holding > 0:
        if closelong(diff_df['diff'], i):
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
        if closeshort(diff_df['diff'], i):
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
df_records.to_excel('../结果/call_绝对条件.xlsx')

