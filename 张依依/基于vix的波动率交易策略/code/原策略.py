import numpy as np
import pandas as pd
from scipy import stats

def test(rank, percent, col):
    df_test = df_volatility.loc[rank-window:rank]
    test = np.percentile(df_test[col],percent)
    return test
def openshort(rank):
    opens = 0
    if (test(rank, short3, 'iVIX') < df_volatility.iVIX[rank]) & (
            test(rank, short3, 'Spread') < df_volatility.Spread[rank]):
        opens = 3
    elif (test(rank, short2, 'iVIX') <df_volatility.iVIX[rank]) & (
            test(rank, short2, 'Spread') < df_volatility.Spread[rank]):
        opens = 2
    elif (test(rank, short1, 'iVIX') < df_volatility.iVIX[rank]) & (
            test(rank, short1, 'Spread') < df_volatility.Spread[rank]):
        opens = 1

        #if (df_volatility.EMA[rank]< df_volatility.MA[rank]) & (df_volatility.EMA[rank-1]> df_volatility.MA[rank-1]):
        #if (df_volatility.iVIX[rank] > df_volatility.HV[rank]):
    return opens
def openlong(rank):
    openl = 0
    if (df_volatility.iVIX[rank] < df_volatility.HV[rank]):
        if (test(rank,long3,'iVIX') > df_volatility.iVIX[rank]) & (test(rank,long3,'Spread') > df_volatility.Spread[rank]):
            openl = 3
        elif (test(rank,long2,'iVIX') > df_volatility.iVIX[rank]) & (test(rank,long2,'Spread') > df_volatility.Spread[rank]):
            openl = 2
        elif (test(rank,long1,'iVIX') > df_volatility.iVIX[rank]) & (test(rank,long1,'Spread') > df_volatility.Spread[rank]):
            openl = 1
        #if (df_volatility.EMA[rank]> df_volatility.MA[rank]) & (df_volatility.EMA[rank-1]< df_volatility.MA[rank-1]):
    return openl
def closelong(rank):
    closel = 0
    if (test(rank, close_l3, 'iVIX') < df_volatility.iVIX[rank]) & (
            test(rank, close_l3, 'Spread') < df_volatility.Spread[rank]):
        closel = 3
    elif (test(rank, close_l2, 'iVIX') < df_volatility.iVIX[rank]) & (
            test(rank, close_l2, 'Spread') < df_volatility.Spread[rank]):
        closel = 2
    elif (test(rank, close_l1, 'iVIX') < df_volatility.iVIX[rank]) & (
            test(rank, close_l1, 'Spread') < df_volatility.Spread[rank]):
        closel = 1
    #if (df_volatility.EMA[rank]< df_volatility.MA[rank]) & (df_volatility.EMA[rank-1]> df_volatility.MA[rank-1]):
    return closel
def closeshort(rank):
    closes = 0
    if (test(rank, close_s3, 'iVIX') > df_volatility.iVIX[rank]) & (
            test(rank, close_s3, 'Spread') > df_volatility.Spread[rank]):
        closes = 3
    elif (test(rank, close_s2, 'iVIX') > df_volatility.iVIX[rank]) & (
            test(rank, close_s2, 'Spread') > df_volatility.Spread[rank]):
        closes = 2
    elif (test(rank, close_s1, 'iVIX') > df_volatility.iVIX[rank]) & (
            test(rank, close_s1,'Spread') > df_volatility.Spread[rank]):
        closes = 1
           # if (df_volatility.EMA[rank] > df_volatility.MA[rank]) & (df_volatility.EMA[rank - 1] < df_volatility.MA[rank - 1]):
    return closes
def getinfor(i,choice):
    df_tradedate =df_options[df_options['日期'] == datelist[i]]
    etf = df_underlying[df_underlying['日期'] == datelist[i]].close.values[0]
    # d1是第二天所有交易的期权的交易代码,df_tradedate是建仓日的日行情
    d1 = list(df_tradedate['交易代码'].reset_index(drop=True))
    # df_tmrpool是建仓日必要信息，找到购买期权的到期日
    df_tmrpool = pd.DataFrame({'Code': d1, 'maturity': None, 'Strike': None, 'Price': None}).reset_index(
        drop=True)
    for i in range(len(d1)):
        #判断交易代码是否有信息（因除息除权改名）
        if (len(df_contract[df_contract['交易代码'] == d1[i]]) == 0) and ('M' in d1[i]):
            #通过期权代码获取到期日
            contract_code = df_tradedate[df_tradedate['交易代码'] == d1[i]].期权代码.values[0]
            df_tmrpool['maturity'].values[i] = df_contract[df_contract['期权代码'] == contract_code].到期日.values[0]
            df_tmrpool['Strike'].values[i] = df_tradedate[df_tradedate['交易代码'] == d1[i]].行权价.values[0] #从日行情数据中获取strike
        else:
            df_tmrpool['maturity'].values[i] = df_contract[df_contract['交易代码'] == d1[i]].到期日.values[0]
            df_tmrpool['Strike'].values[i] = df_contract[df_contract['交易代码'] == d1[i]].行权价.values[0]
    df_tmrpool = df_tmrpool.reset_index(drop=True)
    # 计算次月合约对应的到期日
    secondMaturity = df_tmrpool['maturity'].drop_duplicates().sort_values(ascending=True).reset_index(drop=True).values[1]
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
        if 'C' in df2.Code.values[j]:
            call = df2.Code.values[j]
        else:
            put = df2.Code.values[j]
    if choice ==1:
        return secondMaturity
    if choice ==2:
        return strike
    if choice ==3:
        return call
    if choice ==4:
        return put
def open(i,choice, df_tradedate):  #i是交易当天的日期
    strike = getinfor(i, 2)
    code_c = getinfor(i,3)
    code_p = getinfor(i,4)
    price_c = df_tradedate[df_tradedate['交易代码']==code_c].收盘价.values[0]
    price_p = df_tradedate[df_tradedate['交易代码'] == code_p].收盘价.values[0]
    delta_c = Greeks(i,1,strike,code_c)
    delta_p = Greeks(i,1,strike,code_p,n =-1)
    vega_c = Greeks(i,4,strike,code_c)
    vega_p = Greeks(i,4,strike,code_p,n =-1)
    ratio = abs(delta_c / delta_p)
    num_c = abs(lot * round(targetvega / ((vega_c + ratio * vega_p) * lot)))
    num_p = abs(lot * round(targetvega * ratio / ((vega_c + ratio * vega_p) * lot)))
    delta = num_c * delta_c + num_p * delta_p
    gamma = num_c * Greeks(i,2,strike,code_c) + num_p * Greeks(i,2,strike,code_p,n=-1)
    vega = num_c * vega_c + num_p * vega_p
    theta = num_c * Greeks(i,3,strike,code_c) + num_p * Greeks(i,3,strike,code_p,n=-1)
    option_1 = num_p*price_p+num_c*price_c
    option_2 = num_c*df_tradedate[df_tradedate['交易代码']==code_c].收盘价.values[0] +num_p*df_tradedate[df_tradedate['交易代码']==code_p].收盘价.values[0]
    #choice1表示open long, choice2表示open short
    cost_o = cost*(num_p+num_c)/lot
    if choice == 1:
        action= 'Open Long'
        cash = df_records.Cash[i-1] - option_1 - cost_o
        end = cash + option_2
        printRecord(i,action,cash,option_1,end,strike,code_c,price_c,num_c,code_p,price_p,num_p,delta,gamma,vega,theta)
    elif choice == 2:
        action = 'Open Short'
        cash = df_records.Cash[i-1] + option_1  - cost_o
        end = cash - option_2
        printRecord(i, action, cash, -option_1, end,strike, code_c, price_c, -num_c, code_p, price_p, -num_p, -delta, -gamma, -vega,
                    -theta)
    df_records.PnL_Others[i] = -cost_o
def close(i, choice, df_tradedate):
    strike = df_records.Strike[i-1]
    code_c = df_records.Code_c[i-1]
    code_p= df_records.Code_p[i-1]
    price_c = df_tradedate[df_tradedate['交易代码']==code_c].收盘价.values[0]
    price_p = df_tradedate[df_tradedate['交易代码']==code_p].收盘价.values[0]
    option = price_c*df_records.Num_c[i-1]+price_p*df_records.Num_p[i-1] #option 可能是负的
    cost_o = cost * abs((df_records.Num_c[i-1] + df_records.Num_p[i-1])) / lot
    cash = df_records.Cash[i-1] + option - cost_o
    #choice1 is to close long, choice2 is to close short
    if choice ==1:
        action = 'Close Long'
    else:
        action = 'Close Short'
    printRecord(i,action,cash,0,cash,0,0,0,0,0,0,0,0,0,0,0)
    print_pnl(i,-cost_o)
def hold(i, choice, df_tradedate):
    action = 'Hold'
    strike = df_records.Strike[i-1]
    code_c = df_records.Code_c[i-1]
    code_p = df_records.Code_p[i-1]
    #判断交易代码是否有信息（因除息除权改名）
    if (len(df_tradedate[df_tradedate['交易代码'] == code_c]) == 0) and ('M' in code_c):
        #通过期权代码获取价格
        df_lastdate = df_options[df_options['日期'] == datelist[i-1]]
        contract_codec = df_lastdate[df_lastdate['交易代码'] == code_c].期权代码.values[0]
        price_c = df_tradedate[df_tradedate['期权代码'] == contract_codec].收盘价.values[0]
        code_c = df_tradedate[df_tradedate['期权代码'] == contract_codec].交易代码.values[0]
    else:
        price_c = df_tradedate[df_tradedate['交易代码'] == code_c].收盘价.values[0]
    if (len(df_tradedate[df_tradedate['交易代码'] == code_p]) == 0) and ('M' in code_p):
        #通过期权代码获取价格
        df_lastdate = df_options[df_options['日期'] == datelist[i-1]]
        contract_codep = df_lastdate[df_lastdate['交易代码'] == code_p].期权代码.values[0]
        price_p = df_tradedate[df_tradedate['期权代码'] == contract_codep].收盘价.values[0]
        code_p = df_tradedate[df_tradedate['期权代码'] == contract_codep].交易代码.values[0]
    else:
        price_p = df_tradedate[df_tradedate['交易代码'] == code_p].收盘价.values[0]
    delta_c = Greeks(i, 1, strike, code_c)
    delta_p = Greeks(i, 1, strike, code_p, n=-1)
    vega_c = Greeks(i, 4, strike, code_c)
    vega_p = Greeks(i, 4, strike, code_p, n=-1)
    ratio = abs(delta_c / delta_p)
    num_c = abs(lot * round(targetvega / ((vega_c + ratio * vega_p) * lot)))
    num_p = abs(lot * round(targetvega * ratio / ((vega_c + ratio * vega_p) * lot)))
    delta = num_c * delta_c + num_p * delta_p
    gamma = num_c * Greeks(i, 2, strike, code_c) + num_p * Greeks(i, 2, strike, code_p, n=-1)
    vega = num_c * vega_c + num_p * vega_p
    theta = num_c * Greeks(i, 3, strike, code_c) + num_p * Greeks(i, 3, strike, code_p, n=-1)
    option_yes = abs(df_records.Num_c[i-1])*price_c+abs(df_records.Num_p[i-1])*price_p
    option = num_p * price_p + num_c * price_c
    option_2 = num_c * df_tradedate[df_tradedate['交易代码'] == code_c].收盘价.values[0] + num_p * df_tradedate[df_tradedate['交易代码'] == code_p].收盘价.values[0]
    #option是正数
    cost_o = (abs(num_c - abs(df_records.Num_c[i-1])) + abs(num_p - abs(df_records.Num_p[i-1]))) * cost / lot
    if choice == 1:
        cash = df_records.Cash[i-1] - cost_o - option+option_yes
        end = cash +option_2
        printRecord(i, action, cash,option,end,strike,code_c, price_c, num_c, code_p, price_p, num_p, delta, gamma, vega,theta)
    else:
        cash = df_records.Cash[i-1] - cost_o + option-option_yes
        end = cash - option_2
        printRecord(i, action,cash,-option,end,strike, code_c, price_c, -num_c, code_p, price_p, -num_p, -delta, -gamma, -vega,-theta)
    print_pnl(i,-cost_o)
def switch(i, choice, df_tradedate):
    strike = getinfor(i, 2)
    code_c = getinfor(i, 3)
    code_p = getinfor(i, 4)
    price_c1 = df_tradedate[df_tradedate['交易代码'] == code_c].收盘价.values[0]
    price_p1 = df_tradedate[df_tradedate['交易代码'] == code_p].收盘价.values[0]
    price_c2 = df_tradedate[df_tradedate['交易代码'] == code_c].收盘价.values[0]
    price_p2 = df_tradedate[df_tradedate['交易代码'] == code_p].收盘价.values[0]
    price_cyes = df_tradedate[df_tradedate['交易代码'] == df_records.Code_c[i-1]].收盘价.values[0]
    price_pyes = df_tradedate[df_tradedate['交易代码'] == df_records.Code_p[i-1]].收盘价.values[0]
    delta_c = Greeks(i, 1, strike, code_c)
    delta_p = Greeks(i, 1, strike, code_p, n=-1)
    vega_c = Greeks(i, 4, strike, code_c)
    vega_p = Greeks(i, 4, strike, code_p, n=-1)
    ratio = abs(delta_c / delta_p)
    num_c = abs(lot * round(targetvega / ((vega_c + ratio * vega_p) * lot)))
    num_p =abs( lot * round(targetvega * ratio / ((vega_c + ratio * vega_p) * lot)))
    delta = num_c * delta_c + num_p * delta_p
    gamma = num_c * Greeks(i, 2, strike, code_c) + num_p * Greeks(i, 2, strike, code_p, n=-1)
    vega = num_c * vega_c + num_p * vega_p
    theta = num_c * Greeks(i, 3, strike, code_c) + num_p * Greeks(i, 3, strike, code_p, n=-1)
    option_1 = num_p * price_p1 + num_c * price_c1
    option_2 = num_c *price_c2 + num_p * price_p2
    option_yes = abs(df_records.Num_c[i-1])*price_cyes+abs(df_records.Num_p[i-1])*price_pyes
    cost_o = (abs(df_records.Num_c[i-1])+abs(df_records.Num_p[i-1])+num_c+num_p)*8/10000
    action = 'Switch'
    if choice ==1:
        cash = df_records.Cash[i-1]+option_yes-option_1 - cost_o
        end = cash +option_2
        printRecord(i, action, cash, option_1, end, strike, code_c, price_c1, num_c, code_p, price_p1, num_p, delta,gamma, vega, theta)
    else:
        cash = df_records.Cash[i-1] - option_yes + option_1 - cost_o
        end = cash - option_2
        printRecord(i, action, cash, -option_1, end,strike, code_c, price_c1, -num_c, code_p, price_p1, -num_p, -delta,-gamma, -vega, -theta)
    print_pnl(i,-cost_o)
def atm(i,old):
    strike = getinfor(i, 2)
    if abs(strike - old) <= 0.1:
        return True
    else:
        return False
def maturity(date, option_code):
    option = df_contract[df_contract['交易代码'] == option_code]
    return (option['到期日'] - date).astype('timedelta64[D]').astype(int).values[0]
def printRecord(i, Action, Cash, Option,End, Strike, Code_c, Price_c, Num_c, Code_p, Price_p, Num_p, Delta, Gamma, Vega, Theta):
    df_records.Action[i] = Action
    df_records.Cash[i] = Cash
    df_records.Option[i] = Option
    df_records.End[i] = End
    if Option ==0:
        margin = 0
    else:
        margin = calmargin(i,Strike,Code_c,Code_p ,Num_c,Num_p)
    df_records.Margin[i] = margin
    df_records.Total[i] = Cash+Option
    if df_records.Total[i] > 0:
        df_records.Return[i] = df_records.Total[i]/df_records.Total[i-1] - 1
    df_records.Strike[i] = Strike
    df_records.Code_c[i] = Code_c
    df_records.Price_c[i] = Price_c
    df_records.Num_c[i] = Num_c
    df_records.Num_p[i ] = Num_p
    df_records.Code_p[i ] = Code_p
    df_records.Price_p[i] = Price_p
    df_records.Delta[i ] = Delta
    df_records.Gamma[i] = Gamma
    df_records.Vega[i ] = Vega
    df_records.Theta[i ] = Theta
def print_pnl(i, cost_o):
    etf_change = df_underlying[df_underlying['日期'] == datelist[i]].close.values[0] -                  df_underlying[df_underlying['日期'] == datelist[i - 1]].close.values[0]
    # pnL_V_c = df_records.Num_c[i-1]*Greeks(i-1,4,strike,code_c)*(Greeks(i,5,strike,code_c,price=price_c)-Greeks(i-1,5,strike,code_c,price=df_records.Price_c[i-1]))
    # pnL_V_p = df_records.Num_p[i-1] * Greeks(i - 1, 4, strike, code_p,n=-1) * (Greeks(i, 5, strike, code_p,price=price_p,n=-1)-Greeks(i - 1, 5, strike, code_p, price=df_records.Price_p[i - 1],n=-1))
    # pnL_Vega = pnL_V_c+pnL_V_p
    df_records.PnL_Total[i] = df_records.Total[i] - df_records.Total[i - 1]
    df_records.PnL_Delta[i] = df_records.Delta[i - 1] * etf_change
    df_records.PnL_Gamma[i] = 0.5 * ((etf_change) ** 2) * df_records.Gamma[i - 1]
    df_records.PnL_Theta[i] = df_records.Theta[i - 1] / 365
    df_records.PnL_Others[i] = cost_o
    df_records.PnL_Vega[i] = df_records.PnL_Total[i] - df_records.PnL_Delta[i] - df_records.PnL_Gamma[i] -                              df_records.PnL_Theta[i] - df_records.PnL_Others[i]
def calmargin(i,strike,code_c, code_p,num_c, num_p ):
    df_date = df_options[df_options['日期'] == datelist[i]]
    settle_c = df_date[df_date['交易代码']==code_c].结算价.values[0]
    settle_p= df_date[df_date['交易代码']==code_p].结算价.values[0]
    etf = df_underlying[df_underlying['日期'] == datelist[i]].close.values[0]
    margin_c = abs(num_c)*(settle_c+0.12*etf)
    margin_p = abs(num_p)*min(strike, settle_p+max(0.07*strike,0.12*etf))
    return margin_c+margin_p
def Greeks(i,choice, strike, code, price=0, n=1): #n默认为1看涨期权的delta, n为-1为看跌期权的delta，price用来算implied vol
    etf = df_underlying[df_underlying['日期'] == datelist[i]].close.values[0]
    hv = df_volatility[df_volatility['日期']==datelist[i]].HV.values[0]/100
    rf = np.log(1+df_rf[df_rf['指标名称']==datelist[i]]['中债国债到期收益率:1年'].values[0] / 100)
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

df_rf = pd.read_excel('../数据/rf_new.xlsx')
df_rf['指标名称'] = pd.to_datetime(df_rf['指标名称'])
df_volatility = pd.read_excel('../数据/df_volatility_new.xlsx')
#删去异常日期
df_volatility = df_volatility[~df_volatility['日期'].isin([pd.DatetimeIndex(['2015-08-25'])[0], pd.DatetimeIndex(['2020-02-03'])[0]])]
df_volatility.index=range(0,len(df_volatility))
df_options = pd.read_excel('../数据/日行情_new.xlsx')
df_underlying = pd.read_excel('../数据/etf_new.xlsx')
df_contract = pd.read_excel('../数据/contract_info_new.xlsx')

window = 30 #回看周期
cost = 3
short1 = 70   #short1,2,3;long1,2,3分别表示分步建仓信号的分位数
short2 = 80
short3 = 90
long1 = 30
long2 = 20
long3 = 10
holding =0     #holding 用于记录目前持有份数，[-3,3]，负数表示short,正数表示long.
threshold =0
close_s1 = 50       #平仓信号
close_s2 = 40
close_s3 = 35
close_l1 = 50
close_l2 = 60
close_l3 = 65
initial = 50000000  #初始资金一千万
targetvega = 1000000  #一次建仓的vega值为一百万
lot = 10000
count_long=0
count_short= 0
datelist = df_volatility.日期[219:] #从16年开始
df_records = pd.DataFrame(columns=('Date','Action','Holdings','Cash','Option','Total','End','Return','Margin','Strike','Code_c','Price_c','Num_c','Code_p','Price_p','Num_p','Delta','Gamma','Vega','Theta','PnL_Total','PnL_Delta','PnL_Gamma','PnL_Theta','PnL_Vega','PnL_Others'))
df_records.Date = datelist
df_records.Cash[268] = initial
df_records.End[268] = initial
df_records.Total[268] = initial
for i in range(269, len(df_volatility.日期)-1):
    print('----',datelist[i],'----')
    print(i)
    etf_close = df_underlying[df_underlying['日期'] == datelist[i]].close.values[0]
    date = datelist[i]
    df_date = df_options[df_options['日期'] == date]
    if holding == 0:
        if openlong(i)!= 0:
            threshold = openlong(i)
            holding = holding + 1
            targetvega = holding*10000000
            count_long= count_long+1
            open(i, 1, df_date)
        elif openshort(i)!=0:
            holding = holding - 1
            threshold = openshort(i)
            targetvega = holding * 10000000
            count_short = count_short+1
            open(i, 2, df_date)
        else:
            df_records.Cash[i] = df_records.Cash[i-1]
            df_records.End[i ] = df_records.End[i-1]
            df_records.Total[i ] = df_records.Total[i-1]
    elif holding > 0:
        if openlong(i) > threshold:
            threshold = openlong(i)
            holding = holding + 1
            #count_long = count_long + 1
            targetvega = holding*10000000
            if atm(i,df_records.Strike[i-1]) == False:
                switch (i,1,df_date)
                df_records.Action[i] = "Add Long"
            else:
                hold(i,1,df_date)
                df_records.Action[i] = "Add Long"
        elif closelong(i)!=0:
            if closelong(i) >= holding:
                holding = 0
                close(i, 1,df_date)
            else:
                holding = holding - closelong(i)
                targetvega = holding*10000000
                if atm(i,df_records.Strike[i - 1]) == False:
                    switch(i, 1, df_date)
                    df_records.Action[i] = "Close Long_P"
                else:
                    hold(i, 1, df_date)
                    df_records.Action[i] = "Close Long_P"
        elif maturity(date, df_records.Code_c[i-1]) < 10 or atm(i,df_records.Strike[i-1]) == False:
            switch(i, 1, df_date)
        else:
            hold(i, 1, df_date)
    else:
        if openshort(i) > threshold:
            threshold = openshort(i)
            holding = holding - 1
            #count_short = count_short + 1
            targetvega = holding*10000000
            if atm(i,df_records.Strike[i-1]) == False:
                switch (i,2,df_date)
                df_records.Action[i] = "Add Short"
            else:
                hold(i,2,df_date)
                df_records.Action[i] = "Add Short"
        elif closeshort(i)!=0:
            if closeshort(i) >= abs(holding):
                holding = 0
                close(i, 2,df_date)
            else:
                holding = holding + closeshort(i)
                targetvega = holding*10000000
                if atm(i,df_records.Strike[i - 1]) == False:
                    switch(i, 2, df_date)
                    df_records.Action[i] = "Close Short_P"
                else:
                    hold(i, 2, df_date)
                    df_records.Action[i] = "Close Short_P"
        elif maturity(date, df_records.Code_c[i-1]) < 10 or atm(i,df_records.Strike[i-1]) == False:
            switch(i, 2, df_date)
        else:
            hold(i, 2, df_date)
    df_records.Holdings[i] = holding
df_records = df_records[df_records['Total'] > 0].reset_index(drop=True)
#df_records.to_excel(r'C:\Users\Boxiu\Documents\实习文件整理_陈斯\基础数据\2019收盘价cost_30d_5_分步_容忍2.xlsx')

#分析盈亏结果
period_long = np.zeros(count_long)
period_short = np.zeros(count_short)
records_long = np.zeros(count_long)
records_short = np.zeros(count_short)
k_long =0
k_short= 0
win_long = 0
win_short = 0
for i in range(0, len(df_records.Date)):
    if df_records.Action[i] == 'Open Long':
        for j in range(i,len(df_records.Date)):
            if df_records.Action[j]=='Close Long':
                records_long[k_long] = df_records.Total[j]-df_records.Total[i]
                period_long[k_long] = j-i+1
                if df_records.Total[j]-df_records.Total[i] > 0:
                    win_long = win_long+1
                k_long = k_long+1
                break
    elif df_records.Action[i] == 'Open Short':
        for j in range(i,len(df_records.Date)):
            if df_records.Action[j]=='Close Short':
                records_short[k_short] = df_records.Total[j] - df_records.Total[i]
                period_short[k_short] = j-i+1
                if df_records.Total[j]-df_records.Total[i] > 0:
                    win_short = win_short+1
                k_short = k_short+1
                break
sum_win = 0
sum_lose =0
for i in range(0,count_long):
    if records_long[i] >= 0:
        sum_win = sum_win+ records_long[i]
    else:
        sum_lose = sum_lose+ records_long[i]
for i in range(0,count_short):
    if records_short[i] >= 0:
        sum_win = sum_win+ records_short[i]
    else:
        sum_lose = sum_lose+ records_short[i]
aver_win = sum_win/(win_long+win_short)
aver_lose = sum_lose/(count_short+count_long-win_long-win_short)

print("Long" )
print( k_long,count_long,sum(period_long)/float(len(period_long)),win_long,win_long/count_long)
print("Short" )
print(k_short,count_short ,sum(period_short)/float(len(period_short)),win_short,win_short/count_short)
print("盈亏比")
print(sum_win*(count_long+count_short-win_short-win_long)/(sum_lose*(win_long+win_short)))
print(aver_win, aver_lose, aver_win/aver_lose)
print("回撤")
print(initial-min(df_records['End']))

df_records.to_excel('../结果/原版.xlsx')


