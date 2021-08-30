# Signal



# zltsql.py

用于连接SQL数据库并获取数据



# daily_iv.py

根据每日收盘时的数据获取当天每个合约的IV、Delta数据并计算远期波动率



**获取每日的远期波动率**

```python
get_daily_iv(code, date, df_rf, df_contract, df_stock, expire_threshold=10):
    """
    计算每天的远期波动率
    :param code: 标的代码（不带'.SH')
    :param date: 日期
    :param df_rf: 包含无风险利率的DataFrame
    :param df_contract: 包含每日期权信息的 Dataframe
    :param df_stock: 标的资产的ohlc数据 DataFrame
    :param expire_threshold: 近月合约距离交割小于一定天数时返回次月合约的0.5delta波动率，默认10天
    :return: 远期波动率
    """
```

例如获取50ETF期权在2021年4月1日的远期波动率，其中df_rf为数据库中的'rf'，df_stock为'etf_50', df_contract为剔除掉除权合约的'contract_info_daily'：

```python
stock_code = '510050.SH'

SQ = SQLConn()
df_contract_all = SQ.GetData('contract_info_daily')
df_contract_all = df_contract_all[df_contract_all['交易代码'].apply(lambda x: x.find('M') >= 0)]
df_contract = df_contract_all[df_contract_all['期权标的'] == stock_code].reset_index(drop=True)  

df_rf = SQ.GetData('rf')  # 无风险利率

df_stock = SQ.GetData('etf_50')
SQ.CloseSql()

get_daily_iv('510050', '20210401', df_rf, df_contract, df_stock, expire_threshold=10)
```



**获取包含当天所有期权合约行权价、认购认沽、到期日、到期时间（年化）、IV、Delta的DataFrame**

```python
get_daily_iv_delta(code, date, df_rf, df_contract, df_stock):
    """
    计算每天所有期权合约的IV和Delta
    :param code: 标的代码（不带'.SH')
    :param date: 日期
    :param df_rf: 包含无风险利率的DataFrame
    :param df_contract: 包含每日期权信息的 Dataframe
    :param df_stock: 标的资产的ohlc数据 DataFrame
    :return: 包含行权价、认购认沽、到期日、到期时间（年化）、IV、delta的DataFrame
    """
```

例如获取2021年4月1日所有50ETF期权（不含除权）的上述信息：

```python
get_daily_iv_delta('510050', '20210401', df_rf, df_contract, df_stock)
```



# vega_signal.py

```python
get_vega_signal(date, code, hv_data, prev_position=None, normal_positon=-0.3, upper_bnd=75, mid_bnd=50, 			    lower_bnd=25,upper_nums=3, lower_nums=2, hv_type='HV20'):
    """
    vega signal
    :param date: 当天的日期
    :param code: 标的代码
    :param hv_data: 包含所有HV数据的DataFrame
    :param prev_position: 前一天的信号
    :param normal_positon: HV在mid档位附近时的信号，默认0.3
    :param upper_bnd: 最高的分位点，默认为75
    :param mid_bnd: 中间的分位点，默认为50
    :param lower_bnd: 最低的分位点，默认为25
    :param upper_nums: 在mid和upper区间内的档位数量，默认为3
    :param lower_nums: 在mid和lower区间内的档位数量，默认为2
    :param hv_type: HV类型，默认为HV20
    :return: signal
    """
```

例如，获取2015年以来每日的vega signal，参数为默认值：

```python
SQ = SQLConn()
df_HVpercentile = SQ.GetData('HV_percentile')
df_contract_all = SQ.GetData('contract_info_daily')
df_contract_all = df_contract_all[df_contract_all['交易代码'].apply(lambda x: x.find('M') >= 0)]
df_contract = df_contract_all[df_contract_all['期权标的'] == '510050.SH'].reset_index(drop=True)  
SQ.CloseSql()

date_list = df_contract['日期'].apply(lambda x: datetime.datetime.strftime(x, '%Y%m%d')).unique()
df_vega = pd.DataFrame({'Date': date_list})
now_positon = None
for i in range(len(df_vega)):
    date_today = df_vega.at[i, 'Date']
    print(date_today)
    now_positon = get_vega_signal(date_today, '510050.SH', hv_data=df_HVpercentile, 				  prev_position=now_positon)
    df_vega.at[i, 'vega_signal'] = now_positon
```



# gamma_signal.py

```python
get_gamma_signal(date, code, hv_data, df_stock, prev_position=None, normal_position=-0.5,
                 normal_percentile=50, hv_short='HV5', hv_long='HV20',
                 upper_bnd_short=75, upper_bnd_long=25, lower_bnd_long=10, min_long_position=0.5,
                 short_nums=1, long_nums=1, vol_type='VIX'):
    """
    vega signal
    :param date: 当天的日期
    :param code: 标的代码
    :param hv_data: 包含所有HV数据的DataFrame
    :param prev_position: 前一天的信号
    :param normal_position: 正常的gamma信号，默认-0.5
    :param normal_percentile: 位于多空中间的一个分位数点，默认50
    :param hv_short: 短期的HV，默认HV5
    :param hv_long: 长期的HV，默认HV20
    :param upper_bnd_short: 当天的短期HV高于这个分位数后的signal都为-1
    :param upper_bnd_long: 当天的短期HV高于这个分位数后long position不再减小，默认为25
    :param lower_bnd_long: 当天的短期HV低于这个分位数后的signal都为1，默认为10
    :param min_long_position: 短期HV触及upper_bnd_long后的signal，默认为0.5
    :param short_nums: 从upper_bnd_short到normal_percentil之间分档的数量，默认为1
    :param long_nums: 从upper_bnd_long到lower_bnd_long之间分档的数量，默认为1
    :param vol_type: 'IV' or 'VIX' or 'HV**', 如果是HV的话需要指定多少日的HV，比如'HV20'
    """
```

例如，获取2015年以来每日的gamma signal，参数为默认值：

```python
date_list = df_contract['日期'].apply(lambda x: datetime.datetime.strftime(x, '%Y%m%d')).unique()
df_gamma = pd.DataFrame({'Date': date_list})
now_positon = None
for i in range(len(df_gamma)):
    date_today = df_gamma.at[i, 'Date']
    print(date_today)
    now_positon = get_gamma_signal(date_today, stock_code, hv_data=df_HVpercentile, df_stock=df_stock, prev_position=None)
    df_gamma.at[i, 'gamma_signal'] = now_positon
```



# ratio_spread_position.py

按照以下标准确定认购正比例、认购反比例、认沽正比例、认沽反比例价差：

根据近月和远月分别筛选出行权价在 ‘0.5delta行权价’ 和 ‘当前标的价格一倍标准差范围’ 之间的期权合约，按照给定的ratio计算每种组合的Delta，返回Delta绝对值最小的组合；比较近月和远月价差的Delta绝对值，取Delta绝对值最小的组合为最终的价差组成部分。

```python
get_ratio_spread_positon(date, code, ratio, df_rf, df_contract, df_stock, df_hv, df_vix, vol_type='IV'):
    """
    获取每天构建认购正比例、认购反比例、认沽正比例、认沽反比例价差的期权信息
    :param date: 日期，例如'20210401'
    :param code: 标的资产的代码，例如 '510050.SH'
    :param ratio:  比例价差的比例（大于1的数字）
    :param df_rf: 含有无风险利率的DataFrame
    :param df_contract: 期权的contract_daily_info信息，DataFrame
    :param df_stock: 含有标的资产每天收盘价的DataFrame
    :param df_hv: 含有每日HV的DataFrame
    :param df_vix: 含有每日VIX的DataFrame
    :param vol_type: 'IV' or 'VIX' or 'HV**', 如果是HV的话需要指定多少日的HV，比如'HV20'
    :return: 依次为认购正比例、认购反比例、认沽正比例、认沽反比例价差的DataFrame
    """
```

例如，获取2021年4月每天应当使用50ETF合约构建的比例价差：

```python
stock_code = '510050.SH'

SQ = SQLConn()
df_HVpercentile = SQ.GetData('HV_percentile')

df_contract_all = SQ.GetData('contract_info_daily')
df_contract_all = df_contract_all[df_contract_all['交易代码'].apply(lambda x: x.find('M') >= 0)]
df_contract = df_contract_all[df_contract_all['期权标的'] == stock_code].reset_index(drop=True)

df_rf = SQ.GetData('rf')

df_stock = SQ.GetData('etf_50')

df_vix = SQ.GetData('df_vol_50etf')
SQ.CloseSql()

date_list = df_contract['日期'].apply(lambda x: datetime.datetime.strftime(x, '%Y%m%d')).unique()
date_list = [x for x in date_list if x[0:6] == '202104']

for date in date_list:
    print(date)
    now_positon = get_ratio_spread_positon(date=date, code=stock_code, ratio=2, df_rf=df_rf, df_contract=df_contract, df_stock=df_stock, df_hv=df_HVpercentile, df_vix=df_vix, vol_type='IV')
    print(now_positon)
```



# iron_butterfly_position.py

按照以下标准构建铁蝶式价差：

1. 优先使用近月合约，近月合约距离到期小于一定天数（threshold）后使用次月合约构建；
2. short call and short put为最接近0.5 delta的合约，long call and long put为行权价最接近标的价格一倍标准差的合约；
3. 如果第2步中选出的4个合约有重合，则无法构建铁蝶式价差；
4. 若第2步为4个不相同的期权，则假设由call构建的垂直价差的delta为delta_call, 数量为m, 由put构建的为delta_put, 数量为n, ratio=n/m且在0.9-1.1之间，调整ratio使得总的头寸delta最接近0。

```python
get_iron_butterfly_positon(date, code, df_rf, df_contract, df_stock, df_hv, df_vix, vol_type='IV'):
    """
    获取每天构建认铁蝶式价差的期权信息
    :param date: 日期，例如'20210401'
    :param code: 标的资产的代码，例如 '510050.SH'
    :param df_rf: 含有无风险利率的DataFrame
    :param df_contract: 期权的contract_daily_info信息，DataFrame
    :param df_stock: 含有标的资产每天收盘价的DataFrame
    :param df_hv: 含有每日HV的DataFrame
    :param df_vix: 含有每日VIX的DataFrame
    :param vol_type: 'IV' or 'VIX' or 'HV**', 如果是HV的话需要指定多少日的HV，比如'HV20'
    :return: 构建铁蝶式价差
    """
```

例如获取2021年4月由50ETF构建的铁蝶式价差：

```python
date_list = df_contract['日期'].apply(lambda x: datetime.datetime.strftime(x, '%Y%m%d')).unique()
date_list = [x for x in date_list if x[0:6] == '202104']

position = None
for date in date_list:
    print(date)
    now_positon = get_iron_butterfly_positon(date=date, code='510050.SH', df_rf=df_rf, df_contract=df_contract, df_stock=df_stock, df_hv=df_HVpercentile, df_vix=df_vix, vol_type='IV')
    if now_positon is not None:
        now_positon['Date'] = date
    if position is None:
        position = now_positon.copy()
    else:
        position = pd.concat([position, now_positon], ignore_index=True)
```



# vertical_spread.py

根据当天的期权合约返回构建垂直价差所用的期权信息，在组成部分的选择标准上与铁蝶式价差类似

```python
get_vertical_positon(date, code, df_rf, df_contract, df_stock, df_hv, df_vix,
                     spread_type, callput, maturity, vol_type='IV'):
    """
    根据选择的垂直价差类型(bull or bear)、期权种类(c or p)和近月/远月确定应当使用的两个期权，一个为最接近0.5delta，
    另一个为行权价最接近标的价格一倍标准差。如果两个期权为同一种，则返回None。
    :param date: 日期，例如'20210401'
    :param code: 标的资产的代码，例如 '510050.SH'
    :param df_rf: 含有无风险利率的DataFrame
    :param df_contract: 期权的contract_daily_info信息，DataFrame
    :param df_stock: 含有标的资产每天收盘价的DataFrame
    :param df_hv: 含有每日HV的DataFrame
    :param df_vix: 含有每日VIX的DataFrame
    :param spread_type: 'bull' or 'baer'
    :param callput: 'c' or 'p'
    :param maturity: 'L1'表示近月合约，'L2'表示远月合约
    :param vol_type: 'IV' or 'VIX' or 'HV**', 如果是HV的话需要指定多少日的HV，比如'HV20'
    :return: DataFrame
        spread_type     long    short   delta   maturity
        bull_call       3.6     3.7     0.3     L1
    """
```

例如获得2021年4月中每天由50ETF合约构成的所有可能的垂直价差：

```python
date_list = df_contract['日期'].apply(lambda x: datetime.datetime.strftime(x, '%Y%m%d')).unique()
date_list = [x for x in date_list if x[0:6] == '202104']

position = None
for date in date_list:
    print(date)
    for maturity in ['L1', 'L2']:
        for callput in ['c', 'p']:
            for bullbear in ['bull', 'bear']:
                now_positon = get_vertical_positon(date=date, code=stock_code, df_rf=df_rf, df_contract=df_contract, df_stock=df_stock, df_hv=df_HVpercentile,df_vix=df_vix, spread_type=bullbear, callput=callput, maturity=maturity, vol_type='IV')
                if now_positon is not None:
                    now_positon.at[0, 'Date'] = date
                if position is None:
                    position = now_positon.copy()
                else:
                    position = pd.concat([position, now_positon], ignore_index=True)
```

