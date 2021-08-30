
from Get_Price import *

global str


select_time = ['2:30']
code_ex_option = {'510050':'sh','000016':'sh','510300':'sh','000300':'sh'}

if os.path.exists('D:/Harper/option_strategy/minbar_lack_data.csv'):
    minbar_lack_data = pd.read_csv('D:/Harper/option_strategy/minbar_lack_data.csv')
else:
    minbar_lack_data = pd.DataFrame({})



# Get option daily price
SQL = SQL_conn('85')
# Check 数据库里面最新的日期
last_record_date = Get_price.diy_query(SQL,'Data_1100','max(datetime)').iloc[0,0].strftime('%Y-%m-%d')
df_option = Get_price._85_database(SQL,'contract_info_daily',"where 日期 > '%s'"%last_record_date)
#df_option = Get_price._85_database(SQL,'contract_info_daily')
SQL.close_conn()


#判断是否需要更新数据库
if len(df_option) == 0:
    raise ValueError("已经更新过了")
else:
    print("更新数据")
# Get Volume from wind
SQL = SQL_conn('wind')
datelist = df_option['日期'].drop_duplicates().reset_index(drop=True)

# Get option price from minbar based on select_time
for i in range(len(datelist)):
    date = datelist[i]
    print('----',i,'/',len(datelist)-1,'  ',date.strftime('%Y%m%d'),'----')
    result = pd.DataFrame({})
    option_lst = df_option.loc[df_option['日期']==date,'期权代码']


    # option data
    for code in option_lst:
        for time in select_time:
            try:
                df = Get_price.min_bar('stock', str(code), date.strftime('%Y%m%d'), 'sh')
                df = df.ffill()
                result = result.append(df.loc[df['datetime']==pd.to_datetime(date.strftime('%Y%m%d') + ' ' + time),['symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume',
                                                                                                               'acc_volume', 'turnover', 'acc_turnover', 'vwap', 'sell_vol', 'buy_vol',
                                                                                                               'first_weighted_ask_prc', 'first_weighted_bid_prc',
                                                                                                               'first_total_ask_vol', 'first_total_bid_vol', 'first_ask_prc1',
                                                                                                               'first_ask_vol1', 'first_bid_prc1', 'first_bid_vol1', 'first_ask_prc2',
                                                                                                               'first_ask_vol2', 'first_bid_prc2', 'first_bid_vol2', 'first_ask_prc3',
                                                                                                               'first_ask_vol3', 'first_bid_prc3', 'first_bid_vol3', 'first_ask_prc4',
                                                                                                               'first_ask_vol4', 'first_bid_prc4', 'first_bid_vol4', 'first_ask_prc5',
                                                                                                               'first_ask_vol5', 'first_bid_prc5', 'first_bid_vol5', 'first_ask_prc6',
                                                                                                                ]],ignore_index = True)

                # result = result.append(Get_price.min_bar('stock', code, date.strftime('%Y%m%d'), 'sh', time, time)[['symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume',
                #                                                                                                'acc_volume', 'turnover', 'acc_turnover', 'vwap', 'sell_vol', 'buy_vol',
                #                                                                                                'first_weighted_ask_prc', 'first_weighted_bid_prc',
                #                                                                                                'first_total_ask_vol', 'first_total_bid_vol', 'first_ask_prc1',
                #                                                                                                'first_ask_vol1', 'first_bid_prc1', 'first_bid_vol1', 'first_ask_prc2',
                #                                                                                                'first_ask_vol2', 'first_bid_prc2', 'first_bid_vol2', 'first_ask_prc3',
                #                                                                                                'first_ask_vol3', 'first_bid_prc3', 'first_bid_vol3', 'first_ask_prc4',
                #                                                                                                'first_ask_vol4', 'first_bid_prc4', 'first_bid_vol4', 'first_ask_prc5',
                #                                                                                                'first_ask_vol5', 'first_bid_prc5', 'first_bid_vol5', 'first_ask_prc6',
                #                                                                                                 ]],ignore_index = True)
            except:
                try:
                    data_from_wind = Get_price.wind_database(SQL,'option',str(code)+'.SH',date.strftime('%Y%m%d'),date.strftime('%Y%m%d'))
                    if data_from_wind['成交量(手)'][0]==0:
                        print('%s not trade on %s'%(code,date.strftime('%Y%m%d')))
                        result = result.append({'symbol':str(code)+'.SH', 'datetime':pd.to_datetime(date.strftime('%Y%m%d') + ' ' + time), 'open':0, 'high':0, 'low':0, 'close':0, 'volume':0,
                                                                                                                   'acc_volume':0, 'turnover':0, 'acc_turnover':0, 'vwap':0, 'sell_vol':0, 'buy_vol':0,
                                                                                                                   'first_weighted_ask_prc':0, 'first_weighted_bid_prc':0,
                                                                                                                   'first_total_ask_vol':0, 'first_total_bid_vol':0, 'first_ask_prc1':0,
                                                                                                                   'first_ask_vol1':0, 'first_bid_prc1':0, 'first_bid_vol1':0, 'first_ask_prc2':0,
                                                                                                                   'first_ask_vol2':0, 'first_bid_prc2':0, 'first_bid_vol2':0, 'first_ask_prc3':0,
                                                                                                                   'first_ask_vol3':0, 'first_bid_prc3':0, 'first_bid_vol3':0, 'first_ask_prc4':0,
                                                                                                                   'first_ask_vol4':0, 'first_bid_prc4':0, 'first_bid_vol4':0, 'first_ask_prc5':0,
                                                                                                                   'first_ask_vol5':0, 'first_bid_prc5':0, 'first_bid_vol5':0, 'first_ask_prc6':0,
                                                                                                                    },ignore_index = True)
                    else:
                        print('%s not in minbar'%code)
                        minbar_lack_data = minbar_lack_data.append({'code':str(code),'date':date,'time':time},ignore_index = True)
                except:
                    result = result.append(
                        {'symbol': str(code) + '.SH', 'datetime': pd.to_datetime(date.strftime('%Y%m%d') + ' ' + time),
                         'open': 0, 'high': 0, 'low': 0, 'close': 0, 'volume': 0,
                         'acc_volume': 0, 'turnover': 0, 'acc_turnover': 0, 'vwap': 0, 'sell_vol': 0, 'buy_vol': 0,
                         'first_weighted_ask_prc': 0, 'first_weighted_bid_prc': 0,
                         'first_total_ask_vol': 0, 'first_total_bid_vol': 0, 'first_ask_prc1': 0,
                         'first_ask_vol1': 0, 'first_bid_prc1': 0, 'first_bid_vol1': 0, 'first_ask_prc2': 0,
                         'first_ask_vol2': 0, 'first_bid_prc2': 0, 'first_bid_vol2': 0, 'first_ask_prc3': 0,
                         'first_ask_vol3': 0, 'first_bid_prc3': 0, 'first_bid_vol3': 0, 'first_ask_prc4': 0,
                         'first_ask_vol4': 0, 'first_bid_prc4': 0, 'first_bid_vol4': 0, 'first_ask_prc5': 0,
                         'first_ask_vol5': 0, 'first_bid_prc5': 0, 'first_bid_vol5': 0, 'first_ask_prc6': 0,
                         }, ignore_index=True)



    for code in code_ex_option:
        for time in select_time:
            try:
                result = result.append(Get_price.min_bar('stock', code, date.strftime('%Y%m%d'), code_ex_option[code], time, time)[list(result.columns)],ignore_index = True)
            except:
                print('%s not in minbar'%code)
                minbar_lack_data = minbar_lack_data.append({'code':code,'date':date,'time':time},ignore_index = True)


    result.to_sql('Data_1100',con=sql_get_save.sql_save(),if_exists='append',index=False)

SQL.close_conn()
del minbar_lack_data['Unnamed: 0']
minbar_lack_data.to_csv('D:/Harper/option_strategy/minbar_lack_data.csv')






