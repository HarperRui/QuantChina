import pandas as pd
import numpy as np
import sys,datetime
#import sql_get_save
from scipy import stats,optimize
from sympy import * #解不等式
from Get_Price import *
import empyrical as ep #计算annual return, sharpe, Max drawdown
global str




class base_strategy():
    def __init__(self,initial,save_result_to_folder,code_list):

        self.code_list = code_list
        self.df_records = pd.DataFrame(columns = {'Date','Code','Symbol','Num','No','Lot','Margin','Price_t','Price_t1'})
        #self.df_pnl = pd.DataFrame(columns = {'Date','Total PnL~cost','Total Cost','Total PnL','Return','Cul Return'})
        self.initial = initial
        self.save_result_to_folder = save_result_to_folder
        self.init_params()
        #self.data = self.data_prepare()

    def init_params(self):
        self.margin_fut_ratio = 0.12
        self.cost_etf = 0.00003
        self.cost_fut = 0.000023
        self.cost_opt_impact = 0.5 #期权冲击成本
        self.cost_opt_trading = 1.7 #期权交易成本, 开仓short 不要手续费


    def init_test_period(self,start_date, end_date = None):
        if end_date != None:
            self.end_date = end_date
        else:
            self.end_date = pd.to_datetime(datetime.date.today())
        self.start_date = pd.to_datetime(start_date)
        print("test_period: {} ---> {}".format(self.start_date,self.end_date))


    #wind/85 可同时用code_list去取多个code的数据
    def data_prepare(self, database_name = None,condition = None,database_address = '85',code = None, symbol = None, start_date = None,selected_time = 'all'):

        '''
        :param database_name: '85'数据库 需要读取的 表名 ----> str/list
        :param condition: SQL的筛选语句 ----> str/list
        :param database_address: 取数据的数据库地址 ---->  'wind'/'85'/'minbar'/'tick'
        :param code: 交易代码，要带交易所symbol ----> eg: '10003437.SH'
        :param symbol: minbar/tick ---->'stock'/ 'future'; wind ---->'stock'/ 'futures'/'index'/'etf'/'option'/'commodity'
        :param start_date: str, eg:'20210625'
        :param selected_time:
        :return: data (dataframe形式)
        '''
        if database_address not in ['85','wind','minbar','tick']:
            raise ValueError ("database")
        else:
            if database_address in ['minbar','tick'] and code != None:
                code, exchange = code.split('.')
                if database_address == 'minbar':
                    return Get_price.tick_data(symbol, code, start_date, exchange,selected_time)
                elif database_address == 'tick':
                    return Get_price.min_bar(symbol, code, start_date, exchange,selected_time)
            else:
                SQL = SQL_conn(database_address)
                if database_address == '85':
                    result = Get_price._85_database(SQL, database_name, condition)
                else:
                    result = Get_price.wind_database(SQL, symbol, code, start_date,selected_time)
                SQL.close_conn()
                return result


    def margin(self,strategy_type = 'single',strike = None,settle_yes_call= None, settle_yes_put= None, num_call= None, num_put= None, num_straddle= None,
               s_yes= None, s_today= None, option_kind = None,fut_price = None, no_fut = None, contract_unit = None):

        def margin_futures(fut_price, no_fut, contract_unit):
            margin = abs(fut_price * no_fut * contract_unit * self.margin_fut_ratio)
            return margin

        def margin_single_option(settle_yes, strike, num, s_yes, s_today, kind):  # kind:'call'/'put'
            '''
            :param settle_yes: option前结算价
            :param strike: K
            :param num: 张数 * 合约乘数 (num > 0---做多 ---> return 0)
            :param s_yes: 现货前收盘价
            :param s_today: 现货当前价格
            :param kind: ‘call’/'put'
            :return: 单张合约 所需保证金 (>=0)
            '''
            if num < 0:
                if kind == 'call':
                    margin = (settle_yes + max(0.12 * s_yes - max(strike - s_today, 0), 0.07 * s_yes)) * abs(num)
                else:
                    margin = (min(settle_yes + max(0.12 * s_yes - max(s_today - strike, 0), 0.07 * strike),
                                  strike)) * abs(num)
            else:
                margin = 0
            return margin

        def margin_straddle(strike_straddle, settle_cdyes, settle_pdyes, num_same, s_yes, s_today):
            '''
            :param strike_straddle: straddle 行权价
            :param settle_cdyes: call 前结算价
            :param settle_pdyes: put 前结算价
            :param num_same: staddle 张数 * 合约乘数
            :param s_yes:
            :param s_close:
            :return:
            '''
            margin = 0
            if num_same < 0:
                margin_c = margin_single_option(settle_cdyes, strike_straddle, num_same, s_yes, s_today, 'call')
                margin_p = margin_single_option(settle_pdyes, strike_straddle, num_same, s_yes, s_today, 'put')
                if margin_c <= margin_p:
                    margin = margin_p + settle_cdyes * abs(num_same)
                elif margin_c > margin_p:
                    margin = margin_c + settle_pdyes * abs(num_same)
            return margin

        def margin_straddle_delta_hedge(strike_straddle, settle_cdyes, settle_pdyes, num_cd, num_pd, s_yes, s_today):
            margin = 0
            if num_cd >= 0 and num_pd >= 0:
                return margin
            elif num_cd < 0 and num_pd < 0:

                if abs(num_cd) > abs(num_pd):
                    num_straddle = num_pd
                    num_c_spare = num_cd - num_straddle
                    margin += margin_straddle(strike_straddle, settle_cdyes, settle_pdyes, num_straddle, s_yes,
                                                   s_today)
                    margin += margin_single_option(settle_cdyes, strike_straddle, num_c_spare, s_yes, s_today, 'call')
                else:
                    num_straddle = num_cd
                    num_p_spare = num_pd - num_straddle
                    margin += margin_straddle(strike_straddle, settle_cdyes, settle_pdyes, num_straddle, s_yes,s_today)
                    margin += margin_single_option(settle_pdyes, strike_straddle, num_p_spare, s_yes, s_today, 'put')
                return margin
            elif num_cd * num_pd == 0:
                if num_cd + num_pd > 0:
                    return margin
                else:
                    if num_cd < 0:
                        print('only short call; put = 0')
                        margin += margin_single_option(settle_cdyes, strike_straddle, num_cd, s_yes, s_today,'call')
                    else:
                        print('only short put; call = 0')
                        margin += margin_single_option(settle_pdyes, strike_straddle, num_pd, s_yes, s_today,'put')
            else:
                raise ValueError('margin_straddle_delta_hedge error')

        #调用margin function（根据不同的strategy_type）
        if strategy_type not in ['futures','single','straddle','straddle_delta_hedge']:
            raise ValueError("Margin error")
        if strategy_type == 'single':
            if option_kind == 'call':
                return margin_single_option(settle_yes_call, strike, num_call, s_yes, s_today, option_kind)
            else:
                return margin_single_option(settle_yes_put, strike, num_put, s_yes, s_today, option_kind)
        elif strategy_type == 'straddle':
            return margin_straddle(strike, settle_yes_call, settle_yes_put, num_straddle, s_yes, s_today)
        elif strategy_type == 'straddle_delta_hedge':
            return margin_straddle_delta_hedge(strike, settle_yes_call, settle_yes_put, num_call, num_put,s_yes, s_today)
        elif strategy_type == 'futures':
            return margin_futures(fut_price, no_fut, contract_unit)

    def Greeks(self, t, s, hv, rf, k, option_type, choice = 'all',price=0):  # n默认为1看涨期权的delta, n为-1为看跌期权的delta，price用来算implied vol
        '''
        :param t:
        :param s: underlying
        :param hv: vol / 100
        :param rf: risk_free rate/100
        :param k: strike
        :param option_type: 'call' / 'put'
        :param choice: 'all'/'delta'/'gamma'...
        :param price: use to calculate iv
        :return: greek or iv
        '''
        if option_type not in ['call','put']:
            raise ValueError ("Option_type is not valid")

        if option_type == 'call':
            n = 1
        else :
            n = -1
        rf = np.log(1 + rf)
        d1 = (np.log(s / k) + (rf + 1 / 2 * hv ** 2) * t) / (hv * np.sqrt(t))
        d2 = d1 - hv * np.sqrt(t)
        delta = n * stats.norm.cdf(n * d1)
        gamma = stats.norm.pdf(d1) / (s * hv * np.sqrt(t))
        theta = -1 * (s * stats.norm.pdf(d1) * hv) / (2 * np.sqrt(t)) - n * rf * k * np.exp( -rf * t) * stats.norm.cdf(n * d2)
        vega = s * np.sqrt(t) * stats.norm.pdf(d1)
        # 计算的都是出现信号的收盘当天的greeks
        if choice == 'all':
            return (delta, gamma, theta, vega)
        else:
            if choice == 'delta':
                return delta
            elif choice == 'gamma':
                return gamma
            elif choice == 'theta':
                return theta
            elif choice == 'vega':
                return vega
            else:
                upper = 1
                lower = float(0)
                sigma = 0.5 * (upper + lower)
                test = 0
                iteration = 0

                while (abs(test - price) > 1e-4) and (iteration < 100):
                    d1_sigma = (np.log(s / k) + (rf + sigma ** 2 / 2) * t) / (sigma * np.sqrt(t))
                    d2_sigma = d1_sigma - sigma * np.sqrt(t)
                    if n == 1:
                        test = s * stats.norm.cdf(d1_sigma, 0., 1.) - k * np.exp(-rf * t) * stats.norm.cdf(
                            d2_sigma, 0., 1.)
                    else:
                        test = k * np.exp(-rf * t) * stats.norm.cdf(-d2_sigma, 0., 1.) - s * stats.norm.cdf(
                            -d1_sigma, 0., 1.)
                    if test - price > 0:
                        upper = sigma
                        sigma = (sigma + lower) / 2
                    else:
                        lower = sigma
                        sigma = (sigma + upper) / 2
                    iteration += 1

                    if sigma < 0.01:
                        sigma = hv
                        break
                return sigma

    #筛选当天满足到期时间 或者 delta的所有可以交易的合约
    def select_option_contract(self,date,df_option, selected_code = None,ex_A = True,require_maturity = None, require_expire_date = None,require_k = None,require_delta = None,s = None, hv= None, rf= None):
        '''
        :param date: 取当天交易日的数据
        :param df_option: 期权日行情
        :param selected_code: 是否有特定的code_list
        :param ex_A: 是否 包含除权除息的 的 期权合约 ----> True，去除； False， 保留
        :param require_maturity: 到期天数大于等于 ？ 天 ----> int, eg:10
        :param require_expire_date: 第几个到期日 ----> ‘t1’/'t2'/'t3'/'t4'
        :param require_k: 想要的k
        :param require_delta: 想要的delta
        :param s: 现货价格
        :param hv: hv/100
        :param rf: rf/100
        :return: df， 满足条件的期权合约
        '''
        #距离到期时间 筛选 or delta 筛选
        df = df_option.loc[pd.to_datetime(df_option['日期'])==pd.to_datetime(date)].copy()
        df['option_type'] = np.where(df['交易代码'].str.contains('C'), 'call', 'put')
        #去掉除权除息的期权合约
        if ex_A == True:
            df = df.loc[~df['交易代码'].str.contains('A')]

        all_maturity_list = df['到期日'].drop_duplicates().sort_values().reset_index(drop=True)


        #计算到期时间（天数）
        if 'maturity_days' not in df.columns:
            df['maturity_days'] = (pd.to_datetime(df['到期日']) - pd.to_datetime(date)).astype('timedelta64[D]').astype(int)
            
        if require_k != None:
            df_k = pd.DataFrame([])
            for each_marturity in all_maturity_list:
                df_each_call = df.loc[(df['到期日']==each_marturity)& (df['交易代码'].str.contains('C'))]
                df_each_put = df.loc[(df['到期日'] == each_marturity) & (df['交易代码'].str.contains('P'))]
                call = df_each_call.iloc[np.abs(df_each_call['行权价'] - require_k).argmin()]
                put = df_each_put.iloc[np.abs(df_each_put['行权价'] - require_k).argmin()]
                df_k = df_k.append([call,put],ignore_index=True)
            df = df_k


        if require_delta != None:
            #计算当天交易option的delta
            df['Delta'] = df.apply(lambda x: self.Greeks(x['maturity_days']/365, s, hv, rf, x['行权价'], x['option_type'], 'delta') , axis = 1)
            #找最接近require_delta 的 合约(每个到期日)
            df_delta = pd.DataFrame([])
            for each_marturity in all_maturity_list:
                df_each = df.loc[(df['到期日']==each_marturity)]
                call = df_each.iloc[np.abs(df_each['Delta'] - require_delta).argmin()]
                put = df_each.iloc[np.abs(df_each['Delta'] + require_delta).argmin()]
                df_delta = df_delta.append([call,put],ignore_index=True)
            df = df_delta
        

        if require_maturity != None and require_expire_date != None:
            select_maturity_list = df.loc[(df['maturity_days'] >= require_maturity),'到期日'].drop_duplicates().sort_values().reset_index(drop=True)
            return df.loc[(df['maturity_days'] >= require_maturity) & (df['到期日'] == select_maturity_list[int(require_expire_date[1:])-1])]
        elif  require_maturity != None:
            return df.loc[(df['maturity_days'] >= require_maturity)]
        elif require_expire_date != None:
            return df.loc[(df['到期日'] == all_maturity_list[int(require_expire_date[1:]) - 1])]
        else:
            if selected_code != None:
                if type(selected_code) != list:
                    selected_code = [selected_code]
                return df.loc[df['期权代码'].astype('str').isin(selected_code)]
            return df



    def get_info(self,asset_type,df,date = None,code = None,return_type= None,
                 data_type = 'minbar',vwap_or_twap = None,start_time='14:30',end_time='14:50',vwap_or_twap_price_type='mid'):
        '''
        :param asset_type: ‘option’/'futures'/'stock'/'index'/'etf'/'commodity'
        :param df: df_option, df_selected_option,..., 可以是包含所有日期 or 特定日期
        :param date: 对应price的日期
        :param code: 代码,如果df只有一个代码，可以不输
        :param return_type: 想返回的值 {option:'k'/'t'/'lot'/'price'/'presettle',
                                        ['etf','stock','index']:['Wind代码', '交易日期', '前收盘价', '开盘价', '最高价', '最低价', '收盘价', '成交量', '涨跌幅%'],
                                        ['futures','commodity']: ['Wind代码','交易日期','前结算价(元)','开盘价(元)','最高价(元)','最低价(元)','收盘价(元)','结算价(元)','成交量(手)']}
        :return: float/int
        '''
        if asset_type == 'option':
            df = df.loc[df['期权代码'].astype('str')==code]
            if vwap_or_twap != None:
                price_cal = Get_price.get_vwap_or_twap_price(data_type, 'stock', code, pd.to_datetime(date).strftime('%Y%m%d'), 'sh', start_time = start_time,
                                       end_time=end_time, price_type=vwap_or_twap_price_type,vwap_or_twap = vwap_or_twap)
                if price_cal/df.loc[(df['期权代码'].astype('str')==code) & (pd.to_datetime(df['日期']) == pd.to_datetime(date)),'收盘价'].values[0] >= 1000:
                    price_cal = price_cal / df.loc[(df['期权代码'].astype('str')==code) & (pd.to_datetime(df['日期']) == pd.to_datetime(date)),'合约单位'].values[0]

                if return_type != 'all':
                    return price_cal
                df.loc[(df['期权代码'].astype('str')==code) & (pd.to_datetime(df['日期']) == pd.to_datetime(date)),'price_%s_%s'%(vwap_or_twap_price_type,vwap_or_twap)] = price_cal


            if return_type == 'k':
                return df.loc[df['期权代码'].astype('str')==code,'行权价'].values[0] #除权除息期权的行权价会变
            elif return_type == 't':
                if 'maturity_days' in df.columns:
                    return df.loc[df['期权代码'].astype('str')==code,'maturity_days'].values[0]
                else:
                    maturity = pd.to_datetime(df.loc[df['期权代码'].astype('str') == code, '到期日'].values[0])
                    return (maturity - pd.to_datetime(date)).days

            elif return_type == 'lot':
                return df.loc[(df['期权代码'].astype('str')==code) & (pd.to_datetime(df['日期']) == pd.to_datetime(date)),'合约单位'].values[0]
            elif return_type == 'price':
                return df.loc[(df['期权代码'].astype('str')==code) & (pd.to_datetime(df['日期']) == pd.to_datetime(date)),'收盘价'].values[0]
            elif return_type == 'presettle':
                try:
                    return df.loc[(df['期权代码'].astype('str')==code) & (pd.to_datetime(df['日期']) == pd.to_datetime(date)),'前结算价'].values[0]
                except:
                    print("%s 没有前收盘价，用收盘价代替"%code)
                    return df.loc[(df['期权代码'].astype('str')==code) & (pd.to_datetime(df['日期']) == pd.to_datetime(date)),'收盘价'].values[0]
            elif return_type == 'all':
                return df.loc[(df['期权代码'].astype('str') == code) & (pd.to_datetime(df['日期']) == pd.to_datetime(date))]


        elif asset_type in ['etf','stock','index']:
            if type(return_type) == list:
                for i in return_type:
                    if i not in ['Wind代码', '交易日期', '前收盘价', '开盘价', '最高价', '最低价', '收盘价', '成交量', '涨跌幅%']:
                        raise ValueError('return_type for %s is not valid'%return_type)
            else:
                if return_type not in ['Wind代码', '交易日期', '前收盘价', '开盘价', '最高价', '最低价', '收盘价', '成交量', '涨跌幅%']:
                    raise ValueError ('return_type for %s is not valid'%return_type)

            if code == None:
                return float(df.loc[pd.to_datetime(df['交易日期']) == pd.to_datetime(date),return_type].values[0])

            return float(df.loc[(df['Wind代码'].astype('str')==code) & (pd.to_datetime(df['交易日期']) == pd.to_datetime(date)),return_type].values[0])

        elif asset_type in ['futures','commodity']:
            if type(return_type) == list:
                for i in return_type:
                    if i not in ['Wind代码','交易日期','前结算价(元)','开盘价(元)','最高价(元)','最低价(元)','收盘价(元)','结算价(元)','成交量(手)']:
                        raise ValueError('return_type for %s is not valid'%return_type)
            else:
                if return_type not in ['Wind代码','交易日期','前结算价(元)','开盘价(元)','最高价(元)','最低价(元)','收盘价(元)','结算价(元)','成交量(手)']:
                    raise ValueError ('return_type for %s is not valid'%return_type)

            if code == None:
                return float(df.loc[pd.to_datetime(df['交易日期']) == pd.to_datetime(date),return_type].values[0])
            return float(df.loc[(df['Wind代码'].astype('str')==code) & (pd.to_datetime(df['交易日期']) == pd.to_datetime(date)),return_type].values[0])

        else:
            raise ValueError ('asset_type: %s is not valid'%asset_type)


    def back_test(self):
        # code = '510050.sh'
        # symbol = 'etf'
        #get data from database
        # df_etf =  self.data_prepare(database_address = 'wind',code = '510050.sh', symbol = self.code_list['510050.sh']['symbol'], start_date = '20150101')
        # df_index = self.data_prepare(database_address = 'wind',code = '000016.sh', symbol = self.code_list['000016.sh']['symbol'], start_date = '20150101')

        df_etf,df_index = self.data_prepare(database_address = 'wind',code = ['510050.sh','000016.sh'], symbol = ['etf','index'], start_date = ['20150101','20150101'])

        datelist = pd.to_datetime(df_etf['交易日期']).drop_duplicates().sort_values().reset_index(drop=True)
        for i in range(200):
            print(i)
            date = datelist[i]
            tmr = datelist[i+1]
            self.df_records = self.df_records.append({'Date':date,'Code':'510050.sh','Symbol':self.code_list['510050.sh']['symbol'],'Num':1,'No':1,'Lot':1,'Margin':0,
                                                      'Price_t':self.get_info(asset_type=self.code_list['510050.sh']['symbol'],df =df_etf,date = date,return_type='收盘价'),
                                                      'Price_t1':self.get_info(asset_type=self.code_list['510050.sh']['symbol'],df =df_etf,date = tmr,return_type='收盘价')}, ignore_index = True)
            self.df_records = self.df_records.append({'Date': date, 'Code': '000016.sh', 'Symbol': self.code_list['000016.sh']['symbol'], 'Num': -1, 'No': -1, 'Lot': 1, 'Margin': 0,
                                                      'Price_t': self.get_info(
                                                          asset_type=self.code_list['000016.sh']['symbol'], df=df_index,
                                                          date=date, return_type='收盘价'),
                                                      'Price_t1': self.get_info(
                                                          asset_type=self.code_list['000016.sh']['symbol'], df=df_index,
                                                          date=tmr, return_type='收盘价')}, ignore_index=True)



            self.df_records =  self.df_records[['Date','Code','Symbol','Num','No','Lot','Margin','Price_t','Price_t1']]

    def daily_cost_calculate(self,df_records = None,if_return = False, fillna_previous_column = None,only_open_short_no_cost = True):
        #self.df_records['Cost'] = -100
        def single_cost(df):
            symbol = df.Symbol[df.index[0]]
            if symbol not in ['etf','index','futures','option']:
                raise ValueError("%s is not in cost"%symbol)
            if symbol == 'etf' or symbol == 'index':
                df['Cost'] = -self.cost_etf * abs(df['No_change']) * df['Lot'] * df['Price_t']
            elif symbol == 'futures':
                df['Cost']= -self.cost_fut * abs(df['No_change']) * df['Lot'] * df['Price_t']
            elif symbol == 'option':
                cost_impact =  -abs(self.cost_opt_impact * df['No_change'])
                if self.only_open_short_no_cost == True:
                    cost_trading = -np.where((df['No_change'] < 0) & (df['No_yes'] <= 0), 0, abs(abs(df['No_change']) * self.cost_opt_trading))
                else:
                    cost_trading = -np.where(df['No_change'] < 0, 0, abs(abs(df['No_change'])*self.cost_opt_trading))
                df['Cost'] = cost_impact + cost_trading
            return df

        def cal_change_num(df):
            df = df.set_index('Date').reindex(full_datelist)
            if fillna_previous_column is not None:
                df.loc[:,fillna_previous_column] = df.loc[:,fillna_previous_column].fillna(method = 'ffill')

            df = df.fillna(0)
            df.loc[df['Code']==0,['Code','Symbol']] = df.loc[df['Code']!=0,['Code','Symbol']].drop_duplicates(subset=['Code','Symbol']).values[0]
            df['No_yes'] = df['No'].shift(1).fillna(0)
            df['No_change'] =  df['No'] - df['No_yes']
            return df


        #calculate change of num
        self.only_open_short_no_cost = only_open_short_no_cost

        if df_records == None:
            df_records = self.df_records
        full_datelist = df_records.Date.drop_duplicates().reset_index(drop=True)
        self.df_records_copy = df_records.groupby('Code', group_keys=False).apply(cal_change_num).reset_index()
        self.df_records_copy = self.df_records_copy.groupby('Symbol', group_keys=False).apply(single_cost).sort_values(by=['Date'])

        if if_return == True:
            return self.df_records_copy



    def daily_pnl_calculate(self,df_records = None,if_return = False,price_name = None):
        '''
        :param df_records: 持仓df
                            +-----------------------------------------------------------------+
                            Date | Code | Symbol | Num | No | Lot | Price_t | Price_t1 | Cost
                            +------------------------------------------------------------------+
        :param if_return: 是否要return 结果
        :param price_name: 命名不是'Price_t1'/'Price_t',可能是Price_t_vwap
        :return: None or df_pnl
        '''
        #单独计算每种code的pnl
        def cal_single_pnl(df):
            if price_name == None:
                df['PnL~cost'] = ((df['Price_t1'] - df['Price_t']) * df['Num']).shift(1)
            else:
                df['PnL~cost'] = ((df['Price_t1_%s'%price_name] - df['Price_t_%s'%price_name]) * df['Num']).shift(1)
            df['PnL~cost'] = df['PnL~cost'].fillna(0)
            return df

        print('---- Calculate daily PnL ----')
        if df_records == None:
            df_records = self.df_records_copy

        df_records = df_records.groupby('Code').apply(cal_single_pnl)

        df_records.loc[:,'PnL~cost'] = df_records.loc[:,'PnL~cost'].astype(float)
        df_records.loc[:, 'Cost'] = df_records.loc[:, 'Cost'].astype(float)
        df_records.loc[:, 'Margin'] = df_records.loc[:, 'Margin'].astype(float)


        # 单独计算每天的整体的pnl
        if 'Delta' in df_records.columns:
            self.df_pnl = df_records[['Date','PnL~cost','Margin','Cost','Delta','Gamma','Vega','Theta']].groupby('Date').sum().rename(columns={'PnL~cost': 'Total PnL~cost', 'Cost': 'Total Cost'})
        else:
            self.df_pnl = df_records.groupby('Date').sum()[['PnL~cost','Cost']].rename(columns={'PnL~cost': 'Total PnL~cost', 'Cost': 'Total Cost'})
        self.df_pnl['Total PnL'] = self.df_pnl['Total PnL~cost'] + self.df_pnl['Total Cost']

        if if_return == True:
            return self.df_pnl


    def daily_return_calculate(self, df_pnl = None, initial = None,if_return = False):
        '''
        :param df_pnl: PnL + Cost
                            +-----------------------------------------------+
                            Date | Total PnL~cost | Total Cost | Total PnL |
                            +------------------------------------------------+
        :param initial: 资金规模 or 所占资金 （depends on return的定义）
        :param if_return: 是否要return 结果
        :return: None or df
                            +-------------------------------------------------------------------------+
                            Date | Total PnL~cost | Total Cost | Total PnL | Daily Return | Cul Return
                            +-------------------------------------------------------------------------+
        '''
        if df_pnl == None:
            df_pnl = self.df_pnl

        if initial == None:
            initial = self.initial

        df_pnl['Daily Return'] = df_pnl['Total PnL']/initial
        df_pnl['Cul Return'] = (df_pnl.loc[:,'Daily Return']+1).cumprod()
        self.df_pnl = df_pnl.reset_index()

        if if_return == True:
            return self.df_pnl


    def annual_return_analysis(self,df_daily_return = None, if_return = False):
        '''
        :param df_daily_return: 'Daily Return', 可以包含多个策略(index 为'Date' or column 里面包括'Date')
        :param if_return: 要不要 return 结果
        :return: None or df(每年的return, vol, sharpe, max_drawdown)
        '''
        # None的话，默认是 self.df_pnl
        if df_daily_return is None:
            df_daily_return = self.df_pnl.loc[:,['Date','Daily Return']]

        self.df_annual_result = pd.DataFrame([])
        if 'Date' not in df_daily_return.columns:
            df_daily_return = df_daily_return.reset_index()

        df_daily_return['Date'] = pd.to_datetime(df_daily_return['Date'])
        return_lst = list(df_daily_return.columns)
        return_lst.remove('Date')

        #Calculate weekly_return
        def weekly_return(df,col):
            df = df.copy().reset_index(drop=True)
            result_df = pd.DataFrame()
            date = df.loc[0, "Date"]
            result_df.loc[date, "Weekly Return"] = (df[col]+1).cumprod().iloc[-1]-1
            return result_df


        for col in return_lst:
            df_result = pd.DataFrame(columns=['年份', '年化收益率_%s' % col, '年化波动率_%s' % col, 'Sharpe_%s' % col, '日回撤_%s' % col,'周回撤_%s' % col, '最大回撤_%s' % col])
            df_result['年份'] = df_daily_return['Date'].dt.year.drop_duplicates().sort_values().reset_index(drop=True)

            for i in range(len(df_result)):
                start = pd.to_datetime(str(df_result.年份[i]) + '0101')
                end =  pd.to_datetime(str(df_result.年份[i]+1) + '0101')
                print('---- cal annual return ----', df_result.年份[i],'----')
                df_daily_return_year = df_daily_return.loc[(df_daily_return['Date'] >= start) & (df_daily_return['Date'] < end)]
                df_daily_return_year.loc[:,'week'] =df_daily_return_year.loc[:,'Date'].copy().dt.isocalendar().week
                df_result.iloc[i, 1] = ep.annual_return(df_daily_return_year[col])
                df_result.iloc[i, 2] = ep.annual_volatility(df_daily_return_year[col]) #这个function 算的是std(population)
                df_result.iloc[i, 3] = ep.annual_return(df_daily_return_year[col]) / ep.annual_volatility(df_daily_return_year[col])
                df_result.iloc[i, 4] = df_daily_return_year[col].min() if df_daily_return_year[col].min() < 0 else 0
                df_result.iloc[i, 5] = ep.max_drawdown(df_daily_return_year.groupby('week').apply(weekly_return, col = col)["Weekly Return"])
                df_result.iloc[i, 6] = ep.max_drawdown(pd.to_numeric(df_daily_return_year[col]))

            if len(self.df_annual_result) == 0:
                self.df_annual_result = df_result
            else:
                self.df_annual_result = pd.merge(self.df_annual_result, df_result, on='年份')
            #self.df_annual_result.loc[:, :] = self.df_annual_result.loc[:,:].astype(float)
        if if_return == True:
            return self.df_annual_result

    def performance_multi_strategy(self,df_multi_daily_return,file_name,folder_path = None,col_name = None):
        '''
        :param df_multi_daily_return: 不同的strategy的结果
        :param file_name: 存储的excel名字
        :param folder_path: 存储结果的path
        :param col_name: 重新命名的列名
        :return: save result to excel
        '''

        if folder_path == None:
            folder_path = self.save_result_to_folder

        if 'Date' not in df_multi_daily_return.columns:
            df_multi_daily_return = df_multi_daily_return.reset_index()

        if col_name == None:
            col_name = df_multi_daily_return.columns.tolist()
            col_name.remove('Date')

        df_multi_daily_return['Date'] = pd.to_datetime(df_multi_daily_return['Date'])
        all_year =  df_multi_daily_return['Date'].dt.year.drop_duplicates().sort_values().reset_index(drop=True)
        sheet_name_and_df_dict = {}
        sheet_name_and_df_dict['result'] = self.annual_return_analysis(df_daily_return=df_multi_daily_return,
                                                                       if_return=True)
        #所有年份的表现对比
        df_all_year_result = pd.DataFrame({'Date': df_multi_daily_return['Date']})
        for n in col_name:
            if type(col_name) == dict:
                df_all_year_result.loc[:, col_name[n]] = (df_multi_daily_return.loc[:, n] + 1).cumprod()
            else:
                df_all_year_result.loc[:,n] = (df_multi_daily_return.loc[:,n]+1).cumprod()

        sheet_name_and_df_dict['all_year'] = df_all_year_result
        df_year_into_sheet = pd.DataFrame(index=range(360))
        #分年份的表现对比
        for year in all_year:
            start = pd.to_datetime(str(year) + '0101')
            end = pd.to_datetime(str(year + 1) + '0101')
            df_year = df_multi_daily_return.loc[(df_multi_daily_return['Date'] >= start) & (df_multi_daily_return['Date'] < end)].sort_values(by='Date').reset_index(drop=True)
            df_year_result = pd.DataFrame({'Date':df_year['Date']})

            for m in col_name:
                if type(col_name) == dict:
                    df_year_result.loc[:, col_name[m]] = (df_year.loc[:, m] + 1).cumprod()
                else:
                    df_year_result.loc[:,m] = (df_year.loc[:,m]+1).cumprod()
            #不同年份写进一个sheet
            for each_col in df_year_result.columns:
                df_year_into_sheet['%s_%s'%(each_col,str(year))] = df_year_result[each_col].reset_index(drop=True)

            #sheet_name_and_df_dict[str(year)] = df_year.div(df_year.iloc[0]).reset_index()
            sheet_name_and_df_dict[str(year)] = df_year_result

        sheet_name_and_df_dict['seperate_year'] = df_year_into_sheet

        self.save_result(file_name, sheet_name_and_df_dict, folder_path)


    def save_result(self,file_name, sheet_name_and_df_dict, folder_path = None):
        '''
        :param file_name: Excel 名称
        :param sheet_name_and_df_dict: {'name1':df1,'name2':df2},需要保存的df（单个or多个）
        :param folder_path: 储存excel的folder
        :return: None
        '''
        if folder_path == None:
            folder_path = self.save_result_to_folder
        # Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter(folder_path+'/%s.xlsx'%file_name, engine='xlsxwriter')
        print("---- write result to excel <%s> ----"%file_name)


        for i in sheet_name_and_df_dict:
            if type(sheet_name_and_df_dict[i]) != pd.DataFrame:
                raise ValueError ("%s is not dataframe"%i)
            sheet_name_and_df_dict[i].to_excel(writer, sheet_name = i,index = False)
        # Close the Pandas Excel writer and output the Excel file.
        writer.save()



#%% Demo
if __name__ == '__main__':

    test = base_strategy(initial = 1000000,
                         save_result_to_folder= 'D:/Harper/Class',
                         code_list = {'510050.sh':{'symbol':'etf','database_address':'wind'},'000016.sh':{'symbol':'index','database_address':'wind'}})
    test.back_test()
    test.daily_cost_calculate(only_open_short_no_cost = True)
    test.daily_pnl_calculate()
    test.daily_return_calculate()
    test.annual_return_analysis()
    test.save_result(file_name = 'test4',sheet_name_and_df_dict = {'持仓':test.df_records,'PnL': test.df_pnl,'result': test.df_annual_result})

