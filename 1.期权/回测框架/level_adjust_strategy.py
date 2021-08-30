from Base_backtest import *
import pandas as pd



class level_adjust_strategy(base_strategy):
    def __init__(self,initial,save_result_to_folder,code_list, s_code, iv_type = 'vix',cost_opt_trading = None):
        super().__init__(initial,save_result_to_folder,code_list)
        # gamma的范围
        self.gamma_float_up = 1.15
        self.gamma_float_down = 0.85
        # vega的范围
        self.vega_float_up = 1.15
        self.vega_float_down = 0.85
        
        #回撤范围
        self.res_param()
        self.s_code = s_code

        #换仓最少天数
        self.change_day = 7
        #Vega， Gamma信号的excel
        self.df_vega_signal = pd.read_csv('D:/Harper/option_strategy/signal_20210802/vega_敞口3_20210802.csv',date_parser=True)
        self.df_gamma_signal = pd.read_csv('D:/Harper/option_strategy/signal_20210804/gamma_1500_调整平滑方式_iv调整为365年化.csv',date_parser=True)
        self.df_vega_signal['Date'] =pd.to_datetime(self.df_vega_signal['Date'])
        self.df_gamma_signal['Date'] = pd.to_datetime(self.df_gamma_signal['Date'])


        self.iv_type = iv_type

        if self.iv_type != 'vix':
            self.df_iv = pd.read_excel('D:/Harper/实习文件整理_张依依/HV_percentile/iv_insert_50etf_0728.xlsx',date_parser=True)
            self.df_iv['Date'] = pd.to_datetime(self.df_iv['Date'],format ='%Y%m%d')
        if cost_opt_trading != None:
            self.cost_opt_trading = cost_opt_trading

    def res_param(self):

        """
        Parameters
        ----------
        vol_up : float -- 确定vega敞口（-）
            波动率上行压力测试
        vol_down : float -- 确定vega敞口（+）
            波动率下行压力测试
        s_up : float -- 确定gamma敞口（-）
            underlying 涨跌幅
        s_down : float -- 确定gamma敞口（+）
            underlying 涨跌幅
        """
        self.vol_up = 0.1
        self.vol_down = 0.05
        self.s_up = 0.05
        self.s_down = 0.05
        #vega 总敞口的限制
        self.vega_min,self.vega_max =  self.vega_res()

    def vega_res(self, max_drawdown_short=0.02, max_drawdown_long=0.01):
        """
        Parameters
        ----------
        max_drawdown_short : float(>0), optional
            short vega最大回撤. The default is 0.02.
        max_drawdown_long : float(>0), optional
            long vega最大回撤. The default is 0.01.

        Returns
        -------
        vega_min : float
            vega敞口限制(short).
        vega_max : float
            vega敞口限制(long).
        """
        #原来的方法
        vega_min = - (self.initial * max_drawdown_short) / self.vol_up
        vega_max = (self.initial * max_drawdown_long) / self.vol_down
        return (vega_min*3, vega_max/3)


    def vega_30days(self,gamma,s,iv,T = 30):
        vega_cal = gamma*iv*s**2 * T/365
        return vega_cal


    def gamma_res(self, s, iv, max_drawdown_up_s=0.02, max_drawdown_down=0.01, t=5):
        """
        Parameters
        ----------
        s : float
            underlying
        iv : float
            implied volatility
        max_drawdown_up_s : float(>0), optional
            short gamma最大回撤. The default is 0.02.
        max_drawdown_down : float(>0), optional
            long gamma最大回撤(Theta PnL 决定). The default is 0.01.
        t : TYPE, optional
            DESCRIPTION. The default is 5.

        Returns
        -------
        gamma_min : float
            gamma敞口限制(short).
        gamma_max : float
            gamma敞口限制(long).

        """
        # 原来的方法
        gamma_min = - 2 * (self.initial * max_drawdown_up_s) / (self.s_up * s) ** 2
        # use Theta PnL来确定 Gamma（+） 敞口, theta = 0.5*gamma*(s)**2*(iv)**2
        theta_down = max_drawdown_down * self.initial
        theta = theta_down / (t / 365)
        gamma_max = 2 * theta / (s ** 2) / (iv ** 2)
        return (gamma_min*3, gamma_max*3)



    def signal(self, date):
        vega_signal = self.df_vega_signal.loc[self.df_vega_signal['Date']==pd.to_datetime(date),'vega_iv'].values[0]
        gamma_signal = self.df_gamma_signal.loc[self.df_gamma_signal['Date'] == pd.to_datetime(date), 'gamma_signal'].values[0]
        return (vega_signal, gamma_signal)

    def load_all_df(self):
        self.df_option,self.df_volatility,self.df_rf = self.data_prepare(database_address='85', database_name=['contract_info_daily','df_vol_50etf','rf'],
                                             condition = ["where 期权标的 = '%s'"%self.s_code.upper(),'None','None'])

        self.df_s = self.data_prepare(database_address='wind', code=self.s_code.upper(), symbol=self.code_list[self.s_code]['symbol'],
                                   start_date=self.start_date.strftime('%Y%m%d'))

        self.df_option['日期'] = pd.to_datetime(self.df_option['日期'])
        self.df_option['期权代码'] = self.df_option['期权代码'].astype('str')
        self.df_volatility['日期'] = pd.to_datetime(self.df_volatility['日期'])
        self.df_rf['日期']  = pd.to_datetime(self.df_rf['日期'])
        self.df_s['交易日期'] = pd.to_datetime(self.df_s['交易日期'])

    def cal_position(self,date,s,hv,rf,pre_position, focus = 'vega'):
        #选定近月+远月合约，决定要不要换仓

        df_t1 = self.select_option_contract(date, self.df_option, ex_A=True, require_maturity=self.change_day, require_expire_date='t1', require_k=s, s = s, hv=hv, rf=rf)
        df_t2 = self.select_option_contract(date, self.df_option, ex_A=True, require_maturity=self.change_day,
                                            require_expire_date='t2', require_k=s, s=s, hv=hv, rf=rf)
        #判断是不是没有不除权除息的合约
        if len(df_t1) == 0:
            df_t1 = self.select_option_contract(date, self.df_option, ex_A=False, require_maturity=self.change_day,
                                                require_expire_date='t1', require_k=s, s=s, hv=hv, rf=rf)
        if len(df_t2) == 0:
            df_t2 = self.select_option_contract(date, self.df_option, ex_A=False, require_maturity=self.change_day,
                                                require_expire_date='t2', require_k=s, s=s, hv=hv, rf=rf)


        call_t1 = df_t1.loc[df_t1['交易代码'].str.contains('C')].to_dict('records')[0]
        put_t1 = df_t1.loc[df_t1['交易代码'].str.contains('P')].to_dict('records')[0]
        call_t2 = df_t2.loc[df_t2['交易代码'].str.contains('C')].to_dict('records')[0]
        put_t2 = df_t2.loc[df_t2['交易代码'].str.contains('P')].to_dict('records')[0]
        t1,t2 = df_t1.loc[:,'maturity_days'].drop_duplicates().values[0],df_t2.loc[:,'maturity_days'].drop_duplicates().values[0]

        if pre_position is not None:  # 不是第一天开仓（已经有持仓）
            df_pre_position = self.select_option_contract(date,self.df_option, selected_code = pre_position['Code'].tolist(), ex_A=False)
            pre_position_t =  df_pre_position.loc[:,'maturity_days'].drop_duplicates().sort_values().reset_index(drop=True)

            if len(pre_position_t) == 1:
                if self.atm(df_pre_position.loc[df_pre_position['maturity_days']==pre_position_t[0],'行权价'].drop_duplicates().values[0],s): 
                    if pre_position_t[0] == t1: #近月不用换合约
                        call_t1 = df_pre_position.loc[df_pre_position['交易代码'].str.contains('C')].to_dict('records')[0]
                        put_t1 = df_pre_position.loc[df_pre_position['交易代码'].str.contains('P')].to_dict('records')[0]
                    elif pre_position_t[0] == t2: #远月不用换合约
                        call_t2 = df_pre_position.loc[df_pre_position['交易代码'].str.contains('C')].to_dict('records')[0]
                        put_t2 = df_pre_position.loc[df_pre_position['交易代码'].str.contains('P')].to_dict('records')[0]
            else:
                pre_position_t1 = pre_position_t[0]
                pre_position_t2 = pre_position_t[1]

                #决定要不要换仓
                if self.atm(df_pre_position.loc[df_pre_position['maturity_days']==pre_position_t1,'行权价'].drop_duplicates().values[0],s): 
                    if pre_position_t1 == t1: #近月不用换合约
                        call_t1 = df_pre_position.loc[(df_pre_position['交易代码'].str.contains('C')) 
                                                      & (df_pre_position['maturity_days']==pre_position_t1)].to_dict('records')[0]
                        put_t1 = df_pre_position.loc[(df_pre_position['交易代码'].str.contains('P')) 
                                                      & (df_pre_position['maturity_days']==pre_position_t1)].to_dict('records')[0]
                    elif pre_position_t1== t2: #远月不用换合约
                        call_t2 = df_pre_position.loc[(df_pre_position['交易代码'].str.contains('C')) 
                                                      & (df_pre_position['maturity_days']==pre_position_t2)].to_dict('records')[0]
                        put_t2 = df_pre_position.loc[(df_pre_position['交易代码'].str.contains('P')) 
                                                      & (df_pre_position['maturity_days']==pre_position_t2)].to_dict('records')[0]
                
                
                if self.atm(df_pre_position.loc[df_pre_position['maturity_days']==pre_position_t2,'行权价'].drop_duplicates().values[0],s): 
                    if pre_position_t2 == t1: #近月不用换合约
                        call_t1 = df_pre_position.loc[(df_pre_position['交易代码'].str.contains('C'))
                                                      & (df_pre_position['maturity_days']==pre_position_t2)].to_dict('records')[0]
                        put_t1 = df_pre_position.loc[(df_pre_position['交易代码'].str.contains('P'))
                                                      & (df_pre_position['maturity_days']==pre_position_t2)].to_dict('records')[0]
                    elif pre_position_t2== t2: #远月不用换合约
                        call_t2 = df_pre_position.loc[(df_pre_position['交易代码'].str.contains('C')) 
                                                      & (df_pre_position['maturity_days']==pre_position_t2)].to_dict('records')[0]
                        put_t2 = df_pre_position.loc[(df_pre_position['交易代码'].str.contains('P')) 
                                                      & (df_pre_position['maturity_days']==pre_position_t2)].to_dict('records')[0]



        for each in [call_t1,put_t1,call_t2,put_t2]:

            each['Delta'],each['Gamma'],each['Theta'],each['Vega'] =  self.Greeks(each['maturity_days']/365, s, hv, rf, each['行权价'], each['option_type'], choice = 'all')

        #delta对冲后远近月的gamma，vega
        delta_ratio_t1,delta_ratio_t2 = abs(call_t1['Delta']/put_t1['Delta']),abs(call_t2['Delta']/put_t2['Delta'])
        gamma_t1,gamma_t2 = call_t1['Gamma'] * call_t1['合约单位'] + delta_ratio_t1 * put_t1['Gamma'] * put_t1['合约单位'],\
                            call_t2['Gamma'] * call_t2['合约单位'] + delta_ratio_t2 * put_t2['Gamma'] * put_t2['合约单位']
        vega_t1,vega_t2 = call_t1['Vega'] * call_t1['合约单位'] + delta_ratio_t1 * put_t1['Vega'] * put_t1['合约单位'],\
                            call_t2['Vega'] * call_t2['合约单位'] + delta_ratio_t2 * put_t2['Vega'] * put_t2['合约单位']

        #判断近月满足target_vega之后满不满足 gamma 敞口, 不满足用 t2 配

        if focus == 'vega':
            minor = 'gamma'
        elif focus == 'gamma':
            minor = 'vega'
            
        ratio_t1= eval('self.target_%s'%focus) / eval('%s_t1'%focus)
        test_minor_t1 = ratio_t1 * eval('%s_t1'%minor)
            
        if test_minor_t1 in range(round(eval('self.target_%s_down'%minor)-1),round((eval('self.target_%s_up'%minor)+1))):
            call_t1['No'],put_t1['No'] = round(ratio_t1), round(ratio_t1*delta_ratio_t1)
            call_t1['Num'], put_t1['Num'] = call_t1['No'] * call_t1['合约单位'],put_t1['No'] * put_t1['合约单位']
            return(call_t1,put_t1)
        else:
            #如果t2>65天，不做t2 合约 并且gamma限制住
            if t2 > 65:
                ratio_onet_minor = eval('self.target_%s'%minor) / eval('%s_t1'%minor)
                call_t1['No'], put_t1['No'] = round(ratio_onet_minor), round(ratio_onet_minor * delta_ratio_t1)
                call_t1['Num'], put_t1['Num'] = call_t1['No'] * call_t1['合约单位'], put_t1['No'] * put_t1['合约单位']
                return (call_t1, put_t1)


            ratio_t1, ratio_t2 = self.cal_ratio([self.target_vega,self.target_gamma],[vega_t1, gamma_t1],[vega_t2, gamma_t2])

            call_t1['No'],put_t1['No'] = round(ratio_t1), round(ratio_t1*delta_ratio_t1)
            call_t1['Num'], put_t1['Num'] = call_t1['No'] * call_t1['合约单位'],put_t1['No'] * put_t1['合约单位']

            call_t2['No'],put_t2['No'] = round(ratio_t2), round(ratio_t2*delta_ratio_t2)
            call_t2['Num'], put_t2['Num'] = call_t2['No'] * call_t2['合约单位'],put_t2['No'] * put_t2['合约单位']

            # 如果只有一边有仓位的话, 另一边也为0；不看这个的仓位
            if call_t1['No'] * put_t1['No'] == 0:
                return (call_t2,put_t2)

            if call_t2['No'] * put_t2['No'] == 0:
                return (call_t1,put_t1)

            return (call_t1,put_t1,call_t2,put_t2)

        
        '''
        #原来的写法
        ratio_t1 = self.target_vega / vega_t1
        test_gamma_t1 = ratio_t1 * gamma_t1

        if (test_gamma_t1 in range(round(self.target_gamma_down)-1,round(self.target_gamma_up)+1)):
            call_t1['No'],put_t1['No'] = round(ratio_t1), round(ratio_t1*delta_ratio_t1)
            call_t1['Num'], put_t1['Num'] = call_t1['No'] * call_t1['合约单位'],put_t1['No'] * put_t1['合约单位']
            return(call_t1,put_t1)
        else:
            #如果t2>65天，不做t2 合约 并且gamma限制住
            if t2 > 65:
                ratio_onet_gamma = self.target_gamma / gamma_t1
                call_t1['No'], put_t1['No'] = round(ratio_onet_gamma), round(ratio_onet_gamma * delta_ratio_t1)
                call_t1['Num'], put_t1['Num'] = call_t1['No'] * call_t1['合约单位'], put_t1['No'] * put_t1['合约单位']
                return (call_t1, put_t1)


            ratio_t1, ratio_t2 = self.cal_ratio([self.target_vega,self.target_gamma],[vega_t1, gamma_t1],[vega_t2, gamma_t2])

            call_t1['No'],put_t1['No'] = round(ratio_t1), round(ratio_t1*delta_ratio_t1)
            call_t1['Num'], put_t1['Num'] = call_t1['No'] * call_t1['合约单位'],put_t1['No'] * put_t1['合约单位']

            call_t2['No'],put_t2['No'] = round(ratio_t2), round(ratio_t2*delta_ratio_t2)
            call_t2['Num'], put_t2['Num'] = call_t2['No'] * call_t2['合约单位'],put_t2['No'] * put_t2['合约单位']

            return (call_t1,put_t1,call_t2,put_t2)
        '''

    # 判断钱够不够用
    def is_moneny_enough(self,s,s_yes,position_option, position_fut = None,initial_require = 0.8):
        '''
        :param s: 标的今天价格
        :param s_yes: 标的t-1价格
        :param position_option: option的持仓
        :param position_fut: future的持仓
        :param initial_require: 最多占规模的%
        :return: dict --- 和cal position 一样
        '''
        position_option = pd.DataFrame(position_option)
        position_option.loc[:,'Margin'] = 0
        option_value = (position_option['收盘价']*position_option['Num']).sum()
        t_list = position_option['到期日'].drop_duplicates().reset_index(drop=True)
        test_margin = 0
        self.remark = ''
        for each_t in t_list:
            each_call = position_option.loc[(position_option['到期日']==each_t)&(position_option['交易代码'].str.contains('C'))].to_dict('records')[0]
            each_put = position_option.loc[(position_option['到期日'] == each_t) & (position_option['交易代码'].str.contains('P'))].to_dict('records')[0]
            test_margin += self.margin(strategy_type = 'straddle_delta_hedge',strike = each_call['行权价'],settle_yes_call= each_call['前结算价'], settle_yes_put= each_put['前结算价'],
                                       num_call= each_call['Num'], num_put= each_put['Num'],s_yes= s_yes, s_today= s)
        if position_fut != None:
            position_fut.loc[:, 'Margin'] = 0
            test_margin += self.margin(strategy_type='futures', fut_price= position_fut['fut_price'], no_fut= position_fut['no_fut'], contract_unit= position_fut['contract_unit'])

        # 计算保证金和option卖出的钱 够不够用
        if (option_value + test_margin > self.initial * initial_require) & (option_value + test_margin > 0):
            restrict_ratio = (self.initial * initial_require) / (option_value + test_margin)
            self.remark = '原仓位所需钱 > %s'%initial_require
            print('----',self.remark,'----')
            position_option.loc[:,'No'] = (position_option.loc[:,'No'] * restrict_ratio).astype('int')
            position_option.loc[:, 'Num'] = position_option.loc[:,'No'] * position_option.loc[:,'合约单位']

        #计算最后的保证金
        for each_t in t_list:
            each_call = position_option.loc[
                (position_option['到期日'] == each_t) & (position_option['交易代码'].str.contains('C'))].to_dict('records')[0]
            each_put = position_option.loc[
                (position_option['到期日'] == each_t) & (position_option['交易代码'].str.contains('P'))].to_dict('records')[0]
            position_option.loc[position_option['到期日'] == each_t, 'Margin'] = 1/2 * self.margin(strategy_type='straddle_delta_hedge', strike=each_call['行权价'],
                                                                                   settle_yes_call=each_call['前结算价'], settle_yes_put=each_put['前结算价'],
                                                                                   num_call=each_call['Num'], num_put=each_put['Num'], s_yes=s_yes, s_today=s)

        if position_fut != None:
            position_fut.loc[:,'no_fut'] = (position_fut.loc[:,'no_fut'] * restrict_ratio).astype('int')
            position_fut.loc[:,'Margin'] = self.margin(strategy_type='futures', fut_price=position_fut['fut_price'], no_fut=position_fut['no_fut'],
                                     contract_unit=position_fut['contract_unit'])
            return position_option.to_dict(orient='records'), position_fut.to_dict(orient='records')
        else:
            return position_option.to_dict(orient='records')



    # 给定vega, gamma 求解二元方程式/不等式
    def cal_ratio(self, target, t1, t2):
        """
        Parameters
        ----------
        target : [target_vega, target_gamma]

        t1 :[vega_t1, gamma_t1]
            近月到期合约--delta 对冲之后,一个lot的straddle下的vega和gamma
        t2 : [vega_t2, gamma_t2]
            远月到期合约--delta 对冲之后,一个lot的straddle下的vega和gamma

        Returns
        -------
        t1, t2 的配比

        """
        # target = [target_vega, target_gamma]
        a = Symbol('a')
        b = Symbol('b')
        formula = [a * t1[i] + b * t2[i] - target[i] for i in range(len(target))]
        result = solve(formula, [a, b])
        #print(result)

        # if target[0] * result[a] < 0:
        #    result[a] = 0
        #    result[b] = target[0]/t2[0]

        # if target[0] * result[b] < 0:
        #    result[b] = 0
        #    result[a] = target[0]/t2[0]

        return (result[a], result[b])

    def atm(self, k_old, etf_close):
        if etf_close <= 3:
            tick = 0.05
        elif etf_close > 3 and etf_close <= 5:
            tick = 0.1
        elif etf_close > 5 and etf_close <= 10:
            tick = 0.25
        elif etf_close > 10 and etf_close <= 20:
            tick = 0.5
        elif etf_close > 20 and etf_close <= 50:
            tick = 1
        elif etf_close > 50 and etf_close <= 100:
            tick = 2.5
        elif etf_close > 100:
            tick = 5

        if abs(etf_close - k_old) <= tick:
            return True
        else:
            return False


    def back_test(self,focus = 'vega',consider_money = False, data_type = None, vwap_or_twap_price_type = None,vwap_or_twap = None,start_time='14:30', end_time='14:50'):
        #data prepare
        self.load_all_df()
        self.data_type,self.vwap_or_twap_price_type,self.vwap_or_twap = data_type, vwap_or_twap_price_type,vwap_or_twap
        self.datelist = self.df_gamma_signal['Date'].drop_duplicates().sort_values().reset_index(drop=True)
        #self.datelist = pd.to_datetime(self.df_option['日期']).drop_duplicates().sort_values().reset_index(drop=True)
        pre_position = None
        #for i in range(10):
        for i in range(len(self.datelist)-1):
            print(i,'/',len(self.datelist)-2,'----',self.datelist[i],'----')
            date = self.datelist[i]
            tmr = self.datelist[i + 1]
            #1. 计算信号和敞口(vega,gamma)
            (self.vega_signal, self.gamma_signal) = self.signal(date)
            '''
            #原来target_vega的定义
            self.target_vega =  self.vega_signal * abs(self.vega_min) if self.vega_signal < 0 else  self.vega_signal * self.vega_max
            '''

            s = self.get_info('etf',self.df_s, date=date, return_type='收盘价')
            s_yes = self.get_info('etf',self.df_s, date=date, return_type='前收盘价')

            hv = self.df_volatility.loc[self.df_volatility['日期']==date,'HV'].values[0]/100
            if self.iv_type == 'vix':
                iv = self.df_volatility.loc[self.df_volatility['日期']==date,'iVIX'].values[0]/100
            else:
                iv = self.df_iv.loc[self.df_iv['Date']==date,'iv_insert'].values[0]/100

            rf = self.df_rf[self.df_rf['日期']==date]['中债国债到期收益率：1年'].values[0] / 100
            (self.gamma_min,self.gamma_max) =  self.gamma_res(s, iv)
            self.target_gamma = self.gamma_signal * abs(self.gamma_min) if self.gamma_signal < 0 else self.gamma_signal * self.gamma_max
            self.target_gamma_down,self.target_gamma_up = self.target_gamma*self.gamma_float_down,self.target_gamma*self.gamma_float_up

            # 更新的30days target_vega(根据gamma而定)
            self.target_vega = self.vega_30days(self.target_gamma,s,iv, T=30)
            self.target_vega_down, self.target_vega_up = self.target_vega * self.vega_float_down, self.target_vega * self.vega_float_up

            # 2. 筛选option + 计算仓位

            result = self.cal_position(date, s, hv, rf, pre_position, focus)
            if consider_money != False:
                result = self.is_moneny_enough(s, s_yes, result, position_fut=None, initial_require=0.8)

            for code in result:
                #持仓都是期权
                code['Symbol'] = 'option'
                if self.vwap_or_twap == None:
                    self.df_records = self.df_records.append(
                        {'Date': date, 'Code': code['期权代码'], 'Symbol': code['Symbol'], 'option_type':code['option_type'],'K':code['行权价'],
                         'Num': code['Num'], 'No': code['No'],'Lot':  code['合约单位'],
                         'Margin': code['Margin'] if 'Margin' in code.keys() else 0,
                         'Price_t': code['收盘价'],
                         'Price_t1': self.get_info(asset_type=code['Symbol'], df=self.df_option, code = code['期权代码'],date=tmr,
                                                   return_type='price'),

                         'Delta':code['Delta'] * code['Num'],'Gamma':code['Gamma']* code['Num'],'Vega':code['Vega']* code['Num'],'Theta':code['Theta']* code['Num'],
                         'T':code['maturity_days'],'Maturity':code['到期日'].strftime('%Y-%m-%d')
                         }, ignore_index=True)
                else:
                    self.df_records = self.df_records.append(
                        {'Date': date, 'Code': code['期权代码'], 'Symbol': code['Symbol'],
                         'option_type': code['option_type'], 'K': code['行权价'],
                         'Num': code['Num'], 'No': code['No'], 'Lot': code['合约单位'],
                         'Margin': code['Margin'] if 'Margin' in code.keys() else 0,
                         'Price_t': code['收盘价'],
                         'Price_t1': self.get_info(asset_type=code['Symbol'], df=self.df_option, code=code['期权代码'],
                                                   date=tmr,
                                                   return_type='price'),
                         'Price_t_%s_%s' % (self.vwap_or_twap_price_type, self.vwap_or_twap): self.get_info(
                                                                                     asset_type=code['Symbol'], df=self.df_option, code=code['期权代码'],
                                                                                     date=date,
                                                                                     return_type='price', data_type=data_type, vwap_or_twap=self.vwap_or_twap,
                                                                                     start_time=start_time, end_time=end_time,
                                                                                     vwap_or_twap_price_type=self.vwap_or_twap_price_type),

                         'Price_t1_%s_%s'%(self.vwap_or_twap_price_type,self.vwap_or_twap): self.get_info(asset_type=code['Symbol'], df=self.df_option, code=code['期权代码'],
                                                                                   date=tmr,
                                                                                   return_type='price', data_type=data_type, vwap_or_twap=self.vwap_or_twap,
                                                                                   start_time=start_time, end_time=end_time,
                                                                                   vwap_or_twap_price_type=self.vwap_or_twap_price_type),


                         'Delta': code['Delta'] * code['Num'], 'Gamma': code['Gamma'] * code['Num'],
                         'Vega': code['Vega'] * code['Num'], 'Theta': code['Theta'] * code['Num'],
                         'T': code['maturity_days'], 'Maturity': code['到期日'].strftime('%Y-%m-%d')
                         }, ignore_index=True)


            pre_position = self.df_records.loc[self.df_records['Date'] == date]
            if consider_money != False:
                self.df_records.loc[self.df_records['Date'] == date, 'Remark'] = self.remark
            self.df_records.loc[self.df_records['Date'] == date, 'S'] = s
            self.df_records.loc[self.df_records['Date'] == date,'Vega_signal'] = self.vega_signal
            self.df_records.loc[self.df_records['Date'] == date, 'Gamma_signal'] = self.gamma_signal
            self.df_records.loc[self.df_records['Date'] == date,'Target_vega'] = self.target_vega
            self.df_records.loc[self.df_records['Date'] == date, 'Target_gamma'] = self.target_gamma


            # self.df_records = self.df_records[
            #     ['Date', 'Code', 'Symbol', 'Num', 'No', 'Lot','t(days)','maturity', 'Margin', 'Price_t', 'Price_t1']]

        self.df_records.loc[:, 'No'] = self.df_records.loc[:, 'No'].astype(int)
        self.df_records.loc[:, 'Num'] = self.df_records.loc[:, 'Num'].astype(int)
        self.df_records.loc[:, ['Delta','Gamma','Vega','Theta']] = self.df_records.loc[:, ['Delta','Gamma','Vega','Theta']].astype(float)
        if 'Remark' in self.df_records.columns:
            if 'Price_t_%s_%s' %(self.vwap_or_twap_price_type, self.vwap_or_twap) in self.df_records.columns:
                self.df_records = self.df_records[
                    ['Date','Code', 'Symbol', 'K', 'S', 'No', 'Lot', 'Num', 'Price_t', 'Price_t1',
                     'Price_t_%s_%s' %(self.vwap_or_twap_price_type, self.vwap_or_twap),
                     'Price_t1_%s_%s' %(self.vwap_or_twap_price_type, self.vwap_or_twap),
                     'Maturity', 'T',
                     'Margin', 'Delta', 'Gamma', 'Theta', 'Vega',
                     'option_type', 'Vega_signal', 'Gamma_signal', 'Target_vega',
                     'Target_gamma','Remark']]
            else:
                self.df_records = self.df_records[
                    ['Date','Code', 'Symbol', 'K', 'S', 'No', 'Lot', 'Num', 'Price_t', 'Price_t1',
                     'Maturity', 'T',
                     'Margin', 'Delta', 'Gamma', 'Theta', 'Vega',
                     'option_type', 'Vega_signal', 'Gamma_signal', 'Target_vega',
                     'Target_gamma','Remark']]

        else:
            self.df_records = self.df_records[['Date','Code', 'Symbol','K','S', 'No','Lot','Num','Price_t', 'Price_t1',
                                               'Maturity', 'T',
                                                'Margin', 'Delta', 'Gamma', 'Theta', 'Vega',
                                               'option_type', 'Vega_signal', 'Gamma_signal', 'Target_vega',
                                               'Target_gamma']]



#%%
if __name__ == "__main__":
    test = level_adjust_strategy(initial = 5000000,
                                 save_result_to_folder= 'D:/Harper/option_strategy/backtest_result',
                                 code_list = {'510050.sh':{'symbol':'etf','database_address':'wind'}},
                                 s_code = '510050.sh',
                                 iv_type='iv',
                                 cost_opt_trading = 2)
    test.init_test_period(start_date = '20150201', end_date = None)
    test.back_test(focus='gamma', consider_money=True)
    #test.back_test(focus = 'gamma',consider_money = True,data_type = 'minbar', vwap_or_twap_price_type = 'mid',vwap_or_twap = 'twap',start_time='14:30', end_time='14:50')
    df_records = test.df_records.copy()
    test.daily_cost_calculate(fillna_previous_column = ['S','Vega_signal','Gamma_signal','Target_vega','Target_gamma'],only_open_short_no_cost = True)
    #test.daily_pnl_calculate(price_name = '%s_%s' %(test.vwap_or_twap_price_type, test.vwap_or_twap))
    test.daily_pnl_calculate()
    test.daily_return_calculate()
    test.df_pnl = pd.merge(test.df_pnl,df_records.loc[:,['Date','Vega_signal','Gamma_signal','Target_vega','Target_gamma','Remark']].drop_duplicates(subset=['Date']),on='Date',how = 'left')
    test.annual_return_analysis()

    test.save_result(file_name = 'test_信号0804_1500_调整平滑方式_iv调整为365年化_vega敞口gamma定_gamma3倍_65_7天不开_保证金限制_仅short0cost_冲击成本2_twap',sheet_name_and_df_dict = {'持仓':test.df_records,
                                                                                                                        'PnL': test.df_pnl,
                                                                                                                        'Result': test.df_annual_result})



