# -*- coding: utf-8 -*-
"""
Created on Fri Jun 18 10:05:35 2021

@author: Xuan
"""




import pandas as pd
import numpy as np
import sys,datetime
import sql_get_save,get_database_data 
from scipy import stats
import empyrical as ep
import matplotlib
matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['font.family']='sans-serif'

class collar_etf_300index():
   def __init__(self, etf_kind, change_day=10, cost_t=1.7, cost_i=0.5, cost_fut = 0.000023, cost_etf = 0.00003,margin_fut_ratio = 0.12):
        """
        Parameters
        ----------
        etf_kind : 300index,50etf,300etf
            DESCRIPTION.
        ps_open : TYPE
            DESCRIPTION.
        cl_open : TYPE
            DESCRIPTION.
        change_day : TYPE, optional
            DESCRIPTION. 换仓时间(自然日), The default is 10.
        cost_t : TYPE, optional
            DESCRIPTION. 交易手续费, The default is 1.7.
        cost_i : TYPE, optional
            DESCRIPTION. 冲击成本, The default is 0.5.
        cost_IH : TYPE, optional
            DESCRIPTION. The default is 0.000023,成交金额的万分之零点二三，其中平今仓手续费为成交金额的万分之三点四五
        cost_etf : TYPE, optional
            DESCRIPTION. The default is 0.00003.

        Returns
        -------
        None.

        """
        self.etf_kind = etf_kind
        self.change_day = change_day
        self.cost_t = cost_t 
        self.cost_i = cost_i 
        self.cost_fut = cost_fut
        self.cost_etf = cost_etf
        self.margin_fut_ratio = margin_fut_ratio 
        
   
   def create_df_records(self):
       if self.etf_kind == '300index':
           # Create df_records
           df_records = pd.DataFrame(columns = ['Date', 'Signal','Remark', 'index','price_fut','Code_fut','No_fut',
                   'Strike_ps', 'Code_ps', 'Num_ps', 'No_ps', 'Lot_ps','T_ps',
                   'Strike_cl', 'Code_cl', 'Num_cl','No_cl', 'Lot_cl','T_cl',
                   'Margin', 'Margin_o', 'Margin_f','Option','Futures', 'Cost', 'Cash','PnL_option', 'PnL_ps','PnL_cl','PnL_fut', 'PnL', 'PnL%','净值'])
       self.df_records = df_records
       
   def read_database(self):
        now = datetime.date.today()
        today = pd.to_datetime(now)
        # trading calendar
        trade_cal = get_database_data.trading_cal(today+datetime.timedelta(days = -6000),today)
        trade_cal['date'] = pd.to_datetime(trade_cal['date'])
        if trade_cal.iloc[-1,0] == today:
           trade_cal = trade_cal[:len(trade_cal)-1] 
           
        
        self.df_rf, self.df_options, self.df_etf, self.df_contract,self.IH = get_database_data.read_database(self.etf_kind,'close')
        
        self.df_options['日期'] = pd.to_datetime(self.df_options['日期'])
        self.df_options['期权代码'] = self.df_options['期权代码'].astype('str')
        self.df_contract['期权代码'] = self.df_contract['期权代码'].astype('str')
        self.df_etf['日期'] = pd.to_datetime(self.df_etf['日期'])
        self.IH['Date'] = pd.to_datetime(self.IH['Date'])
        
        #期权日行情最早Date
        self.last_date_records = self.df_options['日期'].min()
        self.datelist = trade_cal[trade_cal['date']>self.last_date_records].reset_index(drop=True)
        '''
        # 判断需不需要update df_vol, 日行情
        if len(datelist[datelist['date'].isin(df_options['日期'])]) < len(datelist):
            print("Need to update 日行情_%s"%self.etf_kind)
        else:
        '''    


   def getinfor(self, date,df_tradedate, etf, return_type,ps_open, cl_open, ps_maintain , cl_maintain):  #return_type:'atm'/'small','large' 
       # df_tradedate = df_tradedate[~df_tradedate['交易代码'].str.contains('A')] #除权除息的合约一般都不是整数档（剔除）
        #计算到期时间不小于10天的对应合约（<15天加仓加到后面，10天近月平仓）
        maturity_list = df_tradedate.loc[df_tradedate['maturity_days']>=self.change_day,['到期日','maturity_days']].drop_duplicates().sort_values(by='到期日',ascending=True).reset_index(drop=True)
        firstMaturity =  maturity_list['到期日'].drop_duplicates().sort_values(ascending=True).reset_index(drop=True).values[0]
    
        #算strike
        df1 = df_tradedate.loc[df_tradedate['到期日'] == firstMaturity]
        all_k = df1['行权价'].drop_duplicates().sort_values().reset_index(drop=True) 
        strike = all_k.at[np.argmin(np.abs(all_k-etf))] 
        
        #交易期权的code(first maturity)
        all_put = df1[df1['交易代码'].str.contains('P')]
        all_call = df1[df1['交易代码'].str.contains('C')]
    
        t =  maturity_list.loc[maturity_list['到期日']==firstMaturity, 'maturity_days'].values[0]
        
        (range_min,range_max) = self.k_range(etf,'open',ps_open, cl_open, ps_maintain , cl_maintain)
        remark = ''
        if return_type == 'ps':
            strike_small = all_k.at[np.argmin(np.abs(all_k-range_min))]
            if strike_small >= etf: #有可能击穿
                remark += 'small_pk取最小'
                strike_small = all_k[0]
            put_small = df1.loc[(df1['行权价']==strike_small)&(df1['期权代码'].isin(all_put['期权代码'])),'期权代码'].values[0]
            return (t,firstMaturity,strike_small,put_small,remark)
        
        elif return_type == 'cl' :
            strike_large = all_k.at[np.argmin(np.abs(all_k-range_max))]
            if strike_large <= etf: #有可能击穿
                remark += 'large_cl取最大'
                strike_large = all_k.max()
            call_large = df1.loc[(df1['行权价']==strike_large)&(df1['期权代码'].isin(all_call['期权代码'])),'期权代码'].values[0]
            return (t,firstMaturity,strike_large,call_large,remark)



   def cal_position(self,i,yesterday, date, etf_yes, etf, df_tradedate,df_contract, ps_open, cl_open, ps_maintain , cl_maintain,df_fut_date): # buy S + buy put, sell call
        #kind: 'call'/'put'
        remark_ps = ''
        remark_cl = ''
        if pd.isnull(self.df_records[self.df_records['Date']==yesterday]['Code_ps'].values[0]):  
            (t_ps,_,k_ps,code_ps,remark_ps) = self.getinfor(date,df_tradedate, etf, 'ps',ps_open, cl_open, ps_maintain , cl_maintain)
            (t_cl,_,k_cl,code_cl,remark_cl) = self.getinfor(date,df_tradedate, etf, 'cl',ps_open, cl_open, ps_maintain , cl_maintain)
      
        else:
            #近月
            code_ps_old = str(self.df_records[self.df_records['Date']==yesterday]['Code_ps'].values[0])
            k_ps_old = self.get_k(code_ps_old, df_tradedate)
            
            code_cl_old = str(self.df_records[self.df_records['Date']==yesterday]['Code_cl'].values[0])
            k_cl_old = self.get_k(code_cl_old, df_tradedate)
            
        
            t_ps_old = df_tradedate.loc[df_tradedate['期权代码']==code_ps_old,'maturity_days'].values[0]
            t_cl_old = df_tradedate.loc[df_tradedate['期权代码']==code_cl_old,'maturity_days'].values[0]
             
            
            if self.switch_flag(etf,k_ps_old,t_ps_old,'ps',ps_open, cl_open, ps_maintain , cl_maintain) == False:
                (t_ps,k_ps,code_ps) = (t_ps_old,k_ps_old,code_ps_old)
           
            else:
                (t_ps,_,k_ps,code_ps,remark_ps) = self.getinfor(date,df_tradedate, etf, 'ps',ps_open, cl_open, ps_maintain , cl_maintain)
            
            
            if self.switch_flag(etf,k_cl_old,t_cl_old,'cl',ps_open, cl_open, ps_maintain , cl_maintain) == False:
    
                (t_cl,k_cl,code_cl) = (t_cl_old,k_cl_old,code_cl_old)
           
            else:
                (t_cl,_,k_cl,code_cl,remark_ps) = self.getinfor(date,df_tradedate, etf, 'cl',ps_open, cl_open, ps_maintain , cl_maintain)
        
           
            #check call/put lot要相等    
            lot_ps = self.lot(code_ps,df_tradedate,df_contract)
            lot_cl = self.lot(code_cl,df_tradedate,df_contract)
            '''
            if lot_ps != lot_cl:
                if lot_ps != 10000:
                    (t_ps,_,k_ps,code_ps,remark_ps) = getinfor(date,df_tradedate, etf, 'ps')
                else:
                    (t_cl,_,k_cl,code_cl,remark_ps) = getinfor(date,df_tradedate, etf, 'cl')
                    
            '''
        
        #check call/put lot要相等    
        lot_ps = self.lot(code_ps,df_tradedate,df_contract)
        lot_cl = self.lot(code_cl,df_tradedate,df_contract)
        
        #k_list = get_all_k(df_tradedate,t_ps,'M') #不考虑除权除息
        code_fut = df_fut_date.loc[0,'contract']
        price_fut = df_fut_date.loc[0,'price_t']
        no_fut = 1
        no_ps = round(price_fut * 300 * no_fut / etf / lot_ps)
        no_cl = -no_ps
        
        #no_ps,no_cl = 1,-1
        num_ps, num_cl = no_ps * lot_ps, no_cl * lot_cl
        num_etf = abs(no_ps * lot_ps)
        
        p_ps = df_tradedate[df_tradedate['期权代码']==code_ps].收盘价.values[0]
        p_cl = df_tradedate[df_tradedate['期权代码']==code_cl].收盘价.values[0]
        #(code_IH, price_IH) = get_IH(i)
        
        try:
            settle_yes = df_tradedate[df_tradedate['期权代码']==code_cl].前结算价.values[0]
        except:
            settle_yes = p_cl

        
    
        self.printRecord(i,remark_ps+' '+remark_cl, etf, code_fut, no_fut,
                    k_ps, code_ps, num_ps, no_ps, lot_ps, t_ps,
                    k_cl, code_cl, num_cl, no_cl, lot_cl,t_cl)



   def get_data(self,df,date,kind,df_contract = ''): 
        """
        Parameters
        ----------
        df : dataframe, df_option or df_underlying
        date : today or yesterday
        kind : 'etf'/'option'
        df_contract: optional, only 'option' need
        Returns: etf_price or df_option on specific date
        -------
        """
        if kind == 'option':
            df_date = df[df['日期'] == date]
            df_date = pd.merge(df_date,df_contract.loc[:,['期权代码','到期日']],on = '期权代码', how = 'left')
            df_date['maturity_days'] = (pd.to_datetime(df_date['到期日'])-date).astype('timedelta64[D]').astype(int)
            return df_date
        elif kind == 'etf':
            etf_close = df[df['日期'] == date].close.values[0]
            return etf_close
        elif kind == 'index':
            index_close = df.loc[df['Date'] == date,'index'].values[0]
            return index_close
        elif kind == 'fut':
            df_date = df[df['Date'] == date].reset_index(drop=True)
            return df_date
            
            
   def k_range(self,s, kind, ps_open, cl_open, ps_maintain , cl_maintain): #优化的两边尾部期权的行权价选择,t: remaining days
        
        if kind == 'open':
            range_min = s * ps_open/100
            range_max = s * cl_open/100
        elif kind == 'hold':
            range_min = s * ps_maintain/100 
            range_max = s * cl_maintain/100
        else:
            print('error')
        
        return (range_min,range_max)
    
    
   def get_k(self,code,df_tradedate):
        k = df_tradedate.loc[df_tradedate['期权代码'].astype('str')==code,'行权价'].values[0] #除权除息期权的行权价会变
        return k           
    
    
   def lot(self,code,df_tradedate,df_contract):
        if self.etf_kind == '300index':
            lot = 100      
        return lot  
    
    
   def margin_single_option(self,settle_yes,strike,num,etf_yes,etf_close,kind): #kind:'call'/'put'
        if num < 0:
            if kind == 'call':
                margin = (settle_yes+max(0.12*etf_yes-max(strike-etf_close,0), 0.07*etf_yes))*abs(num)
            else:
                margin = (min(settle_yes+max(0.12*etf_yes-max(etf_close-strike,0), 0.07*strike),strike))*abs(num) 
        else:
            margin = 0
        return margin
   
    
   def margin_fut(self,fut_price,num_fut):
        margin = abs(fut_price * 300 * num_fut * self.margin_fut_ratio)
        return margin    
    
   def printRecord(self,i,Remark, index, Code_fut, No_fut,
                    Strike_ps, Code_ps, Num_ps, No_ps, lot_ps, t_ps,
                    Strike_cl, Code_cl, Num_cl, No_cl, lot_cl,t_cl):
        
        self.df_records.loc[i,'Remark'] = Remark
        
        self.df_records.loc[i,'index'] = index
        self.df_records.loc[i,'Code_fut'] = Code_fut
        self.df_records.loc[i,'No_fut'] = No_fut
        
        
        self.df_records.loc[i,'Strike_ps'] = Strike_ps
        self.df_records.loc[i,'Code_ps'] = Code_ps
        self.df_records.loc[i,'Num_ps'] = Num_ps
        self.df_records.loc[i,'No_ps'] = No_ps
        self.df_records.loc[i,'Lot_ps'] = lot_ps
        self.df_records.loc[i,'T_ps'] = t_ps
    
        
        self.df_records.loc[i,'Strike_cl'] = Strike_cl     
        self.df_records.loc[i,'Code_cl'] = Code_cl
        self.df_records.loc[i,'Num_cl'] = Num_cl
        self.df_records.loc[i,'No_cl'] = No_cl
        self.df_records.loc[i,'Lot_cl'] = lot_cl
        self.df_records.loc[i,'T_cl'] = t_cl
    
        
        #df_records.loc[i,'Code_fut'] = Code_IH
        #df_records.loc[i,'Num_fut'] = Num_IH
    
    
   def switch_flag(self,etf,k_old,t_old,option_type,ps_open, cl_open, ps_maintain , cl_maintain): #option_type: 'ps'/'cl'
        # Check if need to change the K
        (range_min,range_max) = self.k_range(etf,'hold',ps_open, cl_open, ps_maintain , cl_maintain)
    
        if option_type == 'ps':
            if  range_min - k_old >= 0:
                flag_atm = True
            else:
                flag_atm = False  
         
        if option_type == 'cl':
            if  k_old - range_max <= 0:
                flag_atm = True
            else:
                flag_atm = False 
        
        #change if need to change the T       
        if (t_old >= self.change_day):
            flag_ismaturity = True
        else:
            flag_ismaturity = False
    
        # Check if need to change the contract
        if flag_atm and flag_ismaturity:
            return False
        else:
            return True
     
   def cal_cost(self,i,df_fut_yes):
        cost = 0
        if i > 0:
            
            #交易手续费
            if i ==1:
                #IH
                cost = abs(self.df_records.No_fut[i] * self.df_records.price_fut[i]*300) * self.cost_fut
                
                cost += abs(self.df_records.No_ps[i]) * self.cost_t
                self.cost_impact = (abs(self.df_records.No_ps[i])+abs(self.df_records.No_cl[i])) * self.cost_i
                cost += self.cost_impact
                
            else:
                if self.df_records.Code_fut[i] == self.df_records.Code_fut[i-1]:
                    cost = abs(self.df_records.No_fut[i]-self.df_records.No_fut[i-1]) * self.df_records.price_fut[i]* 300 *self.cost_fut
                else:  
                    cost = abs(self.df_records.No_fut[i])* self.df_records.price_fut[i] * 300 * self.cost_fut + abs(self.df_records.No_fut[i-1])*300* df_fut_yes.price_t1[0] * self.cost_fut
            
                columns = ['ps','cl']
                code_yes = []
                no_yes = []
                code_t = []
                no_t = []
                for code in columns:
                    code_yes.append(str(self.df_records.at[i-1,'Code_'+code])[:8])
                    no_yes.append(self.df_records.at[i-1,'No_'+code])
                    code_t.append(str(self.df_records.at[i,'Code_'+code])[:8])
                    no_t.append(self.df_records.at[i,'No_'+code])
                
                df_pos_yes = pd.DataFrame({'code':code_yes,'t-1':no_yes})
                df_pos_t = pd.DataFrame({'code':code_t,'t':no_t})
                df_pos = pd.merge(df_pos_yes,df_pos_t,on='code',how='outer')
                df_pos = df_pos.fillna(0)
                df_pos['change'] =  df_pos['t'] - df_pos['t-1']
                df_pos['open_short'] = 0
                df_pos.loc[(df_pos['t-1']==0)&(df_pos['t']<0),'open_short'] = 1
                df_pos['self.cost_impact'] = abs(df_pos['change']) * self.cost_i
                df_pos['self.cost_trading'] = np.where(df_pos['open_short'].apply(lambda x: x) == 1, 0, abs(df_pos['change'])*self.cost_t)
            
                cost += df_pos['self.cost_impact'].sum() + df_pos['self.cost_trading'].sum()      
                    
        return cost
    
   def print_signal(self,i):
        signal = ''
        if i == 1:
            signal = 'open'
        else:
            if (self.df_records.Code_ps[i] == self.df_records.Code_ps[i-1]) and (self.df_records.Code_cl[i] == self.df_records.Code_cl[i-1]):
                signal = 'hold'
            else:
                if self.df_records.Code_ps[i] != self.df_records.Code_ps[i-1]:
                    signal += 'switch ps'
                else:
                    signal += 'hold ps'
                if self.df_records.Code_cl[i] != self.df_records.Code_cl[i-1]:
                    signal += ' switch cl'
                else:
                    signal += ' hold cl'
        return signal    

  
   def backtest_loop(self,ps_open, cl_open, ps_maintain , cl_maintain):    
        df_rf, df_options, df_etf, df_contract,df_fut, datelist, last_date_records = self.df_rf, self.df_options, self.df_etf, self.df_contract,self.IH, self.datelist, self.last_date_records
        # add new date and vix to self.df_records
        datelist = datelist.append({'date':last_date_records},ignore_index=True).sort_values(by='date').reset_index(drop=True)
        #index Data
        df_underlying = df_fut.loc[:,['Date','index']]
        df_new = df_underlying[df_underlying['Date'].isin(datelist['date'])].reset_index(drop=True)[['Date','index']]
        self.create_df_records()
        self.df_records = self.df_records.append(df_new,ignore_index=True)
        datelist = datelist['date'][:-3] #日行情没更新
        self.df_records = self.df_records[self.df_records['Date'].isin(datelist)].reset_index(drop=True)
        self.df_records.loc[0,'净值'] = 1    
        self.df_records.loc[0,'PnL%'] = 0 
        
        for i in range(1,len(datelist)): 
            print(i,'/',len(datelist),'----',datelist[i],'----')
            date = datelist[i]
            yesterday = datelist[i-1]
            # Get etf, option price (today,yesterday)
            etf_close = self.get_data(df_underlying, date, 'index')
            etf_yes = self.get_data(df_underlying, yesterday, 'index')
            df_date = self.get_data(df_options, date, 'option', df_contract)
            df_yesterday = self.get_data(df_options, yesterday, 'option', df_contract)
            df_fut_date =  self.get_data(df_fut, date, 'fut')
            df_fut_yes =  self.get_data(df_fut, yesterday, 'fut')
            #Calculate position
            self.cal_position(i,yesterday, date, etf_yes, etf_close, df_date,df_contract,ps_open, cl_open, ps_maintain , cl_maintain,df_fut_date)
            self.df_records.loc[i,'Signal'] = self.print_signal(i)
            self. df_records.loc[i,'price_fut'] = df_fut_date.loc[0,'price_t'] 
            #Calculate PnL
            try:
                settle_yes = df_date[df_date['期权代码']==self.df_records.Code_cl[i]].前结算价.values[0]
            except:
                settle_yes = df_date[df_date['期权代码']==self.df_records.Code_cl[i]].收盘价.values[0]
        
            self.df_records.loc[i,'Margin_o'] = self.margin_single_option(settle_yes,self.df_records.Strike_cl[i],self.df_records.Num_cl[i],etf_yes,etf_close,'call')
                                            
            self.df_records.loc[i,'Margin_f'] =  self.margin_fut(self.df_records.price_fut[i],self.df_records.No_fut[i])
            self.df_records.loc[i,'Margin'] = self.df_records.loc[i,'Margin_o'] + self.df_records.loc[i,'Margin_f']
            
            self.df_records.loc[i,'Option'] = df_date.loc[df_date['期权代码'] == self.df_records.Code_ps[i],'收盘价'].values[0] * self.df_records.Num_ps[i]\
                                        + df_date.loc[df_date['期权代码'] == self.df_records.Code_cl[i],'收盘价'].values[0] * self.df_records.Num_cl[i]\
        
            self.df_records.loc[i,'Futures'] =  self.df_records.price_fut[i] * self.df_records.No_fut[i] * 300
            
            self.df_records.loc[i,'Cost'] = -self.cal_cost(i,df_fut_yes)
            
            self.df_records.loc[i,'Cash'] = self.df_records.loc[i,'Margin'] + self.df_records.loc[i,'Option'] + self.df_records.loc[i,'Futures'] + abs(self.df_records.loc[i,'Cost'])
        
        
            #t-1 postion
            if pd.isnull(self.df_records.Code_ps[i-1]) == False:
                code_ps_yes = self.df_records.Code_ps[i-1]
                code_cl_yes = self.df_records.Code_cl[i-1]
               
                code_yes = [code_ps_yes,code_cl_yes]
                num_yes = [self.df_records.Num_ps[i-1],self.df_records.Num_cl[i-1]]
                price_yes = [0]*len(code_yes)
                price_today = [0]*len(code_yes)
                for code in range(len(code_yes)):
                    price_yes[code] = df_yesterday.loc[df_yesterday['期权代码'] == code_yes[code],'收盘价'].values[0]
                    price_today[code] = df_date.loc[df_date['期权代码'] == code_yes[code],'收盘价'].values[0]
                
                price_diff = [price_today[z] - price_yes[z] for z in range(len(code_yes))]
                
           
                self.df_records.at[i,'PnL_option'] = sum(x*y for x,y in zip(price_diff,num_yes))     
                self.df_records.at[i,'PnL_ps'] =  price_diff[0] * num_yes[0]
                self.df_records.at[i,'PnL_cl'] =  price_diff[1] * num_yes[1]                   
                self.df_records.at[i,'PnL_fut'] = (df_fut_yes.price_t1[0]- df_fut_yes.price_t[0]) * self.df_records.No_fut[i-1] * 300
                
                self.df_records.loc[i,'PnL'] = self.df_records['PnL_option'][i] + self.df_records['PnL_fut'][i] + self.df_records['Cost'][i]
                self.df_records.loc[i,'PnL%'] = self.df_records.loc[i,'PnL'] / self.df_records.loc[i-1,'Cash']
                #self.df_records.loc[i,'净值'] = self.df_records['PnL%'][i] + self.df_records['净值'][i-1]
            else:
                self.df_records.loc[i,'PnL'] = self.df_records.loc[i,'Cost']
                self.df_records.loc[i,'PnL%'] = self.df_records.loc[i,'Cost']/self.df_records.loc[i,'Cash']
                #self.df_records.loc[i,'净值'] = self.df_records['PnL%'][i] + self.df_records['净值'][i-1]
                
        self.df_records['index_chg'] = self.df_records['index'] / self.df_records['index'][0]
        self.df_records['daily_r_index'] = self.df_records['index'].pct_change()
        self.df_records['daily_r_index'][0] = 0
        self.df_records.loc[:,'净值'] = (self.df_records.loc[:,'PnL%']+1).cumprod()
        
        print("Update df_records %s(收盘后)"%(self.etf_kind))
        self.df_records.to_excel('./%s/fut/%s_collar_fut_%s_%s_%s.xlsx'%(self.etf_kind,self.etf_kind,datetime.datetime.now().strftime('%m%d'),ps_open,cl_open), index = False) 
        return self.df_records    
    
class cal_return_sharpe():
    
    def cal_return(df):
        """

        Parameters
        ----------
        df : df_return_combine(daily), index:Date

        Returns
        -------
        Annulized R, Annual Vol, Sharpe, Max Drawdown

        """
        df_result_all = pd.DataFrame([])
        df.index = pd.to_datetime(df.index)
        df = df.loc[df.index>=pd.to_datetime('2015-04-16')]

        for j in range(len(df.columns)):
            name = df.columns[j][8:]
            df_result = pd.DataFrame(columns = ['年份','年化收益率_%s'%name,'年化波动率_%s'%name,'Sharpe_%s'%name,'最大回撤_%s'%name])
            df_result['年份'] = df.index.year.drop_duplicates()
            for i in range(len(df_result)):
                start = pd.to_datetime(str(df_result.年份[i]) + '0101')
                print('cal return',df_result.年份[i])
                if i < len(df_result)-1:
                    end =  pd.to_datetime(str(df_result.年份[i+1]) + '0101')
                    df_year = df.loc[(df.index >= start) & (df.index < end)]
                else:
                    df_year = df.loc[(df.index >= start)]
                    
                df_result.iloc[i,1] = ep.annual_return(df_year[df.columns[j]])
                df_result.iloc[i,2] = ep.annual_volatility(df_year[df.columns[j]])
                df_result.iloc[i,3] = ep.annual_return(df_year[df.columns[j]])/ep.annual_volatility(df_year[df.columns[j]]) 
                df_result.iloc[i,4] = ep.max_drawdown(pd.to_numeric(df_year[df.columns[j]]))
                                     
            if len(df_result_all) == 0:
                df_result_all = df_result
            else:
                df_result_all = pd.merge(df_result_all,df_result, on = '年份')
        return df_result_all
#%% Main          
if __name__=='__main__':
    lst_ps_open = list(range(80,100,5))
    lst_cl_open = list(range(105,125,5))
    #lst_ps_open = list(range(80,90,5))
    #lst_cl_open = list(range(105,115,5))
    index = collar_etf_300index('300index')
    index.read_database()
    result = pd.DataFrame()
   
    for ps_open in lst_ps_open:
        for cl_open in lst_cl_open:
            print("ps_open: ", ps_open, "cl_open: ", cl_open)
            ps_maintain = ps_open
            cl_maintain = cl_open
            df = index.backtest_loop(ps_open, cl_open, ps_maintain, cl_maintain)
            df.rename(columns={'PnL%': 'daily_r_%s_%s'%(ps_open,cl_open), '净值': '净值_%s_%s'%(ps_open,cl_open)}, inplace=True)
            if result.empty == True:
                result = df.loc[:,['Date','daily_r_%s_%s'%(ps_open,cl_open),'净值_%s_%s'%(ps_open,cl_open)]]
            else:
                result = pd.merge(result,df.loc[:,['Date','daily_r_%s_%s'%(ps_open,cl_open),'净值_%s_%s'%(ps_open,cl_open)]],on = 'Date', how = 'outer')
            
    result = pd.merge(result,df.loc[:,['Date','daily_r_index','index_chg']],on = 'Date', how = 'outer')
    # write result to excel
    #result.to_excel('./%s/%s_collar_%s_result_combine.xlsx'%(index.etf_kind,index.etf_kind,datetime.datetime.now().strftime('%m%d')), sheet_name='Result_combine',index= False)
    writer = pd.ExcelWriter("./%s/%s_collar_fut_%s_result_combine.xlsx"%(index.etf_kind,index.etf_kind,(datetime.datetime.now().strftime('%m%d'))), engine='xlsxwriter')
    result.to_excel(writer, sheet_name='Result_combine',index= False)
    #Calculate return
    df_return = cal_return_sharpe.cal_return(result.set_index('Date').filter(regex='daily_r'))
    df_return.to_excel(writer, sheet_name='Return',index= False)
    result.filter(regex='Date|净值|index_chg').to_excel(writer, sheet_name='Performance',index= False)
    #test.filter(regex='年化收益率|年份').style.background_gradient(cmap='PuBu')
    writer.save()
    print('finish')
    #Plot
    fig = result.filter(regex='Date|净值|index_chg').set_index('Date').plot(title='300index_fut').get_figure()
    fig.tight_layout()
    fig.savefig('./300index_fut.pdf',bbox_inches="tight")
   
    
    