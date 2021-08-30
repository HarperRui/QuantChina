# -*- coding: utf-8 -*-
"""
Created on Fri Jul  9 15:01:17 2021

@author: Xuan
"""


import pandas as pd
import numpy as np
from functools import reduce
import sql_get_save
import sys
sys.path.append('D:/Harper/Class')
from Get_Price import *
#%%
def Get_data(kind, code, start_date = '20010101'):
    SQL = SQL_conn('wind')  
    df = Get_price.wind_database(SQL,kind,code,start_date)  
    SQL.close_conn()
    del df['成交量']
    
    df.columns = ['Code','Date','preclose','open','high','low','close','etf_chg(%)']
    df['Date'] = pd.to_datetime(df['Date'])
    df.loc[:,'preclose':'etf_chg(%)'] =df.loc[:,'preclose':'etf_chg(%)'].astype(np.float32)
    return df
    
#%%
class HV:
    def __init__(self, data, days, code, period_update = False, now_date=None, data_iv=None):

        if data is None:
            sys.exit("Data is not valid")
            
        self.data = data
        self.days = days
        self.code =  code
        self.vol = pd.DataFrame()
        self.period_update = period_update
        """这里新加了now_date和data_iv参数，now_date表示当前数据库中HV_Percentile的最新日期，data_iv为包含vix和iv的DataFrame"""
        self.now_date = now_date
        self.data_iv = data_iv

    def Close_to_Close(self):
        print('calculate close_to_close')
        res = pd.DataFrame(self.data['Date'])
        res['cc'] = np.log(self.data['close'] / self.data['preclose'])

        for n in self.days:
            res['HV%i' % n] = res['cc'].rolling(n).std(ddof=1) * np.sqrt(252) * 100
        del res['cc']
        res['Code'] = self.code

        return res

    def Parkinson(self):
        print('calculate parkinson')
        res = pd.DataFrame(self.data['Date'])
        res['hl_square'] = np.log(self.data['high'] / self.data['low']) ** 2

        for n in self.days:
            res['P%i' % n] = np.sqrt(res['hl_square'].rolling(n).mean() / 4 / np.log(2)) * np.sqrt(252) * 100
        del res['hl_square']
        res['Code'] = self.code

        return res

    def Garman_Klass(self):
        print('calculate GK')
        res = pd.DataFrame(self.data['Date'])
        res['hl_square'] = np.log(self.data['high'] / self.data['low']) ** 2
        res['cc_square'] = np.log(self.data['close'] / self.data['preclose']) ** 2

        for n in self.days:
            var = res['hl_square'].rolling(n).mean() / 2 - (2 * np.log(2) - 1) * res['cc_square'].rolling(
                n).mean()  # 可能计算出来是负值
            res['GK%i' % n] = var.apply(lambda x: np.sqrt(x * 252) if x >= 0 else np.nan) * 100  # 如果方差为负值，标准差填入nan
        del res['hl_square'], res['cc_square']
        res['Code'] = self.code

        return res

    def Rogers_Satchell(self):
        print('calculate RS')
        res = pd.DataFrame(self.data['Date'])
        res['hc'] = np.log(self.data['high'] / self.data['close'])
        res['ho'] = np.log(self.data['high'] / self.data['open'])
        res['lc'] = np.log(self.data['low'] / self.data['close'])
        res['lo'] = np.log(self.data['low'] / self.data['open'])

        for n in self.days:
            res['RS%i' % n] = np.sqrt((res['hc'] * res['ho'] + res['lc'] * res['lo']).rolling(n).mean() * 252) * 100
        res.drop(['hc', 'ho', 'lc', 'lo'], axis=1, inplace=True)
        res['Code'] = self.code

        return res

    def Yang_Zhang(self):
        print('calculate YZ')
        res = pd.DataFrame(self.data['Date'])
        res['o'] = np.log(self.data['open'] / self.data['preclose'])
        res['c'] = np.log(self.data['close'] / self.data['open'])
        res['hc'] = np.log(self.data['high'] / self.data['close'])
        res['ho'] = np.log(self.data['high'] / self.data['open'])
        res['lc'] = np.log(self.data['low'] / self.data['close'])
        res['lo'] = np.log(self.data['low'] / self.data['open'])

        for n in self.days:
            var_o = res['o'].rolling(n).var(ddof=1)
            var_c = res['c'].rolling(n).var(ddof=1)
            var_rs = (res['hc'] * res['ho'] + res['lc'] * res['lo']).rolling(n).mean()
            k = 0.34 / (1.34 + (n + 1) / (n - 1))
            res['YZ%i' % n] = np.sqrt(var_o + k * var_c + (1 - k) * var_rs) * np.sqrt(252) * 100
        
        res.drop(['c', 'o', 'hc', 'ho', 'lc', 'lo'], axis=1, inplace=True)
        res['Code'] = self.code
        
        return res

    #所有涨跌幅超过4%的交易日数据只会在计算s天内realized vol时用到
    def hv_ghost(self,s=5,etf_chg = 0.04):
        print('calculate HV_ghost')
        #rolling window (ex the last 5 rows) 
        res = self.data.loc[:,['Date','close','preclose','etf_chg(%)']]
        res['cc'] = np.log(res['close']/res['preclose'])
        
        def ex_ghost(ser):
            select = res.loc[ser.index]
            select.iloc[:-s,select.columns.get_loc('cc')] = np.where(select.iloc[:-s,select.columns.get_loc('etf_chg(%)')].apply(lambda x: abs(x)) > etf_chg*100, (np.nan), select.iloc[:-s,select.columns.get_loc('cc')])
            return select.loc[:,'cc'].std(ddof=1)    
    
        for n in self.days:
            res['HV%s_ghost'%n] = res['cc'].rolling(n).apply(ex_ghost) * np.sqrt(252) * 100
    
        del res['close'],res['preclose'], res['cc'],res['etf_chg(%)']
        
        res['Code'] = self.code
    
        return res

    def get_merge_data(self):
        data = [self.Close_to_Close(),
                self.Parkinson(),
                self.Garman_Klass(),
                self.Rogers_Satchell(),
                self.Yang_Zhang(),
                self.hv_ghost()]

        df_merge = reduce(lambda left, right: pd.merge(left, right, on=['Date', 'Code']), data)
        # 调整'Code'列的位置
        self.df_merge = df_merge.drop('Code', axis=1)
        self.df_merge.insert(1, 'Code', self.code)

        """对于50ETF，将HV与IV数据merge后方便同时计算分位数"""
        if self.data_iv is not None:
            self.df_merge = pd.merge(self.df_merge, self.data_iv, on=['Date'], how='left')
        #return df_merge    
    
   #全历史分位数
    def add_historical_percentile(self):
        
        self.get_merge_data()
        print("calculate Percentile")

        """只计算数据库中没有的日期的数据，开始计算的index(start_index)即为当前HV_Percentile最新日期的后一天的index"""
        """如果在生成class时不指定now_date，则默认从第一天开始计算所有的percentile数据"""
        start_idx = 0 if self.now_date is None else self.df_merge.loc[self.df_merge['Date']==self.now_date].index[0] + 1

        """计算的分位数变成了0-100和25、75分位数，原本的min和max用0和100分位数替代"""
        percentile_lst = list(np.arange(0, 110, 10))
        percentile_lst.append(25)
        percentile_lst.append(75)  # 计算0-100+25,75分位数

        def get_percentile_data(values, percentiles, eliminate_extremum=True):
            """
            获取values中的指定分位数，可选择是否剔除大于95%和小于5%的极值
            :param values: array or series
            :param percentiles: list, array or series
            :param eliminate_extremum: whether eliminate extreme values (top and low 5% values)
            :return: list that contains percentile values
            """
            """这个函数换成了'HV_Percentile.py'中的'get_percentile_data'函数"""
            if len(values) == 0:
                return [np.nan] * (len(percentiles))

            if eliminate_extremum:
                last_5 = np.percentile(values, 5)
                top_5 = np.percentile(values, 95)
                values1 = values[(values > last_5) & (values < top_5)]
                if len(values1) != 0:
                    values = values1

            result = []
            for percentile in percentiles:
                result.append(np.percentile(values, percentile))
            return result

        """最终返回的结果只包含更新的日期的数据"""
        res = pd.DataFrame(self.df_merge.loc[start_idx:, 'Date'])
        hv_types = [x for x in self.df_merge.columns if x not in ['Date', 'Code']]  # 包含了'iv'与'iv_insert'
        for i in hv_types:
            print(i)
            """从更新的日期开始遍历"""
            for idx in range(start_idx, len(res)):
                _values = self.df_merge[i][0:idx+1].copy()
                if len(_values.dropna()) > 0:
                    percentile_data = get_percentile_data(_values.dropna(), percentiles=percentile_lst)
                    for p in range(len(percentile_lst)):
                        res.at[idx, '%s_%i' % (i, percentile_lst[p])] = percentile_data[p]

        """merge HV和percentile中更新的数据"""
        df_merge = pd.merge(self.df_merge.loc[start_idx:], res, on=['Date'], how = 'left')
        return df_merge

#%%


if __name__ == '__main__':

    code_lst = {'510050.SH':'etf','510300.SH':'etf','000016.SH':'index','000300.SH':'index'}
    
    """读取现在数据库中已有的数据"""
    from zltsql import SQLConn
    SQ = SQLConn()
    df_HVPercentile = SQ.GetData('HV_percentile')
    SQ.CloseSql()
    df_vix = "读取vix数据，设置columns为['Date','iv']"
    df_iv_insert = "读取iv_insert数据，设置columns为['Date','iv_insert']"
    df_iv = pd.merge(df_vix, df_iv_insert, on=['Date'], how='left')

    days = [5, 10, 20, 40, 60]

    """如果是第一次填入数据"""
    for i in code_lst:
        df = Get_data(code_lst[i], i)
        if i == '510050.SH':
            df_result = HV(df, days, i, data_iv=df_iv).add_historical_percentile()
        else:
            df_result = HV(df, days, i).add_historical_percentile()
        df_result.to_sql('HV_percentile', con=sql_get_save.sql_save(), if_exists='append', index=False)

    """如果是在已有数据库中填充更新的数据"""
    for i in code_lst:     
        df = Get_data(code_lst[i], i)
        df_prev = df_HVPercentile[df_HVPercentile['Code'] == i].reset_index(drop=True)
        pre_date = df_prev.at[len(df_prev)-1, 'Date']  # 现有数据的最后一天日期
        new_date = df.at[len(df)-1, 'Date']  # 需要更新到的日期
        if pre_date < new_date:
            # 只更新目前数据库中没有的日期的数据
            if i == '510050.SH':
                df_result = HV(df, days, i, now_date=pre_date, data_iv=df_iv).add_historical_percentile()
            else:
                df_result = HV(df, days, i, now_date=pre_date).add_historical_percentile()
            df_result.to_sql('HV_percentile', con=sql_get_save.sql_save(), if_exists='append', index=False)

        # df_result = HV(df, days,i).add_historical_percentile()
        # df_result.to_sql('HV_percentile',con = sql_get_save.sql_save(),if_exists = 'append',index=False)
