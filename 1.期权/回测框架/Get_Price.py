# -*- coding: utf-8 -*-
"""
Created on Mon Jun 28 10:33:28 2021

@author: Xuan
"""



import pandas as pd
import os
import pymssql,pymysql
import numpy as np


#%% SQL Connect
class SQL_conn():
    def __init__(self,kind):
        """
        Parameters
        ----------
        kind : '85'/'wind'

        """
        self.kind = kind
        if kind == 'wind':
            self.host='database.quantchina.info'
            self.user='quantchina'
            self.password='zMxq7VNYJljTFIQ8'
            self.database= 'wind'
            self.charset='gbk'
            #self.port = 3306
            self.conn = pymysql.Connect(host = self.host,user = self.user,password = self.password,database = self.database,charset = self.charset)
            print('wind数据库连接成功')
        elif kind == '85':
            self.host='192.168.9.85'
            self.user='sa'
            self.password='lhtzb.123'
            self.database = 'Option'
            self.conn = pymssql.connect(self.host, self.user,self.password,self.database)
            print('85数据库连接成功')
            
            
    def get_data(self,sql_query):
        if self.kind == 'wind':
            with self.conn.cursor() as cursor:
                cursor.execute(sql_query)
                data = cursor.fetchall()
                df=pd.DataFrame(list(data))
        elif self.kind == '85':
            df = pd.read_sql(sql_query, self.conn)    
        return df    

    def close_conn(self):
        self.conn.close()
        print('数据库断开成功')
        


#%% Get_price
class Get_price():
    def min_bar(folder,code,date,exchange_code,start_time=None,end_time = None):
        """
        Parameters
        ----------
        folder : 'future' or 'stock'
            folder name in datahouse
        code : string
            like '510300'
        date : 'YYYYmmdd'
            '20140109'
        exchange: 'sz'/'sh';上期所(shf)，大商所(dce)，上能所(ine)，郑商所(czc)，中金所(sfe)，上金所(sge)
        
        start_time : '9:00:00'
            price at particular time. The default is None.

        end_time : '15:00:00'
            price at particular time. The default is None.

        Returns
        -------
        parquet
        """
        filepath = os.path.join('/datahouse.quantchina.info/data/minbar',folder,date,'1min','%s_%s_%s_1min.parquet'%(exchange_code.lower(),code,date)).replace('\\','/')
        if os.path.exists('/'+filepath) == False:
            print(code,' not in minbar files')
        else:
            df = pd.read_parquet('/'+filepath)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.loc[(df['datetime']>= pd.to_datetime(date+' 09:30:00'))&(df['datetime']<= pd.to_datetime(date+' 15:00:00'))].reset_index(drop=True)

            if (start_time == None) & (end_time == None):
                return df
            else:
                if (start_time != None) & (end_time != None):
                    return df.loc[(df['datetime'] >= pd.to_datetime(date + ' ' + start_time))&(df['datetime'] <= pd.to_datetime(date + ' ' + end_time))]
                elif start_time == None:
                    return df.loc[df['datetime'] <= pd.to_datetime(date + ' ' + end_time)]
                elif end_time == None:
                    return df.loc[df['datetime'] >= pd.to_datetime(date + ' ' + start_time)]



    def tick_data(folder,code,date,exchange_code,start_time=None,end_time = None):
        """
        Parameters
        ----------
        folder : 'future' or 'stock'
            folder name in datahouse
        code : string
            like '510300'
        date : 'YYYYmmdd'
            '20140109'
        exchange: 'sz'/'sh';上期所(shf)，大商所(dce)，上能所(ine)，郑商所(czc)，中金所(sfe)，上金所(sge)
        
        start_time : '9:00:00'
            price at particular time. The default is None.

        end_time : '15:00:00'
            price at particular time. The default is None.

        Returns
        -------
        parquet
        """
        filepath = os.path.join('/datahouse.quantchina.info/data/tick',folder,date,'quote','%s_%s_%s_quote.parquet'%(exchange_code.lower(),code,date)).replace('\\','/')
        if os.path.exists('/'+filepath) == False:
            print(code,' not in tick files')
        else:
            df = pd.read_parquet('/'+filepath).drop_duplicates()
            df = df.loc[((df['time']>= 93000000)&(df['time']<= 113000000)) | (df['time']>= 130000000)].reset_index(drop=True)
            df['datetime'] = pd.to_datetime((df['date'].astype('str') + df['time'].astype('str')),format = '%Y%m%d%H%M%S%f')      
            #df['datetime'] = pd.to_datetime(df['time'],format = '%H%M%S%f').replace(year = 2021,month = 6, day = 25)

            if (start_time == None) & (end_time == None):
                return df
            else:
                if (start_time != None) & (end_time != None):
                    return df.loc[(df['datetime'] >= pd.to_datetime(date + ' ' + start_time))&(df['datetime'] <= pd.to_datetime(date + ' ' + end_time))]
                elif start_time == None:
                    return df.loc[df['datetime'] <= pd.to_datetime(date + ' ' + end_time)]
                elif end_time == None:
                    return df.loc[df['datetime'] >= pd.to_datetime(date + ' ' + start_time)]



    def wind_database(SQL,kind,code,start_date,end_date=None):
        """ 
        Parameters
        ----------
        SQL: connect to sql database
        kind : 'index'/'stock'/'etf'/'option' --- can be list
            
        code : '510300.SH','IF2106.CFE' --- can be list
            上交所（sh）、深交所（sz）
            上期所(shf)，大商所(dce)，上能所(ine)，郑商所(czc)，中金所(sfe)，上金所(sge)
       
        start_date :  %YY%mm%dd, '20060523'
            
        end_date : %YY%mm%dd, '20060523'
            DESCRIPTION. The default is None.

        Returns
        -------
        df

        """
        def wind_database_single(SQL,kind,code,start_date,end_date=None):
            """
            Parameters
            ----------
            SQL: connect to sql database
            kind : 'index'/'stock'/'etf'/'option'

            code : '510300.SH','IF2106.CFE'
                上交所（sh）、深交所（sz）
                上期所(shf)，大商所(dce)，上能所(ine)，郑商所(czc)，中金所(sfe)，上金所(sge)

            start_date :  %YY%mm%dd, '20060523'

            end_date : %YY%mm%dd, '20060523'
                DESCRIPTION. The default is None.

            Returns
            -------
            df
            """
            if kind == 'index':
                form = 'AINDEXEODPRICES'
            elif kind == 'stock':
                form = 'ASHAREEODPRICES'
            elif kind == 'etf':
                form = 'CHINACLOSEDFUNDEODPRICE'
            elif kind == 'option':
                form = 'CHINAOPTIONEODPRICES'
            elif kind == 'futures':
                form = 'CINDEXFUTURESEODPRICES'
            elif kind == 'commodity':
                form = 'CCOMMODITYFUTURESEODPRICES'


            if kind == 'option' or kind =='futures' or kind == 'commodity':
                if end_date == 'None':
                    sql_query = "select S_INFO_WINDCODE, TRADE_DT, S_DQ_PRESETTLE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE, S_DQ_SETTLE,S_DQ_VOLUME from %s where S_INFO_WINDCODE = '%s' and TRADE_DT >= '%s' order by TRADE_DT"%(form,code,start_date)
                    df = SQL.get_data(sql_query)
                else:
                    sql_query = "select S_INFO_WINDCODE, TRADE_DT, S_DQ_PRESETTLE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE, S_DQ_SETTLE,S_DQ_VOLUME from %s where S_INFO_WINDCODE = '%s' and TRADE_DT = '%s' and TRADE_DT <= '%s'order by TRADE_DT"%(form,code,start_date,end_date)
                    df = SQL.get_data(sql_query)

                df.columns = ['Wind代码','交易日期','前结算价(元)','开盘价(元)','最高价(元)','最低价(元)','收盘价(元)','结算价(元)','成交量(手)']

            else:
                if end_date == 'None':
                    sql_query = "select S_INFO_WINDCODE, TRADE_DT, S_DQ_PRECLOSE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE,S_DQ_VOLUME,S_DQ_PCTCHANGE from %s where S_INFO_WINDCODE = '%s' and TRADE_DT >= '%s' order by TRADE_DT"%(form,code,start_date)
                    df = SQL.get_data(sql_query)
                else:
                    sql_query = "select S_INFO_WINDCODE, TRADE_DT, S_DQ_PRECLOSE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE,S_DQ_VOLUME,S_DQ_PCTCHANGE from %s where S_INFO_WINDCODE = '%s' and TRADE_DT >= '%s' and TRADE_DT <= '%s' order by TRADE_DT"%(form,code,start_date,end_date)
                    df = SQL.get_data(sql_query)

                df.columns = ['Wind代码','交易日期','前收盘价','开盘价','最高价','最低价','收盘价','成交量','涨跌幅%']

            return df

        if type(kind) == str:
            return wind_database_single(SQL,kind,code.upper(),start_date,end_date)
        else:
            result = []
            if end_date == None:
                end_date = [None] * len(kind)
            for i in range(len(kind)):
                result.append(wind_database_single(SQL,kind[i],code[i].upper(),start_date[i],end_date[i]))
            return result

    def _85_database(SQL, database_name, condition=None):
        """
        :param database_name: 调用的数据库名称， 可以是单独的str，也可以是list（如果取多个表格）
        :param condition: sql取数据的限制条件
        :return: df
        """
        def _85_database_single(SQL, database_name, condition=None):
            """
            :param database_name_name: 数据库表格的名称, str
            :param condition: sql 筛选的条件, str
            :return: df
            """
            sql_query = "select * from %s" % database_name
            if condition != None:
                sql_query += ' ' + condition
            df = SQL.get_data(sql_query)
            return df

        if type(database_name) is str:
            df = _85_database_single(SQL, database_name, condition)
            return df
        elif type(database_name) is list:
            result = []
            if condition == None:
                condition = [None] * len(database_name)
            for i in range(len(database_name)):
                print("Get %s" % database_name[i])
                result.append(_85_database_single(SQL, database_name[i], condition[i]))
            return result

    def diy_query(SQL,database_name,select_col,condition = None):
        if condition == None:
            sql_query = "select %s from %s"%(select_col,database_name)
            return SQL.get_data(sql_query)
        else:
            sql_query = "select %s from %s where %s" % (select_col, database_name,condition)
            return SQL.get_data(sql_query)


    @classmethod
    def twap(cls,df,price_type = 'mid'):
        '''
        :param df: 筛选出的minbar/tick df
        :param price_type: minbar/tick column

                ['symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume',
               'acc_volume', 'turnover', 'acc_turnover', 'vwap', 'sell_vol', 'buy_vol',
               'first_weighted_ask_prc', 'first_weighted_bid_prc',
               'first_total_ask_vol', 'first_total_bid_vol', 'first_ask_prc1',
               'first_ask_vol1', 'first_bid_prc1', 'first_bid_vol1', 'first_ask_prc2',
               'first_ask_vol2', 'first_bid_prc2', 'first_bid_vol2', 'first_ask_prc3',
               'first_ask_vol3', 'first_bid_prc3', 'first_bid_vol3', 'first_ask_prc4',
               'first_ask_vol4', 'first_bid_prc4', 'first_bid_vol4', 'first_ask_prc5',
               'first_ask_vol5', 'first_bid_prc5', 'first_bid_vol5', 'first_ask_prc6',
               'first_ask_vol6', 'first_bid_prc6', 'first_bid_vol6', 'first_ask_prc7',
               'first_ask_vol7', 'first_bid_prc7', 'first_bid_vol7', 'first_ask_prc8',
               'first_ask_vol8', 'first_bid_prc8', 'first_bid_vol8', 'first_ask_prc9',
               'first_ask_vol9', 'first_bid_prc9', 'first_bid_vol9', 'first_ask_prc10',
               'first_ask_vol10', 'first_bid_prc10', 'first_bid_vol10']
        :return:
        '''
        if price_type == 'mid':
            df = df.loc[~((df.loc[:,'high'] == 0) | (df.loc[:,'low'] == 0)),:]
            df.loc[:,'mid'] = (df.loc[:,'high'] + df.loc[:,'low'])/2

        return df.loc[df.loc[:,price_type]!=0,price_type].mean()

    @classmethod
    def vwap(cls,df,price_type = 'mid'):
        '''
        :param df: 筛选出的minbar/tick df
        :param price_type: minbar/tick column

                ======= minbar column =======
                ['symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume',
               'acc_volume', 'turnover', 'acc_turnover', 'vwap', 'sell_vol', 'buy_vol',
               'first_weighted_ask_prc', 'first_weighted_bid_prc',
               'first_total_ask_vol', 'first_total_bid_vol', 'first_ask_prc1',
               'first_ask_vol1', 'first_bid_prc1', 'first_bid_vol1', 'first_ask_prc2',
               'first_ask_vol2', 'first_bid_prc2', 'first_bid_vol2', 'first_ask_prc3',
               'first_ask_vol3', 'first_bid_prc3', 'first_bid_vol3', 'first_ask_prc4',
               'first_ask_vol4', 'first_bid_prc4', 'first_bid_vol4', 'first_ask_prc5',
               'first_ask_vol5', 'first_bid_prc5', 'first_bid_vol5', 'first_ask_prc6',
               'first_ask_vol6', 'first_bid_prc6', 'first_bid_vol6', 'first_ask_prc7',
               'first_ask_vol7', 'first_bid_prc7', 'first_bid_vol7', 'first_ask_prc8',
               'first_ask_vol8', 'first_bid_prc8', 'first_bid_vol8', 'first_ask_prc9',
               'first_ask_vol9', 'first_bid_prc9', 'first_bid_vol9', 'first_ask_prc10',
               'first_ask_vol10', 'first_bid_prc10', 'first_bid_vol10']

               ====== tick column ======
               ['symbol', 'date', 'time', 'recv_time', 'prev_close', 'open', 'high',
               'low', 'last_prc', 'num_trades', 'volume', 'turnover', 'iopv', 'status',
               'total_ask_vol', 'weighted_ask_prc', 'total_bid_vol',
               'weighted_bid_prc', 'high_limited', 'low_limited', 'ask_prc1',
               'ask_prc2', 'ask_prc3', 'ask_prc4', 'ask_prc5', 'ask_prc6', 'ask_prc7',
               'ask_prc8', 'ask_prc9', 'ask_prc10', 'bid_prc1', 'bid_prc2', 'bid_prc3',
               'bid_prc4', 'bid_prc5', 'bid_prc6', 'bid_prc7', 'bid_prc8', 'bid_prc9',
               'bid_prc10', 'ask_vol1', 'ask_vol2', 'ask_vol3', 'ask_vol4', 'ask_vol5',
               'ask_vol6', 'ask_vol7', 'ask_vol8', 'ask_vol9', 'ask_vol10', 'bid_vol1',
               'bid_vol2', 'bid_vol3', 'bid_vol4', 'bid_vol5', 'bid_vol6', 'bid_vol7',
               'bid_vol8', 'bid_vol9', 'bid_vol10', 'datetime']

        :return:
        '''
        if price_type == 'mid':
            df = df.loc[~((df.loc[:,'high'] == 0) | (df.loc[:,'low'] == 0)),:]
            df.loc[:,'mid'] = (df.loc[:,'high'] + df.loc[:,'low'])/2
        df = df.loc[df.loc[:, price_type] != 0]
        return np.average(df[price_type], weights=df['volume'])

    @classmethod
    def get_vwap_or_twap_price(cls,data_type,folder,code,date,exchange_code,start_time=None,end_time = None,price_type = 'mid',vwap_or_twap = 'twap'):
        '''
        :param data_type: 'minbar'/'tick'
        :param folder : 'future' or 'stock'
            folder name in datahouse
        :param code : string
            like '510300'
        :param date : 'YYYYmmdd'
            '20140109'
        :param exchange: 'sz'/'sh';上期所(shf)，大商所(dce)，上能所(ine)，郑商所(czc)，中金所(sfe)，上金所(sge)
    
        :param start_time : '9:00:00'
            price at particular time. The default is None.
        :param end_time : '15:00:00'
            price at particular time. The default is None.
        
        :param price_type: minbar/tick 的 column name    
        :param vwap_or_twap: 'twap'/'vwap'
            
            
        :return: price, float
        '''
        if data_type == 'minbar':
            df = cls.min_bar(folder, code, date, exchange_code, start_time, end_time)
        elif data_type == 'tick':
            df = cls.tick_data(folder, code, date, exchange_code, start_time=None, end_time=None)

        if vwap_or_twap == 'twap':
            return cls.twap(df,price_type)
        elif vwap_or_twap == 'vwap':
            return cls.vwap(df, price_type)







#%% Demo
if __name__ == '__main__':
    # #Min bar
    # result_min = Get_price.min_bar('stock','10003437','20210625','sh')
    # result_min_start = Get_price.min_bar('stock', '10003437', '20210625', 'sh','10:00')
    # result_min_end = Get_price.min_bar('stock','10003437','20210625','sh',end_time='14:00')
    # result_min_period = Get_price.min_bar('stock', '10003437', '20210625', 'sh', '14:30','14:50')
    #
    # #---vwap or twap
    # twap_min = Get_price.get_vwap_or_twap_price('minbar','stock','10003437', '20210625', 'sh', '14:30','14:50', price_type='mid',vwap_or_twap='twap')
    # vwap_min = Get_price.get_vwap_or_twap_price('minbar', 'stock', '10003437', '20210625', 'sh', '14:30', '14:50',
    #                                             price_type='mid', vwap_or_twap='vwap')
    #
    # twap_tick = Get_price.get_vwap_or_twap_price('tick','stock','10003437', '20210625', 'sh', '14:30','14:50', price_type='mid',vwap_or_twap='twap')
    # vwap_tick = Get_price.get_vwap_or_twap_price('tick', 'stock', '10003437', '20210625', 'sh', '14:30', '14:50',
    #                                             price_type='mid', vwap_or_twap='vwap')
    #
    #
    # # #tick
    # result_tick = Get_price.tick_data('stock','10003437','20210625','sh')
    # result_tick_start = Get_price.tick_data('stock', '10003437', '20210625', 'sh','10:00')
    # result_tick_end = Get_price.tick_data('stock','10003437','20210625','sh',end_time='14:00')
    # result_tick_period = Get_price.tick_data('stock', '10003437', '20210625', 'sh', '14:30','14:50')

    #
    #wind_database
    # SQL = SQL_conn('wind')
    # result_wind = Get_price.wind_database(SQL, 'index', '000300.SH', '20000101')
    # result_wind = Get_price.wind_database(SQL,'option','10003437.SH','20210625')
    # result_wind_time = Get_price.wind_database(SQL,'option','10003437.SH','20210625','20210628')
    # df1,df2 = Get_price.wind_database(SQL, ['option','etf'], ['10003437.SH','510050.SH'], ['20210625','20210601'])
    # SQL.close_conn()

    #85_database
    # SQL = SQL_conn('85')
    # result_rf = Get_price._85_database(SQL,'rf')
    # rf, hv = Get_price._85_database(SQL,['rf','HV'])
    # result_hv_condition = Get_price._85_database(SQL,['rf','hv'],[None,"where Code = '510300.SH'"])
    # SQL.close_conn()

    #DIY query
    SQL = SQL_conn('wind')
    result_diy = Get_price.diy_query(SQL,
                                    database_name='AINDEXEODPRICES',
                                     select_col="S_INFO_WINDCODE, TRADE_DT, S_DQ_PRECLOSE, S_DQ_OPEN, S_DQ_HIGH, S_DQ_LOW, S_DQ_CLOSE,S_DQ_PCTCHANGE,S_DQ_VOLUME, S_DQ_AMOUNT",
                                     condition= "S_INFO_WINDCODE = '000300.SH' order by TRADE_DT")
    result_diy.columns = ['Wind代码', '交易日期', '前收盘价', '开盘价', '最高价', '最低价', '收盘价','涨跌幅%','成交量（手）','成交金额（千元）']

    SQL.close_conn()
    
     