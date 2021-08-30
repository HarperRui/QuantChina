# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 11:15:20 2021

@author: Xuan
"""


#SQL get data (85 database)
import pymssql,pymysql
import pandas as pd
def sql_select(sql,host='192.168.9.85',user='sa',password='lhtzb.123',database = 'Option'):
    conn = pymssql.connect(host, user,password,database)
    df = pd.read_sql(sql, conn)    
    conn.close()
    return df



''' 
#How to use(Example)  
sql="SELECT * FROM option_list_300etf "
print("Processing 300ETF Option list")
option_list_300etf = sql_data.sql_select(sql)
option_list_300etf.columns = ['日期','期权代码','交易代码','行权价','期权简称','前结算价']  
'''  



#SQL get data (wind database)
def sql_select_wind(sql,host='database.quantchina.info',user='quantchina', password='zMxq7VNYJljTFIQ8', database= 'wind', charset='gbk',port=3306):
    conn = pymysql.Connect(host = host,user = user,password = password,database = database,charset = charset)
    #conn = pymssql.connect(host, user,password,database)
    #df = pd.read_sql(sql, conn) 
    with conn.cursor() as cursor:
        cursor.execute(sql)
        data = cursor.fetchall()
        df=pd.DataFrame(list(data))
    conn.close()
    return df

'''
def sql_select_wind(sql,host='database.quantchina.info', user='quantchina', password='zMxq7VNYJljTFIQ8', database= 'wind', charset='gbk'):
    conn = pymysql.connect(host = host,user = user,passwd = password,db = database,charset = charset)
    df = pd.read_sql(sql, conn) 
    conn.close()
    return df
'''



#sql delete data
def sql_delete(sql,host='192.168.9.85',user='sa',password='lhtzb.123',database = 'Option'):
    conn = pymssql.connect(host, user,password,database)
    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()
    conn.close()
    print('delete sql data sucessfully')
'''
#Example
sql = "delete FROM etf_300 WHERE 日期 = '%s
sql_delete(sql)
'''


#SQL save data
from sqlalchemy import create_engine
def sql_save():
    host='192.168.9.85'
    user='sa'
    password='lhtzb.123'
    database='Option'
    engine = create_engine("mssql+pymssql://{0}:{1}@{2}/{3}?charset=utf8".format(user, password, host, database))
    return engine


'''
Example
df_etf.to_sql('etf_300',con = sql_get_save.sql_save(),if_exists = 'append',index = False)
'''



#EoF