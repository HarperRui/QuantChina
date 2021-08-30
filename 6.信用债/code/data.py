# -*- coding: utf-8 -*-


import pandas as pd
import pymssql
import datetime
from sqlalchemy import create_engine


def sql_select(sql):
    conn = pymssql.connect(host='14.116.138.19',  
                               user='GuestUser',
                               password='GuestUser',
                               database='JYDB')
    with conn.cursor() as cursor:
        cursor.execute(sql)
        data = cursor.fetchall()
        df=pd.DataFrame(list(data))
        return df

#Save Data to SQL Database
def sql_save():
    host='192.168.9.85'
    user='sa'
    password='lhtzb.123'
    database='BondMonitor'
    engine = create_engine("mssql+pymssql://{0}:{1}@{2}/{3}?charset=utf8".format(user, password, host, database))
    return engine
     


today = datetime.datetime.today()
offset = max(1, (today.weekday() + 6) % 7 - 3)
timedelta = datetime.timedelta(offset)
last_date = today - timedelta


#Yield >20
sql="SELECT InnerCode, EndDate,VPYield FROM Bond_CBValuationAll WHERE VPYield > 20 and EndDate >= %s"%('\''+last_date.strftime("%Y-%m-%d")+'\'') 
print("Processing Valuation Data")
bond_val = sql_select(sql)
bond_val.columns = ['InnerCode', 'PriceDate','VPYield']
bond_val = bond_val.sort_values(by=['InnerCode','PriceDate'],ascending=True)
df = bond_val.drop_duplicates(subset=['InnerCode'],keep='first')
#bond_val.to_parquet('../data/bond_value.parquet')


#Bond Code
sql="SELECT InnerCode, SecuCode, SecuAbbr,ChiName,Issuer,SecuMarket, DelistDate,BondTypeLevel1, BondTypeLevel1Desc FROM Bond_Code"
print("Processing Bond Code")
bond_code = sql_select(sql)
bond_code.columns = ['InnerCode', 'Code', 'Name','Fullname','Issuer','SecuMarket', 'DelistDate','Type1','Type1_Des']

df1 = pd.merge(df,bond_code,on='InnerCode',how = 'left')
filter_issuer = df1['Issuer'].drop_duplicates()


#Bond Info
sql="SELECT InnerCode,EndDate FROM Bond_BasicInfoN"
print("Processing Bond Info")
bond_info = sql_select(sql)
bond_info.columns = ['InnerCode','EndDate']


#Selection
print("Filtering Bond List")
ipo = bond_code[(bond_code['SecuMarket'].isin([83,90]))& (bond_code['Issuer'].isin(df1['Issuer'].drop_duplicates()))&(bond_code['Type1'].isin([1600,1700]))]
ipo['Code'] = ipo['Code'].astype(int)
ipo1 = ipo[ipo['DelistDate'].isna()]
df_ipo = pd.merge(ipo1,bond_info,on='InnerCode',how='left')


#df_ipo_old = pd.read_excel('../result/bond_list%s.xlsx'%(str(last_date.month)+str(last_date.day)))
df_ipo_old = pd.read_excel('../result/bond_list%s.xlsx'%(last_date.strftime('%m%d')))
df_ipo = pd.concat([df_ipo_old,df_ipo]).drop_duplicates()
df_ipo1 = df_ipo[df_ipo['EndDate']>datetime.datetime.today()]

df_ipo1 = df_ipo1.sort_values(by=['Issuer','EndDate'],ascending=True).reset_index(drop=True)
df_ipo1.to_excel('../result/bond_list%s.xlsx'%(today.strftime('%m%d')),index=False)


print("Writing to SQL Database")
df_database = df_ipo1[['InnerCode','Code','Name','Issuer','SecuMarket']]

df_database.to_sql('Bond_list', con=sql_save(), if_exists='replace')






'''
#Valuation
sql="SELECT InnerCode, EndDate,ValueFullPrice FROM Bond_CBValuationAll WHERE ValueFullPrice < 50" 
bond_val = sql_select(sql)
bond_val.columns = ['InnerCode', 'PriceDate','ValueFullPrice']
bond_val = bond_val.sort_values(by=['InnerCode','PriceDate'],ascending=True)
df = bond_val.drop_duplicates(subset=['InnerCode'],keep='first')
bond_val.to_parquet('../data/bond_value.parquet')

#Yield>20 code
sql="SELECT Distinct InnerCode FROM Bond_CBValuationAll WHERE VPYield > 20" 
code_yield = sql_select(sql)
code_yield.columns = ['InnerCode']
code_yield.to_parquet('../data/bond_yield_code.parquet')

'''

'''
issuer =  pd.read_parquet('../data/issuer.parquet')
#code_yield = pd.read_parquet('../data/bond_yield_code.parquet')
bond_val= pd.read_parquet('../data/yield_kptfirst.parquet')

sql="SELECT InnerCode, EndDate,VPYield FROM Bond_CBValuationAll WHERE in (' + ','.join((str(n) for n in code_yield)) + ')" 
bond_val = sql_select(sql)
bond_val.columns = ['InnerCode', 'PriceDate','VPYield']
#bond_val.to_parquet('../data/bond_value.parquet')'''

'''
#df1 = pd.merge(bond_val,issuer,on='InnerCode',how = 'left')

filter_issuer = df6[['Issuer']]
ipo = bond_code[(bond_code['SecuMarket'].isin([83,90]))& (bond_code['Issuer'].isin(df6['Issuer']))]
ipo1 = ipo[ipo['DelistDate'].isna()]

df_ipo = pd.merge(ipo,bond_info,on='InnerCode',how='left')
df_ipo1 = df_ipo[df_ipo['EndDate']>datetime.datetime.strptime('2020-11-24','%Y-%m-%d')]

'''



