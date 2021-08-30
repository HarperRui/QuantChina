import pandas as pd
import datetime
import numpy as np
import pymssql
import pymysql
from sqlalchemy import create_engine
from WindPy import w
w.start()
#w.isconnected()

#issuer = pd.read_csv('../data/issuer_only.csv')
signal = pd.read_excel('../data/resultv2.xlsx')
signal = signal[['Issuer','PriceDate']]
signal.columns=['Issuer','信号出现日期']
wind_code = pd.read_excel('../data/wind_code.xlsx').dropna().drop_duplicates() #Bond Code and Name
wind_code.columns = ['wind_code','Name','EndDate']
df_cash = pd.read_excel('../data/df_cash.xlsx')
df_cash['cash_flows_date'] = pd.to_datetime(df_cash['cash_flows_date'])


def wind(code):
    try:
        request = w.wset("cashflow","windcode=%s"%code,usedf=True)
        if request[0]==0:
            df = request[1]
            df['wind_code'] = code
            df = df.sort_values(by=['cash_flows_date']).reset_index(drop=True)
        else:
            print('data ERROR')
    except KeyError:
        print('code ERROR') 
   
    return df
    

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

def quant_china(sql):
    
    conn = pymysql.Connect(host='database.quantchina.info',user='quantchina',password='zMxq7VNYJljTFIQ8',db='wind',charset='gbk')

    with conn.cursor() as cursor:
        cursor.execute(sql)
        data = cursor.fetchall()
        df=pd.DataFrame(list(data))
        return df



def type(df):
    string=np.nan
    if df['兑付类型'].str.contains('还本').any():
        if df.loc[df['兑付类型']=='还本','百元兑付本金'].sum()<100:
            string = '部分本金'
        else:
            string = '全部本金'
    
        #if df['兑付类型'].str.contains('付息').any():
        if df['百元兑付利息'].sum()>0:
            string += '+付息'
    else:
        if df['兑付类型'].str.contains('付息').any():
            if df.loc[df['兑付类型']=='付息','百元兑付利息'].sum()<df.loc[df['兑付类型']=='付息','当期票息'].sum():
                string = '部分付息'
            else:
                string = '全部付息'
    result_df = pd.DataFrame(columns= ['兑付状况'])
    result_df.loc[0, :] = [string]
    return result_df

def default(df):
    df.loc[df['Name'].isin(all_default['债券简称']),'违约状况'] = '违约'
    df.loc[~df['Name'].isin(all_default['债券简称']),'违约状况'] = '未违约'  
    df.loc[df['Name'].isin(details['债券名称']),'实质违约'] = '实质违约' #包含在违约明细里面的都算作实质违约
    df2 = pd.merge(df,df_type[['债券名称','兑付状况']],left_on='Name',right_on='债券名称',how = 'left')
    df2.loc[df2['EndDate']<='2020-11-30','到期'] = '到期'
    df2.loc[df2['EndDate']>'2020-11-30','到期'] = '未到期'
    df2.loc[(df2['到期']=='到期')&(df2['实质违约']=='实质违约')&(df2['兑付状况'].isna()),'兑付状况'] ='未偿还'
    return df2
'''
#Bond Info
sql="SELECT InnerCode, IssueDate,EndDate,InitialPaymentDate,PMRemark,IntPaymentMethod FROM Bond_BasicInfoN"
print("Processing Bond Info")
bond_info = sql_select(sql)
bond_info.columns = ['InnerCode','IssueDate','EndDate','InitialPaymentDate','PMRemark','IntPaymentMethod']
'''
#Bond Code
sql="SELECT InnerCode, SecuCode, SecuAbbr,ChiName,Issuer,SecuMarket, DelistDate,BondTypeLevel1, BondTypeLevel1Desc FROM Bond_Code"
print("Processing Bond Code")
bond_code = sql_select(sql)
bond_code.columns = ['InnerCode', 'Code', 'Name','Fullname','Issuer','SecuMarket', 'DelistDate','Type1','Type1_Des']
bond_code.loc[bond_code['SecuMarket']==83,'wind_code'] =  bond_code.loc[bond_code['SecuMarket']==83,'Code'] + '.SH'
bond_code.loc[bond_code['SecuMarket']==90,'wind_code'] =  bond_code.loc[bond_code['SecuMarket']==90,'Code'] + '.SZ'
#bond_code.loc[bond_code['SecuMarket']==89,'wind_code'] =  bond_code.loc[bond_code['SecuMarket']==89,'Code'] + '.IB'
#bond_code1 = bond_code[bond_code['Issuer'].isin(signal['Issuer'])]
bond_code2 = pd.merge(bond_code,signal,on='Issuer',how = 'inner')
bond_code2 = bond_code2[~(bond_code2['DelistDate']<bond_code2['信号出现日期'])]
bond_code3 = pd.merge(bond_code2,wind_code,on='Name',how = 'left').reset_index(drop=True)
bond_code3['wind_code_x'] = bond_code3['wind_code_x'].fillna(bond_code3['wind_code_y'])
'''
#Cash Flow---数据不全
sql="SELECT B_INFO_WINDCODE, B_INFO_PAYMENTDATE, B_INFO_PAYMENTINTEREST,B_INFO_PAYMENTPARVALUE,B_INFO_PAYMENTSUM FROM CBONDACTUALCF"
print("Processing Bond Cash Flow")
df_cash = quant_china(sql)
df_cash.columns = ['wind_code', 'pay_date', 'pay_int','pay_par','pay_total']
'''
'''
跑完已经存在xlsx里面
df_cash = pd.DataFrame([])
for i in range(len(bond_code3)):
    print(i,'/',len(bond_code3)-1)
    
    df = wind(bond_code3['wind_code_x'][i])
    df['Issuer'] = bond_code3['Issuer'][i]
    df_cash = pd.concat([df_cash,df])

'''
df_cash1 = pd.merge(df_cash,signal,on='Issuer',how = 'left')
df_cash1 = pd.merge(df_cash1,wind_code,on='wind_code',how='left')

df_int = df_cash1[df_cash1['cash_flows_date']>=df_cash1['信号出现日期']].sort_values(by=['Issuer','cash_flows_date'])
#第一次付息
df_int1 = df_int.drop_duplicates(subset=['Issuer'],keep='first').reset_index(drop=True)
#第一次还本(还本付息有可能是同一个)
df_face = df_int[df_int['cf_type']=='兑付']
df_face1 = df_face.drop_duplicates(subset=['Issuer'],keep='first').reset_index(drop=True)




'''

bond_info1 = bond_info[bond_info['InnerCode'].isin(bond_code1['InnerCode'])]
bond_info2 = pd.merge(bond_info,bond_code1,on='InnerCode',how='inner')

#第一次付息
#df_int = bond_info2[(bond_info2['IntPaymentMethod']!=3)& ~(bond_info2['IntPaymentMethod'].isna())] # 1-每年付息，2-半年付息，3-到期一次还本付息，4-按季付息，5-按月付息
df_int = bond_info2[~(bond_info2['IntPaymentMethod'].isna())] 
df_int = df_int.sort_values(by=['Issuer','InitialPaymentDate'])
df_int1 = df_int.drop_duplicates(subset=['Issuer'],keep='first')
notin =signal[~(signal['Issuer'].isin(df_int1['Issuer']))].reset_index(drop=True)

#第一次还本(还本付息有可能是同一个)
#df_face = bond_info2[~bond_info2['InnerCode'].isin(df_int['InnerCode'])] 
df_face = bond_info2
df_face = df_face.sort_values(by=['Issuer','EndDate'],ascending=True)
df_face1 = df_face.drop_duplicates(subset=['Issuer'],keep='first')
notinf =signal[~(signal['Issuer'].isin(df_face1['Issuer']))].reset_index(drop=True)

test1 = bond_info2[bond_info2['Issuer']==notinf['Issuer'][0]]

#还本付息同一个
same = df_int1[df_int1['InnerCode'].isin(df_face1['InnerCode'])]
'''
#违约状况
all_default = pd.read_excel('../data/债券违约大全(20140101-20201119).xls').sort_values(by=['债券简称','违约日期'],ascending=True).reset_index(drop=True)
actual_default = all_default[all_default['最新状态']=='实质违约']
details = pd.read_excel('../data/违约债券兑付明细.xlsx').sort_values(by=['债券名称','付息日'],ascending=True).reset_index(drop=True)
all_default['债券名称'] = all_default['债券简称'] 
details = pd.merge(details,all_default[['债券名称','违约日期']].drop_duplicates(subset='债券名称',keep='first'),on='债券名称',how='left')
details['违约日期'] = pd.to_datetime(details['违约日期'])
details1 = details[details['付息日']>details['违约日期']]
df_type = details1.groupby(by='债券名称').apply(type).reset_index()
#违约后开始兑付时间
df_pay= details1.sort_values(by=['债券名称','付息日']).drop_duplicates(subset='债券名称',keep='first')
df_pay['违约后兑付间隔'] = df_pay['付息日'] - df_pay['违约日期']

#第一次还本付息违约的issuer
int1_default = df_int1[df_int1['Name'].isin(all_default['债券简称'])]
face1_default = df_face1[df_face1['Name'].isin(all_default['债券简称'])]
select_default = pd.concat([int1_default,face1_default])
#第二个还本的债券
df_face2 = df_face[(~df_face['wind_code'].isin(df_face1['wind_code']))].sort_values(by=['Issuer','cash_flows_date'],ascending=True)
df_face2 = df_face2.drop_duplicates(subset=['Issuer'],keep='first')
notinf2 =select_default[['Issuer']].drop_duplicates()[~(select_default[['Issuer']].drop_duplicates()['Issuer'].isin(df_face2['Issuer']))].reset_index(drop=True)



'''df_int1.loc[df_int1['Name'].isin(all_default['债券简称']),'违约状况'] = '违约'
df_int1.loc[df_int1['Name'].isin(details['债券名称']),'实质违约'] = '实质违约' #包含在违约明细里面的都算作实质违约
df_int2 = pd.merge(df_int1,df_type[['债券名称','兑付状况']],left_on='Name',right_on='债券名称',how = 'left')
df_int2.loc[df_int2['EndDate']<='2020-11-30','到期'] = '到期'
df_int2.loc[df_int2['EndDate']>'2020-11-30','到期'] = '未到期'
df_int2.loc[(df_int2['到期']=='到期')&(df_int2['实质违约']=='实质违约')&(df_int2['兑付状况'].isna()),'兑付状况'] ='未偿还'''
#统计兑付状况
df_int1_2 = default(df_int1)
df_face1_2 = default(df_face1)
df_face2_2 = default(df_face2)

#df = signal[['Issuer']]
df = df_cash['Issuer'].drop_duplicates().reset_index(drop=True)
df = pd.merge(df,df_int1_2[['Issuer','Name','违约状况','实质违约','兑付状况','到期']],on='Issuer',how='left')
df = pd.merge(df,df_face1_2[['Issuer','Name','违约状况','实质违约','兑付状况','到期']],on='Issuer',how='left')
df = pd.merge(df,df_face2_2[['Issuer','Name','违约状况','实质违约','兑付状况','到期']],on='Issuer',how='left')
df = df.dropna(subset = ['Name_x']).reset_index(drop=True)
df.columns = ['Issuer','Name(付息1)','违约状况(付息1)','实质违约(付息1)','兑付状况(付息1)','到期(付息1)','Name(还本1)','违约状况(还本1)','实质违约(还本1)','兑付状况(还本1)','到期(还本1)','Name(还本2)','违约状况(还本2)','实质违约(还本2)','兑付状况(还本2)','到期(还本2)']
df.to_excel('../result/兑付比例_价格信号出现后v2.xlsx')


