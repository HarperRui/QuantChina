import pandas as pd
import datetime as dt
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
    if df['债券余额(亿元)'].min()==0:
        string = '全部本金'
        if df['付息(万元)'].any()>0:
            string += '+付息'

    else:
        if df['兑付本金(亿元)'].sum()>0:
            string = '部分本金'
            if df['付息(万元)'].any()>0:
                string += '+付息'
        else:
            if df['付息(万元)'].sum()>0:
                string = '仅付息'
                
    day1 = df['逾期兑付天数'].min()
    day2 = df['逾期兑付天数'].max()     
        
    result_df = pd.DataFrame(columns= ['兑付状况','逾期兑付天数（首次兑付）','逾期兑付天数（最后兑付）'])
    result_df.loc[0, '兑付状况'] = string
    result_df.loc[0, '逾期兑付天数（首次兑付）'] = day1
    result_df.loc[0, '逾期兑付天数（最后兑付）'] = day2
    
    return result_df

def default(df):
    df.loc[df['Name'].isin(all_default['债券简称']),'违约状况'] = '违约'
    df.loc[~df['Name'].isin(all_default['债券简称']),'违约状况'] = '未违约'  
    df.loc[df['Name'].isin(all_default[~all_default['最新状态'].isin(['展期','触发交叉条款'])]['债券简称']),'实质违约'] = '实质违约' #包含在违约明细里面的都算作实质违约
    df2 = pd.merge(df,df_type,left_on='Name',right_on='债券简称',how = 'left')
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




#违约状况 + 违约后开始兑付时间
all_default = pd.read_excel('../data/债券违约大全(20140101-20201119).xls').sort_values(by=['债券简称','违约日期'],ascending=True).reset_index(drop=True)
actual_default = all_default[all_default['最新状态']=='实质违约']
details = pd.read_excel('../data/兑付明细.xlsx').sort_values(by=['债券简称','兑付日'],ascending=True).reset_index(drop=True)
details['逾期兑付天数'] = details['兑付日']-details['首次违约日']
details1 = details[details['逾期兑付天数'].dt.days>=0] 
details1 = details1.replace('--',np.nan)
#details3 = pd.merge(details2,all_default[['债券简称','违约日期']].drop_duplicates(subset='债券简称',keep='first'),on='债券简称',how='left')
df_type = details1.groupby(by='债券简称').apply(type).reset_index()


#第一次还本付息违约的issuer
int1_default = df_int1[df_int1['Name'].isin(all_default['债券简称'])]
face1_default = df_face1[df_face1['Name'].isin(all_default['债券简称'])]
select_default = pd.concat([int1_default,face1_default])
#第二个还本的债券
df_face2 = df_face[(~df_face['wind_code'].isin(df_face1['wind_code']))].sort_values(by=['Issuer','cash_flows_date'],ascending=True)
df_face2 = df_face2.drop_duplicates(subset=['Issuer'],keep='first')
notinf2 =select_default[['Issuer']].drop_duplicates()[~(select_default[['Issuer']].drop_duplicates()['Issuer'].isin(df_face2['Issuer']))].reset_index(drop=True)



#统计兑付状况
df_int1_2 = default(df_int1)
df_face1_2 = default(df_face1)
df_face2_2 = default(df_face2)

#df = signal[['Issuer']]
df = df_cash['Issuer'].drop_duplicates().reset_index(drop=True)
df = pd.merge(df,df_int1_2[['Issuer','Name','违约状况','实质违约','兑付状况','到期','逾期兑付天数（首次兑付）','逾期兑付天数（最后兑付）']],on='Issuer',how='left')
df = pd.merge(df,df_face1_2[['Issuer','Name','违约状况','实质违约','兑付状况','到期','逾期兑付天数（首次兑付）','逾期兑付天数（最后兑付）']],on='Issuer',how='left')
df = pd.merge(df,df_face2_2[['Issuer','Name','违约状况','实质违约','兑付状况','到期','逾期兑付天数（首次兑付）','逾期兑付天数（最后兑付）']],on='Issuer',how='left')
df = df.dropna(subset = ['Name_x']).reset_index(drop=True)
df.columns = ['Issuer','Name(付息1)','违约状况(付息1)','实质违约(付息1)','兑付状况(付息1)','到期(付息1)','逾期兑付天数（首次兑付）','逾期兑付天数（最后兑付）','Name(还本1)','违约状况(还本1)','实质违约(还本1)','兑付状况(还本1)','到期(还本1)','逾期兑付天数（首次兑付）','逾期兑付天数（最后兑付）','Name(还本2)','违约状况(还本2)','实质违约(还本2)','兑付状况(还本2)','到期(还本2)','逾期兑付天数（首次兑付）','逾期兑付天数（最后兑付）']
df.to_excel('../result/兑付比例_价格信号出现后v2.xlsx')


