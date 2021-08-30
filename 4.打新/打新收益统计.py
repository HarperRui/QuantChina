import pymysql
import pandas as pd
import numpy as np
def connect_mysql(host, port, username, password, db_name, charset):
    conn = pymysql.Connect(host = host,
                           port = port,
                           user = username,
                           password = password,
                           db = db_name,
                           charset = charset
                        )
    return conn

if __name__ == '__main__':
    
    df_new = pd.read_excel('./新股发行资料1230.xlsx')
    df_mkt1 = pd.read_excel('./新股上市后市场表现1230.xlsx','首日')
    df_mkt6 = pd.read_excel('./新股上市后市场表现1230.xlsx','上市6个月')
    
    
    
    df = pd.merge(df_new[['代码','名称','上市日期','上市板','发行价格']],df_mkt1[['代码','上市天数(交易日)','涨跌幅(%)']],on='代码').dropna().sort_values(by=['上市日期']).reset_index(drop=True)
    
    
    from_date='20200101'
    to_date='20201230'

    conn = connect_mysql(host='database.quantchina.info', port = 3306,username='quantchina', password='zMxq7VNYJljTFIQ8',
                         db_name='wind', charset='gbk')
    with conn.cursor() as cursor:
 
        print('processing 取获配信息')
        string_code=" "
        for i in range(len(df)):
            if i==len(df)-1:
                tmp_string='\''''+df['代码'][i]+'\''
            else:
                tmp_string='\''+df['代码'][i]+'\','
            string_code=string_code+tmp_string
        string_code="("+string_code+")"
   
        sql="SELECT S_INFO_WINDCODE,S_HOLDER_NAME,S_HOLDER_TYPE,TYPEOFINVESTOR,PLACEMENT,TRADE_DT,LOCKMONTH,TRADABLE_DT FROM `ASHAREPLACEMENTDETAILS` " \
    "WHERE S_INFO_WINDCODE IN %s and TRADABLE_DT<=%s"% (string_code,to_date)
        cursor.execute(sql)
        holder=cursor.fetchall()
        df_holder = pd.DataFrame(list(holder))
        df_holder.columns = ['代码', '持有人名称','股东类型','法人投资类型','获配数量(股)','截止日期','锁定期(月)','可流通日期']

        
        
        df1 = pd.merge(df_holder,df,on='代码')
        df1['月份'] = df1['可流通日期'].str[:6]
        df1['首日收益'] = 0
        df1['锁定期收益'] = 0
        df1.loc[df1['锁定期(月)']==0,'首日收益'] = (df1['获配数量(股)'].astype(float))*df1['发行价格']*df1['涨跌幅(%)']/100
        
        

        df2 = pd.merge(df1,df_mkt6[['代码','收盘价(后复权)']],on='代码',how = "left")
        df2.loc[df2['锁定期(月)']>0,'锁定期收益'] = (df2['获配数量(股)'].astype(float))*(df2['收盘价(后复权)'].astype(float) - df2['发行价格'])
        df2['打新收益'] = df2['首日收益'] + df2['锁定期收益']
        
        df_sum =df2.groupby(['持有人名称','月份'])['打新收益'].sum()
        
        c = df_holder[['持有人名称','股东类型','法人投资类型','可流通日期']]
        c= c[c['法人投资类型'] =='C类法人投资者'].drop_duplicates()
        name = pd.DataFrame(c.loc[c['法人投资类型'] =='C类法人投资者','持有人名称'].drop_duplicates().reset_index(drop=True))
        name['股东类型'] = np.nan
        name['法人投资类型'] = np.nan
        
        
        for i in range(len(name)):
            print(i,'/',len(name)-1)
            test = c.loc[c['持有人名称'] == name['持有人名称'][i],:].drop_duplicates()
            if len(test) ==1:
                name.股东类型[i] = test['股东类型'].values[0]
                name.法人投资类型[i] = test['法人投资类型'].values[0]
            else:
                test = test.sort_values(by=['可流通日期'],ascending=False).reset_index(drop = True)
                name.股东类型[i] = test['股东类型'].values[0]
                name.法人投资类型[i] = test['法人投资类型'].values[0]
                
            
        
        df_sum1 = pd.DataFrame(df_sum)
        df_sum1 = df_sum1.reset_index()
        df_end = pd.merge(df_sum1,name,on ='持有人名称')
        
        df_end.to_csv('./打新收益1230.csv',encoding='utf_8_sig')
        
        
        
        
       #df_sum =df2.groupby(['持有人名称']).agg({"打新收益"："sum"})
        
        
    
        
        





