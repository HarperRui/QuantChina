import pandas as pd
import datetime
import numpy as np

def type(df):
    string=np.nan
    if df['兑付类型'].str.contains('还本').any():
        if df.loc[df['兑付类型']=='还本','百元兑付本金'].sum()<100:
            string = '部分本金'
        else:
            string = '全部本金'
    
        if df['兑付类型'].str.contains('付息').any():
            string += '+付息'
    else:
        if df['兑付类型'].str.contains('付息').any():
            string = '付息'
    return string
    
def count_num(df):
    all_num = len(df[df['Name'].isin(details1['债券名称'])])
    inter = len(df[df['兑付类型']=='付息'])
    partial = len(df[df['兑付类型']=='部分本金+付息'])
    total = len(df[df['兑付类型']=='全部本金+付息'])
    result_df = pd.DataFrame(columns= ["到期债数","付息", "部分本金+付息", "全部本金+付息"])
    result_df.loc[0, :] = [all_num,inter, partial, total]
    return result_df


df_yield = pd.read_excel('../data/result_yield.xlsx').drop_duplicates()
#df_yield = df_yield[df_yield['DelistDate']<'2020-11-20']
all_default = pd.read_excel('../data/债券违约大全(20140101-20201119).xls').sort_values(by=['债券简称','违约日期']).drop_duplicates(subset=['债券简称'],keep='last')
details = pd.read_excel('../data/违约债券兑付明细.xlsx')
default = pd.read_excel('../data/违约债券报表.xlsx')

#diff = default[~default['名称'].isin(all_default['债券简称'])]
#diff2 = all_default[~all_default['债券简称'].isin(default['名称'])]

#df = pd.DataFrame(columns={'发行主体'})
#df['发行主体'] = df_yield['Issuer'].drop_duplicates().reset_index(drop=True)

df1= pd.merge(df_yield[['Name','Issuer']],all_default[['债券简称','发行人','最新状态','企业性质']],left_on='Name',right_on='债券简称',how ='left')

df=df1[['Issuer','企业性质']].dropna().drop_duplicates(subset=['Issuer'],keep='first')


df2 = df1.groupby(by=['Issuer']).count().reset_index()
df2['违约比例'] = df2['债券简称']/df2['Name']
df2['违约总债数'] = df2['债券简称']
df3 = df1.groupby(by=['Issuer','最新状态']).count().reset_index()
#df3.loc[df3['最新状态']=='实质违约','实质违约比例']=df3.loc[df3['最新状态']=='实质违约','债券简称']/df3.loc[df3['最新状态']=='实质违约','Name']
df3.loc[df3['最新状态']=='实质违约','实质违约数量']=df3.loc[df3['最新状态']=='实质违约','Name']

df4 = pd.merge(df2,df3[df3['最新状态']=='实质违约'][['Issuer', '实质违约数量']],on='Issuer',how='left')
df4['实质违约比例'] = df4['实质违约数量']/df4['债券简称']
df4 ['总发债数'] = df4['Name']

dfd = df1[df1['最新状态']=='实质违约']
details1 = details[(details['债券名称'].isin(dfd['Name'])) & (details['到期日期']<'2020-11-27')]

df_details = pd.merge(df_yield[['Name','Issuer']],details1[['债券名称','兑付类型','百元兑付本金']],left_on='Name',right_on='债券名称',how ='left')
df_type = pd.DataFrame(df_details.groupby(by='Name').apply(type))
df_type.columns=['兑付类型']
df_type1 = pd.merge(df_yield[['Name','Issuer']],df_type,left_on='Name',right_index=True,how ='left')

df_type2 = df_type1.groupby(by=['Issuer']).apply(count_num).reset_index()
df=pd.merge(df_type2[["Issuer","到期债数","付息", "部分本金+付息", "全部本金+付息"]],df4[['Issuer','总发债数','违约总债数','实质违约数量']],on='Issuer',how='left')
df.loc[(df['实质违约数量']>0)&(df['到期债数']>0),'未偿还'] = df.loc[(df['实质违约数量']>0)&(df['到期债数']>0),'到期债数']-df.loc[(df['实质违约数量']>0)&(df['到期债数']>0),'付息']-df.loc[(df['实质违约数量']>0)&(df['到期债数']>0),'部分本金+付息']-df.loc[(df['实质违约数量']>0)&(df['到期债数']>0),'全部本金+付息']
df=df[["Issuer","总发债数",'违约总债数','实质违约数量',"到期债数","未偿还","付息", "部分本金+付息", "全部本金+付息"]]
df['违约比例'] = df['违约总债数']/df['总发债数']
df['实质违约比例'] = df['实质违约数量']/df['违约总债数']
df.loc[df['到期债数']>0,'未偿还比例'] = df.loc[df['到期债数']>0,'未偿还']/df.loc[df['到期债数']>0,'到期债数']
df.loc[df['到期债数']>0,'付息比例'] = df.loc[df['到期债数']>0,'付息']/df.loc[df['到期债数']>0,'到期债数']
df.loc[df['到期债数']>0,'部分还本比例'] = df.loc[df['到期债数']>0,'部分本金+付息']/df.loc[df['到期债数']>0,'到期债数']
df.loc[df['到期债数']>0,'全部本金+付息比例'] = df.loc[df['到期债数']>0,'全部本金+付息']/df.loc[df['到期债数']>0,'到期债数']
#df.loc[df['实质违约数量']>0,'未偿还比例'] = 1-df['付息比例']-df['部分还本比例']-df['全部还本比例']
df=df.fillna(0)
df.to_excel('../result/兑付比例v2.xlsx',index=False)


