# -*- coding: utf-8 -*-
# @Time    : 2021/6/29 9:51
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : HV_Percentile.py
# @Software: PyCharm

import numpy as np
import pandas as pd
from zltsql import SQLConn

#%%
def historical_percentile(data, code):
    data = data[data['Code'] == code].reset_index(drop=True)

    def get_percentile_data(values):
    # if len(values) <= 2:
        #     return values.min(), np.percentile(values, 25), np.percentile(values, 50), np.percentile(values, 75), values.max()
        last_5 = np.percentile(values.dropna(), 5)
        top_5 = np.percentile(values.dropna(), 95)
        #exclude top5 and last5
        values1 = values[(values > last_5) & (values < top_5)]
        if len(values1) == 0:
            return values.min(), np.percentile(values, 10), np.percentile(values, 25), np.percentile(values, 50), np.percentile(values, 75), values.max()
        else:    
            values = values1[(values1 > last_5) & (values1 < top_5)]
            _min = values1.min()
            _max = values1.max()
            _25 = np.percentile(values1, 25)
            _50 = np.percentile(values1, 50)
            _75 = np.percentile(values1, 75)
            return _min, _25, _50, _75, _max

    res = pd.DataFrame(data[['Date', 'Code']])
    hv_types = [x for x in data.columns if x not in ['Date', 'Code']]
    for i in hv_types:
        for idx in range(len(res)):
            _values = data[i][0:idx+1].copy()
            percentile_data = get_percentile_data(_values)
            res.at[idx, '%s_min' % i] = percentile_data[0]
            res.at[idx, '%s_25' % i] = percentile_data[1]
            res.at[idx, '%s_50' % i] = percentile_data[2]
            res.at[idx, '%s_75' % i] = percentile_data[3]
            res.at[idx, '%s_max' % i] = percentile_data[4]
    return res

def get_percentile_data(values, percentiles, eliminate_extremum=True):
    """
    获取values中的指定分位数，可选择是否剔除大于95%和小于5%的极值
    :param values: array or series
    :param percentiles: list, array or series
    :param eliminate_extremum: whether eliminate extreme values (top and low 5% values)
    :return: list that contains percentile values
    """

    if len(values) == 0:
        return [np.nan]

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

#%%
if __name__ == '__main__':
    '''
    #锦文原代码
    SQ = SQLConn()
    #df_hv = SQ.GetData('HV').dropna().reset_index(drop=True)
    df_hv = SQ.GetData('HV_percentile').dropna().reset_index(drop=True)
    SQ.CloseSql()
    
    percentile_50 = historical_percentile(df_hv, '510050.SH')
    percentile_300 = historical_percentile(df_hv, '510300.SH')
    df_percentile = pd.concat([percentile_50, percentile_300]).sort_values('Date').reset_index(drop=True)
    '''
    #锦文基础上跑了所有的分位数
    import datetime
    SQ = SQLConn()
    df_iv = SQ.GetData('df_vol_50etf').sort_values(by='日期').reset_index(drop=True)
    df_hv = SQ.GetData('HV_percentile').sort_values(by='Date').reset_index(drop=True)
    SQ.CloseSql()
    df_iv['Date'] = df_iv['日期'].dt.strftime('%Y%m%d')
    df_hv['Date'] = df_hv['Date'].dt.strftime('%Y%m%d')
    df_iv_insert = pd.read_excel('D:/Harper/实习文件整理_张依依/HV_percentile/iv_insert_50etf_0728.xlsx')
    df_iv_insert['Date'] = df_iv_insert['Date'].astype('str')
    df_result = pd.DataFrame({'Date':df_iv['Date']})
    percentile_lst = np.arange(0,110,10).tolist()
    for i in range(len(df_result)):
        date_today = df_result.at[i, 'Date']
        print(date_today)
        for kind in ['iv_insert','iv','HV5','HV10','HV20','HV40','HV60']:
            if kind == 'iv_insert':
                df = df_iv_insert
                col = kind
            elif kind == 'iv':
                df = df_iv
                col = 'iVIX'
            else:
                df = df_hv.loc[df_hv['Code']=='510050.SH']
                col = kind
            for p in percentile_lst: 
                df_result.at[i, '%s_%s'%(kind,p)] = get_percentile_data(df.loc[(df['Date'] <= date_today), col].copy().dropna(), [p])[0]
           
    df_result.to_excel("D:/Harper/实习文件整理_张依依/HV_percentile/all_percentile_50etf_0728.xlsx",index=False)        
           

    