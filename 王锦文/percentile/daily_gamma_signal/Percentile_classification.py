import numpy as np
import pandas as pd

from zltsql import SQLConn


def classification(series):
    """
    分类一组分位数
    :param series: series
    :return: int
    """
    ref_val = series[0]
    ranges = series[0:(len(series) + 1)]
    cond_ls = [ranges[index] <= ref_val < ranges[index + 1] for index in range(1, len(ranges) - 1)]
    cond_ls.append(ref_val >= ranges[len(ranges) - 1])
    choice_ls = np.array(range(11)) * 10
    typ = np.select(cond_ls, choice_ls.tolist())
    return typ


def percentile_classification(values, percentiles):
    """
    分类一个波动率的每天的分位数
    :param values: pandas.Dataframe
    :param percentiles: pandas.Dataframe
    :return: array or series
    """
    df = pd.merge(values, percentiles, how="right", left_index=True, right_index=True)
    df_classed = df.apply(classification, axis=1)
    print(f'{df.columns[0]} 完成')
    return df_classed

#%%

if __name__ == '__main__':
    """
    获取hv,iv, iv_insert 的原始数据当作reference -- ref
    获取hv,iv, iv_insert 分位数数据当作分类间隔 -- pct
    根据ref与pct做分类，获得每天分位数分类好的数据 -- classified_ref
    将分类好的数据 write xlxs 文件
    """
    # 获取数据
    conn = SQLConn()
    hv_ref = conn.GetData('HV_percentile').loc[:, 'Date':'HV60']
    hv_ref = hv_ref[hv_ref['Code'] == "510050.SH"].drop('Code', axis=1).sort_values(by='Date').set_index('Date')
    iv_ref = conn.GetData('df_vol_50etf').loc[:, '日期':'iVIX'].sort_values(by='日期').set_index('日期')
    conn.CloseSql()
    iv_ins_ref = pd.read_excel('D:/Harper/实习文件整理_张依依/HV_percentile/iv_insert_50etf_0728.xlsx', index_col='Date', parse_dates=True)
    # 合并reference df并删除subpart
    ref = pd.merge(hv_ref, iv_ref, how='right', left_index=True, right_index=True)
    ref = pd.merge(ref, iv_ins_ref, how='left', left_index=True, right_index=True).rename(columns={'iVIX': 'iv'})
    ref = ref[ref.columns[[6, 5, 0, 1, 2, 3, 4]]]
    del [hv_ref, iv_ref, iv_ins_ref]
    pct = pd.read_excel("D:/Harper/实习文件整理_张依依/HV_percentile/all_percentile_50etf_0728.xlsx", index_col='Date', parse_dates=True)
    print('数据读取完成')

    pct_level = (np.array(range(11)) * 10).astype(str).tolist()

    # 处理一个类别的
    # test = percentile_classification(ref["HV5"], pct.loc[:, 'HV5_0':'HV5_100'])

    # Initialize the output dataframe
    classified_df = pd.DataFrame(columns=ref.columns)
    classified_df["Date"] = ref.index
    classified_df = classified_df.set_index('Date')

    for i in range(len(ref.columns)):
        if ref.columns[i] in ['iv','iv_insert']: #iv, vix 以HV20为分位数画档
            colname = ['HV20'+ '_' + pct_level[j] for j in range(len(pct_level))]
        else:
            colname = [ref.columns[i] + '_' + pct_level[j] for j in range(len(pct_level))]
        classified_df[ref.columns[i]] = percentile_classification(ref[ref.columns[i]], pct.loc[:, colname])
    print("全数据分类完成")

    # Write Data

    classified_df = classified_df.reset_index().dropna()
    classified_df["Date"].astype(str)
    for i in range(1, 8):
        classified_df[classified_df.columns[i]] = classified_df[classified_df.columns[i]].astype(float)


    classified_df.to_excel('D:/Harper/实习文件整理_张依依/HV_percentile/Percentile_Classified_50etf_hv20画档_0728.xlsx', index=False)
