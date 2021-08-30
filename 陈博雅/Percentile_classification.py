# -*- coding: utf-8 -*-
# @Time    : 2021-07-19 11:10 a.m.
# @Author  : cindy
# @File    : Percentile_classification.py
# @Software: PyCharm

import numpy as np
import pandas as pd

from zltsql import SQLConn
from HV_Percentile import get_percentile_data

#%% 一次性计算分类用方程


def classification(series):
    """
    分类一组分位数
    :param series: series
    :return: int
    """
    ref_val = series[0]
    if ref_val != ref_val:
        return np.nan
    ranges = series[0:(len(series) + 1)]
    if any(np.array(ranges) != np.array(ranges)):
        return np.nan
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


def classify(reference, percentiles):
    """
    分类分位数
    :param reference:pandas.Dataframe
    :param percentiles: pandas.Dateframe
    :return: pandas.Dataframe
    """
    pct_level = (np.array(range(11)) * 10).astype(str).tolist()
    # 处理一个类别的
    # test = percentile_classification(reference["HV5"], percentiles.loc[:, 'HV5_0':'HV5_100'])
    # Initialize the output dataframe
    class_df = pd.DataFrame(columns=reference.columns)
    class_df["Date"] = reference.index
    class_df = class_df.set_index('Date')
    for i in range(len(reference.columns)):
        if reference.columns[i] in ['iv', 'iv_insert']:  # iv, vix 以HV20为分位数画档
            colname = ['HV20' + '_' + pct_level[j] for j in range(len(pct_level))]
        else:
            colname = [reference.columns[i] + '_' + pct_level[j] for j in range(len(pct_level))]
        class_df[reference.columns[i]] = percentile_classification(reference[reference.columns[i]],
                                                                   percentiles.loc[:, colname])
    print("全数据分类完成")
    return class_df

#%% 更新用方程


def updated_pct(updated_ref, start_date):
    """
    根据新的数据 updated_ref，更新去除上下5%极值的percentile数据。
    开始日期为 start_date, 一直更新到 update_ref 中最后一个日期
    如果其中几组数据在日期上领先其他数据，则领先日期中其他数据设为nan
    :param updated_ref: pandas.Dataframe
    :param start_date: datetime64
    :return:pandas.Dataframe
    """
    new_pct = pd.DataFrame(index=pd.date_range(start_date, updated_ref.index[-1]))
    day_0 = updated_ref.index[0]
    percentiles = np.arange(0, 110, 10)

    def get_end_date(values):
        """
        return the index just before the index (int) of the first nan value
        :param values: array or pandas.series
        :return: int
        """
        end_val = len(values) - len(values[values.isna()])
        return values.reset_index()['日期'].iloc[end_val - 1]

    def get_new_pct(values, ref_day):
        """
        return a dataframe of newly calculated percentiles from start date to end date
        :param ref_day: the date for which we start doing percentile calculations
        :param values: array or pandas.series
        :return: pandas.dataframe
        """
        colnames = [values.name + '_' + percentiles.astype(str)[i] for i in range(len(percentiles))]
        end_date = get_end_date(values)
        if values.name.startswith('HV'):
            ref_day = hv_ref[values.name].dropna().index[0]
            values = hv_ref[values.name]
        new_dt_range = pd.date_range(start_date, end_date)
        temp_df = pd.DataFrame(index=new_dt_range, columns=colnames)
        for day in new_dt_range:
            new_row = get_percentile_data(values[ref_day:day], percentiles)
            temp_df.loc[day] = new_row
        return temp_df

    print('开始计算新分位数数据')
    for cols in updated_ref.columns:
        cols_def = get_new_pct(updated_ref[cols], day_0)
        new_pct = new_pct.merge(cols_def, how='left', right_index=True, left_index=True)
    print('新分位数数据计算完成')
    return new_pct


def update_percentile_classification(updated_ref, current_class, write=True):
    """
    根据新的分位数数据, updated_ref， 去更新已经完成分类的分位数表格，current_class,
    并且把更新后的数据写出
    :param write: bool, default=True. If False, 则不写更新好的数据
    :param updated_ref: pandas.Dataframe
    :param current_class: pandas.Dataframe
    :return: pandas.Dataframe
    """
    print('开始更新')
    # 如果之前分好类的分位数里面有还未计算的数值(i.e. nan)，则当天分类在更新时重新计算
    # 反之则从最后一天的下一个开盘日开始计算
    if current_class.isna().sum().sum() == 0:
        last_date = current_class.reset_index().index[-1]
        last_date = updated_ref.index[last_date + 1]
    else:
        last_date = current_class.isna().agg(sum, axis=1)
        last_date = last_date[last_date != 0].index[0]

    # 根据最后一天的类别情报，提取还未分类的数值，与计算新的percentile
    new_ref = updated_ref.reset_index().rename(columns={'日期': 'Date'}).query('Date >= @last_date').set_index('Date')
    new_pct = updated_pct(updated_ref, last_date)
    updated_class = classify(new_ref, new_pct)
    new_class = current_class.append(updated_class.reset_index()).set_index('Date').sort_index().dropna(how='all')\
        .reset_index()
    print('更新数据合并完成')

    for i in range(1, 8):
        new_class[new_class.columns[i]] = new_class[new_class.columns[i]].astype(float)

    if write:
        new_class.to_excel('Percentile_Classified_50etf_hv20画档_0728.xlsx', index=False)
    return new_class

#%% Main


if __name__ == '__main__':
    """
    获取hv,iv, iv_insert 的原始数据当作reference -- ref
    获取hv,iv, iv_insert 分位数数据当作分类间隔 -- pct
    根据ref与pct做分类，获得每天分位数分类好的数据 -- classified_ref
    将分类好的数据 write xlxs 文件 -- classified_df
    或者更新之前分类好的数据 -- updated
    """
    # 获取数据
    conn = SQLConn()
    hv_ref = conn.GetData('HV_percentile').loc[:, 'Date':'HV60']
    hv_ref = hv_ref[hv_ref['Code'] == "510050.SH"].drop('Code', axis=1).sort_values(by='Date').set_index('Date')
    iv_ref = conn.GetData('df_vol_50etf').loc[:, '日期':'iVIX'].sort_values(by='日期').set_index('日期')
    conn.CloseSql()
    iv_ins_ref = pd.read_excel("iv_insert_50etf.xlsx", index_col='Date', parse_dates=True)
    # 合并reference df并删除subpart
    ref = pd.merge(hv_ref, iv_ref, how='right', left_index=True, right_index=True)
    ref = pd.merge(ref, iv_ins_ref, how='left', left_index=True, right_index=True).rename(columns={'iVIX': 'iv'})
    ref = ref[ref.columns[[6, 5, 0, 1, 2, 3, 4]]]
    del [iv_ref, iv_ins_ref]
    pct = pd.read_excel("all_percentile_50etf(2)(1).xlsx", index_col='Date', parse_dates=True)
    print('数据读取完成')

    # 给数据分类
    classified_df = classify(ref, pct)
    classified_df = classified_df.reset_index().dropna()
    classified_df["Date"].astype(str)
    for i in range(1, 8):
        classified_df[classified_df.columns[i]] = classified_df[classified_df.columns[i]].astype(float)

    # 更新数据
    updated = update_percentile_classification(ref, classified_df, write=False)

    # Write Date
    # updated.to_excel('Percentile_Classified_50etf_hv20画档_0728.xlsx', index=False)
