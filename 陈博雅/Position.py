# -*- coding: utf-8 -*-
# @Time    : 2021-07-20 4:17 p.m.
# @Author  : cindy
# @File    : Position.py
# @Software: PyCharm

import numpy as np
import pandas as pd
import chinese_calendar as cal
import datetime as dt
import zltsql as sql


def is_trade_day(date):
    """
    判断一个日期是不是中国的交易日
    :param date: datetime
    :return: boolean
    """
    return (not cal.is_holiday(date)) & (not cal.is_in_lieu(date)) & (date.weekday() < 5)


def get_next_trading_day(date):
    """
    求出下一个交易日日期
    :param date: datetime
    :return: datetime
    """
    next_date = date + dt.timedelta(days=1)
    while not is_trade_day(next_date):
        next_date = next_date + dt.timedelta(days=1)
    return next_date


def divide_code(code_df):
    """
    把数据根据平仓日再一次区分
    如果 code 10000008 有两个平仓日，则根据平仓日分为 10000008_1 与 1000000_2
    :param code_df: pd.Dataframe
    :return: pd.Dataframe
    """
    # 寻找与标记平仓日
    next_trad = code_df['Date'].apply(get_next_trading_day).shift(1)
    code_df = code_df.assign(next_trd=next_trad)
    code_df['next_trd'][code_df.index[0]] = code_df['Date'][code_df.index[0]]
    code_df = code_df.assign(day_ct=np.where(code_df['next_trd'] == code_df['Date'], 0, 1))
    code_df['day_ct'] = code_df['day_ct'].shift(-1)
    code_df['day_ct'].iloc[-1] = 1
    # 区分平仓日
    code_df = code_df.reset_index()
    idx = (code_df[code_df['day_ct'] == 1]).index.tolist()
    differentiate = list(map(str, list(range(1, len(idx) + 1))))
    idx = [-1] + idx
    code = str(code_df['Code'][0])
    for i in range(0, len(differentiate)):
        code_df.loc[(idx[i] + 1):idx[i + 1], 'Code'] = code + '_' + differentiate[i]
    code_df = code_df.set_index('index')
    return code_df


def calculate_changed_no(code_df):
    """
    计算持仓量的变化, 在input df 中增加一列 dltNum
    第一天直接取当天持仓的绝对值
    之后如果数量增加则为买入，数量减少则为卖出
    :param code_df: pandas.dataframe
    :return: pandas.dataframe
    """
    num = code_df['No']
    dlt_num = pd.Series(index=num.index, dtype=int, name='dltNum')
    dlt_num[dlt_num.index[0]] = num.at[num.index[0]]
    for i in range(1, len(num)):
        dlt_num[dlt_num.index[i]] = num[num.index[i]] - num[num.index[i - 1]]
    code_df = code_df.assign(dltNum=dlt_num)
    return code_df


def calculate_cost(code_df, coef_op1=0.5, coef_op2=1.7, coef_stk=0.0004, coef_fut=0.000023, coef_etf=0.00003,
                   coef_idx=0.0007):
    """
    计算手续费，在code_df中增加一列cost
    coef 开头参数皆为手续费系数
    :param code_df: pandas.dataframe
    :param coef_op1: float option 手续费系数
    :param coef_op2: float option 手续费系数, buy/sell 相关
    :param coef_stk: float stock 手续费系数
    :param coef_fut: float futures 手续费系数
    :param coef_etf: float etf 手续费系数
    :param coef_idx: float index 手续费系数
    :return: pandas.dataframe
    """
    # 计算变化的持仓数，加到df最后一列
    code_df = calculate_changed_no(code_df)

    # 识别类型，提取正确的手续费系数
    code_type = code_df['Symbol'][code_df.index[0]]
    cond_ls = [code_type == 'option', code_type == 'stock', code_type == 'futures',
               code_type == 'etf', code_type == 'index']
    choice_ls = [coef_op1, coef_stk, coef_fut, coef_etf, coef_idx]
    coef = np.select(cond_ls, choice_ls)

    # 计算手续费
    cost = code_df['Price_t'] * code_df['dltNum'] * coef
    if coef == coef_op1:
        cost_2 = np.where(code_df['dltNum'] > 0, code_df['dltNum'] * coef_op2, 0)
        cost = cost / code_df['Price_t']
        cost = cost + cost_2
    code_df = code_df.assign(cost=cost)

    # 计算下一个交易日的平仓手续费
    covers = 0 - code_df['No'].iloc[-1]
    cover_cost = covers * coef
    if (coef == coef_op1) & (covers >= 0):
        cover_cost = cover_cost + covers * coef_op2
    covers = pd.Series(0, index=code_df.index)
    covers.iloc[-1] = cover_cost
    code_df = code_df.assign(cover=covers)

    # 计算盈亏
    code_df = calculate_pnl(code_df)
    return code_df


def calculate_pnl(code_df):
    code_df = code_df.assign(pnl=(code_df['Price_t1'] - code_df['Price_t']) * code_df['Num'])
    code_df = code_df.append(code_df.iloc[-1])
    code_df['Date'].iloc[-1] = get_next_trading_day(code_df.iloc[-1]['Date'])
    code_df.iloc[-1, 3:] = 0
    code_df['pnl'] = code_df['pnl'].shift(1).fillna(0)
    return code_df


def calculate_total_cost(pos_cost):
    """
    计算每天的总手续费
    :param pos_cost: pd.dataframe
    :return: pd.series
    """
    pos_cost['abs_cost'] = pos_cost['cost'].abs()
    pos_cost = pos_cost[['Date', 'abs_cost', 'pnl']]
    pos_cost = pos_cost.groupby('Date').sum()
    # tot_cost = pos_cost['abs_cost']
    pos_cost = pos_cost.rename(columns={'pnl': 'Total_pnl_cost'})
    return pos_cost


def date_ct(pos_df):
    """
    把每个code 截至目前为止的最后一天定为平仓日 day_ct == 1, 其他为0
    pos_def 增加一列 day_ct
    :param pos_df: pd.Dataframe
    :return: pd.Dataframe
    """

    def add_ref(df):
        day_ct = pd.Series(0, index=df.index)
        day_ct.iloc[-1] = 1
        df = df.assign(day_ct=day_ct)
        return df

    pos_df = pos_df.groupby('Code').apply(add_ref)
    return pos_df


def calc_total_cost(pos_df):
    # 数据处理
    print('数据导入完成，开始处理')
    pos_df['Code'] = pos_df['Code'].astype(str)
    pos_df = pos_df[['Date', 'Code', 'Symbol', 'Price_t', 'Price_t1', 'Num', 'No']]
    pos_df = date_ct(pos_df)
    pos_df = pos_df.groupby('Code').apply(divide_code)
    print('数据处理完成，开始计算')
    # 数据计算
    pos = pos_df.groupby('Code').apply(calculate_cost)
    pos = pos.assign(abs_cover=pos['cover'].abs())
    cover = pd.DataFrame(pos.groupby('Date')['abs_cover'].sum().shift(1)).rename(columns={0: 'cover'}).fillna(0)
    pos = pos.reset_index(drop=True).sort_values(['Date', 'Code', 'Symbol'])
    total_cost = calculate_total_cost(pos)
    total_cost['Total_cost'] = 0 - (total_cost['abs_cost'] + cover['abs_cover'])
    total_cost = total_cost.drop('abs_cost', axis=1)
    total_cost = total_cost.assign(Total_pnl=total_cost['Total_pnl_cost'] + total_cost['Total_cost'])
    print('数据计算完成')
    return total_cost


def check_price_ts(price_info):
    # 提取 reference 价格
    conn = sql.SQLConn()
    etf50_ref = conn.GetData('日行情_50etf')[['日期', '期权代码', '收盘价', '前结算价']].\
        rename(columns={'日期': 'Date', '期权代码': 'Code', '收盘价': 'Close', '前结算价': 'Prev_Close'}).\
        sort_values(['Date', 'Code']).set_index(['Date', 'Code'])
    conn.CloseSql()

    # 对比
    price_info['Code'] = price_info['Code'].astype('int64')
    price_info = price_info.set_index(['Date', 'Code'])

    # 检测 Price_t 是否对应 Close
    merged_prc = price_info.merge(etf50_ref, how='left', left_index=True, right_index=True)
    cond_t = all(np.isclose(merged_prc['Price_t'], merged_prc['Close'], atol=1e20))
    cond_t1 = all(np.isclose(merged_prc['Price_t1'], merged_prc['Prev_Close'], atol=1e20))
    if not (cond_t & cond_t1):
        return 0
    return 1


if __name__ == '__main__':
    # 获取数据
    position = pd.read_excel('test2_position(1).xlsx')
    if check_price_ts(position[['Date', 'Code', 'Price_t', 'Price_t1']]):
        tot = calc_total_cost(position)
    else:
        raise ValueError('The price input is not correct')

    # Write Data
    # tot.to_excel('test2_position(1)_tot_cost.xlsx', index=True)
