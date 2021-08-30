# -*- coding: utf-8 -*-
# @Time    : 2021/4/27 16:57
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : Snowball.py
# @Software: PyCharm

import numpy as np
import pandas as pd
from scipy.stats import norm

# %%
basic_param = {}
basic_param['s0'] = 6716.67  # 期初价格
basic_param['mu'] = 0.03  # 预期收益
basic_param['sigma'] = 0.5744  # 波动率
basic_param['coupon'] = 0.084  # 敲出票息率
basic_param['ko_barrier'] = basic_param['s0'] * 1.01  # 敲出价格
basic_param['ki_barrier'] = basic_param['s0'] * 0.75  # 敲入价格
basic_param['r'] = 0.03  # 无风险收益率
basic_param['t'] = 1.  # 期限
basic_param['q'] = 0.00  # 分红
basic_param['period'] = 21.00  # 敲出观察间隔
basic_param['simulations'] = 100000  # MC模拟次数
basic_param['dt'] = 1.00 / 252  # 股价间隔为1天


# %%
class Snowball:
    def __init__(self, basic_params):
        self.basic_params = basic_params
        self.mc_path = np.zeros((0, 0))
        self.bt_path = np.zeros((0, 0))

    def get_mc_path(self):
        s0 = self.basic_params['s0']
        mu = self.basic_params['mu']
        sigma = self.basic_params['sigma']
        dt = self.basic_params['dt']
        simulations = self.basic_params['simulations']
        steps = int(self.basic_params['t'] / self.basic_params['dt'])  # 模拟步数
        path = np.zeros((steps + 1, simulations))
        path[0] = s0
        for i in range(1, steps + 1):
            z = np.random.standard_normal(simulations)
            path[i] = path[i - 1] * np.exp((mu - 0.5 * sigma ** 2) * dt + sigma * np.sqrt(dt) * z)
        self.mc_path = path.T

    def mc_exp_return(self, path):
        s0 = self.basic_params['s0']  # 期初价格
        coupon = self.basic_params['coupon']  # 敲出票息率
        ko_barrier = self.basic_params['ko_barrier']  # 敲出价格
        ki_barrier = self.basic_params['ki_barrier']  # 敲入价格
        r = self.basic_params['r']  # 无风险利率
        period = self.basic_params['period']  # 敲出观察间隔
        simulations = path.shape[0]  # 路径模拟个数
        steps = path.shape[1]  # 单个路径包含的天数
        df = np.array([np.exp(-r * i / 252) for i in range(steps)])  # 每天的折现因子
        obs_date = np.array([True if i % period == 0 else False for i in range(steps)])  # 敲出观察日
        obs_date[0] = False

        is_ko = path[:, obs_date] > ko_barrier  # 在观察日是否敲出
        ko_path = is_ko.any(axis=1)  # 发生敲出的path
        if ko_path.sum() != 0:
            ko_month = is_ko[ko_path].argmax(axis=1)  # 发生敲出的月份

            def monthly_discount(x):
                return np.exp(-r * (x + 1) / 12)

            ko_df = monthly_discount(ko_month)  # 对应敲出月份的折现因子
            ko_coupon = (ko_month + 1) * coupon / 12  # 对应敲出月份获得的票息
            ko_value = (ko_df * ko_coupon).sum()  # 敲出的总收益
        else:
            ko_value = 0.0  # 所有path均没有发生敲出

        ki_path = (path[~ko_path] < ki_barrier).any(axis=1)  # 未敲出但发生敲入的path
        if ki_path.sum() == 0:
            ki_value = 0.0  # 无敲入产生的损失
        else:
            ki_value = ((path[~ko_path][ki_path, -1] - s0).clip(max=0) * df[-1]).sum() / s0  # 敲入的损失

        nki_nko_value = (~ki_path).sum() * coupon * df[-1] * (steps - 1) / 252  # 未敲入也未敲出的收益

        return (ko_value + ki_value + nki_nko_value) / simulations

    def get_mc_return(self):
        self.get_mc_path()
        path = self.mc_path
        return self.mc_exp_return(path)

    def get_bt_path(self):
        s0 = self.basic_params['s0']
        steps = int(self.basic_params['t'] / self.basic_params['dt'])
        u = np.exp(self.basic_params['sigma'] * np.sqrt(self.basic_params['dt']))  # 二叉树股价上涨幅度
        d = 1 / u
        mu = np.arange(steps + 1)
        mu = np.resize(mu, (steps + 1, steps + 1))
        md = mu.T
        U = u ** (mu - md)
        D = d ** md
        bt = s0 * U * D
        self.bt_path = bt

    def get_knockin_value(self, path, ki_date):
        s0 = self.basic_params['s0']
        coupon = self.basic_params['coupon']
        period = self.basic_params['period']
        ko_barrier = self.basic_params['ko_barrier']
        u = np.exp(self.basic_params['sigma'] * np.sqrt(self.basic_params['dt']))  # 二叉树股价上涨幅度
        d = 1 / u
        p = (np.exp(self.basic_params['r'] * self.basic_params['dt']) - d) / (u - d)  # 二叉树股价上涨概率
        df = np.exp(-self.basic_params['r'] * self.basic_params['dt'])  # 二叉树每步折现率

        isin = np.ones_like(path)  # 记录随后的节点中是否有发生敲出的节点
        nodes = path.shape[0]
        layers = path.shape[1]
        # 遍历找出发生敲出的节点
        for layer in range(0, layers):
            for node in range(0, nodes):
                if (layer + ki_date) % period == 0 and path[node, layer] > ko_barrier:
                    # 在敲出日价格大于敲出价格，计算当前节点的敲出收益，并将isin中相同位置改为0
                    path[node, layer] = coupon * (layer + ki_date) / 252
                    isin[node, layer] = 0
        # 对于最后一层中未发生敲出的节点，计算其value
        for node in range(0, nodes):
            if isin[node, layers - 1]:
                path[node, layers - 1] = min(path[node, layers - 1] / s0 - 1, 0)
        # 从最后一层向前折现得到根节点value
        for layer in range(layers - 2, -1, -1):
            for node in range(0, nodes):
                # 如果该节点未敲出，其value为下一层两节点价值的加权平均折现
                if isin[node, layer] and node != nodes - 1:
                    path[node, layer] = (p * path[node, layer + 1] + (1 - p) * path[node + 1, layer + 1]) * df

        return path[0, 0]

    def bt_exp_return(self, path):
        coupon = self.basic_params['coupon']
        period = self.basic_params['period']
        ko_barrier = self.basic_params['ko_barrier']
        ki_barrier = self.basic_params['ki_barrier']
        steps = int(self.basic_params['t'] / self.basic_params['dt'])
        u = np.exp(self.basic_params['sigma'] * np.sqrt(self.basic_params['dt']))  # 二叉树股价上涨幅度
        d = 1 / u
        p = (np.exp(self.basic_params['r'] * self.basic_params['dt']) - d) / (u - d)  # 二叉树股价上涨概率
        df = np.exp(-self.basic_params['r'] * self.basic_params['dt'])  # 二叉树每步折现率

        isnorm = np.ones_like(path)  # 记录节点是否发生敲入或敲出
        layers = path.shape[0]  # 树的层数
        # 遍历找出发生敲入和敲出的节点
        for layer in range(0, layers):
            for node in range(0, layer + 1):
                # 如果当前节点之前的路径上还未发生敲入和敲出
                if isnorm[node, layer] == 1:
                    # 如果在观察日价格高于敲出价格
                    if layer != 0 and layer % period == 0 and path[node, layer] > ko_barrier:
                        path[node, layer] = coupon * layer / 252  # 节点value为票息收入
                        isnorm[node, layer] = -1  # 敲出节点标记为-1
                        if layer != layers - 1:
                            isnorm[node, layer + 1:] = 0  # 该节点右侧（价格一直上升路径的节点）视为敲出
                    # 如果价格低于敲入价格
                    elif path[node, layer] < ki_barrier:
                        new_tree = path[node:, layer:].copy()  # 以敲入节点为根节点的树
                        path[node, layer] = self.get_knockin_value(path=new_tree, ki_date=layer)  # 计算当前节点的敲入价值
                        isnorm[node, layer] = -1  # 敲入节点标记为-1
                        if layer != layers - 1:
                            for i in range(1, min(layers - 1 - node, layers - 1 - layer) + 1):
                                isnorm[node + i, layer + i] = 0  # 该节点右下方（价格一直下降路径的节点）视为敲入
        # 对于最后一层没有敲入和敲出的节点，其价值为coupon
        for node in range(0, layers):
            if isnorm[node, layers - 1] == 1:
                path[node, layers - 1] = coupon * steps / 252
        # 从最后一层向前折现得到根节点value
        for layer in range(layers - 2, -1, -1):
            for node in range(0, layer + 1):
                # 如果当前节点未敲入和敲出，其价值为下一层两个节点的加权平均折现
                if isnorm[node, layer] == 1:
                    path[node, layer] = (p * path[node, layer + 1] + (1 - p) * path[node + 1, layer + 1]) * df

        return path[0, 0]

    def get_bt_return(self):
        self.get_bt_path()
        path = self.bt_path.copy()
        return self.bt_exp_return(path)

    def get_pde_price(self):
        s0 = self.basic_params['s0']
        K = s0
        r = self.basic_params['r']
        q = self.basic_params['q']
        T = self.basic_params['t']
        sigma = self.basic_params['sigma']
        H = self.basic_params['ko_barrier']
        L = self.basic_params['ki_barrier']

        def up_out_put():
            lambda1 = (r - q) / sigma - sigma / 2
            lambda2 = lambda1 + sigma
            alpha1 = (H / s0) ** (2 * lambda1 / sigma)
            alpha2 = (H / s0) ** (2 * lambda2 / sigma)
            h = 1 / sigma / np.sqrt(T) * np.log(H / s0)
            d3 = lambda2 * np.sqrt(T) - h
            d4 = lambda1 * np.sqrt(T) - h
            d5 = -lambda1 * np.sqrt(T) - h
            d6 = -lambda2 * np.sqrt(T) - h
            price = K * np.exp(-r * T) * (1 - norm.cdf(d4) - alpha1 * norm.cdf(d5)) - np.exp(-q * T) * s0 * (
                    1 - norm.cdf(d3) - alpha2 * norm.cdf(d6))
            return price

        def double_out():
            def auxiliary(n):
                c = 2 * r / sigma ** 2 + 1
                d1 = (np.log(s0 * H ** (2 * n) / L ** (2 * n + 1)) + (r + sigma ** 2 / 2) * T) / sigma / np.sqrt(T)
                d2 = (np.log(s0 * H ** (2 * n) / L ** (2 * n) / K) + (r + sigma ** 2 / 2) * T) / sigma / np.sqrt(T)
                d3 = (np.log(L ** (2 * n + 1) / s0 / H ** (2 * n)) + (r + sigma ** 2 / 2) * T) / sigma / np.sqrt(T)
                d4 = (np.log(L ** (2 * n + 2) / K / s0 / H ** (2 * n)) + (r + sigma ** 2 / 2) * T) / sigma / np.sqrt(T)
                return c, d1, d2, d3, d4

            price = 0
            for n in range(-12, 13):
                c, d1, d2, d3, d4 = auxiliary(n)
                part1 = (H / L) ** (n * (c - 2)) * (
                        norm.cdf(d1 - sigma * np.sqrt(T)) - norm.cdf(d2 - sigma * np.sqrt(T))) - \
                        (L ** (n + 1) / s0 / H ** n) ** (c - 2) * (
                                norm.cdf(d3 - sigma * np.sqrt(T)) - norm.cdf(d4 - sigma * np.sqrt(T)))
                part2 = (H / L) ** (n * c) * (norm.cdf(d1) - norm.cdf(d2)) - (L ** (n + 1) / s0 / H ** n) ** c * (
                        norm.cdf(d3) - norm.cdf(d4))
                price += K * np.exp(-r * T) * part1 - s0 * np.exp(-q * T) * part2
            return price

        return up_out_put() - double_out()


# %%
s = Snowball(basic_param)
print('Binomial tree return: ' + str(s.get_bt_return()))
print('Monte Carlo return: ' + str(s.get_mc_return()))
print('PDE price: ' + str(s.get_pde_price()))

