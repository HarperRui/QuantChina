# OptionData

根据期权的minbar数据计算每日分钟级的合成期货、IV、Greeks、期限结构、偏度、Key point和波动率曲面（SABR、Heston、CEV）



# future.py

```python
get_synthetic_futures(code, date, rf, df_contract, input_stock_path, input_future_path, output_path):
    """
    计算合成期货价格
    :param code: 标的资产代码，'510050' or '510300' or '000300'
    :param date: 处理当天的日期，格式为 '20210521'
    :param rf: 年化的无风险利率
    :param df_contract: 包含每日期权信息的 Dataframe
    :param input_stock_path: 输入的minbar股票数据路径
    :param input_future_path: minbar期货数据路径
    :param output_path: 输出结果的路径
    :return: 包含当天分钟级合成期货、升贴水、分红率的Dataframe
    """
```



# greeks.py

计算Delta, Gamma, Vega, Theta, Rho的函数



# iv.py

```python
get_iv_greeks(code, date, rf, df_contract_all, df_contract_code, input_stock_path, input_future_path, synthetic_future_path, output_path):
    """
    在minbar数据中添加期权基本信息以及bid,ask,mid,close四个价格对应的IV和Greeks
    :param code: 标的资产代码，'510050' or '510300'
    :param date: 处理当天的日期，格式为 '20210521'
    :param rf: 年化的无风险利率
    :param df_contract_all: 包含每日期权信息的 Dataframe（包含所有50和300合约）
    :param df_contract_code: 包含每日期权信息的 Dataframe(只包含当前标的的合约)
    :param input_stock_path: 输入的minbar股票数据路径
    :param input_future_path: minbar期货数据路径
    :param output_path: 输出结果的路径
    :param synthetic_future_path: 合成期货的路径
    :return: 将处理好的minbar数据保存
    """
```



# term_structure.py

计算分钟级的0.25、0.5和0.75Delta隐含波动率，默认分开计算看涨和看跌期权，插值方法分简单线性插值和cubic spine两种

```python
get_termstructure(code, date, input_iv_path, input_future_path, output_path, df_contract):
    """
    计算指定日期的0.25,0.5,0.75delta 隐含波动率差值结果并保存至输出路径
    :param code: '510050' or '510300' or '000300'
    :param date: 日期，格式为 '20210401'
    :param input_iv_path: 储存期权合约IV数据的路径
    :param input_future_path: 储存合成期货数据的路径
    :param output_path: 期限结构结果的输出路径
    :param df_contract: 储存期权合约信息的DataFrame
    :return: 将插值结果保存至输出路径下
    """
```

额外多加了一个5min平均的版本（因为分钟数据变化可能有点大）



# key_point.py

计算到期日为30、60和90天的波动率，分简单线性插值和cubic spine两种

```python
get_keypoint(code, date, input_iv_path, input_termstructure_path, output_path, df_contract):
    """
    计算给定日期的30,60,90天波动率并将结果储存至输出路径
    :param code: '510050' or '510300' or '000300'
    :param date: 日期，格式为 '20210401'
    :param input_iv_path: 储存期权合约IV数据的路径
    :param input_termstructure_path: 储存期限结构数据的路径
    :param output_path: 结果输出路径
    :param df_contract: 储存期权合约信息的DataFrame
    :return: 将插值结果保存至输出路径下
    """
```



# SABR.py

拟合分钟级SABR model的 $\alpha, \beta, \nu, \rho$参数，其中：

1. 在calibrate时默认使用QuantLib的sabrVolatility函数：

   ```python
   ql.sabrVolatility(strike, forward, expiryTime, alpha, beta, nu, rho)
   ```

   code里也有另外一个根据paper写的函数 sabr_implied_volatility：

   ```python
   sabr_implied_volatility(alpha, beta, nu, rho, f, K, T):
       """
       计算SABR模型的波动率
       :param alpha: 参数alpha
       :param beta: 参数beta
       :param nu: 参数nu
       :param rho: 参数rho
       :param f: 标的合成期货价格
       :param K: 行权价
       :param T: 距离到期时间（年化）
       :return: 波动率，float
       """
   ```

   依照：

   $$
   \sigma(K,f)={\alpha \cdot \left[
   1+\left[
   {(1-\beta)^2\over 24}{\alpha^2\over (fK)^{1-\beta}}+{1\over 4}{\rho \beta\nu \alpha\over(fK)^{(1-\beta)/2}}+{2-3\rho^2\over 24}\nu^2
   \right]T+\cdots
   \right]\over (fK)^{(1-\beta)/2}\left[
   1+{(1-\beta)^2\over 24}ln^2{f\over K}+{(1-\beta)^4\over 1920}ln^4{f\over K}+\cdots
   \right]
   }\cdot\left(
   {z\over x(z)}
   \right)
   $$

   

   $$
   z={\nu\over \alpha}(fK)^{(1-\beta)/2}ln{f\over K}
   $$

   

   $$
   x(z)=ln{\sqrt{1-2\rho z+z^2}+z-\rho \over1-\rho}
   $$

   但是比较起来QuantLib的更快一些，并且遇到异常输入值更robust。

2. calibrate时使用的数据默认为距离标的价格最近的6个(对于50ETF和300ETF期权)或12个(对于300index期权)合约，具体的阈值可以在get_available_options_data函数主体中更改：

   ```python
   # 剔除深度虚值的合约
   num = 6 if code[0:3] == '510' else 12
   selected_strikes = find_nearest_value(np.array(result['strike_prc'].copy()), stock_price, num)
   threshold_low = selected_strikes[0]
   threshold_high = selected_strikes[-1]
   result = result[(result['strike_prc'] >= threshold_low) & (result['strike_prc'] <= threshold_high)]
   result.reset_index(inplace=True, drop=True)
   ```

3. optimize的方法为scipy.optimize.minimize中的“L-BFGS-B”，使得SABR model的波动率与IV的残差平方和最小。



# Heston.py

拟合分钟级别的theta, kappa, sigma, rho, v0参数

```python
get_minbar_heston_params(code, date, expiration_date, rf, minbar_idx, path_minbar, df_future, strike_prcs, option_dict, get_all=False):
    """
    根据交易日期、期权到期时间及分钟bar所在的index计算对应的Heston model parameters
    :param code: 标的ETF的代码，'510050' or '510300'
    :param date: 交易日期，格式为'20210401'
    :param expiration_date: 期权的到期日，格式为'20210428'
    :param rf: 无风险利率（年化）
    :param minbar_idx: 分钟所在的index
    :param path_minbar: 交易当天期权合约和标的ETF minbar data所在的路径
    :param df_future: 含有交易当天合成期货价格和分红率的DataFrame
    :param strike_prcs: 行权价，ndarray
    :param option_dict: 当天所有的期权合约, dict
    :param get_all: 是否使用所有行权价的样本进行拟合
    :return: theta, kappa, sigma, rho, v0
    """
```

calibrate所用的数据和optimize method与SABR类似，Heston model的波动率依靠QuantLib的HestonModel及HestonBlackVolSurface。



# CEV.py

拟合CEV model的beta与sigma参数，实际拟合中发现CEV的volatility不太符合volatility smile，所以将期权合约分成OTM call和OTM put两部分分别拟合。call部分在calibrate时设定beta>1，put部分的beta<1。（最终结果仍然不是很好，特别是与SABR与Heston相比）

```python
get_cev(code, date, rf, df_contract, path_future, path_minbar, path_iv, output_path):
    """
    计算指定日期的CEV参数并将结果储存在本地
    :param code: '510050' or '510300' or '000300'
    :param date: 日期，格式为 '20210401'
    :param rf: 无风险利率（年化）
    :param df_contract: 储存期权合约信息的DataFrame
    :param path_future: 储存合成期货数据的路径
    :param path_minbar: minbar数据路径
    :param path_iv: 储存期权合约IV数据的路径
    :param output_path: CEV结果的输出路径
    :return: 将参数拟合结果保存至输出路径下
    """
```



# main.py

整合上述所有的期权数据到一个function中

```python
get_options_data(code, input_minbar_path, output_path, specify_date=None):
    """
    计算50或300合约的合成期货、IV Greeks、SABR、0.25 0.5 0.75delta、30 60 90天波动率数据并储存本地
    :param code: '510050' or '510300' or '000300'
    :param input_minbar_path: 期权合约的minbar路径
    :param output_path: 输出结果的储存路径
    :param specify_date: 可以指定只计算某个月的数据
    :return: 结果保存至本地
    """
```

例如计算2021年4月50ETF期权所有的数据：

```python
stock_code = '510050'
daily_minbar_path = '\\\\datahouse.quantchina.info\\data\\minbar\\stock'  # minbar路径
result_outpath = 'C:\\Users\\lenovo\\OneDrive\\桌面\\output_%s' % stock_code  # 输出路径
if __name__ == '__main__':
    get_options_data(stock_code, daily_minbar_path, result_outpath, specify_date='202104')
```



# VolSurfPlot.py

一个用来画图看SABR或Heston或CEV模型拟合效果的小脚本（前提是先跑出来参数数据）

1. 设定保存期权数据的路径：

   ```python
   # eg
   data_path = 'C:\\Users\\lenovo\\OneDrive\\桌面'
   ```

2. 创建一个Volsurf类对象，传入date、code、model、maturity：

   ```python
   # 2021年4月1日，50ETF的4月到期的期权，model为SABR
   model = Volsurf('20210401', '510050', 'SABR', '2104')
   ```

3. 使用get_plot(minbar_index)获取图像，minbar_index为分钟所在的index

   ```python
   model.get_plot(211)
   ```

4. 可以使用update_info更新数据，调整日期、标的或到期日

   ```python
   # 调整为2021年4月12日50ETF的5月到期期权
   model.update_info('20210412', '510050', '2105')
   ```

5. 调整model type（不区分大小写）

   ```python
   model.model_type = 'HESTON'
   model.model_type = 'sabr'
   model.model_type = 'cev'
   ```

