# Snowball.py

根据敲入边界、敲出边界、敲出票息、敲入观察日、敲出观察日、波动率等参数对雪球类型产品进行定价：

1. 创建Snowball类对象，传入包含各种预先设定参数的basic_params：

   这里默认每日观察是否敲入，每个月观察是否敲出

   ```python
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
   
   s = Snowball(basic_param)  # 创建对象
   ```

2. 总共有三种方法进行定价：

   （1）Mont Carlo:

   ​		使用GBM对股价路径进行模拟，计算每一条路径的收益（站在雪球购买方的角度）并求贴现的均值；

   （2）Binomial Tree：

   ​		构建二叉树对股价走势进行模拟，类似于MC的方法求收益的贴现均值，相比较与MC的话收敛慢一些，计算也更耗时；

   （3）PDE method：

   ​		这个是参考了paper里的方法，对雪球期权进行了简化成最基本的向上敲出向下敲入的期权（实际不太准），即它相当于一个“向  		上敲出、向下敲出的看跌障碍期权”多头与一个“向上敲出的看跌期权”空头的组合：

   ​	**The valuation of up-out-put option**
   $$
   p_{up-out} = Ke^{-rT}N(-d_2) - S_0e^{-qT}N(-d_1) + S_0e^{-qT}\left({H\over S_0}\right)^{2\lambda}N(-y) - Ke^{-rT}\left({H\over S_0}\right)^{2\lambda-2}N(-y + \sigma\sqrt{T})
   $$

   $$
   d_1 = {ln(S_0 / K) + (r - q + \sigma^2 / 2)T\over \sigma\sqrt{T}} \quad d_2 = {ln(S_0 / K) + (r - q - \sigma^2 / 2)T\over \sigma\sqrt{T}} = d_1 - \sigma\sqrt{T}
   $$

   $$
   \lambda = {r - q + \sigma^2 / 2 \over \sigma^2} \quad y = {ln[H^2 / (S_0K)] \over \sigma\sqrt{T}} + \lambda\sigma\sqrt{T}
   $$

   

   **The valuation of double-out barrier put option**
   $$
   p_{double-out} = ({S\over L})^\alpha e^{\beta(T-t)} \sum_{n=1}^\infty {2\over l}sin({n\pi\over l}ln{S\over L})\cdot e^{-{n^2\pi^2\sigma^2\over 2l^2}(T-t)} \int_L^U ({S \over L})^{-\alpha} ({K\over S} - 1)^+ sin({n\pi\over l}ln{S\over L})dS
   $$

   $$
   \alpha = -{1\over \sigma^2}(r - q - {\sigma^2\over 2}) \quad \beta = -r - {1\over 2\sigma^2}(r - q -{\sigma^2\over 2})^2 \quad l = ln{U\over L}
   $$

   

3. 分别使用不同的method可以得到三种方式的定价结果：

   ```python
   print('Binomial tree return: ' + str(s.get_bt_return()))
   print('Monte Carlo return: ' + str(s.get_mc_return()))
   print('PDE price: ' + str(s.get_pde_price()))
   ```

