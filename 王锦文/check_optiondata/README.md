# check_optiondata.py

检查minbar数据是否存在以下问题：

1. 当天的minbar数据中是否有期权合约不在contract_daily_info中
2. 每个期权合约是否有太多缺失或为0的ask/bid price（默认阈值为20个）
3. 是否存在first ask/bid volume为0，但其余的volume不为0的情况