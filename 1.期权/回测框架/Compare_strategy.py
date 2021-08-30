from Base_backtest import *
from functools import reduce





def compare_strategy(path_list,file_name,folder_path,_format = 'excel',sheet_name = None):
    def read_all_strategy(path_list,_format,sheet_name):
        '''
        :param path_list: 策略保存的路径
        :param _format: 格式,excel / parquet / csv
        :param sheet_name: Dict, Daily return 保存的sheetname，不同策略保存的可能不一样
        :return: 不同策略的daily return的combine
        '''
        df = []
        if sheet_name == None:
            sheet_name = {}
            for key in path_list:
                sheet_name[key] = 'PnL'

        for path in path_list:
            df.append(eval("pd.read_%s('%s','%s')"%(_format,path,sheet_name[path])).loc[:,['Date','Daily Return']].rename(columns={"Daily Return":path_list[path]}).sort_values(by='Date').reset_index(drop=True))
        result = reduce(lambda left,right: pd.merge(left, right, how = 'outer',on='Date'), df)
        return result

    df_combine = read_all_strategy(path_list, _format, sheet_name)

    test = base_strategy(initial = 1000000,
                         save_result_to_folder= 'D:/Harper/Class',
                         code_list = {'510050.sh':{'symbol':'etf','database_address':'wind'},'000016.sh':{'symbol':'index','database_address':'wind'}})
    test.performance_multi_strategy(df_multi_daily_return = df_combine, file_name= file_name, folder_path= folder_path, col_name=None)


#%% Demo
if __name__ == '__main__':

    path_list = {
        'D:/Harper/option_strategy/backtest_result/test_信号0804_230_iv_vega敞口gamma定_gamma3倍_65_7天不开_保证金限制_仅short0cost_冲击成本2_twap.xlsx': 'signal0804_230,冲击成本=2',
        'D:/Harper/option_strategy/backtest_result/test_信号0804_230_调整平滑方式_iv_vega敞口gamma定_gamma3倍_65_7天不开_保证金限制_仅short0cost_冲击成本2_twap.xlsx':'signal0804_230_调整平滑方式,冲击成本=2',
        'D:/Harper/option_strategy/backtest_result/test_信号0804_230_调整平滑方式_iv调整为365年化_vega敞口gamma定_gamma3倍_65_7天不开_保证金限制_仅short0cost_冲击成本2_twap.xlsx': 'signal0804_230_调整平滑方式_iv调整为365年化,冲击成本=2',
        'D:/Harper/option_strategy/backtest_result/test_信号0804_1500_调整平滑方式_vega敞口gamma定_gamma3倍_65_7天不开_保证金限制_仅short0cost_冲击成本2_twap.xlsx': 'signal0804_1500_调整平滑方式,冲击成本=2',
        'D:/Harper/option_strategy/backtest_result/test_信号0804_1500_调整平滑方式_iv调整为365年化_vega敞口gamma定_gamma3倍_65_7天不开_保证金限制_仅short0cost_冲击成本2_twap.xlsx': 'signal0804_1500_调整平滑方式_iv调整为365年化,冲击成本=2'
    }

    file_name = 'signal0804_230_300对比_iv调整为365年化'
    folder_path = 'D:/Harper/option_strategy/backtest_compare'
    # df_combine = read_all_strategy(path_list, _format='excel', sheet_name=None)
    # test = base_strategy(initial = 1000000,
    #                      save_result_to_folder= 'D:/Harper/Class',
    #                      code_list = {'510050.sh':{'symbol':'etf','database_address':'wind'},'000016.sh':{'symbol':'index','database_address':'wind'}})
    # test.performance_multi_strategy(df_multi_daily_return = df_combine, file_name= file_name, folder_path= folder_path, col_name=None)
    compare_strategy(path_list,file_name,folder_path,_format = 'excel',sheet_name = None)