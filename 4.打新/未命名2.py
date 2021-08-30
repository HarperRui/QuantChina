# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 14:52:23 2020

@author: Xuan
"""


       ''' 
        print('processing 取行情数据')
        sql = "SELECT TRADE_DT,S_info_windcode,S_DQ_CLOSE,S_DQ_ADJCLOSE,S_DQ_ADJPRECLOSE,S_DQ_ADJCLOSE/S_DQ_ADJPRECLOSE-1,S_DQ_PCTCHANGE/100,s_dq_tradestatus,s_dq_amount FROM %s where TRADE_DT>=%s " \
              "and TRADE_DT<=%s" % ('ASHAREEODPRICES', from_date, to_date)  
        cursor.execute(sql)
        data = cursor.fetchall()
        df_price = pd.DataFrame(list(data))
        df_price.columns=['日期','股票代码','收盘价','复权收盘价','复权前收盘价','涨跌幅-复权','涨跌幅-收盘价','交易状态','成交额（千元）']
      
        #filename = 'ASHAREEODPRICES_pct_%s_%s.csv' % (from_date, to_date)
        #df.to_csv(filename,encoding='utf_8_sig')
        
        print('processing 取上市日期信息')
        sql='select S_INFO_WINDCODE,S_INFO_NAME,S_INFO_LISTDATE,S_INFO_DELISTDATE from ASHAREDESCRIPTION where S_INFO_LISTDATE>=%s and S_INFO_LISTDATE<=%s'% (from_date,to_date)
        cursor.execute(sql)
        info=cursor.fetchall()
        df_info = pd.DataFrame(list(info))
        df_info.columns=['股票代码','股票名称','上市日期','退市日期']
        df_info = df_info.sort_values(by=['上市日期'])
        '''
        
        
        
        
        
                lock_code = df1.loc[df1['锁定期(月)']>0,'代码'].drop_duplicates()
        
        lock_code1=" "
        for i in range(len(lock_code)):
            if i==len(lock_code)-1:
                tmp_string='\''''+df['代码'][i]+'\''
            else:
                tmp_string='\''+df['代码'][i]+'\','
            lock_code1=lock_code1+tmp_string
        lock_code1="("+lock_code1+")"
        
        print('processing 取行情数据')
        sql = "SELECT TRADE_DT,S_info_windcode,S_DQ_CLOSE FROM %s where  S_INFO_WINDCODE IN %s and TRADE_DT>=%s " \
              "and TRADE_DT<=%s" % ('ASHAREEODPRICES',lock_code1, from_date, to_date)  
        cursor.execute(sql)
        data = cursor.fetchall()
        df_price = pd.DataFrame(list(data))
        df_price.columns=['可流通日期','代码','收盘价']
        