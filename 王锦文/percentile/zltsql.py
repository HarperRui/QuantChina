# -*- coding: utf-8 -*-
# @Time    : 2021/5/11 9:51
# @Author  : Jinwen Wang
# @Email   : jw4013@columbia.edu
# @File    : zltsql.py
# @Software: PyCharm


import pandas as pd
import pyodbc


class SQLConn:
    def __init__(self):
        userid = 'sa'
        pw = 'lhtzb.123'
        self.cnxn_string = 'DRIVER={SQL SERVER};SERVER=192.168.9.85;DATABASE=Option;UID=' + userid + ';PWD=' + pw
        self.cnxn = pyodbc.connect(self.cnxn_string, unicode_results='True')
        self.SqlObj = self.cnxn.cursor()
        print('数据库连接成功')

    def GetData(self, sheetname):
        sql = "select * from %s" % sheetname
        return pd.read_sql(sql, self.cnxn)

    def CloseSql(self):
        self.SqlObj.close()
        self.cnxn.close()
        print('数据库断开成功')