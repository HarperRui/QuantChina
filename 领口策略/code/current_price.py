# -*- coding: utf-8 -*-
"""
Created on Thu Mar 18 13:48:05 2021

@author: Xuan
"""

import datetime
today = datetime.datetime.today()

# Connect MangoDB to get real-time data
import pymongo
def connect_mongodb():
    myclient = pymongo.MongoClient("mongodb://192.168.9.189:15009/")
    mydb = myclient.data
    mydb.authenticate("zlt01", "zlt_ujYH")
    return mydb

def connect_mongodb_backup():
    myclient = pymongo.MongoClient("mongodb://192.168.7.208:27017/")
    mydb = myclient.data
    mydb.authenticate("middledesk", "zltmiddle")
    return mydb





mongo = connect_mongodb()
c = mongo["quote_data"]
#q = mongo["sample_data"]

'''
test = q.find({"symbol": "IF2103.CFE"})

for i in test:
    print(i)
'''
#%% Get option list from wind API

from WindPy import w
w.start()

#取今天交易的option list(code,trading_code,name,strike)
def option_list(underlying):#underlying: 510050.SH,510300.SH
    lst = w.wset("optioncontractbasicinformation","exchange=sse;windcode=%s;status=trading;field=wind_code,trade_code,exercise_price,sec_name"%underlying,usedf=True)[1]
    #lst = w.wset("optioncontractbasicinformation","exchange=sse;windcode=%s;status=all;field=wind_code,trade_code,exercise_price,sec_name"%underlying,usedf=True)[1]
    lst.columns = ['期权代码','交易代码',	'行权价','期权简称']
    return lst

#取option list的前结算价
def pre_settle(code,date): #date:'2021-03-18'
    code_lst = code + '.SH'
    if date == today.strftime('%Y-%m-%d'):
        lst2 =  w.wsq(code_lst.tolist(), "rt_pre_settle",usedf=True)[1]
    else:
        lst2 =  w.wsd(code_lst.tolist(), "pre_settle", date, date, "",usedf=True)[1]
    lst2.columns = ['前结算价']
    lst2['期权代码'] = lst2.index.astype(str).str[:8]
    return lst2

#取option list的结算价
def settle(code,date): #date:'2021-03-18'
    code_lst = code + '.SH'
    lst2 =  w.wsd(code_lst.tolist(), "settle", date, date, "",usedf=True)[1]
    lst2.columns = ['结算价']
    lst2['期权代码'] = lst2.index.str[:8]
    return lst2


#取option contract info (上市新合约或除权除息会更新)
def contract_info(underlying):#underlying: 510050.SH,510300.SH
    lst3 = w.wset("optioncontractbasicinformation","exchange=sse;windcode=%s;status=trading;field=wind_code,trade_code,sec_name,option_mark_code,call_or_put,exercise_price,contract_unit,listed_date,expire_date"%underlying,usedf = True)[1]
    lst3.columns = ['期权代码','交易代码','期权简称','期权标的','认购认沽','行权价','合约单位','上市日','到期日']
    return lst3

#中金所股指期权合约
def contract_info_300index(underlying):#underlying: 000300.SH
    lst3 = w.wset("optioncontractbasicinfo","startdate=2019-10-17;enddate=2021-06-17;exchange=cffex;windcode=%s;status=all;field=wind_code,trade_code,sec_name,option_mark_code,call_or_put,exercise_price,contract_unit,listed_date,expire_date"%underlying,usedf = True)[1]
    lst3.columns = ['期权代码','交易代码','期权简称','期权标的','认购认沽','行权价','合约单位','上市日','到期日']
    return lst3

#取future月合约代码
def active_future_code(underlying,date): #underlying: IF.CFE, date:'2021-03-18'
    fut_code = w.wsd(underlying, "trade_hiscode", date, date, "",usedf=True)[1]
    fut_code.columns = ['contract']
    return fut_code


#%% get current price of ETF,option
import pandas as pd
def get_price(code,field,type='option'): #code: 510300.SH, type: 'etf'/'option' ,field:'pre_close'/'last_price'
    mongo = connect_mongodb()
    c = mongo["quote_data"]

    try:
        if type != 'option':
            etf = c.find_one({"code_name": code})[field]
            return etf
        else:
            code_lst = code.astype(str) + '.SH'
            data = {}
            volume = {}
            pre_close = {}
            option_data = c.find({"code_name": {"$in":code_lst.to_list()}})
            for i in option_data:
                data[i['code_name'][:-3]] = i[field]
                
                if (field == 'last_price') &(i['volume'] < 1):
                    data[i['code_name'][:-3]] = i['pre_close']
            
            df_price = pd.DataFrame({field:data})    
            return df_price
    except:
        print("Input code is not in MangoDB datebase")

            
        
        
        

        