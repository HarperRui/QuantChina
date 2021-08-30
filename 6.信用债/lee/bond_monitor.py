import pandas as pd
import pymongo
import pymssql
import tkinter as tk
from  tkinter  import ttk
import re

convert_list = [ "128", "117", "125", "126", "110", "113", "131"]

def get_bond_names():
    bond_names = {}
    conn = pymssql.connect(host='192.168.8.120', port=14333, user='GuestUser', password='GuestUser', database='JYDB',charset='GBK')
    with conn.cursor() as cursor:
        sql = ''' SELECT SecuCode, SecuAbbr,SecuMarket FROM  Bond_Code '''
        cursor.execute(sql)
        data = cursor.fetchall()
        for i in data:
            if i[2] == 83: 
                bond_names[i[0]+".SH"]=i[1]
            if i[2] == 90:
                bond_names[i[0]+".SZ"]=i[1] 
    return bond_names    


def is_number(s):
    try:
        float(s)
        return True
    except:
        pass

    return False

def treeview_sort_column(tv,col,reverse):
    l = [(tv.set(k,col),k) for k in tv.get_children('')]
    if is_number(l[0][0]):
        l.sort(key = lambda x: float(x[0]),reverse=reverse)
    else:
        l.sort(reverse = reverse)

    for index,(val,k) in enumerate(l):
        tv.move(k,'',index)

    tv.heading(col,command=lambda :treeview_sort_column(tv,col,not reverse))


class basedesk():
    def __init__(self, master):
        self.root = master
        self.root.title('future monitor')
        self.root.geometry('1080x720')
        self.table_init = False
        self.signal_data = {} 
        self.bond_names = get_bond_names()            
        
        myclient = pymongo.MongoClient("mongodb://192.168.9.189:15009/")
        self.mongo = myclient.data
        self.mongo.authenticate("zlt01", "zlt_ujYH")

        self.mysql = pymssql.connect(host='192.168.9.85', user='sa', password='lhtzb.123', database='BondMonitor')
        self.target_bond = []
        self.signal_list = []
        self.get_target_bond()

        self.db_lookup()

    def get_target_bond(self):       
        with self.mysql.cursor() as cursor:
            ##取日常要订的表
            sql = ''' SELECT * FROM  Bond_list '''
            cursor.execute(sql)
            data = cursor.fetchall()
            for i in data:
                if i[5] != 90 and i[5] != 83:
                    print(i)
                if i[5] == 90:
                    self.target_bond.append(str(i[2]) + ".SZ")
                if i[5] == 83:
                    self.target_bond.append(str(i[2]) + ".SH")
            sql = ''' SELECT * FROM  Abnormal_Bond_list '''
            cursor.execute(sql)
            data = cursor.fetchall()
            for i in data:
                print(i)                
                self.target_bond.append(i[0])

    def add_new_abnormal_bond(self,code):
        with self.mysql.cursor() as cursor:            
            sql = "insert into Abnormal_Bond_list values (" +" ' "+code + "'"+ ","+"'"+self.bond_names[code]+ "'"+")" 
            print(sql)
            cursor.execute(sql)
        self.mysql.commit()               

    def show_warning(self,code):
        top = tk.Toplevel()
        top.geometry('640x480')
        top.title('warnnig')        
        l3 =tk.Label(top,text='{} {}'.format(code,self.bond_names[code]))
        l3.pack(side='top')
       
                
    def db_lookup(self):
        mongo = self.mongo
        temp = mongo["quote_data"]
        sample = temp.find({"code_name":re.compile("^1")})
        # print(str(sample[0]["time"])[:4])
        self.data = []
        self.table_list = []
        for s in sample:
            if s["code_name"][0:3] not in convert_list:
                if ((int(s["code_name"][0:3]) >= 150 or s["code_name"][0:3] == '127' or s["code_name"][0:3] == '123') and s["code_name"][-2::] == "SZ"):
                    pass
                else:                    
                    rate = 0
                    if s["pre_close"] != 0:
                        rate = (s["last_price"] -  s["pre_close"])/s["pre_close"]                        
                    if rate > 0.05 or (s["code_name"] in self.target_bond and (s["volume"] > 5000 or s["total_ask_vol"] > 2000 or s["total_bid_vol"] > 2000)):
                        self.signal_calc(s)
                        tags = ""
                        if  self.signal_data[s["code_name"]]["signal"]:
                            tags= "warning"
                        self.data.append({
                                            "code_name":s["code_name"],
                                            "bond_name":self.bond_names[s["code_name"]],
                                            "volume":s["volume"],
                                            "signal":self.signal_data[s["code_name"]]["signal"],
                                            "total_ask_vol":s["total_ask_vol"],
                                            "total_bid_vol":s["total_bid_vol"],
                                            "price":"{:.2f}".format(s["last_price"]), 
                                            "tags":tags
                                        })
                        self.table_list.append(s["code_name"])
                        if rate > 0.05 and (s["code_name"] not in self.target_bond):
                            self.target_bond.append(s["code_name"])
                            self.add_new_abnormal_bond(s["code_name"])
                        if s["code_name"] not in self.signal_list:
                            self.signal_list.append(s["code_name"])  
                    if s["code_name"] in self.signal_list:
                        self.signal_calc(s)

        print("bond total:",len(self.data))        
        self.show_table()        
        self.root.after(10000,self.db_lookup)

    def signal_calc(self,s):
        minute = str(s["time"])[:4]
        if s["code_name"] not in self.signal_data.keys():        
            self.signal_data[s["code_name"]] = {}
            self.signal_data[s["code_name"]]["time"] = minute 
            self.signal_data[s["code_name"]]["pirce"] = []
            self.signal_data[s["code_name"]]["pirce"].append(s["last_price"])
            self.signal_data[s["code_name"]]["signal"] = False 
        else:
            if self.signal_data[s["code_name"]]["time"] != minute :
                self.signal_data[s["code_name"]]["time"] = minute             
                self.signal_data[s["code_name"]]["pirce"].append(s["last_price"])
                pirce_len = len(self.signal_data[s["code_name"]]["pirce"])                
                if pirce_len >= 5 :
                    pirce_base = self.signal_data[s["code_name"]]["pirce"][-5]
                    if pirce_base != 0:
                        rate = (s["last_price"] - pirce_base) / pirce_base
                        if abs(rate) > 0.01:
                            self.show_warning(s["code_name"])
                if  pirce_len < 14:
                    pass
                else:
                    total = 0.0
                    if len(self.signal_data[s["code_name"]]["pirce"]) != 15:
                        print("signal cacl error")
                    for i in self.signal_data[s["code_name"]]["pirce"]:
                        total = total + i
                    avg =  total / 15
                    if s["last_price"] > avg:
                        self.signal_data[s["code_name"]]["signal"] = True
                    del  self.signal_data[s["code_name"]]["pirce"][0]       

    def set_tv_head(self,tv):
        tv["columns"] = self.title
        for i in range(len(self.title)):
            if self.title[i] == "account_name":
                tv.column(self.title[i],width=180,anchor='center')          
            else:
                tv.column(self.title[i],width=100,anchor='center')
            tv.heading(self.title[i],text=self.title[i],command=lambda _col=self.title[i]:treeview_sort_column(tv,_col,False))    

    def show_table(self):  
        if not self.table_init:
            self.title = ["code_name","bond_name","volume","signal","total_ask_vol","total_bid_vol","price"]                       

            scrollbar = tk.Scrollbar(self.root)
            scrollbar.pack(side=tk.RIGHT,fill=tk.Y)  
            self.main_tv = ttk.Treeview(self.root,columns=self.title,
                                    yscrollcommand=scrollbar.set,
                                    show='headings')
            self.set_tv_head(self.main_tv)
            for data in [self.data]:
                for i in range(len(data)):
                    self.main_tv.insert('','end',values=[data[i][y] for y in self.title],tags=data[i]["tags"]) 
                
            scrollbar.config(command=self.main_tv.yview)
            self.main_tv.tag_configure('warning', background='red')
            self.main_tv.pack(side="top",expand="yes",fill="both")
            self.table_init = True 
        else:

            all_items = self.main_tv.get_children("");
            for item in all_items:
               values = self.main_tv.item(item,"values")                            
               if (len(values) != 0) and  (values[0] not in self.table_list):
                    self.main_tv.delete(item)
                    continue  
            
            all_items = self.main_tv.get_children("");           
            data = self.data
            for i in range(len(data)):
                showed = False
                for item in all_items:
                    values = self.main_tv.item(item,"values")                        
                    if len(values) != 0 and values[0] == data[i]["code_name"]:
                        self.main_tv.item(item,values = [data[i][y] for y in self.title],tags=data[i]["tags"])                            
                        showed = True
                        break
                if not showed:
                    self.main_tv.insert('','end',values=[data[i][y] for y in self.title],tags=data[i]["tags"])
    

if __name__ == '__main__':
    root = tk.Tk()
    basedesk(root)
    root.mainloop()





