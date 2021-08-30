var stocks_list = document.getElementsByClassName("datagrid-cell datagrid-cell-c1-SECURITIES_CODE");
var id = document.getElementsByClassName("datagrid-cell datagrid-cell-c1-INFO_ID");
for (i in stocks_list){	
	if(stocks_list[i].textContent == stock_code){
		id[i].firstElementChild.click();		
		var confirm = document.getElementById("div_addContent")
		if(confirm){
			confirm.getElementsByTagName("a")[0].click();	
		}
	}	
}	




