var index = 0;
var last_url  = ""; 

function sleep(d){
  for(var t = Date.now();Date.now() - t <= d;);
}

function mySend(_msg){
	chrome.runtime.sendMessage({msg:_msg}, function(response) {
		if(response){ //使用 response.status 如果返回的数据为空或不存在此值会报错
			// index = response.msg;
		}
	});	
}

function relocate(){
	sleep(1000);
	chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {		
		chrome.tabs.executeScript(tabs[0].id,{code: 'document.getElementsByClassName("topmenu_item_link")[0].children[0].click();'},again);
    });	
}
function close(){
	chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {		
		chrome.tabs.executeScript(tabs[0].id,{code: 'document.getElementsByClassName("l-btn-text icon-ok l-btn-icon-left")[0].click();'},relocate);
    });
}

function again(){	
	sleep(1000);			
	index = index + 1;	
	if(index < config.stocks.length){
		open();
	}
}

function fill(){			
 	sleep(1000);
 	chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
    	chrome.tabs.executeScript(
        	tabs[0].id,
         	{file:'config.js'},
        	function(){
          		chrome.tabs.executeScript(tabs[0].id,{file: 'menu.js'},close);		
          	}
        );
    });
}

function open(){
	chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
		mySend(config.stocks[index].code);		
    	chrome.tabs.executeScript(
        	tabs[0].id,
         	{ code:'var stock_code =' + config.stocks[index].code +';'},
        	function(){
          		chrome.tabs.executeScript(tabs[0].id,{file: 'open.js'},fill);		
          	}
        );
        last_url = tabs[0].url;
    });	
}

autoFill.onclick = function(element) {
	chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {		
		chrome.tabs.executeScript(tabs[0].id,{code: 'document.getElementsByClassName("topmenu_item_link")[0].children[0].click();'},open);
    });	
};


menuFill.onclick = function(element) {    
    chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
    	chrome.tabs.executeScript(
        	tabs[0].id,
         	{file:'config.js'},
        	function(){
          		chrome.tabs.executeScript(tabs[0].id,{file: 'menu.js'});		
          	}
        );
    });
};

