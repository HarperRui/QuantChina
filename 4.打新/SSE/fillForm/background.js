chrome.runtime.onMessage.addListener(
	function(request, sender, sendResponse) {		
		if (request.msg){
			console.log(request.msg);				
			sendResponse({msg: 200}); //回复
		}else{
			sendResponse({status: 0});
		}
});