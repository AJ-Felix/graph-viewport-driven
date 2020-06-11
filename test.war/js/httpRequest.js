function httpGetAsync(theUrl, callback)
{
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.onreadystatechange = function() { 
        if (xmlHttp.readyState == 4 && xmlHttp.status == 200)
            callback(xmlHttp.responseText);
    }
    xmlHttp.open("GET", theUrl, true); // true for asynchronous 
    xmlHttp.send(null);
}

function callback(responseText){
	console.log("hello");
	console.log("hello");
	return null;
}

	function flinkTestResponse(){
		console.log("this is the  flink test response function");
		console.log($.get("http://localhost:8081/test/testFunction"));
		$.get("http://localhost:8081/test/testFunction", function( data ) {
					console.log(data);
					alert( "Load was performed." );
				})
			.done(function(){
				console.log("success");
			})
			.fail(function(){
				console.log("fail");
			})
			.always(function(){
				console.log("always");
			});
	}


$(document).ready(flinkTestResponse());
$(document).ready(httpGetAsync("localhost:8080/test", callback));

