function loadtestDataSet(){
    client.layout = false;
    client.buildTopViewOperations();
	vertexSet.forEach(
		function(array){
			client.addMessageToQueue(array);
			console.log(array);
		}
    )
    edgeSet.forEach(
		function(array){
			client.addMessageToQueue(array);
			console.log(array);
		}
    )
}

$(document).ready(function(){
// loadtestDataSet();  //uncomment this line if you like to load the test graph data set
});
