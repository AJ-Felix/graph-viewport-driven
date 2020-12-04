function loadtestDataSet(){
    layout = false;
    buildTopViewOperations();
	vertexSet.forEach(
		function(array){
			addMessageToQueue(array);
			console.log(array);
		}
    )
    edgeSet.forEach(
		function(array){
			addMessageToQueue(array);
			console.log(array);
		}
    )
}

loadtestDataSet(); //uncomment this line if you like to load the test graph data set 


