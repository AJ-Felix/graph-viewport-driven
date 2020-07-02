var ws = new WebSocket("ws://localhost:8887/graphData")

ws.onopen = function() {
    console.log("Opened!");
    ws.send("Hello Server");
};

ws.onmessage = function (evt) {
	var dataArray = evt.data.split(";");
    console.log("Message: " + evt.data);
	switch (dataArray[0]){
		case 'clearGraph':
			console.log('clearing graph');
			cy.elements().remove();
		break;
		case 'fitGraph':
			console.log('fitting graph');
			cy.fit();
		break;
		case 'addVertex':
			console.log("add Vertex!!!");
			console.log(dataArray[1]);
			// cy.add(dataArray[1]);
			console.log("{group : 'nodes', data: {id: '" + dataArray[1] + "'}, position: {x: " + dataArray[2] + ", y: " + dataArray[3] + "} }");
			cy.add({group : 'nodes', data: {id: dataArray[1]}, position: {x: parseInt(dataArray[2]) , y: parseInt(dataArray[3])} });
		break;
		case 'removeVertex':
			console.log('remove: vertex id is' + dataArray[1]);
			cy.remove(cy.$id(dataArray[1]));
		break;
		case 'addEdge':
			console.log('add edge: id is '+ dataArray[1]);
			console.log(dataArray[1] +dataArray[2] + dataArray[3]);
			cy.add({group : 'edges', data: {id: dataArray[1], source: dataArray[2] , target: dataArray[3]} });
		break;
	}
};

ws.onclose = function() {
    console.log("Closed!");
};

ws.onerror = function(err) {
    console.log("Error: " + err);
};

function sendSignal(){
	ws.send("buildTopView");
}

function zoomIn(){
	ws.send("zoomTopLeftCorner");
}
