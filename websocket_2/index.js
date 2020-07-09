var ws = new WebSocket("ws://localhost:8887/graphData");

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
		case 'layout':
			// var layout = cy.layout({name: 'fcose', ready: () => {console.log("Layout ready")}, stop: () => {console.log("Layout stopped")}});
			// layout.run();
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
		case 'removeSpatialSelection':
			var top = parseInt(dataArray[1]);
			var right = parseInt(dataArray[2]);
			var bottom = parseInt(dataArray[3]);
			var left = parseInt(dataArray[4]);
			cy.nodes().forEach(
				function (node){
				var pos = node.position();
					// console.log(node.position());
					// console.log(node.position().x);
					if ((pos.x > right) || (pos.x < left) || (pos.y > bottom) || (pos.y < top)) {
						cy.remove(node);
					}
				}
			)
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

function panRight(){
	ws.send("panRight");
}

function displayAll(){
	ws.send("displayAll");
}



var header1 = document.getElementById('header1');
header1.addEventListener("mousedown", 
	function(){
		console.log("Mouse went down on header1");
	}
);

document.getElementById('cy').addEventListener('mousedown', 
	function(){
		console.log("Mouse went down on cytoscape container");
	}
)