var ws = new WebSocket("ws://localhost:8887/graphData");

var vertexIncidenceMap = new Map();

ws.onopen = function() {
    console.log("Opened!");
    ws.send("Hello Server");
};

ws.onmessage = function (evt) {
	console.log("Map size is :" + vertexIncidenceMap.size.toString());
	var dataArray = evt.data.split(";");
	var vertexId = dataArray[1];
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
			if (!vertexIncidenceMap.has(vertexId)){
				vertexIncidenceMap.set(vertexId, 1);
			} else {
				vertexIncidenceMap.set(vertexId, vertexIncidenceMap.get(vertexId) + 1);
			}
			console.log("add Vertex!!!");
			console.log(vertexId);
			// cy.add(vertexId);
			console.log("{group : 'nodes', data: {id: '" + vertexId + "'}, position: {x: " + dataArray[2] + ", y: " + dataArray[3] + "} }");
			if (vertexIncidenceMap.get(vertexId) == 1) {
				cy.add({group : 'nodes', data: {id: vertexId}, position: {x: parseInt(dataArray[2]) , y: parseInt(dataArray[3])} });
			}
		break;
		case 'removeVertex':
			//debug
			if (!vertexIncidenceMap.has(vertexId)) {
				alert("cannot remove vertex because not in vertexIncidenceMap");
			} else {
				vertexIncidenceMap.set(vertexId, vertexIncidenceMap.get(vertexId) - 1);
				if (vertexIncidenceMap.get(vertexId) == 0) {
					vertexIncidenceMap.delete(vertexId);
					cy.remove(cy.$id(vertexId));
					console.log('remove: vertex id is' + vertexId);
				}
			}
		break;
		case 'addEdge':
			console.log('add edge: id is '+ vertexId);
			console.log(vertexId +dataArray[2] + dataArray[3]);
			cy.add({group : 'edges', data: {id: vertexId, source: dataArray[2] , target: dataArray[3]} });
		break;
		case 'removeSpatialSelection':
			var top = parseInt(vertexId);
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