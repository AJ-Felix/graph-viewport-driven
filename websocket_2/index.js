var ws = new WebSocket("ws://localhost:8887/graphData");

var handler;

ws.onopen = function() {
    console.log("Opened!");
    ws.send("Hello Server");
};

class JoinHandler{
	vertexIncidenceMap = new Map();
	
	constructor(){
	}
	
	addVertex(dataArray){
		var vertexId = dataArray[1];
		var vertexX = dataArray[2];
		var vertexY = dataArray[3];
		if (!this.vertexIncidenceMap.has(vertexId)){
					this.vertexIncidenceMap.set(vertexId, 1);
		} else {
			this.vertexIncidenceMap.set(vertexId, this.vertexIncidenceMap.get(vertexId) + 1);
		}
		if (this.vertexIncidenceMap.get(vertexId) == 1) {
			cy.add({group : 'nodes', data: {id: vertexId}, position: {x: parseInt(vertexX) , y: parseInt(vertexY)}});
		}
	}
	
	removeVertex(dataArray){
		var vertexId = dataArray[1];
		if (!this.vertexIncidenceMap.has(vertexId)) {
				alert("cannot remove vertex because not in vertexIncidenceMap");
		} else {
			this.vertexIncidenceMap.set(vertexId, this.vertexIncidenceMap.get(vertexId) - 1);
			if (this.vertexIncidenceMap.get(vertexId) == 0) {
				this.vertexIncidenceMap.delete(vertexId);
				cy.remove(cy.$id(vertexId));
			}
		}
	}
	
	addEdge(dataArray){
		var edgeId = dataArray[1];
		var sourceVertex = dataArray[2];
		var targetVertex = dataArray[3];
		cy.add({group : 'edges', data: {id: edgeId, source: sourceVertex , target: targetVertex}});
	}
}

class MapHandler{
	vertexDegreeMap = new Map();
	edgePotential = false;
	
	constructor(){
	}
	
	addVertex(dataArray){
		var vertexId = dataArray[1];
		var vertexX = dataArray[2];
		var vertexY = dataArray[3];
		var vertexDegree = dataArray[4];
		if (vertexDegreeMap.size < 50){
			cy.add({group : 'nodes', data: {id: vertexId}, position: {x: parseInt(vertexX) , y: parseInt(vertexY)}});
			vertexDegreeMap.set(vertexId, vertexDegree);
			edgePotential = true;
		} else if (vertexDegreeMap.has(vertexId)) {
			edgePotential = true;
		} else {
			edgePotential = false;
			for ([key, value] of vertexDegreeMap.entries()){
				if (vertexDegree > value){
					cy.remove(cy.$id(vertexId));
					cy.add({group : 'nodes', data: {id: vertexId}, position: {x: parseInt(vertexX) , y: parseInt(vertexY)}});
					vertexDegreeMap.delete(key);
					vertexDegreeMap(vertexId, vertexDegree);
					edgePotential = true;
					break;
				}
			}
		}
	}
	
	addEdge(dataArray){
		var edgeId = dataArray[1];
		var sourceVertex = dataArray[2];
		var targetVertex = dataArray[3];
		cy.add({group : 'edges', data: {id: edgeId, source: sourceVertex , target: targetVertex}})
	}
}

ws.onmessage = function (evt) {
	var dataArray = evt.data.split(";");
	var vertexId = dataArray[1];
	var vertexX = dataArray[2];
	var vertexY = dataArray[3];
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
			handler.addVertex(dataArray);
			break;
		case 'removeVertex':
			handler.removeVertex(dataArray);
			break;
		case 'addEdge':
			handler.addEdge(dataArray);
			break;
		case 'removeSpatialSelection':
			var top = parseInt(vertexId);
			var right = parseInt(vertexX);
			var bottom = parseInt(vertexY);
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

function sendSignalRetract(){
	handler = new JoinHandler();
	ws.send("buildTopView;retract");
}

function sendSignalAppendJoin(){
	handler = new JoinHandler();
	ws.send("buildTopView;appendJoin");
}

function sendSignalAppendMap(){
	handler = new MapHandler();
	ws.send("buildTopView;appendMap");
}

function zoomIn(){
	ws.send("zoomTopLeftCorner");
}

function panRight(){
	ws.send("panRight");
}

function displayAll(){
	dataArray = ["addVertex", "0", "200", "300"];
	handler = new RetractHandler();
	handler.addVertex(dataArray);
	// ws.send("displayAll");
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