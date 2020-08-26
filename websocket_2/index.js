var ws = new WebSocket("ws://localhost:8887/graphData");

var handler;

ws.onopen = function() {
    console.log("Opened!");
    ws.send("Hello Server");
};

// class JoinHandler{
	// vertexIncidenceMap = new Map();
	// edgeSet = new Set();
	
	// constructor(){
	// }
	
	// resetMap(){
		// this.vertexIncidenceMap = new Map();
	// }
	
	// addVertex(dataArray){
		// var vertexId = dataArray[1];
		// var vertexDegree = dataArray[2];
		// var vertexX = dataArray[3];
		// var vertexY = dataArray[4];
		// if (!this.vertexIncidenceMap.has(vertexId)){
			// this.vertexIncidenceMap.set(vertexId, {'incidence': 1, 'degree': vertexDegree});
			// // console.log("adding vertex" + vertexId + " " + vertexX + " " + vertexY);
		// } else {
			// // console.log("vertex already there: " + vertexId + " " + vertexX + " " + vertexY);
			// this.vertexIncidenceMap.set(vertexId, this.vertexIncidenceMap.get(vertexId)['incidence'] + 1);
		// }
		// if (this.vertexIncidenceMap.get(vertexId)['incidence'] == 1) {
			// cy.add({group : 'nodes', data: {id: vertexId}, position: {x: parseInt(vertexX) , y: parseInt(vertexY)}});
		// }
	// }
	
	// removeVertex(dataArray){
		// var vertexId = dataArray[1];
		// if (!this.vertexIncidenceMap.has(vertexId)) {
				// alert("cannot remove vertex because not in vertexIncidenceMap");
		// } else {
			// this.vertexIncidenceMap.set(vertexId, this.vertexIncidenceMap.get(vertexId)['incidence'] - 1);
			// if (this.vertexIncidenceMap.get(vertexId)['incidence'] == 0) {
				// this.vertexIncidenceMap.delete(vertexId);
				// cy.remove(cy.$id(vertexId));
			// }
		// }
	// }
	
	// addEdge(dataArray){
		// var edgeId = dataArray[1];
		// var sourceVertex = dataArray[2];
		// var targetVertex = dataArray[3];
		// if (!this.edgeSet.has(edgeId)) cy.add({group : 'edges', data: {id: edgeId, source: sourceVertex , target: targetVertex}});
		// this.edgeSet.add(edgeId);
	// }
	
	// removeSpatialSelectionTop(bottomModelPrevious, yModelDiff){
		// var map = this.vertexIncidenceMap;
		// var set = this.edgeSet;
		// cy.edges().forEach(
			// function (edge){
				// var sourceId = edge.data('source');
				// var targetId = edge.data('target');
				// var sourcePos = cy.getElementById(sourceId).position();
				// var targetPos = cy.getElementById(targetId).position();
				// if (((sourcePos.y < bottomModelPrevious) && (sourcePos.y > bottomModelPrevious + yModelDiff)) || 
						// ((targetPos.y < bottomModelPrevious) && (targetPos.y > bottomModelPrevious + yModelDiff))) {
					// cy.remove(edge);
					// set.delete(edge.data('id'));
				// }
			// }	
		// )
		// cy.nodes().forEach(
			// function (node){
			// var pos = node.position();
				// if ((pos.y < bottomModelPrevious) && (pos.y > bottomModelPrevious + yModelDiff)){
					// cy.remove(node);
					// map.delete(node.data('id'));
				// }
			// }
		// )
	// }
	
	// removeSpatialSelectionTopRight(bottomModelPrevious, leftModelPrevious, xModelDiff, yModelDiff){
		// var map = this.vertexIncidenceMap;
		// var set = this.edgeSet;
		// cy.edges().forEach(
			// function (edge){
				// var sourceId = edge.data('source');
				// var targetId = edge.data('target');
				// var sourcePos = cy.getElementById(sourceId).position();
				// var targetPos = cy.getElementById(targetId).position();
				// if (((sourcePos.y < bottomModelPrevious) && (sourcePos.y > bottomModelPrevious + yModelDiff)) || 
						// ((targetPos.y < bottomModelPrevious) && (targetPos.y > bottomModelPrevious + yModelDiff)) ||
						// ((sourcePos.x > leftModelPrevious) && (sourcePos.x < leftModelPrevious + xModelDiff)) ||
							// ((targetPos.x > leftModelPrevious) && (targetPos.x < leftModelPrevious + xModelDiff))) {
					// cy.remove(edge);
					// set.delete(edge.data('id'));
				// }
			// }	
		// )
		// cy.nodes().forEach(
			// function (node){
			// var pos = node.position();
				// if (((pos.y < bottomModelPrevious) && (pos.y > bottomModelPrevious + yModelDiff)) ||
						// ((pos.x > leftModelPrevious) && (pos.x < leftModelPrevious + xModelDiff))){
					// cy.remove(node);
					// map.delete(node.data('id'));
				// }
			// }
		// )
	// }
	
	// removeSpatialSelectionRight(leftModelPrevious, xModelDiff){
		// var map = this.vertexIncidenceMap;
		// var set = this.edgeSet;
		// cy.edges().forEach(
			// function (edge){
				// var sourceId = edge.data('source');
				// var targetId = edge.data('target');
				// var sourcePos = cy.getElementById(sourceId).position();
				// var targetPos = cy.getElementById(targetId).position();
				// if (((sourcePos.x > leftModelPrevious) && (sourcePos.x < leftModelPrevious + xModelDiff)) ||
							// ((targetPos.x > leftModelPrevious) && (targetPos.x < leftModelPrevious + xModelDiff))) {
						// cy.remove(edge);
						// set.delete(edge.data('id'));
				// }
			// }	
		// )
		// cy.nodes().forEach(
			// function (node){
			// var pos = node.position();
				// if ((pos.x > leftModelPrevious) && (pos.x < leftModelPrevious + xModelDiff)){
					// cy.remove(node);
					// map.delete(node.data('id'));
				// }
			// }
		// )
	// }
	
	// removeSpatialSelectionBottomRight(topModelPrevious, leftModelPrevious, xModelDiff, yModelDiff){
		// var map = this.vertexIncidenceMap;
		// var set = this.edgeSet;
		// cy.edges().forEach(
			// function (edge){
				// var sourceId = edge.data('source');
				// var targetId = edge.data('target');
				// var sourcePos = cy.getElementById(sourceId).position();
				// var targetPos = cy.getElementById(targetId).position();
				// if (((sourcePos.x > leftModelPrevious) && (sourcePos.x < leftModelPrevious + xModelDiff)) ||
							// ((targetPos.x > leftModelPrevious) && (targetPos.x < leftModelPrevious + xModelDiff)) ||
							// ((sourcePos.y > topModelPrevious) && (sourcePos.y < topModelPrevious + yModelDiff)) ||
							// ((targetPos.y > topModelPrevious) && (targetPos.y < topModelPrevious + yModelDiff))) {
						// console.log("removing edge: " + sourcePos.x + " " + sourcePos.y + " " + targetPos.x + " " + targetPos.y);
						// cy.remove(edge);
						// set.delete(edge.data('id'));
				// }
			// }	
		// )
		// cy.nodes().forEach(
			// function (node){
			// var pos = node.position();
				// if (((pos.y > topModelPrevious) && (pos.y < topModelPrevious + yModelDiff)) || 
						// ((pos.x > leftModelPrevious) && (pos.x < leftModelPrevious + xModelDiff))) { 
					// console.log("removing node: " + node.data('id') + " " + pos.x + " " + pos.y);
					// cy.remove(node);
					// map.delete(node.data('id'));
				// }
			// }
		// )
	// }
	
	// removeSpatialSelectionBottom(topModelPrevious,  yModelDiff){
		// var map = this.vertexIncidenceMap;
		// var set = this.edgeSet;
		// cy.edges().forEach(
			// function (edge){
				// var sourceId = edge.data('source');
				// var targetId = edge.data('target');
				// var sourcePos = cy.getElementById(sourceId).position();
				// var targetPos = cy.getElementById(targetId).position();
				// if (((sourcePos.y > topModelPrevious) && (sourcePos.y < topModelPrevious + yModelDiff)) ||
							// ((targetPos.y > topModelPrevious) && (targetPos.y < topModelPrevious + yModelDiff))) {
						// cy.remove(edge);
						// set.delete(edge.data('id'));
				// }
			// }	
		// )
		// cy.nodes().forEach(
			// function (node){
			// var pos = node.position();
				// if ((pos.y > topModelPrevious) && (pos.y < topModelPrevious + yModelDiff)) { 
					// cy.remove(node);
					// map.delete(node.data('id'));
				// }
			// }
		// )
	// }
	
	// removeSpatialSelectionBottomLeft(topModelPrevious, rightModelPrevious, xModelDiff, yModelDiff){
		// var map = this.vertexIncidenceMap;
		// var set = this.edgeSet;
		// cy.edges().forEach(
			// function (edge){
				// var sourceId = edge.data('source');
				// var targetId = edge.data('target');
				// var sourcePos = cy.getElementById(sourceId).position();
				// var targetPos = cy.getElementById(targetId).position();
				// if (((sourcePos.y > topModelPrevious) && (sourcePos.y < topModelPrevious + yModelDiff)) ||
							// ((targetPos.y > topModelPrevious) && (targetPos.y < topModelPrevious + yModelDiff)) ||
							// ((sourcePos.x < rightModelPrevious) && (sourcePos.x > rightModelPrevious + xModelDiff)) || 
							// ((targetPos.x < rightModelPrevious) && (targetPos.x > rightModelPrevious + xModelDiff))) {
						// cy.remove(edge);
						// set.delete(edge.data('id'));
				// }
			// }	
		// )
		// cy.nodes().forEach(
			// function (node){
			// var pos = node.position();
				// if (((pos.y > topModelPrevious) && (pos.y < topModelPrevious + yModelDiff)) ||
						// ((pos.x < rightModelPrevious) && (pos.x > rightModelPrevious + xModelDiff))) { 
					// cy.remove(node);
					// map.delete(node.data('id'));
				// }
			// }
		// )
	// }
	
	// removeSpatialSelectionLeft(rightModelPrevious, xModelDiff){
		// var map = this.vertexIncidenceMap;
		// var set = this.edgeSet;
		// cy.edges().forEach(
			// function (edge){
				// var sourceId = edge.data('source');
				// var targetId = edge.data('target');
				// var sourcePos = cy.getElementById(sourceId).position();
				// var targetPos = cy.getElementById(targetId).position();
				// if (((sourcePos.x < rightModelPrevious) && (sourcePos.x > rightModelPrevious + xModelDiff)) || 
							// ((targetPos.x < rightModelPrevious) && (targetPos.x > rightModelPrevious + xModelDiff))) {
						// cy.remove(edge);
						// set.delete(edge.data('id'));
				// }
			// }	
		// )
		// cy.nodes().forEach(
			// function (node){
			// var pos = node.position();
				// if ((pos.x < rightModelPrevious) && (pos.x > rightModelPrevious + xModelDiff)) { 
					// cy.remove(node);
					// map.delete(node.data('id'));
				// }
			// }
		// )
	// }
	
	// removeSpatialSelectionTopLeft(rightModelPrevious, bottomModelPrevious, xModelDiff, yModelDiff){
		// var map = this.vertexIncidenceMap;
		// var set = this.edgeSet;
		// cy.edges().forEach(
			// function (edge){
				// var sourceId = edge.data('source');
				// var targetId = edge.data('target');
				// var sourcePos = cy.getElementById(sourceId).position();
				// var targetPos = cy.getElementById(targetId).position();
				// if (((sourcePos.x < rightModelPrevious) && (sourcePos.x > rightModelPrevious + xModelDiff)) || 
							// ((targetPos.x < rightModelPrevious) && (targetPos.x > rightModelPrevious + xModelDiff)) ||
							// ((sourcePos.y < bottomModelPrevious) && (sourcePos.y > bottomModelPrevious + yModelDiff)) || 
							// ((targetPos.y < bottomModelPrevious) && (targetPos.y > bottomModelPrevious + yModelDiff))) {
						// cy.remove(edge);
						// set.delete(edge.data('id'));
				// }
			// }	
		// )
		// cy.nodes().forEach(
			// function (node){
			// var pos = node.position();
				// if (((pos.y < bottomModelPrevious) && (pos.y > bottomModelPrevious + yModelDiff)) || 
						// ((pos.x < rightModelPrevious) && (pos.x > rightModelPrevious + xModelDiff))) { 
					// cy.remove(node);
					// map.delete(node.data('id'));
				// }
			// }
		// )
	// }
	// removeSpatialSelectionZoomIn(topModel, rightModel, bottomModel, leftModel){
		// var map = this.vertexIncidenceMap;
		// var set = this.edgeSet;
		// var countRemoved = 0;
		// cy.edges().forEach(
			// function (edge){
				// var sourceId = edge.data('source');
				// var targetId = edge.data('target');
				// var sourcePos = cy.getElementById(sourceId).position();
				// var targetPos = cy.getElementById(targetId).position();
				// if ((sourcePos.x > rightModel) || (sourcePos.x < leftModel) || 
						// (targetPos.x > rightModel) || (targetPos.x < leftModel) ||
						// (sourcePos.y > bottomModel) || (sourcePos.y < topModel) ||
						// (targetPos.y > bottomModel) || (targetPos.y < topModel)) {
					// cy.remove(edge);
					// set.delete(edge.data('id'));
				// }
			// }
		// )
		// cy.nodes().forEach(
			// function (node){
			// var pos = node.position();
				// if ((pos.x > rightModel) || (pos.x < leftModel) || (pos.y > bottomModel) || (pos.y < topModel)) {
					// cy.remove(node);
					// map.delete(node.data('id'));
					// countRemoved += 1;
				// }
			// }
		// )
		// return countRemoved;
	// }
	
	// addVertexZoomOut(){
		
	// }
	
	// removeSpatialSelectionZoomOut(topModel, rightModel, bottomModel, leftModel){
		// cy.edges().forEach(
			// function (edge){
				
			// }
		// )
	// }
// }

class MapHandler{
	vertexDegreeMap = new Map();
	edgePotentialSource;
	edgePotentialTarget;
	
	constructor(vertexCountMax){
		this.vertexCountMax = vertexCountMax;
	}
	
	resetMap(){
		this.vertexDegreeMap = new Map();
	}
	
	addVertexHelper(vertexId, vertexX, vertexY, vertexDegree){
		var edgePotential = false;
		if (this.vertexDegreeMap.has(vertexId)) {
			edgePotential = true;
		} else if (this.vertexDegreeMap.size < this.vertexCountMax){
			cy.add({group : 'nodes', data: {id: vertexId}, position: {x: parseInt(vertexX) , y: parseInt(vertexY)}});
			this.vertexDegreeMap.set(vertexId, vertexDegree);
			edgePotential = true;
		} else {
			let removalCandidateKey = -1;
			let removalCandidateDegree =  Infinity;
			for (const [key, value] of this.vertexDegreeMap.entries()){
				if ((parseInt(value) < removalCandidateDegree) || (parseInt(value) == removalCandidateDegree && key > removalCandidateKey)){
					removalCandidateKey = key;
					removalCandidateDegree = value;
				}
			}
			if ((vertexDegree > removalCandidateDegree) || (vertexDegree == removalCandidateDegree && vertexId < removalCandidateKey)){
				cy.remove(cy.$id(removalCandidateKey));
				cy.add({group : 'nodes', data: {id: vertexId}, position: {x: parseInt(vertexX) , y: parseInt(vertexY)}});
				this.vertexDegreeMap.delete(removalCandidateKey);
				this.vertexDegreeMap.set(vertexId, vertexDegree);
				edgePotential = true;
			}
		}
		return edgePotential;
	}
		
	addWrapper(dataArray){
		this.edgePotentialSource = false;
		this.edgePotentialTarget = false;
		const sourceVertexId = dataArray[1];
		const sourceVertexX = dataArray[2];
		const sourceVertexY = dataArray[3];
		const sourceVertexDegree = parseInt(dataArray[4]);
		const targetVertexId = dataArray[5];
		const targetVertexX = dataArray[6];
		const targetVertexY = dataArray[7];
		const targetVertexDegree = parseInt(dataArray[8]);
		const edgeIdGradoop = dataArray[9];
		this.edgePotentialSource = this.addVertexHelper(sourceVertexId, sourceVertexX, sourceVertexY, sourceVertexDegree);
		this.edgePotentialTarget = this.addVertexHelper(targetVertexId, targetVertexX, targetVertexY, targetVertexDegree);
		if (this.edgePotentialSource && this.edgePotentialTarget){
			cy.add({group : 'edges', data: {id: edgeIdGradoop, source: sourceVertexId , target: targetVertexId}});
		}
	}

}

ws.onmessage = function (evt) {
	console.log(evt.data);
	const dataArray = evt.data.split(";");
	switch (dataArray[0]){
		case 'clearGraph':
			console.log('clearing graph');
			cy.elements().remove();
			break;
		case 'layout':
			var layout = cy.layout({name: 'fcose', ready: () => {console.log("Layout ready")}, stop: () => {console.log("Layout stopped")}});
			layout.run();
			break;
		case 'fitGraph':
			// cy.fit();
			cy.zoom(0.25);
			cy.pan({x:0, y:0});
			break;
		case 'positioning':
			console.log('position viewport!');
			cy.zoom(parseFloat(dataArray[1]));
			cy.pan({x:parseInt(dataArray[2]), y:parseInt(dataArray[3])});
			break;
		// case 'pan':
			// console.log('panning');
			// cy.pan({x:parseInt(dataArray[1]), y:parseInt(dataArray[2])});
			// break;
		case 'addVertex':
			handler.addVertex(dataArray);
			break;
		case 'removeVertex':
			handler.removeVertex(dataArray);
			break;
		case 'addEdge':
			handler.addEdge(dataArray);
			break;
		case 'addWrapper':
			handler.addWrapperToQueue(dataArray);
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
	handler.operation = "initial";
	handler.newVerticesMap = new Map();
	ws.send("buildTopView;appendJoin");
}

function sendSignalAppendMap(){
	handler = new MapHandler(50);
	ws.send("buildTopView;appendMap");
}

function zoomOut(){
	handler.operation = "zoomOut";
	const topModel = 0;
	const rightModel = 4000;
	const bottomModel = 4000;
	const leftModel = 0;
	handler.prepareOperation(topModel, rightModel, bottomModel, leftModel);
	ws.send("zoomOut;0;0;0.25");
}

function zoomIn(){
	handler.operation = "zoomIn";
	const topModel = 0;
	const rightModel = 2000;
	const bottomModel = 2000;
	const leftModel = 0;
	handler.prepareOperation(topModel, rightModel, bottomModel, leftModel);
	ws.send("zoomIn;0;0;0.5");
}

// function zoomIn(){
	// var previousTop = 0;
	// var previousRight = 4000;
	// var previousBottom = 4000;
	// var previousLeft = 0;
	// var top = 0;
	// var right = 2000;
	// var bottom = 2000;
	// var left = 0;
	// ws.send("zoomIn;" + previousTop.toString() + ";" + previousRight.toString() + ";" + previousBottom.toString() + ";" + previousLeft.toString() + ";" + top.toString() + ";" + 
		// right.toString() + ";" + bottom.toString() + ";" + left.toString());
	// handler.removeSpatialSelection(top, right, bottom, left);
// }

function pan(){
	const topModel = 0;
	const rightModel = 3000;
	const bottomModel = 2000;
	const leftModel = 1000;
	handler.operation = "pan";
	handler.prepareOperation(topModel, rightModel, bottomModel, leftModel);
	ws.send("pan;" + 1000 + ";" + 0);
}

// function pan(){
	// //hard-coded example
	// var top = 0;
	// var right = 2000;
	// var bottom = 2000;
	// var left = 0;
	// var xDiff = -400;
	// var yDiff = 0;
	// cy.pan({x:-400, y:0});
	// ws.send("pan;" + xDiff.toString() + ";" + yDiff.toString());
	// handler.removeSpatialSelection(top + yDiff, right + xDiff, bottom + yDiff, left + xDiff);
// }

function removeSpatialSelection(top, right, bottom, left){
	cy.nodes().forEach(
		function (node){
		const pos = node.position();
			// console.log(node.position());
			// console.log(node.position().x);
			if ((pos.x > right) || (pos.x < left) || (pos.y > bottom) || (pos.y < top)) {
				cy.remove(node);
				
			}
		}
	)
}

function removeAll(){
	cy.elements().remove();
	handler.resetMap();
}

function displayAll(){
	handler = new JoinHandler();
	ws.send("displayAll");
}

function cancelJob(){
	let id;
	// var x = new XMLHttpRequest();
	// x.open("patch", "/");
	// x.send(null);
	$.get('http://localhost:8081/jobs', function (data, textStatus, jqXHR) {
        console.log('status: ' + textStatus + ', data:' + Object.keys(data));
		console.log(data.jobs[0].id);
		id = data.jobs[0].id;
		// ws.send('cancel;' + id);
		$.get('http://localhost:8081/jobs/' + id, function (data, textStatus, jqXHR) {
			console.log('status: ' + textStatus + ', data:' + Object.keys(data));
		});
		// var x = new XMLHttpRequest();
		// x.open("PATCH",  'http://localhost:8081/jobs/' + id);
		// x.send();
		fetch('http://localhost:8081/jobs/' + id, {method: 'PATCH'});
		$.ajax({
			type: 'GET',
			url: 'http://localhost:8081/jobs/' + id
		});
		$.ajax({
			type: 'PATCH',
			url: 'http://localhost:8081/jobs/' + id,
			data: JSON.stringify({}),
			processData: false,
			headers: {
				"Access-Control-Allow-Origin": '*',
				'Accept' : 'application/json; charset=UTF-8',
                'Content-Type' : 'application/json; charset=UTF-8'},
				 error : function(jqXHR, textStatus, errorThrown) {

                // log the error to the console

                console.log("The following error occured: " + textStatus, errorThrown);

            },
		});
    });
	console.log(id);
	console.log("job cancelled");
	// ws.send("cancel;" + id);
}

function testThread(){
	handler = new JoinHandler();
	ws.send("TestThread");
}

let header1 = document.getElementById('header1');
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