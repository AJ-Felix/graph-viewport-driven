var ws = new WebSocket("ws://localhost:8887/graphData");

var handler;

ws.onopen = function() {
    console.log("Opened!");
    ws.send("Hello Server");
};

let messageQueue = new Array();
let messageProcessing;

function addMessageToQueue(dataArray){
		messageQueue.push(dataArray);
		if (!messageProcessing) {
			messageProcessing = true;
			processMessage(); 
		}
	}

ws.onmessage = function (evt) {
	console.log(evt.data);
	const dataArray = evt.data.split(";");
	addMessageToQueue(dataArray);
}
	
async function processMessage(){
	if (messageQueue.length > 0){
		let dataArray = messageQueue.shift();
		let promise = new Promise((resolve, reject) => {
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
					handler.addWrapperToQueue(dataArray, true);
					break;
				case 'removeWrapper':
					handler.addWrapperToQueue(dataArray, false);
					break;
				case 'addVertexServer':
					cy.add({group : 'nodes', data: {id: dataArray[1]}, position: {x: parseInt(dataArray[2]) , y: parseInt(dataArray[3])}});
					break;
				case 'addEdgeServer':
					console.log(dataArray[1]);
					console.log(dataArray[2]);
					console.log(dataArray[3]);
					cy.add({group : 'edges', data: {id: dataArray[1], source: dataArray[2], target: dataArray[3]}});
					break;
				case 'removeObjectServer':
					cy.remove(cy.$id(dataArray[1]));
					break;
			}
			resolve(true);
		});
		await promise;
		processMessage();
	} else {
		messageProcessing = false;
	}
}

ws.onclose = function() {
    console.log("Closed!");
};

ws.onerror = function(err) {
    console.log("Error: " + err);
};

function sendSignalRetract(){
	handler = new RetractHandler();
	handler.operation = "initial";
	handler.newVerticesMap = new Map();
	ws.send("buildTopView;retract");
}

function sendSignalAppendJoin(){
	handler = new AppendHandler();
	handler.operation = "initial";
	handler.newVerticesMap = new Map();
	ws.send("buildTopView;appendJoin");
}

function sendSignalAppendMap(){
	handler = new MapHandler(50);
	ws.send("buildTopView;appendMap");
}

function sendSignalAdjacency(){
	handler = new AppendHandler();
	handler.operation = "initial";
	handler.newVerticesMap = new Map();
	ws.send("buildTopView;adjacency");
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

function pan(){
	const topModel = 0;
	const rightModel = 3000;
	const bottomModel = 2000;
	const leftModel = 1000;
	handler.operation = "pan";
	handler.prepareOperation(topModel, rightModel, bottomModel, leftModel);
	ws.send("pan;" + 1000 + ";" + 0);
}

function displayAll(){
	handler = new AppendHandler();
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
	handler = new AppendHandler();
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