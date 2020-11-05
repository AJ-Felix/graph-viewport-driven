let ws = new WebSocket("ws://localhost:8897/graphData");

let handler;

ws.onopen = function() {
    console.log("Opened!");
    ws.send("Hello Server");
};

let messageQueue = new Array();
let messageProcessing;
let graphOperationLogic = "serverSide";
let layout = true;
let boundingBoxVar;
let boundingBoxVarOld;

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
				case 'addWrapper':
					handler.addWrapper(dataArray);
					break;
				case 'removeWrapper':
					handler.removeWrapper(dataArray);
					break;
				case 'addVertexServer':
					cy.add({group : 'nodes', data: {id: dataArray[1], label: dataArray[4]}, position: {x: parseInt(dataArray[2]) , y: parseInt(dataArray[3])}});
					if (!layout){
						clearTimeout(this.timeOut);
						this.timeOut = setTimeout(finalOperations, 1000);
					}
					break;
				case 'addVertexServerToBeLayouted':
					addVertexToLayoutBase(dataArray);
						clearTimeout(this.timeOut);
						this.timeOut = setTimeout(finalOperations, 1000);
					break;
				case 'addEdgeServer':
					cy.add({group : 'edges', data: {id: dataArray[1], source: dataArray[2], target: dataArray[3]}});
					console.log(cy.$id(dataArray[1]).style());
					if (!layout){
						clearTimeout(this.timeOut);
						this.timeOut = setTimeout(finalOperations, 1000);
					}
					break;
				case 'removeObjectServer':
					console.log(cy.$id(dataArray[1]));
					if (!layout) {
						console.log(layoutBase.length);
						console.log(layoutBase);
						layoutBase.delete(dataArray[1]);
						console.log(layoutBase.length);
					}
					cy.remove(cy.$id(dataArray[1]));
					console.log(cy.$id(dataArray[1]));
					break;
				case 'operationAndStep':
					operation = dataArray[1];
					operationStep = dataArray[2];
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
	if (graphOperationLogic == "clientSide") handler = new RetractHandler(maxNumberVertices);
	if (!layout) layoutBase = new Set();
	boundingBoxVar = {x1: 0, y1: 0, w: 4000, h: 4000};
	ws.send("buildTopView;retract");
}

function sendSignalAppendJoin(){
	if (graphOperationLogic == "clientSide") handler = new AppendHandler(maxNumberVertices);
	if (!layout) layoutBase = new Set();
	boundingBoxVar = {x1: 0, y1: 0, w: 4000, h: 4000};
	ws.send("buildTopView;appendJoin");
}

function sendSignalAdjacency(){
	if (graphOperationLogic == "clientSide")	handler = new AppendHandler(maxNumberVertices);
	if (!layout) layoutBase = new Set();
	boundingBoxVar = {x1: 0, y1: 0, w: 4000, h: 4000};
	ws.send("buildTopView;adjacency");
}

// function zoomOut(){
	// handler.operation = "zoomOut";
	// const topModel = 0;
	// const rightModel = 4000;
	// const bottomModel = 4000;
	// const leftModel = 0;
	// handler.prepareOperation(topModel, rightModel, bottomModel, leftModel);
	// ws.send("zoomOut;0;0;0.25");
// }

// function zoomIn(){
	// handler.operation = "zoomIn";
	// const topModel = 0;
	// const rightModel = 2000;
	// const bottomModel = 2000;
	// const leftModel = 0;
	// handler.prepareOperation(topModel, rightModel, bottomModel, leftModel);
	// ws.send("zoomIn;0;0;0.5");
// }

// function pan(){
	// const topModel = 0;
	// const rightModel = 3000;
	// const bottomModel = 2000;
	// const leftModel = 1000;
	// handler.operation = "pan";
	// handler.prepareOperation(topModel, rightModel, bottomModel, leftModel);
	// ws.send("pan;" + 1000 + ";" + 0);
// }

// function displayAll(){
	// handler = new AppendHandler();
	// ws.send("displayAll");
// }

// function cancelJob(){
	// let id;
	// var x = new XMLHttpRequest();
	// x.open("patch", "/");
	// x.send(null);
	// $.get('http://localhost:8081/jobs', function (data, textStatus, jqXHR) {
        // console.log('status: ' + textStatus + ', data:' + Object.keys(data));
		// console.log(data.jobs[0].id);
		// id = data.jobs[0].id;
		// // ws.send('cancel;' + id);
		// $.get('http://localhost:8081/jobs/' + id, function (data, textStatus, jqXHR) {
			// console.log('status: ' + textStatus + ', data:' + Object.keys(data));
		// });
		// var x = new XMLHttpRequest();
		// x.open("PATCH",  'http://localhost:8081/jobs/' + id);
		// x.send();
		// fetch('http://localhost:8081/jobs/' + id, {method: 'PATCH'});
		// $.ajax({
			// type: 'GET',
			// url: 'http://localhost:8081/jobs/' + id
		// });
		// $.ajax({
			// type: 'PATCH',
			// url: 'http://localhost:8081/jobs/' + id,
			// data: JSON.stringify({}),
			// processData: false,
			// headers: {
				// "Access-Control-Allow-Origin": '*',
				// 'Accept' : 'application/json; charset=UTF-8',
                // 'Content-Type' : 'application/json; charset=UTF-8'},
				 // error : function(jqXHR, textStatus, errorThrown) {
                // console.log("The following error occured: " + textStatus, errorThrown);

            // },
		// });
    // });
	// console.log(id);
	// console.log("job cancelled");
	// ws.send("cancel;" + id);
// }

// function testThread(){
	// handler = new AppendHandler();
	// ws.send("TestThread");
// }

function clientSideLogic(){
	graphOperationLogic = "clientSide";
	ws.send("clientSideLogic");
}

function serverSideLogic(){
	graphOperationLogic = "serverSide";
	ws.send("serverSideLogic");
}

function sendMaxVertices(maxVertices){
	maxNumberVertices = maxVertices;
	console.log("executing sendMaxVertices");
	ws.send("maxVertices;" + maxVertices);
}

function resetVisualization(){
	cy.elements().remove();
}

function preLayout(){
	layout = false;
	ws.send("preLayout");
}

function postLayout(){
	layout = true;
	ws.send("postLayout");
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