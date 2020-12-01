// let ws = new WebSocket("ws://139.18.13.19:8897/graphData");

let ws;
let handler;

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
						this.timeOut = setTimeout(finalOperations, 2000);
					}
					break;
				case 'addVertexServerToBeLayouted':
					addVertexToLayoutBase(dataArray);
						clearTimeout(this.timeOut);
						this.timeOut = setTimeout(finalOperations, 2000);
					break;
				case 'addEdgeServer':
					cy.add({group : 'edges', data: {id: dataArray[1], source: dataArray[2], target: dataArray[3]}});
					console.log(cy.$id(dataArray[1]).style());
					if (!layout){
						clearTimeout(this.timeOut);
						this.timeOut = setTimeout(finalOperations, 2000);
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
				case 'fit':
					cy.fit();
					const pan = cy.pan();
					ws.send("fitted;" + pan.x + ";" + pan.y + ";" + cy.zoom());
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

function sendClusterEntryAddress(address){
	ws.send("clusterEntryAddress;" + address);
}

function sendHDFSEntryAddress(address){
	ws.send("hDFSEntryAddress;" + address);
}

function sendHDFSEntryPort(port){
	ws.send("hDFSEntryPointPort;" + port);
}

function sendHDFSGraphFilesDirectory(directory){
	ws.send("hDFSGraphFilesDirectory;" + directory);
}

function sendGradoopGraphId(id){
	ws.send("gradoopGraphId;" + id);
}

function sendVerticesHaveDegrees(haveDegrees){
	console.log(haveDegrees);
	ws.send("degrees;" + haveDegrees.to);
}

function sendGraphIsLayouted(IsLayouted){
	console.log(IsLayouted);
	layout = IsLayouted;
	ws.send("layoutMode;" + IsLayouted);
}

function sendSignalHBase(){
	buildTopViewOperations();
	ws.send("buildTopView;HBase");
}

function sendSignalGradoop(){
	buildTopViewOperations();
	ws.send("buildTopView;gradoop");
}

function sendSignalCSV(){
	buildTopViewOperations();
	ws.send("buildTopView;CSV");
}

function sendSignalAdjacency(){
	buildTopViewOperations();
	ws.send("buildTopView;adjacency");
}

function buildTopViewOperations(){
	if (!layout) layoutBase = new Set();
	boundingBoxVar = {x1: 0, y1: 0, x2: 4000, y2: 4000};
	cy.zoom(1 / (4000 / Math.min(cyWidth, cyHeight)));
}

function sendMaxVertices(maxVertices){
	maxNumberVertices = maxVertices;
	console.log("executing sendMaxVertices");
	ws.send("maxVertices;" + maxVertices);
}

function resetVisualization(){
	cy.elements().remove();
	cy.pan({x: 0, y: 0});
	nodeWidth = 50;
	nodeHeight = 50;
	nodeLabelFontSize = 32;
	edgeWidth = 5;
	edgeArrowSize = 2;
	cy.style().selector('node').style({
			'width': nodeWidth,
			'height': nodeHeight,
			'font-size': nodeLabelFontSize
		}).update();
		cy.style().selector('edge').style({
			'width': edgeWidth,
			'arrow-scale': edgeArrowSize
		}).update();
	ws.send("resetWrapperHandler");
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

$(document).ready(function(){
    ws = new WebSocket("ws://" + jsonObject.ServerIp4 + ":8897/graphData");
	
	ws.onopen = function() {
		console.log("Opened!");
		ws.send("Hello Server");
		resized();
	}

	ws.onmessage = function (evt) {
		console.log(evt.data);
		const dataArray = evt.data.split(";");
		addMessageToQueue(dataArray);
	}
	
	ws.onclose = function() {
		console.log("Closed!");
	};

	ws.onerror = function(err) {
		console.log("Error: " + err);
	};
});

const heightOutput = document.querySelector('#height');
const widthOutput = document.querySelector('#width');

const boundingClientRect = document.getElementById('cy').getBoundingClientRect();
let cyHeight = boundingClientRect.height;
let cyWidth = boundingClientRect.width;
let cyHeightHalf = cyHeight / 2;
let cyWidthHalf = cyWidth / 2;



function resized(){
	const boundingClientRect = document.getElementById('cy').getBoundingClientRect();
	cyHeightOld = cyHeight;
	cyWidthOld = cyWidth;
	cyHeight = boundingClientRect.height;
	cyWidth = boundingClientRect.width;
	if (someCondition){
		resizeGraph(cyHeightOld, cyWidthOld, cyHeight, cyWidth);
	}
	cyHeightHalf = cyHeight / 2;
	cyWidthHalf = cyWidth / 2;
	console.log("resizing after timeout");
	heightOutput.textContent = cyWidth;
	widthOutput.textContent = cyHeight;
	const pan = cy.pan();
	ws.send("viewportSize;" + pan.x + ";" + pan.y + ";" + cy.zoom() + ";" +  cyWidth + ";" + cyHeight);
}

function resizeGraph(cyHeightOld, cyWidthOld, cyHeight, cyWidth){
	
}

var resizedTimeOut;
window.onresize = function(){
  clearTimeout(resizedTimeOut);
  resizedTimeOut = setTimeout(resized, 100);
};