class Client {

	constructor (ws){
		this.ws = ws;
		this.messageQueue = new Array();
		this.messageProcessing = false;
		this.layout = true
		this.graphVisualizer = new GraphVisualizer();
		this.timeOut = 200;
	}

	addMessageToQueue(dataArray){
		this.messageQueue.push(dataArray);
		if (!this.messageProcessing) {
			this.messageProcessing = true;
			this.processMessage(); 
		}
	}
		
	async processMessage(){
		if (this.messageQueue.length > 0){
			const dataArray = this.messageQueue.shift();
			let promise = new Promise((resolve) => {
				let client;
				switch (dataArray[0]){
					case 'addVertexServer':
						this.graphVisualizer.cy.add({group : 'nodes', data: {id: dataArray[1], label: dataArray[4], degree: parseInt(dataArray[5])}, position: {x: parseInt(dataArray[2]) , y: parseInt(dataArray[3])}});
						this.graphVisualizer.updateVertexSize(dataArray[1])
						this.graphVisualizer.updateDegreeExtrema(parseInt(dataArray[5]));
						clearTimeout(window.timeOut);
						client = this;
						window.timeOut = setTimeout(function(){
							client.finalOperations()
						}, this.timeOut);						
						break;
					case 'addVertexServerToBeLayouted':
						this.graphVisualizer.addVertexToLayoutBase(dataArray);
						this.graphVisualizer.updateVertexSize(dataArray[1]);
						this.graphVisualizer.updateDegreeExtrema(parseInt(dataArray[2]));
						clearTimeout(window.timeOut);
						client = this;
						window.timeOut = setTimeout(function(){
							client.finalOperations()
						}, this.timeOut);						
						break;
					case 'addEdgeServer':
						this.graphVisualizer.cy.add({group : 'edges', data: {id: dataArray[1], source: dataArray[2], target: dataArray[3]}});
						clearTimeout(window.timeOut);
						client = this;
						window.timeOut = setTimeout(function(){
							client.finalOperations()
						}, this.timeOut);
						break;
					case 'removeObjectServer':
						if (!this.layout) this.graphVisualizer.layoutBase.delete(dataArray[1]);
						this.graphVisualizer.cy.remove(this.graphVisualizer.cy.$id(dataArray[1]));
						break;
					case 'fit':
						this.graphVisualizer.cy.fit();
						this.graphVisualizer.zoomLevel = this.graphVisualizer.cy.zoom();
						const pan = this.graphVisualizer.cy.pan();
						this.ws.send("fitted;" + pan.x + ";" + pan.y + ";" + this.graphVisualizer.cy.zoom());
						break;
					case 'enableMouse':
						if (!this.mouseEnabled) this.enableMouseEvents();
						break;
				}
				resolve(true);
			});
			await promise;
			this.processMessage();
		} else {
			this.messageProcessing = false;
		}
	}

	sendClusterEntryAddress(){
		const address = document.getElementById('clusterEntryPointAddress').value;
		this.ws.send("clusterEntryAddress;" + address);
	}

	sendHDFSEntryAddress(){
		const address = document.getElementById('hDFSEntryPointAddress').value;
		this.ws.send("hDFSEntryAddress;" + address);
	}

	sendHDFSEntryPort(){
		const port = document.getElementById('hDFSEntryPointPort').value;
		this.ws.send("hDFSEntryPointPort;" + port);
	}

	sendHDFSGraphFilesDirectory(){
		const directory = document.getElementById('hDFSgraphFileDirectory').value
		this.ws.send("hDFSGraphFilesDirectory;" + directory);
	}

	sendGradoopGraphId(){
		const id = document.getElementById('gradoopGraphID').value;
		this.ws.send("gradoopGraphId;" + id);
	}

	sendVerticesHaveDegrees(){
		const haveDegrees = !document.getElementById('verticesHaveDegrees').checked;
		this.ws.send("degrees;" + haveDegrees);
	}

	sendGraphIsLayouted(){
		this.layout = !document.getElementById('graphIsLayouted').checked;
		this.ws.send("layoutMode;" + this.layout);
	}

	sendSignalHBase(){
		this.buildTopViewOperations();
		this.ws.send("buildTopView;HBase");
	}

	sendSignalGradoop(){
		this.buildTopViewOperations();
		this.ws.send("buildTopView;gradoop");
	}

	sendSignalCSV(){
		this.buildTopViewOperations();
		this.ws.send("buildTopView;CSV");
	}

	sendSignalAdjacency(){
		this.buildTopViewOperations();
		this.ws.send("buildTopView;adjacency");
	}

	buildTopViewOperations(){
		if (!this.layout) {
			this.graphVisualizer.layoutBase = new Set();
		}
		this.graphVisualizer.layoutWindow = {x1: 0, y1: 0, x2: 4000, y2: 4000};
		this.graphVisualizer.cy.zoom(1 / (4000 / Math.min(this.cyWidth, this.cyHeight)));
		this.graphVisualizer.zoomLevel = this.graphVisualizer.cy.zoom();
		this.disableMouseEvents();
	}

	sendMaxVertices(){
		this.maxNumberVertices = document.getElementById('maxVerticesInput').value;
		this.ws.send("maxVertices;" + maxVertices);
	}

	resetVisualization(){
		this.graphVisualizer = new GraphVisualizer();
		ws.send("resetWrapperHandler");
	}

	resize(){
		console.log("Resizing viewport!");
		const boundingClientRect = document.getElementById('cy').getBoundingClientRect();
		let cyHeightOld = this.cyHeight;
		let cyWidthOld = this.cyWidth;
		this.cyHeight = boundingClientRect.height;
		this.cyWidth = boundingClientRect.width;
		if (this.graphVisualizer.cy.nodes().length != 0) this.resizeGraph(cyHeightOld, cyWidthOld);
		this.cyHeightHalf = this.cyHeight / 2;
		this.cyWidthHalf = this.cyWidth / 2;
		const pan = this.graphVisualizer.cy.pan();
		this.ws.send("viewportSize;" + pan.x + ";" + pan.y + ";" + this.graphVisualizer.cy.zoom() + ";" +  this.cyWidth + ";" + this.cyHeight);
	}

	resizeGraph(cyHeightOld, cyWidthOld){
		const yVar = (cyHeightOld - this.cyHeight) / 2;
		const xVar = (cyWidthOld - this.cyWidth) / 2;
		const pan = this.graphVisualizer.cy.pan();
		this.graphVisualizer.cy.pan({x: - xVar + pan.x, y: - yVar + pan.y});
	}
	
	finalOperations(){
		console.log("Final Operations!");
		this.graphVisualizer.updateVerticesSize();
		if (!this.layout){
			let layoutBaseString = "";
			console.log("layoutBase size: " + this.graphVisualizer.layoutBase.size);
			if (this.graphVisualizer.layoutBase.size > 0){
				console.log("performing layout!");
				console.log(this.graphVisualizer.layoutWindow);
				let layoutOptions = {};
				layoutOptions.name = 'random';
				layoutOptions.randomize = true;
				layoutOptions.fit = false;
				layoutOptions.boundingBox = this.graphVisualizer.layoutWindow;
				let layoutBaseCy = this.graphVisualizer.cy.collection();
				console.log("layoutBase size" + this.graphVisualizer.layoutBase.size);
				const cy = this.graphVisualizer.cy;
				this.graphVisualizer.layoutBase.forEach(function (vertexId){
					layoutBaseCy = layoutBaseCy.add(cy.$id(vertexId));
				});
				this.graphVisualizer.cy.layout(fdLayout).run();
				// this.graphVisualizer.cy.layout(layoutOptions).run();
				console.log("layout performed");
				layoutBaseCy.forEach(function(node){
					let pos = node.position();
					layoutBaseString += ";" + node.data("id") + "," + pos.x + "," + pos.y;
				})
				this.graphVisualizer.cy.nodes().lock();
				this.graphVisualizer.layoutBase = new Set();
			}	
			this.ws.send("layoutBaseString" + layoutBaseString);
		}
		this.enableMouseEvents();
	}

	disableMouseEvents(){
		$("body").css("cursor", "progress");
		console.log("disabling mouse events");
		const cyto = document.getElementById('cy');
		cyto.removeEventListener("mousedown", mouseDown);
		cyto.removeEventListener("mouseup", mouseUp);
		cyto.removeEventListener("wheel", mouseWheel);
		this.mouseEnabled = false;
	}

	enableMouseEvents(){
		$("body").css("cursor", "default");
		console.log("enabling mouse events");
		const cyto = document.getElementById('cy');
		cyto.addEventListener("mouseup", mouseUp);
		cyto.addEventListener("mousedown", mouseDown);
		cyto.addEventListener("wheel", mouseWheel);
		this.mouseEnabled = true;
	}
}

$(document).ready(function(){
	ws = new WebSocket("ws://" + jsonObject.ServerIp4 + ":8897/graphData");
	
	ws.onopen = function() {
		console.log("Opened!");
		ws.send("Hello Server");
		ws.send("resetWrapperHandler");
		client.resize(client);
	}

	ws.onmessage = function (evt) {
		// console.log(evt.data);
		const dataArray = evt.data.split(";");
		client.addMessageToQueue(dataArray);
	}
	
	ws.onclose = function() {
		console.log("Closed!");
	};

	ws.onerror = function(err) {
		console.log("Error: " + err);
	};

	client = new Client(ws);

	let resizedTimeOut;
	window.onresize = function(){
		client.disableMouseEvents();
		// console.log($('#myContainer'));
		clearTimeout(resizedTimeOut);
		resizedTimeOut = setTimeout(function(){client.resize();}, 500);
	};

	client.enableMouseEvents();
});

function mouseWheel(e) {
	client.disableMouseEvents();
	e.preventDefault();
	const delta = Math.sign(e.deltaY);
	const rect = e.target.getBoundingClientRect();
	const scroll = document.documentElement.scrollTop || document.body.scrollTop;
	const cytoX = e.pageX - rect.left;
	const cytoY = e.pageY - (rect.top + scroll);
	let pan = client.graphVisualizer.cy.pan();
	client.graphVisualizer.layoutBase = new Set();
	if (delta < 0){
		client.graphVisualizer.styleOnZoom("in");
		client.graphVisualizer.cy.zoom(client.graphVisualizer.cy.zoom() * client.graphVisualizer.zFactor);
		client.graphVisualizer.cy.pan({x:- client.cyWidthHalf + client.graphVisualizer.zFactor * pan.x + (client.cyWidthHalf - cytoX) * client.graphVisualizer.zFactor, 
			y:- client.cyHeightHalf + client.graphVisualizer.zFactor * pan.y + (client.cyHeightHalf - cytoY) * client.graphVisualizer.zFactor});
		pan = client.graphVisualizer.cy.pan();
		client.graphVisualizer.zoomLevel = client.graphVisualizer.cy.zoom();
		const topModel = - pan.y / client.graphVisualizer.zoomLevel;
		const leftModel = - pan.x / client.graphVisualizer.zoomLevel;
		const bottomModel = topModel + client.cyHeight / client.graphVisualizer.zoomLevel;
		const rightModel = leftModel + client.cyWidth / client.graphVisualizer.zoomLevel;
		console.log("ZoomIn... top , right, bottom, left: " + topModel + " " + rightModel + " " + bottomModel + " " + leftModel);
		client.graphVisualizer.layoutWindow = client.graphVisualizer.derivelayoutWindow(topModel, rightModel, bottomModel, leftModel);
		client.ws.send("zoomIn;" + pan.x + ";" + pan.y + ";" + client.graphVisualizer.zoomLevel);
	} else {
		client.graphVisualizer.styleOnZoom("out");
		client.graphVisualizer.cy.zoom(client.graphVisualizer.cy.zoom() / client.graphVisualizer.zFactor);
		client.graphVisualizer.cy.pan({x:client.cyWidthHalf + pan.x - (client.cyWidthHalf + pan.x) / client.graphVisualizer.zFactor - (cytoX - client.cyWidthHalf) /client.graphVisualizer.zFactor, 
			y:client.cyHeightHalf + pan.y - (client.cyHeightHalf + pan.y) / client.graphVisualizer.zFactor - (cytoY - client.cyHeightHalf) / client.graphVisualizer.zFactor});
		pan = client.graphVisualizer.cy.pan();
		client.graphVisualizer.zoomLevel = client.graphVisualizer.cy.zoom();
		client.graphVisualizer.zoomLevel = client.graphVisualizer.zoomLevel;
		client.ws.send("zoomOut;" + pan.x + ";" + pan.y + ";" + client.graphVisualizer.zoomLevel);
	}
}

function mouseDown(e){
	this.xRenderDiff = 0;
	this.yRenderDiff = 0;
	this.onmousemove = function (e){
		this.xRenderDiff = this.xRenderDiff + e.movementX;
		this.yRenderDiff = this.yRenderDiff + e.movementY;
		const xRenderPos = client.graphVisualizer.cy.pan().x;
		const yRenderPos = client.graphVisualizer.cy.pan().y;
		console.log("accumulated movement x: " + this.xRenderDiff.toString());
		console.log("accumulated movement y: " + this.yRenderDiff.toString());
		client.graphVisualizer.cy.pan({x:xRenderPos + e.movementX, y:yRenderPos + e.movementY});
	}
}

function mouseUp(e){
	client.disableMouseEvents();
	this.onmousemove = null;
	const xModelDiff = - (this.xRenderDiff / client.graphVisualizer.zoomLevel);
	const yModelDiff = - (this.yRenderDiff / client.graphVisualizer.zoomLevel);
	const pan = client.graphVisualizer.cy.pan();
	const xRenderPos = pan.x;
	const yRenderPos = pan.y;
	const topModel= - yRenderPos / client.graphVisualizer.zoomLevel;
	const leftModel = - xRenderPos / client.graphVisualizer.zoomLevel;
	const bottomModel = topModel + client.cyHeight / client.graphVisualizer.zoomLevel;
	const rightModel = leftModel + client.cyWidth / client.graphVisualizer.zoomLevel;
	console.log("Pan... top , right, bottom, left: " + topModel + " " + rightModel + " " + bottomModel + " " + leftModel);
	client.graphVisualizer.layoutWindow = client.graphVisualizer.derivelayoutWindow(topModel, rightModel, bottomModel, leftModel);
	client.graphVisualizer.layoutBase = new Set();
	client.ws.send("pan;" + xModelDiff + ";" + yModelDiff + ";" + client.graphVisualizer.zoomLevel);
}

let fdLayout = {
	name: 'cose', // Called on `layoutready`
	ready: function(){console.log('fd layout ready')}, // Called on `layoutstop`
	stop: function(){}, // Whether to animate while running the layout
	// true : Animate continuously as the layout is running
	// false : Just show the end result
	// 'end' : Animate with the end result, from the initial positions to the end positions
	animate: true, // Easing of the animation for animate:'end'
	animationEasing: undefined, // The duration of the animation for animate:'end'
	animationDuration: undefined, // A function that determines whether the node should be animated
	// All nodes animated by default on animate enabled
	// Non-animated nodes are positioned immediately when the layout starts
	animateFilter: function ( node, i ){ return true; }, // The layout animates only after this many milliseconds for animate:true
	// (prevents flashing on fast runs)
	animationThreshold: 250, // Number of iterations between consecutive screen positions update
	refresh: 20, // Whether to fit the network view after when done
	fit: true, // Padding on fit
	padding: 30, // Constrain layout bounds; { x1, y1, x2, y2 } or { x1, y1, w, h }
	boundingBox: undefined, // Excludes the label when calculating node bounding boxes for the layout algorithm
	nodeDimensionsIncludeLabels: false, // Randomize the initial positions of the nodes (true) or use existing positions (false)
	randomize: false, // Extra spacing between components in non-compound graphs
	componentSpacing: 40, // Node repulsion (non overlapping) multiplier
	nodeRepulsion: function( node ){ return 2048; }, // Node repulsion (overlapping) multiplier
	nodeOverlap: 4, // Ideal edge (non nested) length
	idealEdgeLength: function( edge ){ return 32; }, // Divisor to compute edge forces
	edgeElasticity: function( edge ){ return 32; }, // Nesting factor (multiplier) to compute ideal edge length for nested edges
	nestingFactor: 1.2, // Gravity force (constant)
	gravity: 1, // Maximum number of iterations to perform
	numIter: 1000, // Initial temperature (maximum node displacement)
	initialTemp: 1000, // Cooling factor (how the temperature is reduced between consecutive iterations
	coolingFactor: 0.99, // Lower temperature threshold (below this point the layout will end)
	minTemp: 1.0
   }