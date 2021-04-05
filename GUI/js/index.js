class Client {

	constructor (ws){
		this.ws = ws;
		this.messageQueue = new Array();
		this.messageProcessing = false;
		this.layout = true
		this.graphVisualizer = new GraphVisualizer();
		this.timeOut = 2000;
		this.vertexZoomLevel = 0;
		this.timeBeforeQuery;
		this.timeLastResponse;
		this.evaluationCount = 0;
		this.evaluationCountThreshold = 10;
		this.lastAutomatedOperation = "zoomIn";
		this.automatedEvaluation = false;
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
						this.graphVisualizer.cy.add({group : 'nodes', data: {id: dataArray[1], label: dataArray[4], degree: parseInt(dataArray[5]), zoomLevel: parseInt(dataArray[6])}, 
							position: {x: parseInt(dataArray[2]) , y: parseInt(dataArray[3])}});
						this.graphVisualizer.updateVertexSize(dataArray[1]);
						this.graphVisualizer.colorVertex(dataArray[1], dataArray[4]);
						this.graphVisualizer.updateDegreeExtrema(parseInt(dataArray[5]));
						if (eval) this.updateResponseTimes();				
						break;
					case 'addVertexServerToBeLayouted':
						this.graphVisualizer.addVertexToLayoutBase(dataArray);
						this.graphVisualizer.updateVertexSize(dataArray[1]);
						this.graphVisualizer.colorVertex(dataArray[1], dataArray[2]);
						this.graphVisualizer.updateDegreeExtrema(parseInt(dataArray[3]));
						if (eval) this.updateResponseTimes();	
						break;			
					case 'addEdgeServer':
						this.graphVisualizer.cy.add({group : 'edges', data: {id: dataArray[1], label: dataArray[4], source: dataArray[2], target: dataArray[3]}});
						console.log("label: " + dataArray[4]);
						if (eval) this.updateResponseTimes();				
						break;
					case 'removeObjectServer':
						if (!this.layout) this.graphVisualizer.layoutBase.delete(dataArray[1]);
						this.graphVisualizer.cy.remove(this.graphVisualizer.cy.$id(dataArray[1]));
						if (eval) this.updateResponseTimes();				
						break;
					case 'nextSubStep':
						this.ws.send("layoutingStreamOperation;" + dataArray[1]);
						break;
					case 'zoomAndPan':
						this.graphVisualizer.zoomLevel = parseFloat(dataArray[1]);
						this.graphVisualizer.cy.zoom(this.graphVisualizer.zoomLevel);
						this.graphVisualizer.cy.pan({x: parseFloat(dataArray[2]), y: parseFloat(dataArray[3])});
						if (this.automatedEvaluation) this.topView();
						else this.enableMouseEvents();
						break;
					case 'modelBorders':
						this.graphVisualizer.setModelBorders(parseInt(dataArray[1]), parseInt(dataArray[2]), parseInt(dataArray[3]), parseInt(dataArray[4]));
						console.log(dataArray[1]);
						console.log(dataArray[2]);
						console.log(dataArray[3]);
						console.log(dataArray[4]);
						break;
					case 'operation':
						this.operation = dataArray[1];
						break;
					case 'operationAndStep':
						this.operation = dataArray[1];
						this.operationStep = parseInt(dataArray[2]);
						break;
					case 'finalOperations':
						this.finalOperations();
						break;
					case 'enableMouse':
						if (!this.mouseEnabled) this.enableMouseEvents();
						break;
					case 'resetSuccessful':
						this.resize();
						break;
					case 'automatedEvaluation':
						this.automatedEvaluation = true;
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

	sendGraphFolderDirectory(){
		const directory = document.getElementById('graphFolderDirectory').value
		this.graphName = directory.split("/")[directory.split("/").length - 1];
		this.ws.send("graphFolderDirectory;" + directory);
	}

	sendGradoopGraphId(){
		const id = document.getElementById('gradoopGraphID').value;
		this.ws.send("gradoopGraphId;" + id);
	}

	sendGraphIsLayouted(){
		this.layout = !document.getElementById('graphIsLayouted').checked;
		this.ws.send("layoutMode;" + this.layout);
	}

	sendParallelism(){
		this.parallelism = document.getElementById('parallelism').value
		this.ws.send("parallelism;" + this.parallelism);
	}

	sendSignalGradoop(){
		if (eval) {
			this.initiateEvaluation();
			this.backendVariant = "gradoop";
		}
		this.buildTopViewOperations();
		this.ws.send("buildTopView;gradoop");
	}

	sendSignalCSV(){
		if (eval) {
			this.initiateEvaluation();
			this.backendVariant = "tableStream";
		}
		this.buildTopViewOperations();
		this.ws.send("buildTopView;CSV");
	}

	sendSignalAdjacency(){
		if (eval) {
			this.initiateEvaluation();
			this.backendVariant = "adjacencyMatrix";
		}
		this.buildTopViewOperations();
		this.ws.send("buildTopView;adjacency");
	}

	buildTopViewOperations(){
		if (!this.layout) this.graphVisualizer.layoutBase = new Set();
		this.disableMouseEvents();
	}

	resetVisualization(){
		this.disableMouseEvents();
		this.graphVisualizer = new GraphVisualizer();
		this.vertexZoomLevel = 0;
		this.ws.send("resetVisualization");
	}

	resize(){
		const boundingClientRect = document.getElementById('cy').getBoundingClientRect();
		let cyHeightOld = this.cyHeight;
		let cyWidthOld = this.cyWidth;
		this.cyHeight = boundingClientRect.height;
		this.cyWidth = boundingClientRect.width;
		if (this.graphVisualizer.cy.nodes().length != 0) this.resizeGraph(cyHeightOld, cyWidthOld);
		this.cyHeightHalf = this.cyHeight / 2;
		this.cyWidthHalf = this.cyWidth / 2;
		const pan = this.graphVisualizer.cy.pan();
		if (eval) this.initiateEvaluation();
		this.ws.send("viewportSize;" + pan.x + ";" + pan.y + ";" + this.graphVisualizer.cy.zoom() + ";" +  this.cyWidth + ";" + this.cyHeight);
	}

	resizeGraph(cyHeightOld, cyWidthOld){
		const yVar = (cyHeightOld - this.cyHeight) / 2;
		const xVar = (cyWidthOld - this.cyWidth) / 2;
		const pan = this.graphVisualizer.cy.pan();
		this.graphVisualizer.cy.pan({x: - xVar + pan.x, y: - yVar + pan.y});
	}
	
	finalOperations(){
		this.graphVisualizer.updateVerticesSize();
		if (this.operation == "initial") this.fitTopView();
		if (eval) this.outputQueryTime();
		if (!this.layout){
			let layoutBaseString = "";
			if (this.graphVisualizer.layoutBase.size > 0){
				let layoutOptions = {};
				layoutOptions.name = 'random';
				layoutOptions.randomize = true;
				layoutOptions.fit = false;
				layoutOptions.boundingBox = this.graphVisualizer.layoutWindow;
				let layoutBaseCy = this.graphVisualizer.cy.collection();
				const cy = this.graphVisualizer.cy;
				this.graphVisualizer.layoutBase.forEach(function (vertexId){
					layoutBaseCy = layoutBaseCy.add(cy.$id(vertexId));
				});
				layoutBaseCy.layout(layoutOptions).run();
				layoutBaseCy.forEach(function(node){
					let pos = node.position();
					layoutBaseString += ";" + node.data("id") + "," + pos.x + "," + pos.y + "," + node.data('zoomLevel');
				})
				this.graphVisualizer.cy.nodes().lock();
				this.graphVisualizer.layoutBase = new Set();
			}	
			this.initiateEvaluation();
			this.ws.send("layoutBaseString" + layoutBaseString);
		}
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
		if (this.automatedEvaluation && this.evaluationCount < this.evaluationCountThreshold){
			if (this.operation == "initial"){
				this.evaluationCount += 1;
				zoom(-1, this.cyWidthHalf, this.cyHeightHalf);
			} else if (this.operation == this.lastAutomatedOperation){
				this.resetVisualization();
			}
		} else {
			$("body").css("cursor", "default");
			console.log("enabling mouse events");
			const cyto = document.getElementById('cy');
			cyto.addEventListener("mouseup", mouseUp);
			cyto.addEventListener("mousedown", mouseDown);
			cyto.addEventListener("wheel", mouseWheel);
			this.mouseEnabled = true;
		}
	}

	topView(){
		if (this.backendVariant == "gradoop") this.sendSignalGradoop();
		else if (this.backendVariant == "tableStream") this.sendSignalCSV();
		else if (this.backendVariant == "adjacencyMatrix") this.sendSignalAdjacency();
	}


	fitTopView(){
		const cy = this.graphVisualizer.cy;
		const zoomBefore = cy.zoom();
		cy.fit();
		this.graphVisualizer.styleOnFit(zoomBefore);
		const pan = cy.pan();
		this.ws.send("fit;" + cy.zoom() + ";" + pan.x + ";" + pan.y);
	}

	initiateEvaluation(){
		console.info("initializing evaluation");
		this.timeBeforeQuery = new Date().getTime();
		this.firstResponse = null;
		this.timeLastResponse = null;
	}

	updateResponseTimes(){
		this.timeLastResponse = new Date().getTime();
		if (this.firstResponse == null) this.firstResponse = this.timeLastResponse;
	}

	outputQueryTime(){
		this.updateResponseTimes();
		const firstToLastDuration = this.timeLastResponse - this.firstResponse; 
		const fullQueryDuration = this.timeLastResponse - this.timeBeforeQuery;
		const output = this.operation + "," + this.operationStep + "," + fullQueryDuration + "," + firstToLastDuration;
		if (this.automatedEvaluation){
			// download(output, "client_evaluation_be_" + this.backendVariant + "_layout_" + this.layout + "_gN_" + this.graphName
			// + "_plsm_" + this.parallelism + "_" + this.evaluationCount, "string");
			if (this.outputConcat == null) this.outputConcat = "";
			if (this.evaluationCount < this.evaluationCountThreshold){
				this.outputConcat += output + "\n";
			} else {
				this.outputConcat += output + "\n";
				if (this.operation == this.lastAutomatedOperation) 
					download(this.outputConcat, "client_evaluation_be_" + this.backendVariant + "_layout_" + this.layout + "_gN_" + this.graphName
						+ "_plsm_" + this.parallelism, "string");
			}
		} else {
			download(output, "client_evaluation", "string");
		}
	}
}

$(document).ready(function(){

	//color mapping
	if (typeof colorMapping != "undefined"){
		if (!$.isEmptyObject(colorMapping)){
			createColorMapHeading()
			for (let [key, value] of Object.entries(colorMapping)) {
				createColorMapElement(key, value);
			}
		}
	}

	//web socket
	ws = new WebSocket("ws://" + jsonObject.ServerIp4 + ":8897");
	
	ws.onopen = function() {
		console.log("Opened!");
		ws.send("Hello Server");
		// client.resetVisualization();
		client.resize(client);
	}

	ws.onmessage = function (evt) {
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
		clearTimeout(resizedTimeOut);
		resizedTimeOut = setTimeout(function(){client.resize();}, 500);
	};

	client.enableMouseEvents();
});

function createColorMapHeading(){
	let colorMapCol = document.getElementById('colorMapCol');
	let headingRow = document.createElement('div');
	colorMapCol.insertBefore(headingRow, colorMapCol.firstChild);
	let headingCol = document.createElement('div');
	headingRow.appendChild(headingCol);
	headingCol.innerHTML = "Color Mapping";
	headingCol.classList.add("text-center");
	headingCol.style.fontSize = "20px";
	headingCol.style.textDecoration = "underline";
}

function createColorMapElement(label, color){
	let row = document.getElementById("colorMapRow");

	let col = document.createElement('div');
	row.appendChild(col);
	col.classList.add("col-xl-3");
	col.classList.add("col-lg-4");
	col.classList.add("col-md-6");
	col.classList.add("col-sm-4");

	let colRow = document.createElement('div');
	colRow.classList.add("row");
	col.appendChild(colRow);


	let colColor = document.createElement('div');
	colRow.appendChild(colColor);
	colColor.classList.add("col-xs-1");
	colColor.classList.add("my-auto");
	colColor.classList.add("p-1");

	let colDiv = document.createElement('div');
	colColor.appendChild(colDiv);
	colDiv.style.width = "10px";
	colDiv.style.height = "10px";
	colDiv.style.background = color;
	colDiv.style.borderRadius = "5px";

	let colLabel = document.createElement('div');
	colRow.appendChild(colLabel);
	colLabel.classList.add("col-xs-11");
	colLabel.classList.add("p-1");
	colLabel.innerHTML = label;
}

function download(data, filename, type) {
    var file = new Blob([data], {type: type});
    if (window.navigator.msSaveOrOpenBlob) // IE10+
        window.navigator.msSaveOrOpenBlob(file, filename);
    else { // Others
        var a = document.createElement("a"),
                url = URL.createObjectURL(file);
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        setTimeout(function() {
            document.body.removeChild(a);
            window.URL.revokeObjectURL(url);  
        }, 0); 
    }
}

function mouseWheel(e) {
	client.disableMouseEvents();
	e.preventDefault();
	const delta = Math.sign(e.deltaY);
	const rect = e.target.getBoundingClientRect();
	const scroll = document.documentElement.scrollTop || document.body.scrollTop;
	const cytoX = e.pageX - rect.left;
	const cytoY = e.pageY - (rect.top + scroll);
	zoom(delta, cytoX, cytoY);
}

function zoom(delta, cytoX, cytoY){
	let pan = client.graphVisualizer.cy.pan();
	client.graphVisualizer.layoutBase = new Set();
	if (delta < 0){
		client.vertexZoomLevel += 1;
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
		client.graphVisualizer.layoutWindow = client.graphVisualizer.derivelayoutWindow(topModel, rightModel, bottomModel, leftModel);
		if (eval) client.initiateEvaluation();
		client.ws.send("zoomIn;" + pan.x + ";" + pan.y + ";" + client.graphVisualizer.zoomLevel);
	} else {
		if (client.vertexZoomLevel == 0){
			alert("Top Zoom Level reached already!")
			client.enableMouseEvents();
		} else {
			client.vertexZoomLevel -= 1;
			client.graphVisualizer.styleOnZoom("out");
			client.graphVisualizer.cy.zoom(client.graphVisualizer.cy.zoom() / client.graphVisualizer.zFactor);
			client.graphVisualizer.cy.pan({x:client.cyWidthHalf + pan.x - (client.cyWidthHalf + pan.x) / client.graphVisualizer.zFactor - (cytoX - client.cyWidthHalf) /client.graphVisualizer.zFactor, 
				y:client.cyHeightHalf + pan.y - (client.cyHeightHalf + pan.y) / client.graphVisualizer.zFactor - (cytoY - client.cyHeightHalf) / client.graphVisualizer.zFactor});
			pan = client.graphVisualizer.cy.pan();
			client.graphVisualizer.zoomLevel = client.graphVisualizer.cy.zoom();
			client.graphVisualizer.zoomLevel = client.graphVisualizer.zoomLevel;
			if (eval) client.initiateEvaluation();
			client.ws.send("zoomOut;" + pan.x + ";" + pan.y + ";" + client.graphVisualizer.zoomLevel);
		}
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
	client.graphVisualizer.layoutWindow = client.graphVisualizer.derivelayoutWindow(topModel, rightModel, bottomModel, leftModel);
	client.graphVisualizer.layoutBase = new Set();
	if (eval) client.initiateEvaluation();
	client.ws.send("pan;" + xModelDiff + ";" + yModelDiff + ";" + client.graphVisualizer.zoomLevel);
}