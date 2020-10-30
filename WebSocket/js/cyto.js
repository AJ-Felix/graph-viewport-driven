const vPix = 1000
const vPixHalf = 500;
const zFactor = 2;
let maxNumberVertices = 100;

let operation;
let operationStep;

let layoutBase;

var cy = cytoscape({
  container: $('#cy'),
  elements: [ 
    // { 
      // data: { id: 'a' }
    // },
    // { 
      // data: { id: 'b' }
    // },
    // { 
      // data: { id: 'ab', source: 'a', target: 'b' }
    // }
  ],

  style: [ 
    {
      selector: 'node',
      style: {
        'background-color': '#666',
        'label': 'data(label)',
		'height': '30',
		'width':'30'
      }
    },

    {
      selector: 'edge',
      style: {
        'width': 3,
        'line-color': '#ccc',
        'target-arrow-color': '#ccc',
        'target-arrow-shape': 'triangle',
        'curve-style': 'bezier'
      }
    }
  ],

  layout: {
    name: 'grid',
    rows: 1
  },
  
    // initial viewport state:
  zoom: 0.25,
  pan: { x: 0, y: 0 },

  // interaction options:
  minZoom: 1e-1,
  maxZoom: 1e1,
  zoomingEnabled: true,
  userZoomingEnabled: false,
  panningEnabled: true,
  userPanningEnabled: false,
  boxSelectionEnabled: false,
  selectionType: 'single',
  touchTapThreshold: 8,
  desktopTapThreshold: 4,
  autolock: false,
  autoungrabify: false,
  autounselectify: false,

  // rendering options:
  headless: false,
  styleEnabled: true,
  hideEdgesOnViewport: false,
  textureOnViewport: false,
  motionBlur: false,
  motionBlurOpacity: 0.2,
/*   wheelSensitivity: 1,
 */  pixelRatio: 'auto'
});

let xRenderDiff = 0;
let yRenderDiff = 0;

function finalOperations(){
	console.log("in finalOperations funtion");
	let layoutBaseString = "";
	console.log("layoutBase size: " + layoutBase.size);
	if (layoutBase.size > 0){
		console.log("performing layout!");
		console.log(boundingBoxVar);
		let layoutOptions = {};
		layoutOptions.name = "random";
		layoutOptions.fit = false;
		layoutOptions.boundingBox = boundingBoxVar;
		layoutBaseCy = cy.collection();
		console.log("layoutBase size" + layoutBase.size);
		layoutBase.forEach(function (vertexId){
			layoutBaseCy = layoutBaseCy.add(cy.$id(vertexId));
		});
		let layout = layoutBaseCy.layout(layoutOptions);
		layout.run();
		console.log("layout performed");
		layoutBaseCy.forEach(function(node){
			let pos = node.position();
			layoutBaseString += ";" + node.data("id") + "," + pos.x + "," + pos.y;
		})
		cy.nodes().lock();
		layoutBase = new Set();
	}
	ws.send("layoutBaseString" + layoutBaseString);
}

function addVertexToLayoutBase(dataArray){
	cy.add({group : 'nodes', data: {id: dataArray[1], label: dataArray[3]}});
	const vertex = cy.$id(dataArray[1]);
	console.log(vertex);
	console.log(dataArray[3]);
	layoutBase.add(dataArray[1]);
	console.log("layoutBase size: " + layoutBase.size);
	// clearTimeout(this.timeOut);
	// this.timeOut = setTimeout(performLayout, 1000);
}

// function performLayout(){
	// console.log("performing layout!");
	// console.log(boundingBoxVar);
	// let layoutOptions = {};
	// layoutOptions.name = "random";
	// layoutOptions.fit = false;
	// layoutOptions.boundingBox = boundingBoxVar;
	// layoutBaseCy = cy.collection();
	// console.log("layoutBase size" + layoutBase.size);
	// layoutBase.forEach(function (vertexId){
		// layoutBaseCy = layoutBaseCy.add(cy.$id(vertexId));
	// });
	// let layout = layoutBaseCy.layout(layoutOptions);
	// layout.run();
	// console.log("layout performed");
	// let layoutBaseString = "";
	// layoutBaseCy.forEach(function(node){
		// let pos = node.position();
		// layoutBaseString += ";" + node.data("id") + "," + pos.x + "," + pos.y;
	// })
	// // operationStep += 1;
	// // console.log("operation: " + operation);
	// // console.log("operationStep: " + operationStep);
	// ws.send("layoutBaseString" + layoutBaseString);
// }

let cyto = document.getElementById('cy');
cyto.addEventListener('mousedown', function(e){
	console.log("mouse went down in cy (drag)");
	xRenderDiff = 0;
	yRenderDiff = 0;
	this.onmousemove = function (e){
		xRenderDiff = xRenderDiff + e.movementX;
		yRenderDiff = yRenderDiff + e.movementY;
		let xRenderPos = cy.pan().x;
		let yRenderPos = cy.pan().y;
		console.log("accumulated movement x: " + xRenderDiff.toString());
		console.log("accumulated movement y: " + yRenderDiff.toString());
		cy.pan({x:xRenderPos + e.movementX, y:yRenderPos + e.movementY});
	}
});

cyto.addEventListener("mouseup", function(e){
    this.onmousemove = null;
	const zoomLevel = cy.zoom();
	const xModelDiff = - (xRenderDiff / zoomLevel);
	const yModelDiff = - (yRenderDiff / zoomLevel);
	const pan = cy.pan();
	const xRenderPos = pan.x;
	const yRenderPos = pan.y;
	const topModelPos= - yRenderPos / zoomLevel;
	const leftModelPos = - xRenderPos / zoomLevel;
	const bottomModelPos = topModelPos + vPix / zoomLevel;
	const rightModelPos = leftModelPos + vPix / zoomLevel;
	console.log("Pan... top , right, bottom, left: " + topModelPos + " " + rightModelPos + " " + bottomModelPos + " " + leftModelPos);
	boundingBoxVar = {x1: leftModelPos, y1: topModelPos, x2: rightModelPos, y2: bottomModelPos};
	console.log("new boundingBox");
	console.log(boundingBoxVar);
	cy.nodes().lock();
	layoutBase = new Set();
	if (graphOperationLogic == "clientSide"){
		handler.operation = "pan";
		handler.prepareOperation(topModelPos, rightModelPos, bottomModelPos, leftModelPos);
	}
	ws.send("pan;" + xModelDiff + ";" + yModelDiff);
});

document.addEventListener("click",
	function(e){
		console.log("Mouse clicked anywhere in document!");
		console.log("cytoscape panning:");
		console.log(cy.pan());
		console.log("cytoscape zoom:");
		console.log(cy.zoom());
		console.info("cytoX: " + (e.pageX - cyto.offsetLeft));
		console.info("cytoY: " + (e.pageY - cyto.offsetTop));
	}
);

cyto.addEventListener("wheel", function(e) {
	e.preventDefault();
    const delta = Math.sign(e.deltaY);
	const cytoX = e.pageX - cyto.offsetLeft;
	const cytoY = e.pageY - cyto.offsetTop;
	let pan = cy.pan();
	layoutBase = new Set();
	if (delta < 0){
		cy.zoom(cy.zoom() * zFactor);
		cy.pan({x:-vPixHalf + zFactor * pan.x + (vPixHalf - cytoX) * zFactor, y:-vPixHalf + zFactor * pan.y + (vPixHalf - cytoY) * zFactor});
		pan = cy.pan();
		const zoomLevel = cy.zoom();
		const topModelPos = - pan.y / zoomLevel;
		const leftModelPos = - pan.x / zoomLevel;
		const bottomModelPos = topModelPos + vPix / zoomLevel;
		const rightModelPos = leftModelPos + vPix / zoomLevel;
		console.log("zoomIn... top , right, bottom, left: " + topModelPos + " " + rightModelPos + " " + bottomModelPos + " " + leftModelPos);
		boundingBoxVar = {x1: leftModelPos, y1: topModelPos, x2: rightModelPos, y2: bottomModelPos};
		console.log("new boundingBox");
		console.log(boundingBoxVar);
		cy.nodes().lock();
		if (graphOperationLogic == "clientSide"){
			handler.operation = "zoomIn";
			handler.prepareOperation(topModelPos, rightModelPos, bottomModelPos, leftModelPos);
		}
		ws.send("zoomIn;" + pan.x + ";" + pan.y + ";" + zoomLevel);
	} else {
		cy.zoom(cy.zoom() / zFactor);
		cy.pan({x:vPixHalf + pan.x - (vPixHalf + pan.x) / zFactor - (cytoX - vPixHalf) / zFactor, y:vPixHalf + pan.y - (vPixHalf + pan.y) / zFactor - (cytoY - vPixHalf) / zFactor});
		pan = cy.pan();
		const zoomLevel = cy.zoom();
		if (graphOperationLogic == "clientSide"){
			const topModelPos = - pan.y / zoomLevel;
			const leftModelPos = - pan.x / zoomLevel;
			const bottomModelPos = topModelPos + vPix / zoomLevel;
			const rightModelPos = leftModelPos + vPix / zoomLevel;
			handler.operation = "zoomOut";
			handler.prepareOperation(topModelPos, rightModelPos, bottomModelPos, leftModelPos);
		}
		ws.send("zoomOut;" + pan.x + ";" + pan.y + ";" + zoomLevel);
	}
});

