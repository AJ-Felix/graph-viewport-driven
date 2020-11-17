// const vPix = 1000
// const vPixHalf = 500;
const zFactor = 2;
let maxNumberVertices = 100;

let operation;
let operationStep;

let layoutBase;
let nodeWidth = 50;
let nodeHeight = 50;
let nodeLabelFontSize = 32;
let edgeWidth = 5;
let edgeArrowSize = 2;

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
		'height': nodeHeight,
		'width': nodeWidth,
		'font-size': nodeLabelFontSize
      }
    },

    {
      selector: 'edge',
      style: {
        'width': edgeWidth,
        'line-color': '#ccc',
        'target-arrow-color': '#ccc',
        'target-arrow-shape': 'triangle',
        'curve-style': 'bezier',
		'arrow-scale': edgeArrowSize
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
	console.log(cy.elements.style);
	console.log("in finalOperations funtion");
	if (!layout){
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
				// console.log(node.style({"width":"50" ,"height":"50", "font-size":"32px"}));
				let pos = node.position();
				layoutBaseString += ";" + node.data("id") + "," + pos.x + "," + pos.y;
			})
			cy.nodes().lock();
			layoutBase = new Set();
		}	
		ws.send("layoutBaseString" + layoutBaseString);
	}
}

function addVertexToLayoutBase(dataArray){
	let xVertex = boundingBoxVar.x1 + Math.random() * (boundingBoxVar.x2 - boundingBoxVar.x1);
	let yVertex = boundingBoxVar.y1 + Math.random() * (boundingBoxVar.y2 - boundingBoxVar.y1);
	console.log("xVertex: " + xVertex);
	console.log("yVertex: " + yVertex);
	cy.add({group : 'nodes', data: {id: dataArray[1], label: dataArray[3]}, position: {x: xVertex, y: yVertex}});
	const vertex = cy.$id(dataArray[1]);
	console.log(vertex);
	console.log(dataArray[3]);
	layoutBase.add(dataArray[1]);
	console.log("layoutBase size: " + layoutBase.size);
}

let cyto = document.getElementById('cy');
// let cyWidth;
// let cyHeight;
// let cyWidthNew;
// let cyHeightNew;

cyto.addEventListener('mousedown', function(e){
	
	// const boundingClientRect = document.getElementById('cy').getBoundingClientRect();
	// cyWidth = boundingClientRect.width;
	// cyHeight = boundingClientRect.height;
	
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
	
	// const boundingClientRect = document.getElementById('cy').getBoundingClientRect();
	// cyWidthNew = boundingClientRect.width;
	// cyHeightNew = boundingClientRect.height;
	
	// if (cyWidthNew != cyWidth || cyHeightNew != cyHeight){
		// console.log("now send viewport change to server");
	// }
	
    this.onmousemove = null;
	const zoomLevel = cy.zoom();
	const xModelDiff = - (xRenderDiff / zoomLevel);
	const yModelDiff = - (yRenderDiff / zoomLevel);
	const pan = cy.pan();
	const xRenderPos = pan.x;
	const yRenderPos = pan.y;
	const topModelPos= - yRenderPos / zoomLevel;
	const leftModelPos = - xRenderPos / zoomLevel;
	const bottomModelPos = topModelPos + cyHeight / zoomLevel;
	const rightModelPos = leftModelPos + cyWidth / zoomLevel;
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
		console.log(document.getElementById('cy').getBoundingClientRect())
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
		nodeWidth = nodeWidth / zFactor;
		nodeHeight = nodeHeight / zFactor;
		nodeLabelFontSize = nodeLabelFontSize / zFactor;
		edgeWidth = edgeWidth / zFactor;
		edgeArrowSize = edgeArrowSize / zFactor;
		cy.style().selector('node').style({
			'width': nodeWidth,
			'height': nodeHeight,
			'font-size': nodeLabelFontSize
		}).update();
		cy.style().selector('edge').style({
			'width': edgeWidth,
			'arrow-scale': edgeArrowSize
		}).update();
		cy.zoom(cy.zoom() * zFactor);
		cy.pan({x:-cyWidthHalf + zFactor * pan.x + (cyWidthHalf - cytoX) * zFactor, y:-cyHeightHalf + zFactor * pan.y + (cyHeightHalf - cytoY) * zFactor});
		pan = cy.pan();
		const zoomLevel = cy.zoom();
		const topModelPos = - pan.y / zoomLevel;
		const leftModelPos = - pan.x / zoomLevel;
		const bottomModelPos = topModelPos + cyHeight / zoomLevel;
		const rightModelPos = leftModelPos + cyWidth / zoomLevel;
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
		nodeWidth = nodeWidth * zFactor;
		nodeHeight = nodeHeight * zFactor;
		nodeLabelFontSize = nodeLabelFontSize * zFactor;
		edgeWidth = edgeWidth * zFactor;
		edgeArrowSize = edgeArrowSize * zFactor;
		cy.style().selector('node').style({
			'width': nodeWidth,
			'height': nodeHeight,
			'font-size': nodeLabelFontSize
		}).update();
		cy.style().selector('edge').style({
			'width': edgeWidth,
			'arrow-scale': edgeArrowSize
		}).update();
		cy.zoom(cy.zoom() / zFactor);
		cy.pan({x:cyWidthHalf + pan.x - (cyWidthHalf + pan.x) / zFactor - (cytoX - cyWidthHalf) / zFactor, y:cyHeightHalf + pan.y - (cyHeightHalf + pan.y) / zFactor - (cytoY - cyHeightHalf) / zFactor});
		pan = cy.pan();
		const zoomLevel = cy.zoom();
		if (graphOperationLogic == "clientSide"){
			const topModelPos = - pan.y / zoomLevel;
			const leftModelPos = - pan.x / zoomLevel;
			const bottomModelPos = topModelPos + cyHeight / zoomLevel;
			const rightModelPos = leftModelPos + cyWidth / zoomLevel;
			handler.operation = "zoomOut";
			handler.prepareOperation(topModelPos, rightModelPos, bottomModelPos, leftModelPos);
		}
		ws.send("zoomOut;" + pan.x + ";" + pan.y + ";" + zoomLevel);
	}
});




