const vPix = 1000
const vPixHalf = 500;
const zFactor = 2;
let maxNumberVertices = 100;

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
        'label': 'data(id)',
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

let layoutBase = cy.collection();

let xRenderDiff = 0;
let yRenderDiff = 0;

function addVertexToLayoutBase(dataArray){
	cy.add({group : 'nodes', data: {id: dataArray[1]}});
	const vertexId = cy.$id(dataArray[1])
	console.log(vertexId);
	layoutBase = layoutBase.add(vertexId);
	clearTimeout(this.timeOut);
	this.timeOut = setTimeout(performLayout, 500);
}

function performLayout(){
	console.log("performing layout!");
	let layout = layoutBase.layout({name: "random"});
	layout.run();
	console.log("layout performed");
	let layoutBaseString = "";
	layoutBase.forEach(function(node){
		let pos = node.position();
		layoutBaseString += ";" + node.data("id") + "," + pos.x + "," + pos.y;
	})
	ws.send("layoutBaseString" + layoutBaseString);
}

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
	if (graphOperationLogic == "clientSide"){
		const pan = cy.pan();
		const xRenderPos = pan.x;
		const yRenderPos = pan.y;
		const topModelPos= - yRenderPos / zoomLevel;
		const leftModelPos = - xRenderPos / zoomLevel;
		const bottomModelPos = topModelPos + vPix / zoomLevel;
		const rightModelPos = leftModelPos + vPix / zoomLevel
		handler.operation = "pan";
		handler.prepareOperation(topModelPos, rightModelPos, bottomModelPos, leftModelPos);
	}
	// ws.send("pan;" + xModelDiff + ";" + yModelDiff);
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
	if (delta < 0){
		cy.zoom(cy.zoom() * zFactor);
		cy.pan({x:-vPixHalf + zFactor * pan.x + (vPixHalf - cytoX) * zFactor, y:-vPixHalf + zFactor * pan.y + (vPixHalf - cytoY) * zFactor});
		pan = cy.pan();
		const zoomLevel = cy.zoom();
		if (graphOperationLogic == "clientSide"){
			const topModelPos = - pan.y / zoomLevel;
			const leftModelPos = - pan.x / zoomLevel;
			const bottomModelPos = topModelPos + vPix / zoomLevel;
			const rightModelPos = leftModelPos + vPix / zoomLevel;
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

