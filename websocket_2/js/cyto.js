const vPix = 1000
const vPixHalf = 500;
const zFactor = 2;

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
  zoom: 1,
  pan: { x: 0, y: 0 },

  // interaction options:
  minZoom: 1e-1,
  maxZoom: 1e1,
  zoomingEnabled: true,
  userZoomingEnabled: false,
  panningEnabled: true,
  userPanningEnabled: false,
  boxSelectionEnabled: true,
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

// cy.add({
		// group: 'nodes',
		// data: { weight: 75 , id: "200, 200"},
		// position: { x: 200, y: 200 }
	// });
	
// cy.add({
		// group: 'nodes',
		// data: { weight: 75 , id: "100, 100"},
		// position: { x: 100, y: 100 }
	// });
	
// cy.add({
		// group: 'nodes',
		// data: { weight: 75 , id: "300, 300"},
		// position: { x: 300, y: 300 }
	// });
	
// cy.add({
		// group: 'nodes',
		// data: { weight: 75 , id: "100, 200"},
		// position: { x: 100, y: 200 }
	// });
	
// cy.add({
		// group: 'nodes',
		// data: { weight: 75 , id: "100, 300"},
		// position: { x: 100, y: 300 }
	// });
	
// cy.add({
		// group: 'nodes',
		// data: { weight: 75 , id: "200, 100"},
		// position: { x: 200, y: 100 }
	// });
	
// cy.add({
		// group: 'nodes',
		// data: { weight: 75 , id: "200, 300"},
		// position: { x: 200, y: 300 }
	// });
	
// cy.add({
		// group: 'nodes',
		// data: { weight: 75 , id: "300, 100"},
		// position: { x: 300, y: 100 }
	// });
	
// cy.add({
		// group: 'nodes',
		// data: { weight: 75 , id: "300, 200"},
		// position: { x: 300, y: 200 }
	// });
	
// var xRenderPos = 0;
// var yRenderPos = 0;
// var topModelPosPrevious = 0;
// var rightModelPosPrevious = 2000;
// var bottomModelPosPrevious = 2000;
// var leftModelPosPrevious = 0;

var xRenderDiff = 0;
var yRenderDiff = 0;

var cyto = document.getElementById('cy');
cyto.addEventListener('mousedown', function(e){
	console.log("mouse went down in cy (drag)");
	xRenderDiff = 0;
	yRenderDiff = 0;
	this.onmousemove = function (e){
		xRenderDiff = xRenderDiff + e.movementX;
		yRenderDiff = yRenderDiff + e.movementY;
		var xRenderPos = cy.pan().x;
		var yRenderPos = cy.pan().y;
		console.log("accumulated movement x: " + xRenderDiff.toString());
		console.log("accumulated movement y: " + yRenderDiff.toString());
		cy.pan({x:xRenderPos + e.movementX, y:yRenderPos + e.movementY});
	}
});

cyto.addEventListener("mouseup", function(e){
    this.onmousemove = null;
	const zoomLevel = cy.zoom();
	var xModelDiff = - (xRenderDiff / zoomLevel);
	var yModelDiff = - (yRenderDiff / zoomLevel);
	const xRenderPos = cy.pan().x;
	const yRenderPos = cy.pan().y;
	const topModelPosPrevious = - yRenderPos / zoomLevel - yModelDiff;
	const leftModelPosPrevious = - xRenderPos / zoomLevel - xModelDiff;
	const bottomModelPosPrevious = topModelPosPrevious + vPix / zoomLevel;
	const rightModelPosPrevious = leftModelPosPrevious + vPix / zoomLevel
	console.info("topModelPosPrevious " + topModelPosPrevious);
	console.info("rightModelPosPrevious " + rightModelPosPrevious);
	console.info("bottomModelPosPrevious " +bottomModelPosPrevious);
	console.info("leftModelPosPrevious " + leftModelPosPrevious);
	console.info("xModelDiff " + xModelDiff);
	console.info("yModelDiff " + yModelDiff);
	if ((xModelDiff == 0) && (yModelDiff < 0)) {
			handler.removeSpatialSelectionTop(bottomModelPosPrevious, yModelDiff);
			ws.send("panTop;" + xModelDiff + ";" + yModelDiff);
		} else if ((xModelDiff > 0) && (yModelDiff < 0)) {
			handler.removeSpatialSelectionTopRight(bottomModelPosPrevious, leftModelPosPrevious , xModelDiff, yModelDiff);
			ws.send("panTopRight;" + xModelDiff + ";" + yModelDiff);
		} else if ((xModelDiff > 0) && (yModelDiff == 0)) {
			handler.removeSpatialSelectionRight(leftModelPosPrevious , xModelDiff);
			ws.send("panRight;" + xModelDiff + ";" + yModelDiff);
		} else if ((xModelDiff > 0) && (yModelDiff > 0)) {
			handler.removeSpatialSelectionBottomRight(topModelPosPrevious, leftModelPosPrevious , xModelDiff, yModelDiff);
			ws.send("panBottomRight;" + xModelDiff + ";" + yModelDiff);
		} else if ((xModelDiff == 0) && (yModelDiff > 0)) {
			handler.removeSpatialSelectionBottom(topModelPosPrevious, yModelDiff);
			ws.send("panBottom;" + xModelDiff + ";" + yModelDiff);
		} else if ((xModelDiff < 0) && (yModelDiff > 0)) {
			handler.removeSpatialSelectionBottomLeft(topModelPosPrevious, rightModelPosPrevious, xModelDiff, yModelDiff);
			ws.send("panBottomLeft;" + xModelDiff + ";" + yModelDiff);
		} else if ((xModelDiff < 0) && (yModelDiff == 0)) {
			handler.removeSpatialSelectionLeft(rightModelPosPrevious, xModelDiff);
			ws.send("panLeft;" + xModelDiff + ";" + yModelDiff);
		} else {
			handler.removeSpatialSelectionTopLeft(rightModelPosPrevious, bottomModelPosPrevious, xModelDiff, yModelDiff);
			ws.send("panTopLeft;" + xModelDiff + ";" + yModelDiff);
		}
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
	var pan = cy.pan();
	if (delta < 0){
		cy.zoom(cy.zoom() * zFactor);
		cy.pan({x:-vPixHalf + zFactor * pan.x + (vPixHalf - cytoX) * zFactor, y:-vPixHalf + zFactor * pan.y + (vPixHalf - cytoY) * zFactor});
		pan = cy.pan();
		var zoomLevel = cy.zoom();
		const topModelPos = - pan.y / zoomLevel;
		const leftModelPos = - pan.x / zoomLevel;
		const bottomModelPos = topModelPos + vPix / zoomLevel;
		const rightModelPos = leftModelPos + vPix / zoomLevel;
		console.info(topModelPos);
		var countRemoved = handler.removeSpatialSelectionZoomIn(topModelPos, rightModelPos, bottomModelPos, leftModelPos);
		ws.send("zoomIn;" + pan.x + ";" + pan.y + ";" + cy.zoom() + ";" + countRemoved);
	} else {
		cy.zoom(cy.zoom() / zFactor);
		cy.pan({x:vPixHalf + pan.x - (vPixHalf + pan.x) / zFactor - (cytoX - vPixHalf) / zFactor, y:vPixHalf + pan.y - (vPixHalf + pan.y) / zFactor - (cytoY - vPixHalf) / zFactor});
		ws.send("zoomOut;" + pan.x + ";" + pan.y + ";" + cy.zoom());
	}
});

