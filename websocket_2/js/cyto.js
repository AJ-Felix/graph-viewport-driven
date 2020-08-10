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
	
var xRenderPos = 0;
var yRenderPos = 0;
var topModelPos = 0;
var rightModelPos = 2000;
var bottomModelPos = 2000;
var leftModelPos = 0;

var cyto = document.getElementById('cy');
cyto.addEventListener('mousedown', function(e){
	console.log("mouse went down in cy (drag)");
	this.onmousemove = function (e){
		var zoomLevel = cy.zoom();
		var xRenderDiff = e.movementX;
		var yRenderDiff = e.movementY;
		xRenderPos = xRenderPos + xRenderDiff;
		yRenderPos = yRenderPos + yRenderDiff;
		var xModelDiff = - (xRenderDiff / zoomLevel);
		var yModelDiff = - (yRenderDiff / zoomLevel);
		console.log("movement x: " + xRenderDiff.toString());
		console.log("movement y: " + yRenderDiff.toString());
		cy.pan({x:xRenderPos, y:yRenderPos});
		if ((xModelDiff == 0) && (yModelDiff < 0)) {
			handler.removeSpatialSelectionTop(bottomModelPos, yModelDiff);
			ws.send("panTop;" + xModelDiff + ";" + yModelDiff);
		} else if ((xModelDiff > 0) && (yModelDiff < 0)) {
			handler.removeSpatialSelectionTopRight(bottomModelPos, leftModelPos , xModelDiff, yModelDiff);
			ws.send("panTopRight;" + xModelDiff + ";" + yModelDiff);
		} else if ((xModelDiff > 0) && (yModelDiff == 0)) {
			handler.removeSpatialSelectionRight(leftModelPos , xModelDiff);
			ws.send("panRight;" + xModelDiff + ";" + yModelDiff);
		} else if ((xModelDiff > 0) && (yModelDiff > 0)) {
			handler.removeSpatialSelectionBottomRight(topModelPos, leftModelPos , xModelDiff, yModelDiff);
			ws.send("panBottomRight;" + xModelDiff + ";" + yModelDiff);
		} else if ((xModelDiff == 0) && (yModelDiff > 0)) {
			handler.removeSpatialSelectionBottom(topModelPos, yModelDiff);
			ws.send("panBottom;" + xModelDiff + ";" + yModelDiff);
		} else if ((xModelDiff < 0) && (yModelDiff > 0)) {
			handler.removeSpatialSelectionBottomLeft(topModelPos, rightModelPos, xModelDiff, yModelDiff);
			ws.send("panBottomLeft;" + xModelDiff + ";" + yModelDiff);
		} else if ((xModelDiff < 0) && (yModelDiff == 0)) {
			handler.removeSpatialSelectionLeft(rightModelPos, xModelDiff);
			ws.send("panLeft;" + xModelDiff + ";" + yModelDiff);
		} else {
			handler.removeSpatialSelectionTopLeft(rightModelPos, bottomModelPos, xModelDiff, yModelDiff);
			ws.send("panTopLeft;" + xModelDiff + ";" + yModelDiff);
		}
		topModelPos = topModelPos + yModelDiff;
		rightModelPos = rightModelPos + xModelDiff;
		bottomModelPos = bottomModelPos + yModelDiff;
		leftModelPos = leftModelPos + xModelDiff;
	}
});

// var cytoX = 0;
// var cytoY = 0;

function onmousemove(e) {
	// console.info(e.pageY - cyto.offsetTop);
	// console.info(e.pageX - cyto.offsetLeft);
	// cytoX = e.pageX - cyto.offsetLeft;
	// cytoY = e.pageY - cyto.offsetTop;
}

cyto.addEventListener("mouseup", function(e){
    this.onmousemove = null;
});

// console.info(cyto.offsetLeft);
// console.info(cyto.offsetTop);

document.addEventListener("click",
	function(){
		console.log("Mouse clicked anywhere in document!");
		console.log("cytoscape panning:");
		console.log(cy.pan());
		console.log("cytoscape zoom:");
		console.log(cy.zoom());
	}
);


// cyto.addEventListener("mousemove", onmousemove);

cyto.addEventListener("wheel", function(e) {
	e.preventDefault();
    const delta = Math.sign(e.deltaY);
    // console.info(delta);
	// console.info(e.pageX);
	const cytoX = e.pageX - cyto.offsetLeft;
	const cytoY = e.pageY - cyto.offsetTop;
	
	if (delta < 0){
		cy.zoom(cy.zoom() * 2);
		cy.pan({x:-cytoX, y:-cytoY});
		ws.send("zoomIn;" + cytoX.toString() + ";" + cytoY.toString() + ";" + cy.pan().x + ";" + cy.pan().y + ";" + cy.zoom());
	} else {
		var pan = cy.pan();
		cy.zoom(cy.zoom() * 0.5)
		cy.pan({x:-cytoX, y:-cytoY});
		ws.send("zoomOut;" + cytoX.toString() + ";" + cytoY.toString());
	}
});

