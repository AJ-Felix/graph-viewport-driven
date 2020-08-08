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
  userZoomingEnabled: true,
  panningEnabled: true,
  userPanningEnabled: true,
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

cy.add({
		group: 'nodes',
		data: { weight: 75 , id: "200, 200"},
		position: { x: 200, y: 200 }
	});
	
cy.add({
		group: 'nodes',
		data: { weight: 75 , id: "100, 100"},
		position: { x: 100, y: 100 }
	});
	
cy.add({
		group: 'nodes',
		data: { weight: 75 , id: "300, 300"},
		position: { x: 300, y: 300 }
	});
	
cy.add({
		group: 'nodes',
		data: { weight: 75 , id: "100, 200"},
		position: { x: 100, y: 200 }
	});
	
cy.add({
		group: 'nodes',
		data: { weight: 75 , id: "100, 300"},
		position: { x: 100, y: 300 }
	});
	
cy.add({
		group: 'nodes',
		data: { weight: 75 , id: "200, 100"},
		position: { x: 200, y: 100 }
	});
	
cy.add({
		group: 'nodes',
		data: { weight: 75 , id: "200, 300"},
		position: { x: 200, y: 300 }
	});
	
cy.add({
		group: 'nodes',
		data: { weight: 75 , id: "300, 100"},
		position: { x: 300, y: 100 }
	});
	
cy.add({
		group: 'nodes',
		data: { weight: 75 , id: "300, 200"},
		position: { x: 300, y: 200 }
	});
	


var cyto = document.getElementById('cy');
cyto.addEventListener('mousedown', function(e){
	console.log("mouse went down in cy (drag)");
	this.onmousemove = function (e){
		console.log(e.movementX);
		console.log("cytoscape panning: " + cy.pan());
		console.log("cytoscape zoom: " + cy.zoom());
		ws.send("pan;" + e.movementX + ";" + e.movementY);
	}
});

cyto.addEventListener("mouseup", function(e){
    this.onmousemove = null
});

document.addEventListener("click",
	function(){
		console.log("Mouse clicked anywhere in document!");
		console.log("cytoscape panning:");
		console.log(cy.pan());
		console.log("cytoscape zoom:");
		console.log(cy.zoom());
	}
);

