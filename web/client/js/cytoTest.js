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
        'label': 'data(id)'
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
  minZoom: 1e-50,
  maxZoom: 1e50,
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

var socket =  io.connect('http://localhost:9092');

function sendSignal() {
				socket.emit('buildTopView', 'blabla');
}

socket.on('connect', function() {
	output('<span class="connect-msg">Client has connected to the server!</span>');
	cy.add({
		group: 'nodes',
		data: { weight: 75 },
		position: { x: 200, y: 200 }
	});
});

socket.on('disconnect', function() {
	output('<span class="disconnect-msg">The client has disconnected!</span>');
});

function output(message) {
				var currentTime = "<span class='time'>" +  moment().format('HH:mm:ss.SSS') + "</span>";
				var element = $("<div>" + currentTime + " " + message + "</div>");
	$('#console').prepend(element);
}

socket.on('addVertex', function(vertexObject) {
	console.log('add: vertex id is' + vertexObject.id);
	cy.add({
	    group: 'nodes',
	    data: { id: vertexObject.id },
	    position: { x: vertexObject.x, y: vertexObject.y }
	});
});

socket.on('removeVertex', function(vertexObject) {
	console.log('remove: vertex id is' + vertexObject.id);
	cy.remove(cy.$id(vertexObject.id));
});

socket.on('addEdge', function(edgeObject) {
	console.log('add: edge id is' + edgeObject.id);
	// cy.add({
	    // group: 'edges',
	    // data: { id: edgeObject.id, source: edgeObject.source, target: edgeObject.target }
	// });
});

socket.on('removeEdge', function(edgeObject) {
	console.log('remove edge id is' + edgeObject.id);
	cy.remove({
	    group: 'edges',
		data: { id: edgeObject.id, source: edgeObject.source, target: edgeObject.target }
	});
});


