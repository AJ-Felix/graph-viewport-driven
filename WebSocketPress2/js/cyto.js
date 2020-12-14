class GraphVisualizer {

	constructor(){
		this.minNodeWidth = 20;
		this.maxNodeWidth = 80;
		this.minNodeHeight = 20;
		this.maxNodeHeight = 80;
		this.nodeLabelFontSize = 32;
		this.edgeWidth = 5;
		this.edgeArrowSize = 2;
		this.currentMaxDegree;
		this.currentMinDegree;
		this.zoomLevel = 0.25;
		this.zFactor = 2;
		this.currentMaxDegree = null;
		this.currentMaxDegree = null;
		this.cytoConfig = {
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
					'height': this.maxNodeHeight,
					'width': this.maxNodeWidth,
					'font-size': this.nodeLabelFontSize
				}
				},
		
				{
				selector: 'edge',
				style: {
					'width': this.edgeWidth,
					'line-color': '#ccc',
					'target-arrow-color': '#ccc',
					'target-arrow-shape': 'triangle',
					'curve-style': 'bezier',
					'arrow-scale': this.edgeArrowSize
				}
				}
			],
		
			layout: {
				name: 'grid',
				rows: 1
			},
			
				// initial viewport state:
			zoom: this.zoomLevel,
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
			};
		this.cy = cytoscape(this.cytoConfig);
	}
		
	styleOnZoom(direction){
		let zFactor;
		if (direction == "in"){
			zFactor = 1 / this.zFactor;
		} else {
			zFactor = this.zFactor;
		}
		this.nodeLabelFontSize = this.nodeLabelFontSize * zFactor;
		this.edgeWidth = this.edgeWidth * zFactor;
		this.edgeArrowSize = this.edgeArrowSize * zFactor;
		this.cy.style().selector('node').style({
			'font-size': this.nodeLabelFontSize
		}).update();
		this.cy.style().selector('edge').style({
			'width': this.edgeWidth,
			'arrow-scale': this.edgeArrowSize
		}).update();
		this.cy.nodes().forEach( function(node){
			let nodeWidth = node.style().width;
			nodeWidth = parseInt(nodeWidth.substring(0, nodeWidth.length - 2)) * zFactor;
			node.style({'width': nodeWidth, 'height': nodeWidth});
		});
	}

	addVertexToLayoutBase(dataArray){
		const xVertex = this.layoutWindow.x1 + Math.random() * (this.layoutWindow.x2 - this.layoutWindow.x1);
		const yVertex = this.layoutWindow.y1 + Math.random() * (this.layoutWindow.y2 - this.layoutWindow.y1);
		this.cy.add({group : 'nodes', data: {id: dataArray[1], label: dataArray[3], degree: dataArray[2]}, position: {x: 0, y: 0}});
		this.layoutBase.add(dataArray[1]);
	}

	derivelayoutWindow(topModel, rightModel, bottomModel, leftModel){
		let x1Var, x2Var, y1Var, y2Var;
		if (topModel < 0) {
			y1Var = 0;
		} else {
			y1Var = topModel;
		}
		if (rightModel > 4000) {
			x2Var = 4000;
		} else {
			x2Var = rightModel;
		}
		if (bottomModel > 4000) {
			y2Var = 4000;
		} else {
			y2Var = bottomModel;
		}
		if (leftModel < 0) {
			x1Var = 0;
		} else {
			x1Var = leftModel;
		}
		return {x1: x1Var, y1: y1Var, x2: x2Var, y2: y2Var};
	}

	updateVertexSize(vertexId){
		const nodeWidth = this.minNodeWidth / (this.zoomLevel * this.zFactor);
		const nodeHeight = this.minNodeHeight / (this.zoomLevel * this.zFactor);
		this.cy.$id(vertexId).style({'width': nodeWidth, 'height': nodeHeight});
	}

	updateVerticesSize(){
		const zoomScale = this.zoomLevel * this.zFactor;
		const maxNodeHeightThisZoom = this.maxNodeHeight / zoomScale;
		const minNodeHeightThisZoom = this.minNodeHeight / zoomScale;
		const nodeHeightDiff = maxNodeHeightThisZoom - minNodeHeightThisZoom;
		const maxNodeWidthThisZoom = this.maxNodeWidth / zoomScale;
		const minNodeWidthThisZoom = this.minNodeWidth / zoomScale;
		const nodeWidthDiff = maxNodeWidthThisZoom - minNodeWidthThisZoom;
		const nodeLabelFontSizeThisZoom = this.nodeLabelFontSize / zoomScale;
		const degreeRange = this.currentMaxDegree - this.currentMinDegree;
		const currentMinDegree = this.currentMinDegree;
		this.cy.nodes().forEach(function(node){
			let scale;
			if (degreeRange == 0) scale = 1;
			else scale = (node.data('degree') - currentMinDegree) / degreeRange;
			const nodeHeight = minNodeHeightThisZoom + scale * nodeHeightDiff;
			const nodeWidth = minNodeWidthThisZoom + scale * nodeWidthDiff;
			node.style({'height':nodeHeight, 'width':nodeWidth});
		});
	}

	updateDegreeExtrema(degree){
		if (this.currentMaxDegree == null || this.currentMinDegree == null){
			this.currentMaxDegree = degree;
			this.currentMinDegree = degree;
		} else if (degree > this.currentMaxDegree){
			this.currentMaxDegree = degree;
		} else if (degree < this.currentMinDegree){
			this.currentMinDegree = degree;
		}
	}
}