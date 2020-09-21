class AppendHandler{
	vertexGlobalMap;
	vertexInnerMap;
	capacity;
	minDegreeVertex;
	secondMinDegreeVertex;
	newVerticesMap;
	topModel;
	rightModel;
	bottomModel;
	leftModel;
	operation;
	timeOut;
	maxNumberVertices;
	wrapperQueue;
	wrapperRunning;
	
	constructor(){
		this.vertexGlobalMap = new Map();
		this.vertexInnerMap = new Map();
		this.maxNumberVertices = 10;
		this.wrapperQueue = new Array();
		this.minDegreeVertex = null;
		this.secondMinDegreeVertex = null;
	}
	
	prepareOperation(topModel, rightModel, bottomModel, leftModel){
			this.topModel = topModel;
			this.rightModel = rightModel;
			this.bottomModel = bottomModel;
			this.leftModel = leftModel;
		if (this.operation != "zoomOut"){
			cy.edges().forEach(
				function(edge){
					const sourceX = this.vertexGlobalMap.get(edge.data('source')).get('vertex').x;
					const sourceY = this.vertexGlobalMap.get(edge.data('source')).get('vertex').y;
					const targetX = this.vertexGlobalMap.get(edge.data('target')).get('vertex').x;
					const targetY = this.vertexGlobalMap.get(edge.data('target')).get('vertex').y;
					if (((sourceX < leftModel) || (rightModel < sourceX) || (sourceY < topModel) || (bottomModel < sourceY)) &&
							((targetX  < leftModel) || (rightModel < targetX ) || (targetY  < topModel) || (bottomModel < targetY))){
						cy.remove(edge);
					}
				}, this
			)
			this.vertexInnerMap.forEach(
				function(value, key){
					if ((value.x < leftModel) || (rightModel < value.x) || (value.y < topModel) || (bottomModel < value.y)){
						this.vertexInnerMap.delete(key);
					}
				}, this
			)
			this.capacity = this.maxNumberVertices - this.vertexInnerMap.size;
		} else {
			this.capacity = 0;
		}
		if (this.operation == "pan" || this.operation == "zoomOut") {
			this.newVerticesMap = this.vertexInnerMap;
		} else {
			this.newVerticesMap = new Map();
		}
	}
	
	updateMinDegreeVertex(vertex){
		if (vertex.degree < this.minDegreeVertex.degree) {
			this.secondMinDegreeVertex = this.minDegreeVertex;
			this.minDegreeVertex = vertex;
		} else if (vertex.degree < this.secondMinDegreeVertex.degree) {
			this.secondMinDegreeVertex = vertex;
		}
	}
	
	updateMinDegreeVertices(map){
		const iter = map.values();
		this.minDegreeVertex = iter.next().value;
		this.secondMinDegreeVertex = iter.next().value;
		if (this.secondMinDegreeVertex.degree < this.minDegreeVertex.degree) {
			const v = this.minDegreeVertex;
			this.minDegreeVertex = this.secondMinDegreeVertex;
			this.secondMinDegreeVertex = v;
		}
		map.forEach(
			function(vertex, id){
				if (vertex.degree < this.minDegreeVertex.degree && vertex.id != this.secondMinDegreeVertex.id) {
					this.secondMinDegreeVertex = this.minDegreeVertex;
					this.minDegreeVertex = vertex;
				} else if (vertex.degree < this.secondMinDegreeVertex.degree && vertex.id != this.minDegreeVertex.id)  {
					this.secondMinDegreeVertex = vertex;
				}
			}, this
		)
	
	}
	
	reduceNeighborIncidence(vertex){
		cy.$id(vertex.id).neighborhood("node").forEach(
			function (node) {
				const vertexMap = this.vertexGlobalMap.get(node.data('id'));
				vertexMap.set('incidence', vertexMap.get('incidence') - 1);
			}, this
		)
	}
	
	addIdentityWrapperInitial(vertex){
		const added = this.addVertex(vertex);
		if (added) this.vertexInnerMap.set(vertex.id, vertex);	
	}	
	
	addNonIdentityWrapperInitial(edgeId, edgeLabel, sourceVertex, targetVertex){
		const addedSource = this.addVertex(sourceVertex);
		if (addedSource) this.vertexInnerMap.set(sourceVertex.id, sourceVertex);
		const addedTarget = this.addVertex(targetVertex);
		if (addedTarget) this.vertexInnerMap.set(targetVertex.id, targetVertex);
		this.addEdge(edgeId, sourceVertex, targetVertex);
	}
	
	addIdentityWrapper(vertex){
		console.log("in identityWrapper with vertex: " + vertex.id);
		if (this.capacity > 0) {
			const added = this.addVertex(vertex);
			if (added) {
				this.newVerticesMap.set(vertex.id, vertex);
				this.updateMinDegreeVertex(vertex);
				this.capacity -= 1;
			}
		} else {
			if (vertex.degree > this.minDegreeVertex.degree) {
				const added = this.addVertex(vertex);
				if (added) {
					this.newVerticesMap.set(vertex.id, vertex);
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					if (this.newVerticesMap.size > 1) {
						this.updateMinDegreeVertices(this.newVerticesMap);
					} else if (this.newVerticesMap.size == 1) {
						this.minDegreeVertex = vertex;
					}
				}
			} 
		}
	}
		
	addNonIdentityWrapper(edgeId, edgeLabel, sourceVertex, targetVertex){
		let addedSource;
		let addedTarget;
		if (this.capacity > 1){
			addedSource = this.addVertex(sourceVertex);
			if ((sourceVertex.x >= this.leftModel) && (this.rightModel >= sourceVertex.x) && (sourceVertex.y >= this.topModel) && (this.bottomModel >= sourceVertex.y) && addedSource){
				this.updateMinDegreeVertex(sourceVertex);
				this.newVerticesMap.set(sourceVertex.id, sourceVertex);
				this.capacity -= 1;
			}
			addedTarget = this.addVertex(targetVertex);
			if ((targetVertex.x >= this.leftModel) && (this.rightModel >= targetVertex.x) && (targetVertex.y >= this.topModel) && (this.bottomModel >= targetVertex.y) && addedTarget){
				this.updateMinDegreeVertex(targetVertex);
				this.newVerticesMap.set(targetVertex.id, targetVertex);
				this.capacity -= 1;
			}
			this.addEdge(edgeId, sourceVertex, targetVertex);
		} else {
			let sourceIn = true;
			let targetIn = true;
			if ((sourceVertex.x < this.leftModel) || (this.rightModel < sourceVertex.x) || (sourceVertex.y < this.topModel) || (this.bottomModel < sourceVertex.y)){
				sourceIn = false;
			}
			if ((targetVertex.x < this.leftModel) || (this.rightModel < targetVertex.x) || (targetVertex.y < this.topModel) || (this.bottomModel < targetVertex.y)){
				targetIn = false;
			}
			if ((sourceIn && targetIn) && (sourceVertex.degree > this.secondMinDegreeVertex.degree) && (targetVertex.degree > this.secondMinDegreeVertex.degree)) {
				addedSource = this.addVertex(sourceVertex);
				addedTarget = this.addVertex(targetVertex);
				this.addEdge(edgeId, sourceVertex, targetVertex);
				if (addedSource && addedTarget) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.reduceNeighborIncidence(this.secondMinDegreeVertex);
					this.removeVertex(this.secondMinDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					this.newVerticesMap.set(sourceVertex.id, sourceVertex);
					this.newVerticesMap.set(targetVertex.id, targetVertex);
					this.updateMinDegreeVertices(this.newVerticesMap);
				} else if (addedSource || addedTarget) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					if (this.newVerticesMap.size > 1) {
						this.updateMinDegreeVertices(this.newVerticesMap);
					} else if (addedSource) {
						this.minDegreeVertex = sourceVertex;
					} else if (addedTarget) {
						this.minDegreeVertex = targetVertex;
					}
					if (addedSource) this.newVerticesMap.set(sourceVertex.id, sourceVertex);
					if (addedTarget) this.newVerticesMap.set(targetVertex.id, targetVertex);
				}
			} else if (sourceIn && !(targetIn) && sourceVertex.degree > this.minDegreeVertex.degree) {
				addedSource = this.addVertex(sourceVertex);
				addedTarget = this.addVertex(targetVertex);
				this.addEdge(edgeId, sourceVertex, targetVertex);
				if (addedSource) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					if (this.newVerticesMap.size > 1) {
						this.updateMinDegreeVertices(this.newVerticesMap);
					} else {
						this.minDegreeVertex = sourceVertex;
					} 
					this.newVerticesMap.set(sourceVertex.id, sourceVertex);
				}
			} else if (targetIn && !(sourceIn) && targetVertex.degree > this.minDegreeVertex.degree) {
				addedSource = this.addVertex(sourceVertex);
				addedTarget = this.addVertex(targetVertex);
				this.addEdge(edgeId, sourceVertex, targetVertex);
				if (addedTarget) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					if (this.newVerticesMap.size > 1) {
						this.updateMinDegreeVertices(this.newVerticesMap);
					} else {
						this.minDegreeVertex = targetVertex;
					} 
					this.newVerticesMap.set(targetVertex.id, targetVertex);
				}
			}
		}
	}
		
	// addWrapperToQueue(dataArray){
		// this.wrapperQueue.push(dataArray);
		// if (!this.addWrapperRunning) {
			// this.addWrapperRunning = true;
			// this.addWrapper(); 
		// }
	// }
	
	// async 
	addWrapper(dataArray){
		// if (this.wrapperQueue.length > 0) {
			// let dataArray = this.wrapperQueue.shift();
			// let promise = new Promise((resolve, reject) => {
				const edgeId = dataArray[1];
				const edgeLabel = dataArray[2];
				const sourceVertex = new Vertex(dataArray[3], dataArray[4], dataArray[5], dataArray[6]);
				const targetVertex = new Vertex(dataArray[7], dataArray[8], dataArray[9], dataArray[10]);
				// console.log("sourceId: " + sourceVertex.id + ", targetId: " + targetVertex.id + ", edgeId: " + edgeId);
				// if (this.minDegreeVertex != null) console.log("minDegreeVertexId: " + this.minDegreeVertex.id + ", secondMinDegreeVertexId: " + this.secondMinDegreeVertex.id)
				// console.log("capacity: " + this.capacity)
				if (this.operation == "initial") {
					if (edgeLabel == "identityEdge"){
						this.addIdentityWrapperInitial(sourceVertex);
					} else {
						this.addNonIdentityWrapperInitial(edgeId, edgeLabel, sourceVertex, targetVertex);
					}
				} else {
					if (edgeLabel == "identityEdge"){
						this.addIdentityWrapper(sourceVertex);
					} else {
						this.addNonIdentityWrapper(edgeId, edgeLabel, sourceVertex, targetVertex);
					}
				}
				clearTimeout(this.timeOut);
				this.timeOut = setTimeout(clearOperation, 500);
				// resolve(true);
			// });
			// await promise;
			// this.addWrapper();
		// } else {
			// this.addWrapperRunning = false;
		// }
	}
	
	addVertex(vertex){
		if (!this.vertexGlobalMap.has(vertex.id)){
			var map = new Map();
			map.set("incidence", 1);
			map.set("vertex", vertex);
			this.vertexGlobalMap.set(vertex.id, map);	
			cy.add({group : 'nodes', data: {id: vertex.id}, position: {x: parseInt(vertex.x) , y: parseInt(vertex.y)}});
			return true;
		} else {
			var map = this.vertexGlobalMap.get(vertex.id);
			map.set("incidence" , map.get("incidence") + 1);
			return false;
		}
	}
	
	removeVertex(vertex){
		if (!this.vertexGlobalMap.has(vertex.id)) {
				console.log("cannot remove vertex because not in vertexGlobalMap, id: " + vertex.id);
		} else {
			this.newVerticesMap.delete(vertex.id);
			this.vertexGlobalMap.delete(vertex.id);
			cy.remove(cy.$id(vertex.id));				
		}
	}
	
	addEdge(edgeId, sourceVertex, targetVertex){
		cy.add({group : 'edges', data: {id: edgeId, source: sourceVertex.id , target: targetVertex.id}});
	}
}

function clearOperation(){
	if (handler.operation != "initial"){
		handler.vertexInnerMap = new Map([...handler.newVerticesMap, ...handler.vertexInnerMap]);
		cy.nodes().forEach( 
			function(node){
				const pos = node.position();
				//Welche Abfragen sind hier wirklich notwendig?
				//Ein Knoten wird entfernt, wenn er außerhalb des Zielbereichs liegt UND keinen Nachbarn hat
				//Ein Knoten wird entfernt, wenn er innerhalb des Zielbereichs liegt, aber nicht Teil der vertexInnerMap ist
				//Ein Knoten wird entfernt, wenn alle add/delete-Operationen sich gegenseitig aufgehoben haben (i.e. Inzidenz == 0)
				if ((((pos.x < this.leftModel) || (this.rightModel < pos.x) || (pos.y < this.topModel) || (this.bottomModel < pos.y)) && node.neighborhood().length == 0) || 
						((pos.x >= this.leftModel) && (this.rightModel >= pos.x) && (pos.y >= this.topModel) && (this.bottomModel >= pos.y) && !this.vertexInnerMap.has(node.data('id')))) {
					cy.remove(node);
					this.vertexGlobalMap.delete(node.data('id'));
				} 
			}, handler
		)
	} else {
		handler.newVerticesMap = handler.vertexInnerMap;
	}
	var edgeIdString = "";
	cy.edges().forEach(
		function(edge){
			edgeIdString += ";" + edge.data('id');
		}
	)
	var vertexIdString = "";
	handler.vertexInnerMap.forEach(
		function(vertex, id){
			vertexIdString += ";" + id;
		}
	)
	handler.operation = null;
	ws.send("edgeIdString" + edgeIdString);
	ws.send("vertexIdString" + vertexIdString);
	if (handler.newVerticesMap.size > 1) {
		handler.updateMinDegreeVertices(handler.newVerticesMap);
	} else if (handler.newVerticesMap.size == 1) {
		handler.minDegreeVertex = handler.newVerticesMap.values().next().value;
	}
}