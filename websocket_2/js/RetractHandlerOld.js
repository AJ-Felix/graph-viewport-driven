class RetractHandler{
	vertexGlobalMap;
	vertexInnerMap;
	newVerticesMap;
	capacity;
	minDegreeVertex;
	secondMinDegreeVertex;
	topModel;
	rightModel;
	bottomModel;
	leftModel;
	operation;
	timeOut;
	maxVertices;
	// wrapperQueue;
	// wrapperRunning;
	
	constructor(){
		this.vertexGlobalMap = new Map();
		this.vertexInnerMap = new Map();
		this.newVerticesMap = new Map();
		this.maxVertices = 100;
		this.wrapperQueue = new Array();
		this.minDegreeVertex = null;
		this.secondMinDegreeVertex = null;
		this.operation = "initial";
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
		}
		this.capacity = this.maxVertices - this.vertexInnerMap.size;
		if (this.operation == "pan" || this.operation == "zoomOut") {
			this.newVerticesMap = this.vertexInnerMap;
			this.vertexInnerMap = new Map();
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
	
	registerInside(vertex){
		this.newVerticesMap.set(vertex.id, vertex);
		if (this.newVerticesMap.size > 1) {
			this.updateMinDegreeVertices(this.newVerticesMap);
		} else if (this.newVerticesMap.size == 1) {
			this.minDegreeVertex = vertex;
		}
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
		const vertexIsRegisteredInside = this.newVerticesMap.has(vertex.id) || this.vertexInnerMap.has(vertex.id);
		if (this.capacity > 0) {
			this.addVertex(vertex);
			if (!vertexIsRegisteredInside) {
				this.newVerticesMap.set(vertex.id, vertex);
				this.updateMinDegreeVertex(vertex);
				this.capacity -= 1;
			}
		} else {
			if (vertex.degree > this.minDegreeVertex.degree) {
				this.addVertex(vertex);
				if (!vertexIsRegisteredInside) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					this.registerInside(vertex);
				}
			} 
		}
	}
		
	addNonIdentityWrapper(edgeId, edgeLabel, sourceVertex, targetVertex){
		const sourceIsRegisteredInside = this.newVerticesMap.has(sourceVertex.id) || this.vertexInnerMap.has(sourceVertex.id);
		const targetIsRegisteredInside = this.newVerticesMap.has(targetVertex.id) || this.vertexInnerMap.has(targetVertex.id);
		if (this.capacity > 1){
			this.addVertex(sourceVertex);
			if ((sourceVertex.x >= this.leftModel) && (this.rightModel >= sourceVertex.x) && (sourceVertex.y >= this.topModel) && (this.bottomModel >= sourceVertex.y) 
					&& !sourceIsRegisteredInside){
				this.updateMinDegreeVertex(sourceVertex);
				this.newVerticesMap.set(sourceVertex.id, sourceVertex);
				this.capacity -= 1;
			}
			this.addVertex(targetVertex);
			if ((targetVertex.x >= this.leftModel) && (this.rightModel >= targetVertex.x) && (targetVertex.y >= this.topModel) && (this.bottomModel >= targetVertex.y) 
					&& !targetIsRegisteredInside){
				this.updateMinDegreeVertex(targetVertex);
				this.newVerticesMap.set(targetVertex.id, targetVertex);
				this.capacity -= 1;
			}
			this.addEdge(edgeId, sourceVertex, targetVertex);
		} else if (this.capacity == 1) {
			let sourceIn = true;
			let targetIn = true;
			if ((sourceVertex.x < this.leftModel) || (this.rightModel < sourceVertex.x) || (sourceVertex.y < this.topModel) || (this.bottomModel < sourceVertex.y)){
				sourceIn = false;
			}
			if ((targetVertex.x < this.leftModel) || (this.rightModel < targetVertex.x) || (targetVertex.y < this.topModel) || (this.bottomModel < targetVertex.y)){
				targetIn = false;
			}
			if (sourceIn && targetIn) {
				let sourceAdmission = false;
				let targetAdmission = false;
				if (sourceVertex.degree > targetVertex.degree){
					this.addVertex(sourceVertex);
					sourceAdmission = true;
					if (targetVertex.degree > this.minDegreeVertex.degree || sourceIsRegisteredInside){
						this.addVertex(targetVertex);
						targetAdmission = true;
						this.addEdge(edgeId, sourceVertex, targetVertex);
					}
				} else {
					this.addVertex(targetVertex);
					targetAdmission = true;
					if (sourceVertex.degree > this.minDegreeVertex.degree || targetIsRegisteredInside){
						this.addVertex(sourceVertex);
						sourceAdmission = true;
						this.addEdge(edgeId, sourceVertex, targetVertex);
					}
				}
				if (!sourceIsRegisteredInside && sourceAdmission && !targetIsRegisteredInside && targetAdmission) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					this.newVerticesMap.put(sourceVertex.id, sourceVertex);
					this.newVerticesMap.put(targetVertex.id, targetVertex);
				} else if (!sourceIsRegisteredInside && sourceAdmission) {
					this.registerInside(sourceVertex);
				} else if (!targetIsRegisteredInside && targetAdmission) {
					this.registerInside(targetVertex);
				}
				this.capacity -= 1 ;
			} else if (sourceIn) {
				this.addVertex(sourceVertex);
				this.addVertex(targetVertex);
				this.addEdge(edgeId, sourceVertex, targetVertex);
				if (!sourceIsRegisteredInside) {
					this.capacity -= 1 ;
					this.registerInside(sourceVertex);
				}
			} else if (targetIn) {
				this.addVertex(targetVertex);
				this.addVertex(sourceVertex);
				this.addEdge(edgeId, sourceVertex, targetVertex);
				if (!targetIsRegisteredInside) {
					this.capacity -= 1 ;
					this.registerInside(targetVertex);
				}
			}
		} else {
			let sourceIn = true;
			let targetIn = true;
			if ((sourceVertex.x < this.leftModel) || (this.rightModel < sourceVertex.x) || (sourceVertex.y < this.topModel) || (this.bottomModel < sourceVertex.y)){
				sourceIn = false;
			}
			if ((targetVertex.x < this.leftModel) || (this.rightModel < targetVertex.x) || (targetVertex.y < this.topModel) || (this.bottomModel < targetVertex.y)){
				targetIn = false;
			}
			if (sourceIn && targetIn && (sourceVertex.degree > this.secondMinDegreeVertex.degree) && (targetVertex.degree > this.secondMinDegreeVertex.degree)) {
				this.addVertex(sourceVertex);
				this.addVertex(targetVertex);
				this.addEdge(edgeId, sourceVertex, targetVertex);
				if (!sourceIsRegisteredInside && !targetIsRegisteredInside) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.reduceNeighborIncidence(this.secondMinDegreeVertex);
					this.removeVertex(this.secondMinDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					this.newVerticesMap.set(sourceVertex.id, sourceVertex);
					this.newVerticesMap.set(targetVertex.id, targetVertex);
					this.updateMinDegreeVertices(this.newVerticesMap);
				} else if (!sourceIsRegisteredInside) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					this.registerInside(sourceVertex);
				} else if (!targetIsRegisteredInside) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					this.registerInside(targetVertex);
				}
			} else if (sourceIn && !targetIn && (sourceVertex.degree > this.minDegreeVertex.degree || sourceIsRegisteredInside)) {
				this.addVertex(sourceVertex);
				this.addVertex(targetVertex);
				this.addEdge(edgeId, sourceVertex, targetVertex);
				if (!sourceIsRegisteredInside) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					this.registerInside(sourceVertex);
				}
			} else if (targetIn && !sourceIn && (targetVertex.degree > this.minDegreeVertex.degree || targetIsRegisteredInside)) {
				this.addVertex(sourceVertex);
				this.addVertex(targetVertex);
				this.addEdge(edgeId, sourceVertex, targetVertex);
				if (!targetIsRegisteredInside) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					this.registerInside(targetVertex);
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
				// console.log("this.capacity: " + this.capacity)
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
	
	removeWrapper(dataArray){
		console.log("removing wrapper");
		const edgeId = dataArray[1];
		const edgeLabel = dataArray[2];
		const sourceVertex = new Vertex(dataArray[3], dataArray[4], dataArray[5], dataArray[6]);
		const targetVertex = new Vertex(dataArray[7], dataArray[8], dataArray[9], dataArray[10]);
		if (edgeLabel != "identityEdge"){
			cy.remove(cy.$id(edgeId));
			let targetMap = this.vertexGlobalMap.get(targetVertex.id);
			let targetIncidence = targetMap.get("incidence");
			if (targetIncidence == 1) {
				cy.remove(cy.$id(targetVertex.id));
				this.vertexGlobalMap.delete(targetVertex.id);
				if (this.newVerticesMap.has(targetVertex.id)) this.newVerticesMap.delete(targetVertex.id);
				if (this.vertexInnerMap.has(targetVertex.id)) this.vertexInnerMap.delete(targetVertex.id);
				console.log("deleted target vertex");
			} else {
				targetMap.set("incidence", targetIncidence - 1);
			}
		} 
		let sourceMap = this.vertexGlobalMap.get(sourceVertex.id);
		let sourceIncidence = sourceMap.get("incidence");
		if (sourceIncidence == 1) {
			cy.remove(cy.$id(sourceVertex.id));
			this.vertexGlobalMap.delete(sourceVertex.id);
			if (this.newVerticesMap.has(sourceVertex.id)) this.newVerticesMap.delete(sourceVertex.id);
			if (this.vertexInnerMap.has(sourceVertex.id)) this.vertexInnerMap.delete(sourceVertex.id);
			console.log("deleted sourcevertex");
		} else {
			sourceMap.set("incidence", sourceIncidence - 1);
		}
	}
	
	addVertex(vertex){
		if (!this.vertexGlobalMap.has(vertex.id)){
			let map = new Map();
			map.set("incidence", 1);
			map.set("vertex", vertex);
			this.vertexGlobalMap.set(vertex.id, map);	
			cy.add({group : 'nodes', data: {id: vertex.id}, position: {x: parseInt(vertex.x) , y: parseInt(vertex.y)}});
			return true;
		} else {
			let map = this.vertexGlobalMap.get(vertex.id);
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
		if (handler.newVerticesMap.size > 1) {
			handler.updateMinDegreeVertices(handler.newVerticesMap);
		} else if (handler.newVerticesMap.size == 1) {
			handler.minDegreeVertex = handler.newVerticesMap.values().next().value;
		}
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
	// if (handler.newVerticesMap.size > 1) {
		// handler.updateMinDegreeVertices(handler.newVerticesMap);
	// } else if (handler.newVerticesMap.size == 1) {
		// handler.minDegreeVertex = handler.newVerticesMap.values().next().value;
	// }
}