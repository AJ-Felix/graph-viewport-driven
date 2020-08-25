class JoinHandler{
	vertexGlobalMap; //Die Map muss für GradoopUtilJoin möglicherweise in den Inzidenzen verändert werden
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
					var sourceX = this.vertexGlobalMap.get(edge.data('source')).get('vertex').x;
					var sourceY = this.vertexGlobalMap.get(edge.data('source')).get('vertex').y;
					var targetX = this.vertexGlobalMap.get(edge.data('target')).get('vertex').x;
					var targetY = this.vertexGlobalMap.get(edge.data('target')).get('vertex').y;
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
			// console.log(this.vertexGlobalMap);
			this.capacity = 0;
		}
		if (this.operation == "pan" || this.operation == "zoomOut") {
			this.newVerticesMap = this.vertexInnerMap;
			this.updateMinDegreeVertices();
		} else {
			this.newVerticesMap = new Map();
		}
		console.log("capacity: " + this.capacity);
	}
	
	updateMinDegreeVertex(vertex){
		if (this.secondMinDegreeVertex == null) {
			this.secondMinDegreeVertex = vertex;
		} else if (this.minDegreeVertex == null) {
			this.minDegreeVertex = vertex;
		} else if (vertex.degree < this.minDegreeVertex.degree) {
			this.secondMinDegreeVertex = this.minDegreeVertex;
			this.minDegreeVertex = vertex;
		} else if (vertex.degree < this.secondMinDegreeVertex.degree) {
			this.secondMinDegreeVertex = vertex;
		}
		console.log("after updateMinDegreeVertex"); 
		console.log(this.minDegreeVertex);
		console.log(this.secondMinDegreeVertex);
	}
	
	updateMinDegreeVertices(){
		console.log(this.minDegreeVertex);
		console.log(this.secondMinDegreeVertex);
		this.newVerticesMap.forEach(
			function(vertex, id){
				// if (this.secondMinDegreeVertex == null && !(vertex.id == this.minDegreeVertex.id) {
					// this.secondMinDegreeVertex = vertex;
				// } else if (this.minDegreeVertex == null) {
					// this.minDegreeVertex = vertex;
				// } else 
				if (vertex.degree < this.minDegreeVertex.degree && !(vertex.id == this.minDegreeVertex.id)) {
					this.secondMinDegreeVertex = this.minDegreeVertex;
					this.minDegreeVertex = vertex;
				} else if (vertex.degree < this.secondMinDegreeVertex.degree && !(vertex.id == this.secondMinDegreeVertex.id))  {
					this.secondMinDegreeVertex = vertex;
				}
			}, this
		)
		console.log("min degree vertices");
		console.log(this.minDegreeVertex);
		console.log(this.secondMinDegreeVertex);
	}
	
	reduceNeighborIncidence(vertex){
		// console.log("reduce incidence");
		var neighbors = cy.$id(vertex.id).neighborhood("node");
		// console.log("vertex id: " + vertex.id);
		// console.log(neighbors);
		// console.log(this.vertexGlobalMap);
		var map = this.vertexGlobalMap;
		cy.$id(vertex.id).neighborhood("node").forEach(
			function (node) {
				// console.log("node id: " + node.data('id'));
				var vertexMap = map.get(node.data('id'));
				// console.log("before: " + vertexMap.get('incidence'));
				vertexMap.set('incidence', vertexMap.get('incidence') - 1);
				// console.log("after: " + vertexMap.get('incidence'));
			}
		)
		// console.log(this.vertexGlobalMap);
	}
	
	addWrapperInitial(dataArray) {
		const edgeId = dataArray[1];
		const edgeLabel = dataArray[2];
		const sourceId = dataArray[3];
		const sourceDegree = dataArray[4];
		const sourceX = dataArray[5];
		const sourceY = dataArray[6];
		const targetId = dataArray[7];
		const targetDegree = dataArray[8];
		const targetX = dataArray[9];
		const targetY = dataArray[10];
		const sourceVertex = new Vertex(sourceId, sourceDegree, sourceX, sourceY);
		const targetVertex = new Vertex(targetId, targetDegree, targetX, targetY);
		if (edgeLabel == "identityEdge"){
			this.addIdentityWrapperInitial(sourceVertex);
		} else {
			this.addNonIdentityWrapperInitial(edgeId, edgeLabel, sourceVertex, targetVertex);
		}
		clearTimeout(this.timeOut);
		this.timeOut = setTimeout(clearOperation, 1000);
	}
	
	addIdentityWrapperInitial(vertex){
		var added = this.addVertex(vertex);
		if (added) this.vertexInnerMap.set(vertex.id, vertex);	
	}	
	
	addNonIdentityWrapperInitial(edgeId, edgeLabel, sourceVertex, targetVertex){
		var addedSource = this.addVertex(sourceVertex);
		if (addedSource) this.vertexInnerMap.set(sourceVertex.id, sourceVertex);
		var addedTarget = this.addVertex(targetVertex);
		if (addedTarget) this.vertexInnerMap.set(targetVertex.id, targetVertex);
		this.addEdge(edgeId, sourceVertex, targetVertex);
	}
	
	addIdentityWrapper(vertex){
		console.log("vertex Degree identity: " + vertex.degree);
		if (this.minDegreeVertex != null) console.log(this.minDegreeVertex.degree);
		if (this.capacity > 0) {
			var added = this.addVertex(vertex);
			if (added) {
				console.log("added identity with capacity");
				console.log(vertex);
				this.updateMinDegreeVertex(vertex);
				this.newVerticesMap.set(vertex.id, vertex);
				this.capacity -= 1;
			}
		} else {
			if (vertex.degree > this.minDegreeVertex.degree) {
				var added = this.addVertex(vertex);
				if (added) {
					console.log("added identity with no capacity");
					console.log(vertex);
					this.newVerticesMap.set(vertex.id, vertex);
					// if (this.operation == "zoomOut") 
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					this.minDegreeVertex = this.newVerticesMap.values()[0];
					this.updateMinDegreeVertices();
				}
			} 
		}
		// console.log(this.vertexGlobalMap);
		// console.log("added identity wrapper, newVerticesMap size: " + this.newVerticesMap.size);
	}
		
	addNonIdentityWrapper(edgeId, edgeLabel, sourceVertex, targetVertex){
		if (this.capacity > 1){
			var addedSource = this.addVertex(sourceVertex);
			if ((sourceVertex.x >= this.leftModel) && (this.rightModel >= sourceVertex.x) && (sourceVertex.y >= this.topModel) && (this.bottomModel >= sourceVertex.y) && addedSource){
				this.updateMinDegreeVertex(sourceVertex);
				this.newVerticesMap.set(sourceVertex.id, sourceVertex);
				this.capacity -= 1;
			}
			var addedTarget = this.addVertex(targetVertex);
			if ((targetVertex.x >= this.leftModel) && (this.rightModel >= targetVertex.x) && (targetVertex.y >= this.topModel) && (this.bottomModel >= targetVertex.y) && addedTarget){
				this.updateMinDegreeVertex(targetVertex);
				this.newVerticesMap.set(targetVertex.id, targetVertex);
				this.capacity -= 1;
			}
			this.addEdge(edgeId, sourceVertex, targetVertex);
		} else {
			var sourceIn = true;
			var targetIn = true;
			if ((sourceVertex.x < this.leftModel) || (this.rightModel < sourceVertex.x) || (sourceVertex.y < this.topModel) || (this.bottomModel < sourceVertex.y)){
				sourceIn = false;
			}
			if ((targetVertex.x < this.leftModel) || (this.rightModel < targetVertex.x) || (targetVertex.y < this.topModel) || (this.bottomModel < targetVertex.y)){
				targetIn = false;
			}
			if ((sourceIn && targetIn) && (sourceVertex.degree > this.secondMinDegreeVertex.degree) && (targetVertex.degree > this.secondMinDegreeVertex.degree)) {
				var addedSource = this.addVertex(sourceVertex);
				var addedTarget = this.addVertex(targetVertex);
				this.addEdge(edgeId, sourceVertex, targetVertex);
				if (addedSource && addedTarget) {
					// if (this.operation == "zoomOut") {
						this.reduceNeighborIncidence(this.minDegreeVertex);
						this.reduceNeighborIncidence(this.secondMinDegreeVertex);
					// }
					this.removeVertex(this.secondMinDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					this.newVerticesMap.set(sourceVertex.id, sourceVertex);
					this.newVerticesMap.set(targetVertex.id, targetVertex);
					this.secondMinDegreeVertex = this.newVerticesMap.values()[0];
					this.minDegreeVertex = this.newVerticesMap.values()[1];
					this.updateMinDegreeVertices();
				} else if (addedSource || addedTarget) {
					// if (this.operation == "zoomOut") 
						this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					this.minDegreeVertex = this.newVerticesMap.values()[0];
					this.updateMinDegreeVertices();
					if (addedSource) this.newVerticesMap.set(sourceVertex.id, sourceVertex);
					if (addedTarget) this.newVerticesMap.set(targetVertex.id, targetVertex);
				}
			} else if (sourceIn && sourceVertex.degree > this.minDegreeVertex) {
				var addedSource = this.addVertex(sourceVertex);
				var addedTarget = this.addVertex(targetVertex);
				this.addEdge(edgeId, sourceVertex, targetVertex);
				if (addedSource) {
					// if (this.operation == "zoomOut") 
						this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					this.minDegreeVertex = this.newVerticesMap.values()[0];
					this.updateMinDegreeVertices();
					this.newVerticesMap.set(sourceVertex.id, sourceVertex);
				}
			} else if (targetIn && targetVertex.degree > this.minDegreeVertex) {
				var addedSource = this.addVertex(sourceVertex);
				var addedTarget = this.addVertex(targetVertex);
				this.addEdge(edgeId, sourceVertex, targetVertex);
				if (addedTarget) {
					// if (this.operation == "zoomOut") 
						this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					this.minDegreeVertex = this.newVerticesMap.values()[0];
					this.updateMinDegreeVertices();
					this.newVerticesMap.set(targetVertex.id, targetVertex);
				}
			}
		}
		// console.log("added non-identity wrapper, newVerticesMap size: " + this.newVerticesMap.size);
		// console.log(this.vertexGlobalMap);
	}
		
	addWrapperToQueue(dataArray){
		this.wrapperQueue.push(dataArray);
		if (!this.addWrapperRunning) {
			this.addWrapperRunning = true;
			this.addWrapper(); 
		}
	}
	
	async addWrapper(){
		if (this.wrapperQueue.length > 0) {
			var dataArray = this.wrapperQueue.shift();
			let promise = new Promise((resolve, reject) => {
				// resolve(true);
				if (this.operation == "initial") {
					this.addWrapperInitial(dataArray);
				} else {
					const edgeId = dataArray[1];
					const edgeLabel = dataArray[2];
					const sourceId = dataArray[3];
					const sourceDegree = dataArray[4];
					const sourceX = dataArray[5];
					const sourceY = dataArray[6];
					const targetId = dataArray[7];
					const targetDegree = dataArray[8];
					const targetX = dataArray[9];
					const targetY = dataArray[10];
					const sourceVertex = new Vertex(sourceId, sourceDegree, sourceX, sourceY);
					const targetVertex = new Vertex(targetId, targetDegree, targetX, targetY);
					if (edgeLabel == "identityEdge"){
						this.addIdentityWrapper(sourceVertex);
					} else {
						this.addNonIdentityWrapper(edgeId, edgeLabel, sourceVertex, targetVertex);
					}
					// console.log("remaining capacity after adding: " + this.capacity);
					clearTimeout(this.timeOut);
					this.timeOut = setTimeout(clearOperation, 2000);
				}
				// console.log("about to resolve true");
				resolve(true);
			});
			await promise;
			this.addWrapper();
		} else {
			this.addWrapperRunning = false;
		}
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
			console.log(map.get('incidence'));
			map.set("incidence" , map.get("incidence") + 1);
			return false;
		}
	}
	
	removeVertex(vertex){
		if (!this.vertexGlobalMap.has(vertex.id)) {
				console.log("cannot remove vertex because not in vertexGlobalMap");
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
		cy.nodes().forEach( 
			function(node){
				var pos = node.position();
				if (((pos.x < handler.leftModel) || (handler.rightModel < pos.x) || (pos.y < handler.topModel) || (handler.bottomModel < pos.y)) && (node.neighborhood().length == 0) || 
						(this.vertexGlobalMap.get(node.data('id')).get('incidence') < 1)) {
					console.log('deleting node');
					cy.remove(node);
					this.vertexGlobalMap.delete(node.data('id'));
				} 
			}, handler
		)
	}
	// handler.vertexGlobalMap.forEach(
		// function(vertexMap, id){
			// vertexMap.set('incidence', 0);
		// }	
	// )
	handler.vertexInnerMap = new Map([...handler.newVerticesMap, ...handler.vertexInnerMap]);
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
	console.log(handler.vertexGlobalMap);
	console.log(handler.minDegreeVertex);
	console.log(handler.secondMinDegreeVertex);
	handler.updateMinDegreeVertices();
}