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
		this.maxNumberVertices = 100;
		this.wrapperQueue = new Array();
	}
	
	updateVertexCollection(topModel, rightModel, bottomModel, leftModel){
		this.topModel = topModel;
		this.rightModel = rightModel;
		this.bottomModel = bottomModel;
		this.leftModel = leftModel;
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
		this.newVerticesMap = new Map();
	}
	
	updateMinDegreeVertex(vertex){
		if (this.secondMinDegreeVertex == null) {
			this.secondMinDegreeVertex = vertex;
		} else if (this.minDegreeVertex == null) {
			this.minDegreeVertex = vertex;
		} else if (vertex.degree < this.minDegreeVertex) {
			this.secondMinDegreeVertex = this.minDegreeVertex;
			this.minDegreeVertex = vertex;
		} else if (vertex.degree < this.secondMinDegreeVertex) {
			this.secondMinDegreeVertex = vertex;
		}
	}
	
	updateMinDegreeVertices(){
		this.newVerticesMap.forEach(
			function(vertex, id){
				if (this.secondMinDegreeVertex == null) {
					this.secondMinDegreeVertex = vertex;
				} else if (this.minDegreeVertex == null) {
					this.minDegreeVertex = vertex;
				} else if (vertex.degree < this.minDegreeVertex.degree) {
					this.secondMinDegreeVertex = this.minDegreeVertex;
					this.minDegreeVertex = vertex;
				} else if (vertex.degree < this.secondMinDegreeVertex.degree)  {
					this.secondMinDegreeVertex = vertex;
				}
			}, this
		)
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
		console.log("in identity wrapper function");
		if (this.capacity > 0) {
			var added = this.addVertex(vertex);
			if (added) {
				this.updateMinDegreeVertex(vertex);
				this.newVerticesMap.set(vertex.id, vertex);
				this.capacity -= 1;
			}
		} else {
			if (vertex.degree > this.minDegreeVertex.degree) {
				var added = this.addVertex(vertex);
				if (added) {
					this.newVerticesMap.delete(this.minDegreeVertex.id);
					this.newVerticesMap.set(vertex.id, vertex);
					cy.remove(cy.$id(this.minDegreeVertex.id));
					this.minDegreeVertex = null;
					this.updateMinDegreeVertices();
				}
			} 
		}
		console.log("added identity wrapper, newVerticesMap size: " + this.newVerticesMap.size);
	}
		
	addNonIdentityWrapper(edgeId, edgeLabel, sourceVertex, targetVertex){
		console.log("in non-identity wrapper function");
		if (this.capacity > 0){
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
					this.newVerticesMap.delete(this.secondMinDegreeVertex.id);
					this.newVerticesMap.delete(this.minDegreeVertex.id);
					cy.remove(cy.$id(this.secondMinDegreeVertex.id));
					cy.remove(cy.$id(this.minDegreeVertex.id));
					this.newVerticesMap.set(sourceVertex.id, sourceVertex);
					this.newVerticesMap.set(targetVertex.id, targetVertex);
					this.secondMinDegreeVertex = null;
					this.minDegreeVertex = null;
					this.updateMinDegreeVertices();
				} else if (addedSource || addedTarget) {
					this.newVerticesMap.delete(this.minDegreeVertex.id);
					cy.remove(cy.$id(this.minDegreeVertex.id));
					this.minDegreeVertex = null;
					this.updateMinDegreeVertices();
					if (addedSource) this.newVerticesMap.set(sourceVertex.id, sourceVertex);
					if (addedTarget) this.newVerticesMap.set(targetVertex.id, targetVertex);
				}
			} else if (sourceIn && sourceVertex.degree > this.minDegreeVertex) {
				var addedSource = this.addVertex(sourceVertex);
				var addedTarget = this.addVertex(targetVertex);
				this.addEdge(edgeId, sourceVertex, targetVertex);
				if (addedSource) {
					this.newVerticesMap.delete(this.minDegreeVertex.id);
					cy.remove(cy.$id(this.minDegreeVertex.id));
					this.newVerticesMap.set(sourceVertex.id, sourceVertex);
				}
			} else if (targetIn && targetVertex.degree > this.minDegreeVertex) {
				var addedSource = this.addVertex(sourceVertex);
				var addedTarget = this.addVertex(targetVertex);
				this.addEdge(edgeId, sourceVertex, targetVertex);
				if (addedTarget) {
					this.newVerticesMap.delete(this.minDegreeVertex.id);
					cy.remove(cy.$id(this.minDegreeVertex.id));
					this.newVerticesMap.set(targetVertex.id, targetVertex);
				}
			}
		}
		console.log("added non-identity wrapper, newVerticesMap size: " + this.newVerticesMap.size);
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
				resolve(true);
				console.log("adding new wrapper");
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
					console.log("remaining capacity after adding: " + this.capacity);
					clearTimeout(this.timeOut);
					this.timeOut = setTimeout(clearOperation, 1000);
				}
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
		} else {
			var map = this.vertexGlobalMap.get(vertex.id);
			map.set("incidence" , map.get("incidence") + 1);
		}
		if (map.get("incidence") == 1) {
			cy.add({group : 'nodes', data: {id: vertex.id}, position: {x: parseInt(vertex.x) , y: parseInt(vertex.y)}});
			return true;
		} else {
			return false;
		}
	}
	
	removeVertex(dataArray){
		var vertexId = dataArray[1];
		if (!this.vertexGlobalMap.has(vertexId)) {
				alert("cannot remove vertex because not in vertexGlobalMap");
		} else {
			this.vertexGlobalMap.set(vertexId, this.vertexGlobalMap.get(vertexId)['incidence'] - 1);
			if (this.vertexGlobalMap.get(vertexId)['incidence'] == 0) {
				this.vertexGlobalMap.delete(vertexId);
				cy.remove(cy.$id(vertexId));
			}
		}
	}
	
	addEdge(edgeId, sourceVertex, targetVertex){
		cy.add({group : 'edges', data: {id: edgeId, source: sourceVertex.id , target: targetVertex.id}});
	}
}

function clearOperation(){
	if (!handler.operation == "initial"){
		cy.nodes().forEach( 
			function(node){
				var pos = node.position();
				if (((pos.x < handler.leftModel) || (handler.rightModel < pos.x) || (pos.y < handler.topModel) || (handler.bottomModel < pos.y)) && (cy.neighborhood().length == 0)) {
					cy.remove(node);
				} 
			}
		)
	}
	handler.vertexInnerMap = new Map([...handler.newVerticesMap, ...handler.vertexInnerMap]);
	var edgeIdString = "";
	cy.edges().forEach(
		function(edge){
			edgeIdString += ";" + edge.data('id');
		}
	)
	handler.operation = null;
	ws.send("edgeIdString" + edgeIdString);
}