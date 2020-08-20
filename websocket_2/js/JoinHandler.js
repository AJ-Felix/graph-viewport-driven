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
	
	constructor(){
		this.vertexGlobalMap = new Map();
		this.vertexInnerMap = new Map();
		this.timeOut = null;
		console.log("defined maps");
	}
	
	updateVertexCollection(topModel, rightModel, bottomModel, leftModel){
		this.topModel = topModel;
		this.rightModel = rightModel;
		this.bottomModel = bottomModel;
		this.leftModel = leftModel;
		cy.edges().forEach(
			function(edge){
				var sourceX = this.vertexGlobalMap.get(edge.data('source')).x;
				var sourceY = this.vertexGlobalMap.get(edge.data('source')).y;
				var targetX = this.vertexGlobalMap.get(edge.data('target')).x;
				var targetY = this.vertexGlobalMap.get(edge.data('target')).y;
				if (((sourceX < this.leftModel) || (this.rightModel < sourceX) || (sourceY< this.topModel) || (this.bottomModel < SourceY)) &&
						((targetX  < this.leftModel) || (this.rightModel < targetX ) || (targetY  < this.topModel) || (this.bottomModel < targetY))){
					cy.remove(edge);
				}
			}
		)
		this.vertexInnerMap.entries().forEach(
			function(key, value){
				xModel = value.get("x");
				yModel = value.get("y");
				if ((xModel < this.leftModel) || (this.rightModel < xModel) || (yModel < this.topModel) || (this.bottomModel < yModel)){
					vertexInnerMap.delete(key);
				}
			}
		)
		this.capacity = maxNumberVertices - this.vertexInnerMap.keys().length;
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
		this.newVerticesMap.entries.forEach(
			function(id, vertex){
				if (this.secondMinDegreeVertex == null) {
					this.secondMinDegreeVertex = vertex;
				} else if (this.minDegreeVertex == null) {
					this.minDegreeVertex = vertex;
				} else if (vertex.degree < minDegreeVertex.degree) {
					this.secondMinDegreeVertex = this.minDegreeVertex;
					this.minDegreeVertex = vertex;
				} else if (vertex.degree < secondMinDegreeVertex.degree)  {
					this.secondMinDegreeVertex = vertex;
				}
			}
		)
	}
	
	addWrapperInitial(dataArray) {
		console.log("in addwrapperInitial");
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
		this.timeOut = setTimeout(clearOperation, 100);
	}
	
	addIdentityWrapperInitial(vertex){
		this.newVerticesMap.set(vertex.id, vertex);
		this.addVertex(vertex);	
	}	
	
	addNonIdentityWrapperInitial(edgeId, edgeLabel, sourceVertex, targetVertex){
		this.addVertex(sourceVertex);
		this.newVerticesMap.set(sourceVertex.id, sourceVertex);
		this.addVertex(targetVertex);
		this.newVerticesMap.set(targetVertex.id, targetVertex);
		this.addEdge(edgeId, sourceVertex, targetVertex);
	}
	
	addIdentityWrapper(vertex){
		console.log("false wrapper function");
		if (this.capacity > 0) {
			if ((vertex.x >= this.leftModel) && (this.rightModel >= vertex.x) && (vertex.y >= this.topModel) && (this.bottomModel >= vertex.y)){
				updateMinDegreeVertex(vertex);
				this.newVerticesMap.set(vertex.id, vertex);
				var added = this.addVertex(vertex);	
				if (added) this.capacity -= 1;
			}
		} else {
			if ((vertex.x >= this.leftModel) && (this.rightModel >= vertex.x) && (vertex.y >= this.topModel) && (this.bottomModel >= vertex.y)){
				if (vertex.degree > this.minDegreeVertex.degree) {
					this.addVertex(vertex);
					this.newVerticesMap.delete(this.minDegreeVertex.id);
					cy.remove(cy.$id(this.minDegreeVertex.id));
					this.minDegreeVertex = null;
					updateMinDegreeVertices();
				} 
			}
		}
	}
		
	addNonIdentityWrapper(edgeId, edgeLabel, sourceVertex, targetVertex){
		console.log("false wrapper function");
		var sourceIn = true;
		var targetIn = true;
		if (this.capacity > 0){
			if ((sourceVertex.x < this.leftModel) || (this.rightModel < sourceVertex.x) || (sourceVertex.y < this.topModel) || (this.bottomModel < sourceVertex.y)){
				sourceIn = false;
			} else {
				updateMinDegreeVertex(sourceVertex);
			}
			if ((targetVertex.x < this.leftModel) || (this.rightModel < targetVertex.x) || (targetVertex.y < this.topModel) || (this.bottomModel < targetVertex.y)){
				targetIn = false;
			} else {
				updateMinDegreeVertex(targetVertex);
			}
			if (sourceIn || targetIn) {
				var addedSource = this.addVertex(sourceVertex);
				if (addedSource) this.capacity -= 1;
				var addedTarget = this.addVertex(targetVertex);
				if (addedTarget) this.capacity -=1;
				this.addEdge(edgeId, sourceVertex, targetVertex);
			}
			if (sourceIn && targetIn){
				if (sourceVertex.degree < targetVertex.degree){
					this.newVerticesMap.set(sourceVertex.id, sourceVertex);
				} else if (targetVertex.degree < sourceVertex.degree){
					this.newVerticesMap.set(targetVertex.id, targetVertex);
				} else {
					this.newVerticesMap.set(sourceVertex.id, sourceVertex);
					this.newVerticesMap.set(targetVertex.id, targetVertex);
				}
			}
		} else {
			if ((sourceVertex.x < this.leftModel) || (this.rightModel < sourceVertex.x) || (sourceVertex.y < this.topModel) || (this.bottomModel < sourceVertex.y)){
				sourceIn = false;
			}
			if ((targetVertex.x < this.leftModel) || (this.rightModel < targetVertex.x) || (targetVertex.y < this.topModel) || (this.bottomModel < targetVertex.y)){
				targetIn = false;
			}
			if ((sourceIn && targetIn) && (sourceVertex.degree > this.secondMinDegreeVertex.degree) && (targetVertex.degree > this.secondMinDegreeVertex.degree)) {
				this.addVertex(sourceVertex);
				this.addVertex(targetVertex);
				this.addEdge(edgeId, sourceVertex, targetVertex);
				this.newVerticesMap.delete(this.secondMinDegreeVertex.id);
				this.newVerticesMap.delete(this.minDegreeVertex.id);
				cy.remove(cy.$id(this.secondMinDegreeVertex.id));
				cy.remove(cy.$id(this.minDegreeVertex.id));
				this.secondMinDegreeVertex = null;
				this.minDegreeVertex = null;
				updateMinDegreeVertices();
			}
		}
	}
		
	addWrapper(dataArray){
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
			clearTimeout(this.timeOut);
			this.timeOut = setTimeout(clearOperation, 100);
		}
	}
	
	addVertex(vertex){
		if (!this.vertexGlobalMap.has(vertex.id)){
			var map = new Map();
			map.set("incidence", 1);
			map.set("vertex", vertex);
			console.log(this.vertexGlobalMap);
			this.vertexGlobalMap.set(vertex.id, map);	
		} else {
			var map = this.vertexGlobalMap.get(vertex.id);
			map.set("incidence" , map.get("incidence") + 1);
		}
		if (map.get("incidence") == 1) {
			cy.add({group : 'nodes', data: {id: vertex.id}, position: {x: parseInt(vertex.x) , y: parseInt(vertex.y)}});
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
	handler.vertexInnerMap = new Map([handler.vertexInnerMap, handler.newVerticesMap]);
	var edgeIdString = "";
	cy.edges().forEach(
		function(edge){
			edgeIdString += ";" + edge.data('id');
		}
	)
	handler.operation = null;
	ws.send("edgeIdString" + edgeIdString);
}