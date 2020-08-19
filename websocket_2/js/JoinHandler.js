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
	
	constructor(){
		this.vertexGlobalMap = new Map();
		this.vertexInnerMap = new Map();
	}
	
	clearOperation(){
		cy.nodes().forEach( 
			function(node){
				var pos = node.position();
				if (((pos.x < this.leftModel) || (this.rightModel < pos.x) || (pos.y < this.topModel) || (this.bottomModel < pos.y)) && (cy.neighborhood().length == 0)) {
					cy.remove(node);
				} 
			}
		)
		this.vertexInnerMap = new Map([this.vertexInnerMap, this.newVerticesMap]);
		edgeIdString = "";
		cy.edges().forEach(
			function(edge){
				edgeIdString += ";" + edge.data('id');
			}
		)
		ws.send("edgeIdString" + edgeIdString);
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
	
	addIdentityWrapper(vertex){
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
					newVerticesMap.set(sourceId, sourceVertex);
				} else if (targetVertex.degree < sourceVertex.degree){
					newVerticesMap.set(targetId, targetVertex);
				} else {
					newVerticesMap.set(sourceId, sourceVertex);
					newVerticesMap.set(targetId, targetVertex);
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
		console.log(sourceVertex);
		if (edgeLabel == "identity"){
			this.addIdentityWrapper(sourceVertex);
		} else {
			this.addNonIdentityWrapper(edgeId, edgeLabel, sourceVertex, targetVertex);
		}
		ws.setTimeout(this.clearOperation, 100);
	}
	
	addVertex(vertex){
		if (!this.vertexGlobalMap.has(vertex.id)){
			var map = new Map();
			map.incidence = 1;
			map.vertex = vertex;
			this.vertexGlobalMap.set(vertex.id, map);				
		} else {
			this.vertexGlobalMap.set(vertex.id, this.vertexGlobalMap.get(vertex.id).incidence + 1);
		}
		if (this.vertexGlobalMap.get(vertex.id).incidence == 1) {
			cy.add({group : 'nodes', data: {id: vertex.id}, position: {x: vertex.x , y: vertex.y}});
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
		console.log(sourceVertex);
		cy.add({group : 'edges', data: {id: edgeId, source: sourceVertex.id , target: targetVertex.id}});
	}
}