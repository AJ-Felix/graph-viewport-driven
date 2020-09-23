class RetractHandler extends AppendHandler {
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
}