package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

//public class GraphVis implements Serializable{
	private Map<String,Map<String,Object>> globalVertices;
	private Map<String,VertexCustom> innerVertices;
	private Map<String,VertexCustom> newVertices;
	private Set<VVEdgeWrapper> edges;
	private String operation;
	private Integer capacity;
	private Float topModel;
	private Float rightModel;
	private Float bottomModel;
	private Float leftModel;
	private VertexCustom secondMinDegreeVertex;
	private VertexCustom minDegreeVertex;
	private Integer maxNumberVertices;
	private Map<String,Map<String,String>> adjMatrix;

	public GraphVis(Map<String,Map<String,String>> adjMatrix) {
		this.operation = "initial";
		this.globalVertices = new HashMap<String,Map<String,Object>>();
		this.innerVertices = new HashMap<String,VertexCustom>();
		this.newVertices = new HashMap<String,VertexCustom>();
		this.adjMatrix = adjMatrix;
		this.edges = new HashSet<VVEdgeWrapper>();
		this.maxNumberVertices = 10;
	}
	
	public Map<String,VertexCustom> getInnerVertices() {
		return this.innerVertices;
	}
	
	public Map<String,Map<String,Object>> getGlobalVertices(){
		return this.globalVertices;
	}
	
	public void setOperation(String operation) {
		this.operation = operation;
	}
	
	public void prepareOperation(Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		this.topModel = topModel;
		this.rightModel = rightModel;
		this.bottomModel = bottomModel;
		this.leftModel = leftModel;
		if (this.operation != "zoomOut"){
			for (VVEdgeWrapper wrapper : this.edges) {
				Integer sourceX = wrapper.getSourceX();
				Integer sourceY = wrapper.getSourceY();
				Integer targetX = wrapper.getTargetX();
				Integer targetY = wrapper.getSourceY();
				if (((sourceX < leftModel) || (rightModel < sourceX) || (sourceY < topModel) || (bottomModel < sourceY)) &&
						((targetX  < leftModel) || (rightModel < targetX ) || (targetY  < topModel) || (bottomModel < targetY))){
					UndertowServer.sendToAll("removeObjectServer;" + wrapper.getEdgeIdGradoop());
				}
			}
			for (Map.Entry<String, VertexCustom> entry : this.innerVertices.entrySet()) {
				VertexCustom vertex = entry.getValue();
				if ((vertex.getX() < leftModel) || (rightModel < vertex.getX()) || (vertex.getY() < topModel) || (bottomModel < vertex.getY()))
					this.innerVertices.remove(entry.getKey());
			}
			this.capacity = this.maxNumberVertices - this.innerVertices.size();
		} else {
			this.capacity = 0;
		}
		if (this.operation.equals("pan") || this.operation.equals("zoomOut")) {
			this.newVertices = this.innerVertices;
		} else {
			this.newVertices = new HashMap<String,VertexCustom>();
		}
	}
	
	public void addWrapper(VVEdgeWrapper wrapper) {
		System.out.println(wrapper);
		System.out.println("global Vertices " + this.globalVertices.size());
		if (this.operation.equals("initial")) {
			if (wrapper.getEdgeLabel().equals("identityEdge")) {
				this.addWrapperIdentityInitial(wrapper.getSourceVertex());
			} else {
				this.addNonIdentityWrapperInitial(wrapper);
			}
		} else {
			if (wrapper.getEdgeLabel().equals("identityEdge")) {
				this.addWrapperIdentity(wrapper.getSourceVertex());
			} else {
				this.addNonIdentityWrapper(wrapper);
			}
		}
	}
	
	private void addNonIdentityWrapper(VVEdgeWrapper wrapper) {
		VertexCustom sourceVertex = wrapper.getSourceVertex();
		VertexCustom targetVertex = wrapper.getTargetVertex();
		if (this.capacity > 1) {
			boolean addedSource = this.addVertex(sourceVertex);
			if ((sourceVertex.getX() >= this.leftModel) && (this.rightModel >= sourceVertex.getX()) && (sourceVertex.getY() >= this.topModel) && 
					(this.bottomModel >= sourceVertex.getY()) && addedSource){
				this.updateMinDegreeVertex(sourceVertex);
				this.newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
				this.capacity -= 1;
			}
			boolean addedTarget = this.addVertex(targetVertex);
			if ((targetVertex.getX() >= this.leftModel) && (this.rightModel >= targetVertex.getX()) && (targetVertex.getY() >= this.topModel) 
					&& (this.bottomModel >= targetVertex.getY()) && addedTarget){
				this.updateMinDegreeVertex(targetVertex);
				this.newVertices.put(targetVertex.getIdGradoop(), targetVertex);
				this.capacity -= 1;
			}
			this.addEdge(wrapper);
		} else {
			boolean sourceIn = true;
			boolean targetIn = true;
			if ((sourceVertex.getX() < this.leftModel) || (this.rightModel < sourceVertex.getX()) || (sourceVertex.getY() < this.topModel) || 
					(this.bottomModel < sourceVertex.getY())){
				sourceIn = false;
			}
			if ((targetVertex.getX() < this.leftModel) || (this.rightModel < targetVertex.getX()) || (targetVertex.getY() < this.topModel) || 
					(this.bottomModel < targetVertex.getY())){
				targetIn = false;
			}
			if ((sourceIn && targetIn) && (sourceVertex.getDegree() > this.secondMinDegreeVertex.getDegree()) && 
					(targetVertex.getDegree()> this.secondMinDegreeVertex.getDegree())) {
				boolean addedSource = this.addVertex(sourceVertex);
				boolean addedTarget = this.addVertex(targetVertex);
				this.addEdge(wrapper);
				if (addedSource && addedTarget) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.reduceNeighborIncidence(this.secondMinDegreeVertex);
					this.removeVertex(this.secondMinDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					this.newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					this.newVertices.put(targetVertex.getIdGradoop(), targetVertex);
					this.updateMinDegreeVertices(this.newVertices);
				} else if (addedSource || addedTarget) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					if (this.newVertices.size() > 1) {
						this.updateMinDegreeVertices(this.newVertices);
					} else if (addedSource) {
						this.minDegreeVertex = sourceVertex;
					} else if (addedTarget) {
						this.minDegreeVertex = targetVertex;
					}
					if (addedSource) this.newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					if (addedTarget) this.newVertices.put(targetVertex.getIdGradoop(), targetVertex);
				}
			} else if (sourceIn && !(targetIn) && sourceVertex.getDegree() > this.minDegreeVertex.getDegree()) {
				boolean addedSource = this.addVertex(sourceVertex);
				this.addVertex(targetVertex);
				this.addEdge(wrapper);
				if (addedSource) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					if (this.newVertices.size() > 1) {
						this.updateMinDegreeVertices(this.newVertices);
					} else {
						this.minDegreeVertex = sourceVertex;
					} 
					this.newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
				}
			} else if (targetIn && !(sourceIn) && targetVertex.getDegree() > this.minDegreeVertex.getDegree()) {
				this.addVertex(sourceVertex);
				boolean addedTarget = this.addVertex(targetVertex);
				this.addEdge(wrapper);
				if (addedTarget) {
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					if (this.newVertices.size() > 1) {
						this.updateMinDegreeVertices(this.newVertices);
					} else {
						this.minDegreeVertex = targetVertex;
					} 
					this.newVertices.put(targetVertex.getIdGradoop(), targetVertex);
				}
			}
		}
	}
	
	private void updateMinDegreeVertex(VertexCustom vertex) {
		if (vertex.getDegree() < this.minDegreeVertex.getDegree()) {
			this.secondMinDegreeVertex = this.minDegreeVertex;
			this.minDegreeVertex = vertex;
		} else if (vertex.getDegree() < this.secondMinDegreeVertex.getDegree()) {
			this.secondMinDegreeVertex = vertex;
		}		
	}

	private void updateMinDegreeVertices(Map<String, VertexCustom> map) {
		Collection<VertexCustom> collection = map.values();
		Iterator<VertexCustom> iter = collection.iterator();
		this.minDegreeVertex = iter.next();
		this.secondMinDegreeVertex = iter.next();
		if (this.secondMinDegreeVertex.getDegree() < this.minDegreeVertex.getDegree()) {
			VertexCustom temp = this.minDegreeVertex;
			this.minDegreeVertex = this.secondMinDegreeVertex;
			this.secondMinDegreeVertex = temp;
		}
		for (Map.Entry<String, VertexCustom> entry : map.entrySet()) {
			VertexCustom vertex = entry.getValue();
			if (vertex.getDegree() < this.minDegreeVertex.getDegree() && vertex.getIdGradoop() != this.secondMinDegreeVertex.getIdGradoop()) {
				this.secondMinDegreeVertex = this.minDegreeVertex;
				this.minDegreeVertex = vertex;
			} else if (vertex.getDegree() < this.secondMinDegreeVertex.getDegree() && vertex.getIdGradoop() != this.minDegreeVertex.getIdGradoop())  {
				this.secondMinDegreeVertex = vertex;
			}
		}
	}

	private void removeVertex(VertexCustom vertex) {	
		if (!this.globalVertices.containsKey(vertex.getIdGradoop())) {
			System.out.println("cannot remove vertex because not in vertexGlobalMap, id: " + vertex.getIdGradoop());
		} else {
			this.newVertices.remove(vertex.getIdGradoop());
			this.globalVertices.remove(vertex.getIdGradoop());
				UndertowServer.sendToAll("removeObjectServer;" + vertex.getIdNumeric());
		}
	}

	private void reduceNeighborIncidence(VertexCustom vertex) {
		Set<String> neighborIds = this.getNeighborhood(vertex);
		for (String neighbor : neighborIds) {
			Map<String,Object> map = this.globalVertices.get(neighbor);
			map.put("incidence", (int) map.get("incidence") - 1); 
		}
	}

	private void addWrapperIdentity(VertexCustom vertex) {
		if (this.capacity > 0) {
			boolean added = this.addVertex(vertex);
			if (added) {
				this.newVertices.put(vertex.getIdGradoop(), vertex);
				this.updateMinDegreeVertex(vertex);
				this.capacity -= 1;
			}
		} else {
			if (vertex.getDegree() > this.minDegreeVertex.getDegree()) {
				boolean added = this.addVertex(vertex);
				if (added) {
					this.newVertices.put(vertex.getIdGradoop(), vertex);
					this.reduceNeighborIncidence(this.minDegreeVertex);
					this.removeVertex(this.minDegreeVertex);
					if (this.newVertices.size() > 1) {
						this.updateMinDegreeVertices(this.newVertices);
					} else if (this.newVertices.size() == 1) {
						this.minDegreeVertex = vertex;
					}
				}
			} 
		}
	}

	public void addWrapperIdentityInitial(VertexCustom vertex) {
		boolean added = this.addVertex(vertex);
		if (added) this.innerVertices.put(vertex.getIdGradoop(), vertex);
		System.out.println("addWrapperIdentityinitial  " + this.innerVertices.size());
		for (Map.Entry<String, VertexCustom> entry : this.innerVertices.entrySet()) System.out.println(entry);
	}
	
	public void addNonIdentityWrapperInitial(VVEdgeWrapper wrapper) {
		VertexCustom sourceVertex = wrapper.getSourceVertex();
		VertexCustom targetVertex = wrapper.getTargetVertex();
		boolean addedSource = this.addVertex(sourceVertex);
		if (addedSource) this.innerVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
		boolean addedTarget = this.addVertex(targetVertex);
		if (addedTarget) this.innerVertices.put(targetVertex.getIdGradoop(), targetVertex);
			this.addEdge(wrapper);
	}
	
	public boolean addVertex(VertexCustom vertex) {
		String sourceId = vertex.getIdGradoop();
		if (!(this.globalVertices.containsKey(sourceId))) {
			Map<String,Object> map = new HashMap<String,Object>();
			map.put("incidence", (int) 1);
			map.put("vertex", vertex);
			this.globalVertices.put(sourceId, map);
				UndertowServer.sendToAll("addVertexServer;" + vertex.getIdNumeric() + ";" + vertex.getX() + ";" + vertex.getY());
			return true;
		} else {
			Map<String,Object> map = this.globalVertices.get(sourceId);
			map.put("incidence", (int) map.get("incidence") + 1);
			return false;
		}		
	}
	
	public void addEdge(VVEdgeWrapper wrapper) {
		this.edges.add(wrapper);
		UndertowServer.sendToAll("addEdgeServer;" + wrapper.getEdgeIdGradoop() + ";" + wrapper.getSourceIdNumeric() + ";" + wrapper.getTargetIdNumeric());
	}
	
	public void clearOperation(){
		System.out.println("in clear operation 1");
		if (this.operation != "initial"){
			this.innerVertices.putAll(this.newVertices); 
			for (Map.Entry<String, Map<String,Object>> entry : this.globalVertices.entrySet()) {
				Map<String,Object> map = entry.getValue();
				VertexCustom vertex = (VertexCustom) map.get("vertex");
				if ((((vertex.getX() < this.leftModel) || (this.rightModel < vertex.getX()) || (vertex.getY() < this.topModel) || 
						(this.bottomModel < vertex.getY())) && this.adjMatrix.get(vertex.getIdGradoop()).isEmpty()) || 
							((vertex.getX() >= this.leftModel) && (this.rightModel >= vertex.getX()) && (vertex.getY() >= this.topModel) && 
								(this.bottomModel >= vertex.getY()) && !this.innerVertices.containsKey(vertex.getIdGradoop()))) {
					UndertowServer.sendToAll("removeObjectServer;" + vertex.getIdNumeric());
					this.globalVertices.remove(vertex.getIdGradoop());
				} 
			}
		} else {
			System.out.println("innerVertices" + this.innerVertices.size());
			System.out.println("newVertices" + this.newVertices.size());
			this.newVertices = this.innerVertices;
		}
		this.operation = null;
		System.out.println("in clear operation 2");
		System.out.println(this.newVertices.size());
		if (this.newVertices.size() > 1) {
			this.updateMinDegreeVertices(this.newVertices);
		} else if (this.newVertices.size() == 1) {
			this.minDegreeVertex = this.newVertices.values().iterator().next();
		}
	}
	
	public Set<String> getNeighborhood(VertexCustom vertex){
		Set<String> neighborIds = new HashSet<String>();
		for (Map.Entry<String, String> entry : this.adjMatrix.get(vertex.getIdGradoop()).entrySet()) neighborIds.add(entry.getKey());
		return neighborIds;
	}
}
