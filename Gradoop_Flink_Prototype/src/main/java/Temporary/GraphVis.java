package Temporary;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import aljoschaRydzyk.Gradoop_Flink_Prototype.Main;
import aljoschaRydzyk.Gradoop_Flink_Prototype.VVEdgeWrapper;
import aljoschaRydzyk.Gradoop_Flink_Prototype.VertexCustom;

import java.util.Set;

public class GraphVis implements Serializable{
	private static Map<String,Map<String,Object>> globalVertices;
	private static Map<String,VertexCustom> innerVertices;
	private static Map<String,VertexCustom> newVertices;
	private static Set<VVEdgeWrapper> edges;
	private static String operation;
	private static Integer capacity;
	private static Float topModel;
	private static Float rightModel;
	private static Float bottomModel;
	private static Float leftModel;
	private static VertexCustom secondMinDegreeVertex;
	private static VertexCustom minDegreeVertex;
	private static Integer maxNumberVertices;
//	private static Map<String,Map<String,String>> adjMatrix;

	public GraphVis() {
	}
	
	public static void setGraphVis(Map<String,Map<String,String>> adjMatrix) {
		operation = "initial";
		globalVertices = new HashMap<String,Map<String,Object>>();
		innerVertices = new HashMap<String,VertexCustom>();
		newVertices = new HashMap<String,VertexCustom>();
//		GraphVis.adjMatrix = adjMatrix;
		edges = new HashSet<VVEdgeWrapper>();
		maxNumberVertices = 10;
	}
	
//	public static Map<String,Map<String,String>> getAdjMatrix(){
//		return adjMatrix;
//	}
	
	public static Map<String,VertexCustom> getInnerVertices() {
		return innerVertices;
	}
	
	public static Map<String,Map<String,Object>> getGlobalVertices(){
		return globalVertices;
	}
	
	public static void setOperation(String operation) {
		GraphVis.operation = operation;
	}
	
	public static void prepareOperation(Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		GraphVis.topModel = topModel;
		GraphVis.rightModel = rightModel;
		GraphVis.bottomModel = bottomModel;
		GraphVis.leftModel = leftModel;
		if (operation != "zoomOut"){
			for (VVEdgeWrapper wrapper : edges) {
				Integer sourceX = wrapper.getSourceX();
				Integer sourceY = wrapper.getSourceY();
				Integer targetX = wrapper.getTargetX();
				Integer targetY = wrapper.getSourceY();
				if (((sourceX < leftModel) || (rightModel < sourceX) || (sourceY < topModel) || (bottomModel < sourceY)) &&
						((targetX  < leftModel) || (rightModel < targetX ) || (targetY  < topModel) || (bottomModel < targetY))){
//					Main.sendToAll("removeObjectServer;" + wrapper.getEdgeIdGradoop());
				}
			}
			Iterator<Map.Entry<String,VertexCustom>> iter = innerVertices.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String,VertexCustom> entry = iter.next();
				VertexCustom vertex = entry.getValue();
				if ((vertex.getX() < leftModel) || (rightModel < vertex.getX()) || (vertex.getY() < topModel) || (bottomModel < vertex.getY()))
					iter.remove();
			}
//			for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) {
//				VertexCustom vertex = entry.getValue();
//				if ((vertex.getX() < leftModel) || (rightModel < vertex.getX()) || (vertex.getY() < topModel) || (bottomModel < vertex.getY()))
//					innerVertices.remove(entry.getKey());
//			}
			capacity = maxNumberVertices - innerVertices.size();
		} else {
			capacity = 0;
		}
		if (operation.equals("pan") || operation.equals("zoomOut")) {
			newVertices = innerVertices;
		} else {
			newVertices = new HashMap<String,VertexCustom>();
		}
	}
	
	public static void addWrapper(VVEdgeWrapper wrapper) {
		System.out.println(wrapper);
		System.out.println("global Vertices " + globalVertices.size());
		if (operation.equals("initial")) {
			if (wrapper.getEdgeLabel().equals("identityEdge")) {
				addWrapperIdentityInitial(wrapper.getSourceVertex());
			} else {
				addNonIdentityWrapperInitial(wrapper);
			}
		} else {
			if (wrapper.getEdgeLabel().equals("identityEdge")) {
				addWrapperIdentity(wrapper.getSourceVertex());
			} else {
				addNonIdentityWrapper(wrapper);
			}
		}
	}
	
	private static void addNonIdentityWrapper(VVEdgeWrapper wrapper) {
		VertexCustom sourceVertex = wrapper.getSourceVertex();
		VertexCustom targetVertex = wrapper.getTargetVertex();
		if (capacity > 1) {
			boolean addedSource = addVertex(sourceVertex);
			if ((sourceVertex.getX() >= leftModel) && (rightModel >= sourceVertex.getX()) && (sourceVertex.getY() >= topModel) && 
					(bottomModel >= sourceVertex.getY()) && addedSource){
				updateMinDegreeVertex(sourceVertex);
				newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
				capacity -= 1;
			}
			boolean addedTarget = addVertex(targetVertex);
			if ((targetVertex.getX() >= leftModel) && (rightModel >= targetVertex.getX()) && (targetVertex.getY() >= topModel) 
					&& (bottomModel >= targetVertex.getY()) && addedTarget){
				updateMinDegreeVertex(targetVertex);
				newVertices.put(targetVertex.getIdGradoop(), targetVertex);
				capacity -= 1;
			}
			addEdge(wrapper);
		} else {
			boolean sourceIn = true;
			boolean targetIn = true;
			if ((sourceVertex.getX() < leftModel) || (rightModel < sourceVertex.getX()) || (sourceVertex.getY() < topModel) || 
					(bottomModel < sourceVertex.getY())){
				sourceIn = false;
			}
			if ((targetVertex.getX() < leftModel) || (rightModel < targetVertex.getX()) || (targetVertex.getY() < topModel) || 
					(bottomModel < targetVertex.getY())){
				targetIn = false;
			}
			if ((sourceIn && targetIn) && (sourceVertex.getDegree() > secondMinDegreeVertex.getDegree()) && 
					(targetVertex.getDegree()> secondMinDegreeVertex.getDegree())) {
				boolean addedSource = addVertex(sourceVertex);
				boolean addedTarget = addVertex(targetVertex);
				addEdge(wrapper);
				if (addedSource && addedTarget) {
					reduceNeighborIncidence(minDegreeVertex);
					reduceNeighborIncidence(secondMinDegreeVertex);
					removeVertex(secondMinDegreeVertex);
					removeVertex(minDegreeVertex);
					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
					updateMinDegreeVertices(newVertices);
				} else if (addedSource || addedTarget) {
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					if (newVertices.size() > 1) {
						updateMinDegreeVertices(newVertices);
					} else if (addedSource) {
						minDegreeVertex = sourceVertex;
					} else if (addedTarget) {
						minDegreeVertex = targetVertex;
					}
					if (addedSource) newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					if (addedTarget) newVertices.put(targetVertex.getIdGradoop(), targetVertex);
				}
			} else if (sourceIn && !(targetIn) && sourceVertex.getDegree() > minDegreeVertex.getDegree()) {
				boolean addedSource = addVertex(sourceVertex);
				addVertex(targetVertex);
				addEdge(wrapper);
				if (addedSource) {
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					if (newVertices.size() > 1) {
						updateMinDegreeVertices(newVertices);
					} else {
						minDegreeVertex = sourceVertex;
					} 
					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
				}
			} else if (targetIn && !(sourceIn) && targetVertex.getDegree() > minDegreeVertex.getDegree()) {
				addVertex(sourceVertex);
				boolean addedTarget = addVertex(targetVertex);
				addEdge(wrapper);
				if (addedTarget) {
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					if (newVertices.size() > 1) {
						updateMinDegreeVertices(newVertices);
					} else {
						minDegreeVertex = targetVertex;
					} 
					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
				}
			}
		}
	}
	
	private static void updateMinDegreeVertex(VertexCustom vertex) {
		if (vertex.getDegree() < minDegreeVertex.getDegree()) {
			secondMinDegreeVertex = minDegreeVertex;
			minDegreeVertex = vertex;
		} else if (vertex.getDegree() < secondMinDegreeVertex.getDegree()) {
			secondMinDegreeVertex = vertex;
		}		
	}

	private static void updateMinDegreeVertices(Map<String, VertexCustom> map) {
		Collection<VertexCustom> collection = map.values();
		Iterator<VertexCustom> iter = collection.iterator();
		minDegreeVertex = iter.next();
		secondMinDegreeVertex = iter.next();
		if (secondMinDegreeVertex.getDegree() < minDegreeVertex.getDegree()) {
			VertexCustom temp = minDegreeVertex;
			minDegreeVertex = secondMinDegreeVertex;
			secondMinDegreeVertex = temp;
		}
		for (Map.Entry<String, VertexCustom> entry : map.entrySet()) {
			VertexCustom vertex = entry.getValue();
			if (vertex.getDegree() < minDegreeVertex.getDegree() && vertex.getIdGradoop() != secondMinDegreeVertex.getIdGradoop()) {
				secondMinDegreeVertex = minDegreeVertex;
				minDegreeVertex = vertex;
			} else if (vertex.getDegree() < secondMinDegreeVertex.getDegree() && vertex.getIdGradoop() != minDegreeVertex.getIdGradoop())  {
				secondMinDegreeVertex = vertex;
			}
		}
	}

	private static void removeVertex(VertexCustom vertex) {	
		if (!globalVertices.containsKey(vertex.getIdGradoop())) {
			System.out.println("cannot remove vertex because not in vertexGlobalMap, id: " + vertex.getIdGradoop());
		} else {
			newVertices.remove(vertex.getIdGradoop());
			globalVertices.remove(vertex.getIdGradoop());
//				Main.sendToAll("removeObjectServer;" + vertex.getIdNumeric());
		}
	}

	private static void reduceNeighborIncidence(VertexCustom vertex) {
//		Set<String> neighborIds = getNeighborhood(vertex);
		Set<String> neighborIds = null;
		for (String neighbor : neighborIds) {
			Map<String,Object> map = globalVertices.get(neighbor);
			map.put("incidence", (int) map.get("incidence") - 1); 
		}
	}

	private static void addWrapperIdentity(VertexCustom vertex) {
		if (capacity > 0) {
			boolean added = addVertex(vertex);
			if (added) {
				newVertices.put(vertex.getIdGradoop(), vertex);
				updateMinDegreeVertex(vertex);
				capacity -= 1;
			}
		} else {
			if (vertex.getDegree() > minDegreeVertex.getDegree()) {
				boolean added = addVertex(vertex);
				if (added) {
					newVertices.put(vertex.getIdGradoop(), vertex);
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					if (newVertices.size() > 1) {
						updateMinDegreeVertices(newVertices);
					} else if (newVertices.size() == 1) {
						minDegreeVertex = vertex;
					}
				}
			} 
		}
	}

	public static void addWrapperIdentityInitial(VertexCustom vertex) {
		boolean added = addVertex(vertex);
		if (added) innerVertices.put(vertex.getIdGradoop(), vertex);
		System.out.println("addWrapperIdentityinitial  " + innerVertices.size());
		for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) System.out.println(entry);
	}
	
	public static void addNonIdentityWrapperInitial(VVEdgeWrapper wrapper) {
		VertexCustom sourceVertex = wrapper.getSourceVertex();
		VertexCustom targetVertex = wrapper.getTargetVertex();
		boolean addedSource = addVertex(sourceVertex);
		if (addedSource) innerVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
		boolean addedTarget = addVertex(targetVertex);
		if (addedTarget) innerVertices.put(targetVertex.getIdGradoop(), targetVertex);
			addEdge(wrapper);
	}
	
	public static boolean addVertex(VertexCustom vertex) {
		String sourceId = vertex.getIdGradoop();
		if (!(globalVertices.containsKey(sourceId))) {
			Map<String,Object> map = new HashMap<String,Object>();
			map.put("incidence", (int) 1);
			map.put("vertex", vertex);
			globalVertices.put(sourceId, map);
//				Main.sendToAll("addVertexServer;" + vertex.getIdNumeric() + ";" + vertex.getX() + ";" + vertex.getY());
			return true;
		} else {
			Map<String,Object> map = globalVertices.get(sourceId);
			map.put("incidence", (int) map.get("incidence") + 1);
			return false;
		}		
	}
	
	public static void addEdge(VVEdgeWrapper wrapper) {
		edges.add(wrapper);
//		Main.sendToAll("addEdgeServer;" + wrapper.getEdgeIdGradoop() + ";" + wrapper.getSourceIdNumeric() + ";" + wrapper.getTargetIdNumeric());
	}
	
//	public void clearOperation(){
//		System.out.println("in clear operation 1");
//		if (operation != "initial"){
//			innerVertices.putAll(newVertices); 
//			for (Map.Entry<String, Map<String,Object>> entry : globalVertices.entrySet()) {
//				Map<String,Object> map = entry.getValue();
//				VertexCustom vertex = (VertexCustom) map.get("vertex");
//				if ((((vertex.getX() < leftModel) || (rightModel < vertex.getX()) || (vertex.getY() < topModel) || 
//						(bottomModel < vertex.getY())) && adjMatrix.get(vertex.getIdGradoop()).isEmpty()) || 
//							((vertex.getX() >= leftModel) && (rightModel >= vertex.getX()) && (vertex.getY() >= topModel) && 
//								(bottomModel >= vertex.getY()) && !innerVertices.containsKey(vertex.getIdGradoop()))) {
//					UndertowServer.sendToAll("removeObjectServer;" + vertex.getIdNumeric());
//					globalVertices.remove(vertex.getIdGradoop());
//				} 
//			}
//		} else {
//			System.out.println("innerVertices" + innerVertices.size());
//			System.out.println("newVertices" + newVertices.size());
//			newVertices = innerVertices;
//		}
//		operation = null;
//		System.out.println("in clear operation 2");
//		System.out.println(newVertices.size());
//		if (newVertices.size() > 1) {
//			updateMinDegreeVertices(newVertices);
//		} else if (newVertices.size() == 1) {
//			minDegreeVertex = newVertices.values().iterator().next();
//		}
//	}
//	
//	public static Set<String> getNeighborhood(VertexCustom vertex){
//		Set<String> neighborIds = new HashSet<String>();
//		for (Map.Entry<String, String> entry : adjMatrix.get(vertex.getIdGradoop()).entrySet()) neighborIds.add(entry.getKey());
//		return neighborIds;
//	}
}
