package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.nextbreakpoint.flinkclient.api.ApiException;
import com.nextbreakpoint.flinkclient.api.FlinkApi;
import com.nextbreakpoint.flinkclient.model.JobIdWithStatus;
import com.nextbreakpoint.flinkclient.model.JobIdsWithStatusOverview;

public class WrapperHandler implements Serializable {
	
	private Map<String,Map<String,Object>> globalVertices;
	private Map<String,VertexCustom> innerVertices;
	private Map<String,VertexCustom> newVertices;
	private Map<String,VVEdgeWrapper> edges;
	private Map<String,VertexCustom> layoutedVertices;
	private String operation;
	private Integer capacity;
	private Float topModel;
	private Float rightModel;
	private Float bottomModel;
	private Float leftModel;
	private Float topModelOld;
	private Float rightModelOld;
	private Float bottomModelOld;
	private Float leftModelOld;
	private Float xModelDiff;
	private Float yModelDiff;
	private VertexCustom secondMinDegreeVertex;
	private VertexCustom minDegreeVertex;    
    private boolean layout = true;
    private Integer maxVertices = 100;
    private FlinkApi api = new FlinkApi();
    private int operationStep;



    private static final class InstanceHolder {
    	static final WrapperHandler INSTANCE = new WrapperHandler();
    }

    private WrapperHandler () {
    	initializeGraphRepresentation();
	}
	  
	public static WrapperHandler getInstance () {
		return InstanceHolder.INSTANCE;
	}
	  
	private void initializeGraphRepresentation() {
	  	System.out.println("initializing graph representation");
	  	operation = "initial";
		globalVertices = new HashMap<String,Map<String,Object>>();
		innerVertices = new HashMap<String,VertexCustom>();
		newVertices = new HashMap<String,VertexCustom>();
		edges = new HashMap<String,VVEdgeWrapper>();
	}
	  
	public void prepareOperation(){
		System.out.println("top, right, bottom, left:" + topModel + " " + rightModel + " "+ bottomModel + " " + leftModel);
			if (operation != "zoomOut"){
				for (Map.Entry<String, VVEdgeWrapper> entry : edges.entrySet()) {
					VVEdgeWrapper wrapper = entry.getValue();
					System.out.println("globalVertices in prepareOperation: ");
					System.out.println(wrapper.getSourceVertex().getIdGradoop());
					System.out.println((VertexCustom) globalVertices.get(wrapper.getSourceVertex().getIdGradoop()).get("vertex") == null);
					System.out.println(wrapper.getTargetVertex().getIdGradoop());
					System.out.println((VertexCustom) globalVertices.get(wrapper.getTargetVertex().getIdGradoop()).get("vertex") == null);
					Integer sourceX;
					Integer sourceY;
					Integer targetX;
					Integer targetY;
					if (layout) {
						sourceX = wrapper.getSourceX();
						sourceY = wrapper.getSourceY();
						targetX = wrapper.getTargetX();
						targetY = wrapper.getTargetY();
					} else {
						VertexCustom sourceVertex = (VertexCustom) layoutedVertices.get(wrapper.getSourceVertex().getIdGradoop());
						sourceX = sourceVertex.getX();
						sourceY = sourceVertex.getY();
						VertexCustom targetVertex = (VertexCustom) layoutedVertices.get(wrapper.getTargetVertex().getIdGradoop());
						targetX = targetVertex.getX();
						targetY = targetVertex.getY();
					}
					if (((sourceX < leftModel) || (rightModel < sourceX) || (sourceY < topModel) || (bottomModel < sourceY)) &&
							((targetX  < leftModel) || (rightModel < targetX ) || (targetY  < topModel) || (bottomModel < targetY))){
						Main.sendToAll("removeObjectServer;" + wrapper.getEdgeIdGradoop());
						System.out.println("Removing Object in prepareOperation, ID: " + wrapper.getEdgeIdGradoop());
					}
				}			
				System.out.println("innerVertices size before removing in prepareOperation: " + innerVertices.size());
				Iterator<Map.Entry<String,VertexCustom>> iter = innerVertices.entrySet().iterator();
				while (iter.hasNext()) {
					Map.Entry<String,VertexCustom> entry = iter.next();
					VertexCustom vertex = entry.getValue();
					System.out.println("VertexID: " + vertex.getIdGradoop() + ", VertexX: " + vertex.getX() + ", VertexY: "+ vertex.getY());
					if ((vertex.getX() < leftModel) || (rightModel < vertex.getX()) || (vertex.getY() < topModel) || (bottomModel < vertex.getY())) {
						iter.remove();
						System.out.println("Removing Object in prepareOperation in innerVertices only, ID: " + vertex.getIdGradoop());
					}
				}
				System.out.println("innerVertices size after removing in prepareOPeration: " + innerVertices.size());
				//this is necessary in case the (second)minDegreeVertex will get deleted in the clear up step befor
				if (innerVertices.size() > 1) {
					updateMinDegreeVertices(innerVertices);
				} else if (innerVertices.size() == 1) {
					minDegreeVertex = innerVertices.values().iterator().next();
				}
			}
			capacity = maxVertices - innerVertices.size();
			if (operation.equals("pan") || operation.equals("zoomOut")) {
				newVertices = innerVertices;
				for (String vId : newVertices.keySet()) System.out.println("prepareOperation, newVertices key: " + vId);
				innerVertices = new HashMap<String,VertexCustom>();
				System.out.println(newVertices.size());
			} else {
				newVertices = new HashMap<String,VertexCustom>();
			}
		System.out.println("Capacity after prepareOperation: " + capacity);
	}
	
	public void addWrapperInitial(VVEdgeWrapper wrapper) {
		if (wrapper.getEdgeLabel().equals("identityEdge")) {
			addWrapperIdentityInitial(wrapper.getSourceVertex());
		} else {
			addNonIdentityWrapperInitial(wrapper);
		}
	}
	
	private void addWrapperIdentityInitial(VertexCustom vertex) {
		boolean added = addVertex(vertex);
		if (added) innerVertices.put(vertex.getIdGradoop(), vertex);
	}
	
	private void addNonIdentityWrapperInitial(VVEdgeWrapper wrapper) {
		VertexCustom sourceVertex = wrapper.getSourceVertex();
		VertexCustom targetVertex = wrapper.getTargetVertex();
		boolean addedSource = addVertex(sourceVertex);
		if (addedSource) innerVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
		boolean addedTarget = addVertex(targetVertex);
		if (addedTarget) innerVertices.put(targetVertex.getIdGradoop(), targetVertex);
		addEdge(wrapper);
	}
	  
	public void addWrapper(VVEdgeWrapper wrapper) {
		System.out.println("SourceIdNumeric: " + wrapper.getSourceIdNumeric());
		System.out.println("SourceIdGradoop: " + wrapper.getSourceIdGradoop());
		System.out.println("TargetIdNumeric: " + wrapper.getTargetIdNumeric());
		System.out.println("TargetIdGradoop: " + wrapper.getTargetIdGradoop());
		System.out.println("WrapperLabel: " + wrapper.getEdgeLabel());
		System.out.println("Size of innerVertices: " + innerVertices.size());
		System.out.println("Size of newVertices: " + newVertices.size());
		System.out.println("ID Gradoop minDegreeVertex: " + minDegreeVertex.getIdGradoop());
		System.out.println("ID Gradoop secondMinDegreeVertex: " + secondMinDegreeVertex.getIdGradoop());
		System.out.println("Capacity: " + capacity);
		if (wrapper.getEdgeLabel().equals("identityEdge")) {
			addWrapperIdentity(wrapper.getSourceVertex());
		} else {
			addNonIdentityWrapper(wrapper);
		}
	}
	  
	private void addWrapperIdentity(VertexCustom vertex) {
		System.out.println("In addWrapperIdentity");
		String vertexId = vertex.getIdGradoop();
		boolean vertexIsRegisteredInside = newVertices.containsKey(vertexId) || innerVertices.containsKey(vertexId);
		if (capacity > 0) {
			addVertex(vertex);
			if (!vertexIsRegisteredInside) {
				newVertices.put(vertex.getIdGradoop(), vertex);
				updateMinDegreeVertex(vertex);
				capacity -= 1;
			}
		} else {
			System.out.println("In addWrapperIdentity, declined capacity < 0");
			if (vertex.getDegree() > minDegreeVertex.getDegree()) {
				addVertex(vertex);
				if (!vertexIsRegisteredInside) {
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					registerInside(vertex);
				}
			} else {
				System.out.println("In addWrapperIdentity, declined vertexDegree > minDegreeVertexDegree");
			}
		}
	}
	  
	private void addNonIdentityWrapper(VVEdgeWrapper wrapper) {
		VertexCustom sourceVertex = wrapper.getSourceVertex();
		VertexCustom targetVertex = wrapper.getTargetVertex();
		String sourceId = sourceVertex.getIdGradoop();
		String targetId = targetVertex.getIdGradoop();
		boolean sourceIsRegisteredInside = newVertices.containsKey(sourceId) || innerVertices.containsKey(sourceId);
		boolean targetIsRegisteredInside = newVertices.containsKey(targetId) || innerVertices.containsKey(targetId);
		if (capacity > 1) {
			addVertex(sourceVertex);
			if ((sourceVertex.getX() >= leftModel) && (rightModel >= sourceVertex.getX()) && (sourceVertex.getY() >= topModel) && 
					(bottomModel >= sourceVertex.getY()) && !sourceIsRegisteredInside){
				updateMinDegreeVertex(sourceVertex);
				newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
				capacity -= 1;
			}
			addVertex(targetVertex);
			if ((targetVertex.getX() >= leftModel) && (rightModel >= targetVertex.getX()) && (targetVertex.getY() >= topModel) 
					&& (bottomModel >= targetVertex.getY()) && !targetIsRegisteredInside){
				updateMinDegreeVertex(targetVertex);
				newVertices.put(targetVertex.getIdGradoop(), targetVertex);
				capacity -= 1;
			}
			addEdge(wrapper);
		} else if (capacity == 1){
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
			System.out.println("In addNonIdentityWrapper, capacity == 1, sourceID: " + sourceVertex.getIdGradoop() + ", sourceIn: " + sourceIn + 
					", targetID: " + targetVertex.getIdGradoop() + ", targetIn: " + targetIn);
			if (sourceIn && targetIn) {
				boolean sourceAdmission = false;
				boolean targetAdmission = false;
				if (sourceVertex.getDegree() > targetVertex.getDegree()) {
					addVertex(sourceVertex);
					sourceAdmission = true;
					if (targetVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside) {
						addVertex(targetVertex);
						targetAdmission = true;
						addEdge(wrapper);
					}
				} else {
					addVertex(targetVertex);
					targetAdmission = true;
					if (sourceVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside) {
						addVertex(sourceVertex);
						sourceAdmission = true;
						addEdge(wrapper);
					}
				}
				if (!sourceIsRegisteredInside && sourceAdmission && !targetIsRegisteredInside && targetAdmission) {
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
					updateMinDegreeVertices(newVertices);
				} else if (!sourceIsRegisteredInside && sourceAdmission) {
					registerInside(sourceVertex);
				} else if (!targetIsRegisteredInside && targetAdmission) {
					registerInside(targetVertex);
				}
				capacity -= 1 ;
			} else if (sourceIn) {
				addVertex(sourceVertex);
				addVertex(targetVertex);
				addEdge(wrapper);
				if (!sourceIsRegisteredInside) {
					capacity -= 1 ;
					registerInside(sourceVertex);
				}
			} else if (targetIn) {
				addVertex(targetVertex);
				addVertex(sourceVertex);
				addEdge(wrapper);
				if (!targetIsRegisteredInside) {
					capacity -= 1 ;
					registerInside(targetVertex);
				}
			}
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
			if (sourceIn && targetIn && (sourceVertex.getDegree() > secondMinDegreeVertex.getDegree()) && 
					(targetVertex.getDegree() > secondMinDegreeVertex.getDegree())) {
				addVertex(sourceVertex);
				addVertex(targetVertex);
				addEdge(wrapper);
				if (!sourceIsRegisteredInside && !targetIsRegisteredInside) {
					reduceNeighborIncidence(minDegreeVertex);
					reduceNeighborIncidence(secondMinDegreeVertex);
					removeVertex(secondMinDegreeVertex);
					removeVertex(minDegreeVertex);
					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
					updateMinDegreeVertices(newVertices);
				} else if (!sourceIsRegisteredInside) {
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					registerInside(sourceVertex);
				} else if (!targetIsRegisteredInside) {
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					registerInside(targetVertex);
				}
			} else if (sourceIn && !(targetIn) && (sourceVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside)) {
				addVertex(sourceVertex);
				addVertex(targetVertex);
				addEdge(wrapper);
				if (!sourceIsRegisteredInside) {
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					registerInside(sourceVertex);
				}
			} else if (targetIn && !(sourceIn) && (targetVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside)) {
				addVertex(sourceVertex);
				addVertex(targetVertex);
				addEdge(wrapper);
				if (!targetIsRegisteredInside) {
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					registerInside(targetVertex);
				}
			} else {
				System.out.println("nonIdentityWrapper not Added!");
			}
		}											
	}
	
	public void addWrapperLayout(VVEdgeWrapper wrapper) {
		//if in zoomIn3 or pan3: cancel flinkjob and move to next step!
		if (operationStep == 3 && ((operation == "zoomIn") || (operation == "pan"))) {
			if (capacity == 0) {
				try {
					JobIdsWithStatusOverview jobs = api.getJobs();
					List<JobIdWithStatus> list = jobs.getJobs();
					String jobid = list.get(0).getId();
					System.out.println("flink api job list size: " + list.size());
					api.terminateJob(jobid, "cancel");
				} catch (ApiException e) {
					e.printStackTrace();
				}
			}
		} 
		System.out.println("SourceIdNumeric: " + wrapper.getSourceIdNumeric());
		System.out.println("SourceIdGradoop: " + wrapper.getSourceIdGradoop());
		System.out.println("TargetIdNumeric: " + wrapper.getTargetIdNumeric());
		System.out.println("TargetIdGradoop: " + wrapper.getTargetIdGradoop());
		System.out.println("WrapperLabel: " + wrapper.getEdgeLabel());
		System.out.println("Size of innerVertices: " + innerVertices.size());
		System.out.println("Size of newVertices: " + newVertices.size());
		System.out.println("ID Gradoop minDegreeVertex: " + minDegreeVertex.getIdGradoop());
		System.out.println("ID Gradoop secondMinDegreeVertex: " + secondMinDegreeVertex.getIdGradoop());
		System.out.println("Capacity: " + capacity);
		if (wrapper.getEdgeLabel().equals("identityEdge")) {
			addWrapperIdentityLayout(wrapper.getSourceVertex());
		} else {
			addNonIdentityWrapperLayout(wrapper);
		}
	}
	
	private void addWrapperIdentityLayout(VertexCustom vertex) {
		System.out.println("In addWrapperIdentityLayout");
		String vertexId = vertex.getIdGradoop();
		boolean vertexIsRegisteredInside = newVertices.containsKey(vertexId) || innerVertices.containsKey(vertexId);
		if (capacity > 0) {
			addVertex(vertex);
			if (!vertexIsRegisteredInside) {
				newVertices.put(vertex.getIdGradoop(), vertex);
				updateMinDegreeVertex(vertex);
				capacity -= 1;
			}
		} else {
			System.out.println("In addWrapperIdentityLayout, declined capacity > 0");
			if (vertex.getDegree() > minDegreeVertex.getDegree()) {
				addVertex(vertex);
				if (!vertexIsRegisteredInside) {
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					registerInside(vertex);
				}
			} else {
				System.out.println("In addWrapperIdentity, declined vertexDegree > minDegreeVertexDegree");
				System.out.println("identityWrapper not Added!");
			}
		}
	}
	
	private void addNonIdentityWrapperLayout(VVEdgeWrapper wrapper) {
		//checken welche Knoten bereits Koordinaten haben. Es muss NICHT immer mindestens ein Knoten bereits Koordinaten haben! (siehe zoomIn3 und pan4)
		//Wenn beide Koordinaten haben, dann kann mit ihnen wie gewöhnlich verfahren werden
		//Wenn nur ein Knoten Koordinaten hat, dann muss der andere inside sein und damit wird die Kapazität um mindestens 1 verringert
			//TODO
		VertexCustom sourceVertex = wrapper.getSourceVertex();
		VertexCustom targetVertex = wrapper.getTargetVertex();
		String sourceId = sourceVertex.getIdGradoop();
		String targetId = targetVertex.getIdGradoop();
		VertexCustom sourceLayouted = null;
		VertexCustom targetLayouted = null;
		boolean sourceIsRegisteredInside = newVertices.containsKey(sourceId) || innerVertices.containsKey(sourceId);
		boolean targetIsRegisteredInside = newVertices.containsKey(targetId) || innerVertices.containsKey(targetId);
		if (layoutedVertices.containsKey(sourceVertex.getIdGradoop())) sourceLayouted = layoutedVertices.get(sourceVertex.getIdGradoop());
		if (layoutedVertices.containsKey(targetVertex.getIdGradoop())) targetLayouted = layoutedVertices.get(targetVertex.getIdGradoop());
		
		//Both Nodes have coordinates and can be treated as usual
		if (sourceLayouted != null && targetLayouted != null) {
			sourceVertex.setX(sourceLayouted.getX());
			sourceVertex.setY(sourceLayouted.getY());
			targetVertex.setX(targetLayouted.getX());
			targetVertex.setY(targetLayouted.getY());
			addNonIdentityWrapper(wrapper);
		}
		
		//Only one node has coordinates, then this node is necessarily already visualized and the other node necessarily needs to be layouted inside
		else if (sourceLayouted != null) {
			if (capacity > 0) {
				addVertex(targetVertex);
				if (!targetIsRegisteredInside) {
					updateMinDegreeVertex(targetVertex);
					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
					capacity -= 1;
				}
				addEdge(wrapper);
			} else {
				if (targetVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside) {
					addVertex(targetVertex);
					addEdge(wrapper);
					if (!targetIsRegisteredInside) {
						reduceNeighborIncidence(minDegreeVertex);
						removeVertex(minDegreeVertex);
						registerInside(targetVertex);
					}
				} else {
					System.out.println("nonIdentityWrapper not Added!");
				}
			}
		} else if (targetLayouted != null) {
			if (capacity > 0) {
				addVertex(sourceVertex);
				if (!sourceIsRegisteredInside) {
					updateMinDegreeVertex(sourceVertex);
					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					capacity -= 1;
				}
				addEdge(wrapper);
			} else {
				if (sourceVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside) {
					addVertex(sourceVertex);
					addEdge(wrapper);
					if (!sourceIsRegisteredInside) {
						reduceNeighborIncidence(minDegreeVertex);
						removeVertex(minDegreeVertex);
						registerInside(sourceVertex);
					}
				} else {
					System.out.println("nonIdentityWrapper not Added!");
				}
			}
		}
		
		//Both nodes do not have coordinates. Then both nodes necessarily need to be layouted inside
		else {
			if (capacity > 1) {
				addVertex(sourceVertex);
				if (!sourceIsRegisteredInside){
					updateMinDegreeVertex(sourceVertex);
					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					capacity -= 1;
				}
				addVertex(targetVertex);
				if (!targetIsRegisteredInside){
					updateMinDegreeVertex(targetVertex);
					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
					capacity -= 1;
				}
				addEdge(wrapper);
			} else if (capacity == 1) {
				boolean sourceAdmission = false;
				boolean targetAdmission = false;
				if (sourceVertex.getDegree() > targetVertex.getDegree()) {
					addVertex(sourceVertex);
					sourceAdmission = true;
					if (targetVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside) {
						addVertex(targetVertex);
						targetAdmission = true;
						addEdge(wrapper);
					}
				} else {
					addVertex(targetVertex);
					targetAdmission = true;
					if (sourceVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside) {
						addVertex(sourceVertex);
						sourceAdmission = true;
						addEdge(wrapper);
					}
				}
				if (!sourceIsRegisteredInside && sourceAdmission && !targetIsRegisteredInside && targetAdmission) {
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
					updateMinDegreeVertices(newVertices);
				} else if (!sourceIsRegisteredInside && sourceAdmission) {
					registerInside(sourceVertex);
				} else if (!targetIsRegisteredInside && targetAdmission) {
					registerInside(targetVertex);
				}
				capacity -= 1 ;
			} else {
				if ((sourceVertex.getDegree() > secondMinDegreeVertex.getDegree()) && 
						(targetVertex.getDegree() > secondMinDegreeVertex.getDegree())) {
					addVertex(sourceVertex);
					addVertex(targetVertex);
					addEdge(wrapper);
					if (!sourceIsRegisteredInside && !targetIsRegisteredInside) {
						reduceNeighborIncidence(minDegreeVertex);
						reduceNeighborIncidence(secondMinDegreeVertex);
						removeVertex(secondMinDegreeVertex);
						removeVertex(minDegreeVertex);
						newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
						newVertices.put(targetVertex.getIdGradoop(), targetVertex);
						updateMinDegreeVertices(newVertices);
					} else if (!sourceIsRegisteredInside) {
						reduceNeighborIncidence(minDegreeVertex);
						removeVertex(minDegreeVertex);
						registerInside(sourceVertex);
					} else if (!targetIsRegisteredInside) {
						reduceNeighborIncidence(minDegreeVertex);
						removeVertex(minDegreeVertex);
						registerInside(targetVertex);
					}
				} else if (sourceVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside) {
					addVertex(sourceVertex);
					if (!sourceIsRegisteredInside) {
						reduceNeighborIncidence(minDegreeVertex);
						removeVertex(minDegreeVertex);
						registerInside(sourceVertex);
					}
				} else if (targetVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside) {
					addVertex(targetVertex);
					if (!targetIsRegisteredInside) {
						reduceNeighborIncidence(minDegreeVertex);
						removeVertex(minDegreeVertex);
						registerInside(targetVertex);
					}
				} else {
					System.out.println("nonIdentityWrapper not Added!");
				}
			}
		}
	}
	  
	public void removeWrapper(VVEdgeWrapper wrapper) {
		if (wrapper.getEdgeIdGradoop() != "identityEdge") {
			String targetId = wrapper.getTargetIdGradoop();
			int targetIncidence = (int) globalVertices.get(targetId).get("incidence");
			if (targetIncidence == 1) {
				globalVertices.remove(targetId);
				if (innerVertices.containsKey(targetId)) innerVertices.remove(targetId);
				if (newVertices.containsKey(targetId)) newVertices.remove(targetId);
				System.out.println("removing object in removeWrapper, ID: " + wrapper.getTargetIdGradoop());
				Main.sendToAll("removeObjectServer;" + wrapper.getTargetIdGradoop());
			} else {
				globalVertices.get(targetId).put("incidence", targetIncidence - 1);
			}
		}
		String sourceId = wrapper.getSourceIdGradoop();
		int sourceIncidence = (int) globalVertices.get(sourceId).get("incidence");
		if (sourceIncidence == 1) {
			globalVertices.remove(sourceId);
			if (innerVertices.containsKey(sourceId)) innerVertices.remove(sourceId);
			if (newVertices.containsKey(sourceId)) newVertices.remove(sourceId);
			Main.sendToAll("removeObjectServer;" + wrapper.getSourceIdGradoop());
			System.out.println("removing object in removeWrapper, ID: " + wrapper.getSourceIdGradoop());
		} else {
			globalVertices.get(sourceId).put("incidence", sourceIncidence - 1);
		}
		edges.remove(wrapper.getEdgeIdGradoop());
	}
	  
	private boolean addVertex(VertexCustom vertex) {
		System.out.println("In addVertex");
		String sourceId = vertex.getIdGradoop();
		System.out.println("globalVertices is null?: " + globalVertices);
		if (!(globalVertices.containsKey(sourceId))) {
			Map<String,Object> map = new HashMap<String,Object>();
			map.put("incidence", (int) 1);
			map.put("vertex", vertex);
			globalVertices.put(sourceId, map);
			if (layout) {
				Main.sendToAll("addVertexServer;" + vertex.getIdGradoop() + ";" + vertex.getX() + ";" + vertex.getY() + ";" + vertex.getIdNumeric());
				Main.sentToClientInSubStep = true;
			} else {
				if (layoutedVertices.containsKey(vertex.getIdGradoop())) {
					VertexCustom layoutedVertex = layoutedVertices.get(vertex.getIdGradoop());
					Main.sendToAll("addVertexServer;" + vertex.getIdGradoop() + ";" + layoutedVertex.getX() + ";" + layoutedVertex.getY() + ";" 
							+ vertex.getIdNumeric());
					Main.sentToClientInSubStep = true;
				} else {
					Main.sendToAll("addVertexServerToBeLayouted;" + vertex.getIdGradoop() + ";" + vertex.getDegree() + ";" + vertex.getIdNumeric());
					Main.sentToClientInSubStep = true;
				}
			}
			return true;
		} else {
			System.out.println("In addVertex, declined because ID contained in globalVertices");
			Map<String,Object> map = globalVertices.get(sourceId);
			map.put("incidence", (int) map.get("incidence") + 1);
			return false;
		}		
	}
	
	private void removeVertex(VertexCustom vertex) {	
		if (!globalVertices.containsKey(vertex.getIdGradoop())) {
			System.out.println("cannot remove vertex because not in vertexGlobalMap, id: " + vertex.getIdGradoop());
		} else {
			newVertices.remove(vertex.getIdGradoop());
			globalVertices.remove(vertex.getIdGradoop());
			Main.sendToAll("removeObjectServer;" + vertex.getIdGradoop());
			Map<String,String> vertexNeighborMap = Main.flinkCore.getGraphUtil().getAdjMatrix().get(vertex.getIdGradoop());
			Iterator<Map.Entry<String, VVEdgeWrapper>> iter = edges.entrySet().iterator();
			while (iter.hasNext()) if (vertexNeighborMap.values().contains(iter.next().getKey())) iter.remove();
			System.out.println("Removing Obect in removeVertex, ID: " + vertex.getIdGradoop());
		}
	}
	  
	private void addEdge(VVEdgeWrapper wrapper) {
		edges.put(wrapper.getEdgeIdGradoop(), wrapper);
		Main.sendToAll("addEdgeServer;" + wrapper.getEdgeIdGradoop() + ";" + wrapper.getSourceIdGradoop() + ";" + wrapper.getTargetIdGradoop());
		Main.sentToClientInSubStep = true;
	}
	
	private void updateMinDegreeVertex(VertexCustom vertex) {
		//CHANGE: < to <=
		if (vertex.getDegree() <= minDegreeVertex.getDegree()) {
			secondMinDegreeVertex = minDegreeVertex;
			minDegreeVertex = vertex;
		} else if (vertex.getDegree() <= secondMinDegreeVertex.getDegree()) {
			secondMinDegreeVertex = vertex;
		}		
	}

	private void updateMinDegreeVertices(Map<String, VertexCustom> map) {
		System.out.println("in updateMinDegreeVertices");
		Collection<VertexCustom> collection = map.values();
		Iterator<VertexCustom> iter = collection.iterator();
		minDegreeVertex = iter.next();
		secondMinDegreeVertex = iter.next();
		System.out.println("Initial min degree vertices: " + minDegreeVertex.getIdGradoop()+ " " + secondMinDegreeVertex.getIdGradoop());
		if (secondMinDegreeVertex.getDegree() < minDegreeVertex.getDegree()) {
			VertexCustom temp = minDegreeVertex;
			minDegreeVertex = secondMinDegreeVertex;
			secondMinDegreeVertex = temp;
		}
		for (Map.Entry<String, VertexCustom> entry : map.entrySet()) {
			VertexCustom vertex = entry.getValue();
			if (vertex.getDegree() < minDegreeVertex.getDegree() && !vertex.getIdGradoop().equals(secondMinDegreeVertex.getIdGradoop())) {
				secondMinDegreeVertex = minDegreeVertex;
				minDegreeVertex = vertex;
			} else if (vertex.getDegree() < secondMinDegreeVertex.getDegree() && !vertex.getIdGradoop().equals(minDegreeVertex.getIdGradoop()))  {
				secondMinDegreeVertex = vertex;
			}
		}
		System.out.println("Final min degree vertices: " + minDegreeVertex.getIdGradoop() + " " + secondMinDegreeVertex.getIdGradoop());
	}
	
	private void registerInside(VertexCustom vertex) {
		newVertices.put(vertex.getIdGradoop(), vertex);
		if (newVertices.size() > 1) {
			updateMinDegreeVertices(newVertices);
		} else {
			minDegreeVertex = vertex;
		}
	}
	
	private void reduceNeighborIncidence(VertexCustom vertex) {
		Set<String> neighborIds = getNeighborhood(vertex);
		for (String neighbor : neighborIds) {
			if (globalVertices.containsKey(neighbor)) {
				Map<String,Object> map = globalVertices.get(neighbor);
				map.put("incidence", (int) map.get("incidence") - 1); 
			}
		}
	}
	
	private Set<String> getNeighborhood(VertexCustom vertex){
		Set<String> neighborIds = new HashSet<String>();
//		for (VVEdgeWrapper wrapper : edges.values()) {
//			String vertexId = vertex.getIdGradoop();
//			String sourceId = wrapper.getSourceIdGradoop();
//			String targetId = wrapper.getTargetIdGradoop();
//			if (sourceId.equals(vertexId)) neighborIds.add(targetId);
//			else if (targetId.equals(vertexId)) neighborIds.add(sourceId);
//		}
//		return neighborIds;
		Map<String,Map<String,String>> adjMatrix = Main.flinkCore.getGraphUtil().getAdjMatrix();
		for (Map.Entry<String, String> entry : adjMatrix.get(vertex.getIdGradoop()).entrySet()) neighborIds.add(entry.getKey());
		return neighborIds;
	}
	
	private boolean hasVisualizedNeighborsInside(VertexCustom vertex) {
//		for (VVEdgeWrapper wrapper : edges.values()) {
//			String vertexId = vertex.getIdGradoop();
//			String sourceId = wrapper.getSourceIdGradoop();
//			String targetId = wrapper.getTargetIdGradoop();
//			if (sourceId.equals(vertexId) && innerVertices.containsKey(targetId)) return true;
//			else if (targetId.equals(vertexId) && innerVertices.containsKey(sourceId)) return true;
//		}
//		return false;
		Map<String,Map<String,String>> adjMatrix = Main.flinkCore.getGraphUtil().getAdjMatrix();
		for (Map.Entry<String, String> entry : adjMatrix.get(vertex.getIdGradoop()).entrySet()) if (innerVertices.containsKey(entry.getKey())) return true;
		return false;
	}
	
	public void clearOperation(){
		System.out.println("in clear operation");
		System.out.println(operation);
		if (operation != "initial"){
			for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) System.out.println("innerVertex " + entry.getValue().getIdGradoop());
			if (!layout) {
				for (VertexCustom vertex : newVertices.values()) {
					VertexCustom layoutedVertex = layoutedVertices.get(vertex.getIdGradoop());
					vertex.setX(layoutedVertex.getX());
					vertex.setY(layoutedVertex.getY());
				}
			}
			System.out.println("innerVertices size before put all: " + innerVertices.size());
			innerVertices.putAll(newVertices); 
			System.out.println("innerVertices size after put all: " + innerVertices.size());
			for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) System.out.println("innerVertex after putAll" + entry.getValue().getIdGradoop() + " " 
					+ entry.getValue().getX());
			System.out.println("global...");
			Iterator<Map.Entry<String, Map<String,Object>>> iter = globalVertices.entrySet().iterator();
			while (iter.hasNext()) {
				VertexCustom vertex = (VertexCustom) iter.next().getValue().get("vertex");
				if ((((vertex.getX() < leftModel) || (rightModel < vertex.getX()) || (vertex.getY() < topModel) || 
						(bottomModel < vertex.getY())) && !hasVisualizedNeighborsInside(vertex)) ||
							((vertex.getX() >= leftModel) && (rightModel >= vertex.getX()) && (vertex.getY() >= topModel) && (bottomModel >= vertex.getY()) 
									&& !innerVertices.containsKey(vertex.getIdGradoop()))) {
					System.out.println("removing in clear operation " + vertex.getIdGradoop());
					Main.sendToAll("removeObjectServer;" + vertex.getIdGradoop());
					iter.remove();
					Map<String,String> vertexNeighborMap = Main.flinkCore.getGraphUtil().getAdjMatrix().get(vertex.getIdGradoop());
					Iterator<Map.Entry<String, VVEdgeWrapper>> edgesIterator = edges.entrySet().iterator();
					while (edgesIterator.hasNext()) if (vertexNeighborMap.values().contains(edgesIterator.next().getKey())) edgesIterator.remove();
				} 
			}
			//this is necessary in case the (second)minDegreeVertex will get deleted in the clear up step before (e.g. in ZoomOut)
			if (newVertices.size() > 1) {
				updateMinDegreeVertices(newVertices);
			} else if (newVertices.size() == 1) {
				minDegreeVertex = newVertices.values().iterator().next();
			}
		} else {
			if (!layout) {
				for (VertexCustom vertex : innerVertices.values()) {
					VertexCustom layoutedVertex = layoutedVertices.get(vertex.getIdGradoop());
					System.out.println(layoutedVertices.get(vertex.getIdGradoop()).getIdGradoop() + layoutedVertex.getX());
					vertex.setX(layoutedVertex.getX());
					vertex.setY(layoutedVertex.getY());
				}
			}
			newVertices = innerVertices;
			if (newVertices.size() > 1) {
				updateMinDegreeVertices(newVertices);
			} else if (newVertices.size() == 1) {
				minDegreeVertex = newVertices.values().iterator().next();
			}
		}
//		operation = null;
		Set<String> visualizedVertices = new HashSet<String>();
		for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) visualizedVertices.add(entry.getKey());
		Set<String> visualizedWrappers = new HashSet<String>();
		for (Map.Entry<String, VVEdgeWrapper> entry : edges.entrySet()) visualizedWrappers.add(entry.getKey());
		GraphUtil graphUtil =  Main.flinkCore.getGraphUtil();
		graphUtil.setVisualizedVertices(visualizedVertices);
		graphUtil.setVisualizedWrappers(visualizedWrappers);
		System.out.println("global size "+ globalVertices.size());
//		for (Map.Entry<String, Map<String,Object>> map : globalVertices.entrySet()) {
//			VertexCustom vertex = (VertexCustom) map.getValue().get("vertex");
//			System.out.println("vertex id: "  + vertex.getIdGradoop());
//			System.out.println("vertex degree: " + vertex.getDegree());
//			System.out.println("vertex incidence: " + map.getValue().get("incidence"));
//			System.out.println(vertex.getDegree() + 1 >= (Integer) map.getValue().get("incidence"));
//		}
	}
	
}
