package aljoschaRydzyk.viewportDrivenGraphStreaming;

import java.util.ArrayList;
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
import com.nextbreakpoint.flinkclient.model.JobIdWithStatus.StatusEnum;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils.GraphUtil;
import com.nextbreakpoint.flinkclient.model.JobIdsWithStatusOverview;

public class WrapperHandler{
	private Map<String,Map<String,Object>> globalVertices;
	private Map<String,VertexGVD> innerVertices;
	private Map<String,VertexGVD> newVertices;
	private Map<String,WrapperGVD> edges;
	private Map<String,VertexGVD> layoutedVertices;
	private String operation;
	private int vertexCapacity;
	private int edgeCapacity;
	private Float top;
	private Float right;
	private Float bottom;
	private Float left;
	private VertexGVD secondMinDegreeVertex;
	private VertexGVD minDegreeVertex;    
    private boolean layout = true;
    private int maxVertices = 100;
    private FlinkApi api;
    public boolean sentToClientInSubStep;
	private Server server;

    public WrapperHandler () {
    	this.server = Server.getInstance();
    	System.out.println("wrapper handler constructor is executed");
    }
	 
	public void initializeAPI(String clusterEntryPointAddress) {
		api = new FlinkApi();
        api.getApiClient().setBasePath("http://" + clusterEntryPointAddress + ":8081");  
	}
	
	public void initializeGraphRepresentation() {
	  	System.out.println("initializing graph representation");
	  	operation = "initial";
		globalVertices = new HashMap<String,Map<String,Object>>();
		innerVertices = new HashMap<String,VertexGVD>();
		newVertices = new HashMap<String,VertexGVD>();
		edges = new HashMap<String,WrapperGVD>();
		minDegreeVertex = null;
		secondMinDegreeVertex = null;
	}
	  
	public void prepareOperation(){
		if (operation != "zoomOut"){
			for (Map.Entry<String, WrapperGVD> entry : edges.entrySet()) {
				WrapperGVD wrapper = entry.getValue();
				int sourceX;
				int sourceY;
				int targetX;
				int targetY;
				if (layout) {
					sourceX = wrapper.getSourceX();
					sourceY = wrapper.getSourceY();
					targetX = wrapper.getTargetX();
					targetY = wrapper.getTargetY();
				} else {
					VertexGVD sourceVertex = (VertexGVD) layoutedVertices.get(wrapper.getSourceVertex().getIdGradoop());
					sourceX = sourceVertex.getX();
					sourceY = sourceVertex.getY();
					VertexGVD targetVertex = (VertexGVD) layoutedVertices.get(wrapper.getTargetVertex().getIdGradoop());
					targetX = targetVertex.getX();
					targetY = targetVertex.getY();
				}
				if (((sourceX < left) || (right < sourceX) || (sourceY < top) || (bottom < sourceY)) &&
						((targetX  < left) || (right < targetX ) || (targetY  < top) || (bottom < targetY))){
					server.sendToAll("removeObjectServer;" + wrapper.getEdgeIdGradoop());
				}
			}			
			Iterator<Map.Entry<String,VertexGVD>> iter = innerVertices.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String,VertexGVD> entry = iter.next();
				VertexGVD vertex = entry.getValue();
				System.out.println("VertexID: " + vertex.getIdGradoop() + ", VertexX: " + vertex.getX() + ", VertexY: "+ vertex.getY());
				if ((vertex.getX() < left) || (right < vertex.getX()) || (vertex.getY() < top) || (bottom < vertex.getY())) {
					iter.remove();
				}
			}
			
		}
		vertexCapacity = maxVertices - innerVertices.size();
		if (operation.equals("pan") || operation.equals("zoomOut")) {
			newVertices = innerVertices;
			prepareOperationHelper(newVertices, innerVertices);
		} else { 
			prepareOperationHelper(innerVertices, newVertices);
		}		
	}
	
	private void prepareOperationHelper(Map<String,VertexGVD> controlMap, Map<String,VertexGVD> emptyMap) {
		if (vertexCapacity < 0) {
			List<VertexGVD> list = new ArrayList<VertexGVD>(controlMap.values());
			list.sort(new VertexGVDNumericIdComparator().reversed());
			while (vertexCapacity < 0) {
				list.remove(list.size() - 1);
				vertexCapacity += 1;
			}
			controlMap = new HashMap<String,VertexGVD>();
			for (VertexGVD vertex : list) {
				controlMap.put(vertex.getIdGradoop(), vertex);
			}
		}
		
		//this is necessary in case the (second)minDegreeVertex will get deleted in the clear up step befor
		if (controlMap.size() > 1) {
			updateMinDegreeVertices(controlMap);
		} else if (controlMap.size() == 1) {
			minDegreeVertex = controlMap.values().iterator().next();
		}
		emptyMap = new HashMap<String,VertexGVD>();
	}
	
	public void addWrapperCollectionInitial(List<WrapperGVD> wrapperCollection) {
		Iterator<WrapperGVD> iter = wrapperCollection.iterator();
		while (iter.hasNext()) addWrapperInitial(iter.next());
	}
	
	public void addWrapperInitial(WrapperGVD wrapper) {;
		if (wrapper.getEdgeLabel().equals("identityEdge")) {
			addWrapperIdentityInitial(wrapper.getSourceVertex());
		} else {
			addNonIdentityWrapperInitial(wrapper);
		}
	}
	
	private void addWrapperIdentityInitial(VertexGVD vertex) {
		boolean added = addVertex(vertex);
		if (added) innerVertices.put(vertex.getIdGradoop(), vertex);
	}
	
	private void addNonIdentityWrapperInitial(WrapperGVD wrapper) {
		VertexGVD sourceVertex = wrapper.getSourceVertex();
		VertexGVD targetVertex = wrapper.getTargetVertex();
		boolean addedSource = addVertex(sourceVertex);
		if (addedSource) innerVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
		boolean addedTarget = addVertex(targetVertex);
		if (addedTarget) innerVertices.put(targetVertex.getIdGradoop(), targetVertex);
		if (edgeCapacity > 0) addEdge(wrapper);
	}
	
	public void addWrapperCollection(List<WrapperGVD> wrapperCollection) {
		Iterator<WrapperGVD> iter = wrapperCollection.iterator();
		while (iter.hasNext()) addWrapper(iter.next());
	}
	  
	public void addWrapper(WrapperGVD wrapper) {
		if (wrapper.getEdgeLabel().equals("identityEdge")) {
			addWrapperIdentity(wrapper.getSourceVertex());
		} else {
			addNonIdentityWrapper(wrapper);
		}
	}
	  
	private void addWrapperIdentity(VertexGVD vertex) {
		String vertexId = vertex.getIdGradoop();
		boolean vertexIsRegisteredInside = newVertices.containsKey(vertexId) || innerVertices.containsKey(vertexId);
		if (vertexCapacity > 0) {
			addVertex(vertex);
			if (!vertexIsRegisteredInside) {
				newVertices.put(vertex.getIdGradoop(), vertex);
				updateMinDegreeVertex(vertex);
				vertexCapacity -= 1;
			}
		} else {
			if (vertex.getDegree() > minDegreeVertex.getDegree()) {
				addVertex(vertex);
				if (!vertexIsRegisteredInside) {
					removeVertex(minDegreeVertex);
					registerInside(vertex);
				}
			} 
			
			//only applicable if identity wrapper stream  is degree-sorted!
			else {
				try {
					JobIdsWithStatusOverview jobs = api.getJobs();
					List<JobIdWithStatus> list = jobs.getJobs();
					Iterator<JobIdWithStatus> iter = list.iterator();
					JobIdWithStatus job;
					while (iter.hasNext()) {
						job = iter.next();
						if (job.getStatus() == StatusEnum.RUNNING) {
							api.terminateJob(job.getId(), "cancel");
							break;
						}
					}
				} catch (ApiException e) {
					System.out.println("job was cancelled by server application.");
				}
			}
		}
	}
	  
	private void addNonIdentityWrapper(WrapperGVD wrapper) {
		VertexGVD sourceVertex = wrapper.getSourceVertex();
		VertexGVD targetVertex = wrapper.getTargetVertex();
		String sourceId = sourceVertex.getIdGradoop();
		String targetId = targetVertex.getIdGradoop();
		boolean sourceIsRegisteredInside = newVertices.containsKey(sourceId) || innerVertices.containsKey(sourceId);
		boolean targetIsRegisteredInside = newVertices.containsKey(targetId) || innerVertices.containsKey(targetId);
		boolean sourceIn = sourceVertex.getX() >= left && right >= sourceVertex.getX() && sourceVertex.getY() >= top && 
				bottom >= sourceVertex.getY();
		boolean targetIn = targetVertex.getX() >= left && right >= targetVertex.getX() && targetVertex.getY() >= top 
				&& bottom >= targetVertex.getY();
		if (vertexCapacity > 1) {
			if (sourceIn && targetIn) {
				addVertex(sourceVertex);
				if (!sourceIsRegisteredInside) {
					updateMinDegreeVertex(sourceVertex);
					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					vertexCapacity -= 1;
				}
				addVertex(targetVertex);
				if (!targetIsRegisteredInside) {
					updateMinDegreeVertex(targetVertex);
					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
					vertexCapacity -= 1;
				}
				if (edgeCapacity > 0) addEdge(wrapper);
			} else {
				if (edgeCapacity > 0) {
					addVertex(sourceVertex);
					if (!sourceIsRegisteredInside) {
						updateMinDegreeVertex(sourceVertex);
						newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
						vertexCapacity -= 1;
					}
					addVertex(targetVertex);
					if (!targetIsRegisteredInside) {
						updateMinDegreeVertex(targetVertex);
						newVertices.put(targetVertex.getIdGradoop(), targetVertex);
						vertexCapacity -= 1;
					}
					addEdge(wrapper);
				}
			}
		} else if (vertexCapacity == 1){
			if ((sourceVertex.getX() < left) || (right < sourceVertex.getX()) || (sourceVertex.getY() < top) || 
					(bottom < sourceVertex.getY())){
				sourceIn = false;
			}
			if ((targetVertex.getX() < left) || (right < targetVertex.getX()) || (targetVertex.getY() < top) || 
					(bottom < targetVertex.getY())){
				targetIn = false;
			}
			if (sourceIn && targetIn) {
				boolean sourceAdmission = false;
				boolean targetAdmission = false;
				if (sourceVertex.getDegree() > targetVertex.getDegree()) {
					addVertex(sourceVertex);
					sourceAdmission = true;
					if (targetVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside) {
						addVertex(targetVertex);
						targetAdmission = true;
						if (edgeCapacity > 0) addEdge(wrapper);
					}
				} else {
					addVertex(targetVertex);
					targetAdmission = true;
					if (sourceVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside) {
						addVertex(sourceVertex);
						sourceAdmission = true;
						if (edgeCapacity > 0) addEdge(wrapper);
					}
				}
				if (!sourceIsRegisteredInside && sourceAdmission && !targetIsRegisteredInside && targetAdmission) {
					removeVertex(minDegreeVertex);
					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
					updateMinDegreeVertices(newVertices);
				} else if (!sourceIsRegisteredInside && sourceAdmission) {
					registerInside(sourceVertex);
				} else if (!targetIsRegisteredInside && targetAdmission) {
					registerInside(targetVertex);
				}
				vertexCapacity -= 1 ;
			} else if (sourceIn && edgeCapacity > 0) {
				addVertex(sourceVertex);
				addVertex(targetVertex);
				addEdge(wrapper);
				if (!sourceIsRegisteredInside) {
					vertexCapacity -= 1 ;
					registerInside(sourceVertex);
				}
			} else if (targetIn && edgeCapacity > 0) {
				addVertex(targetVertex);
				addVertex(sourceVertex);
				addEdge(wrapper);
				if (!targetIsRegisteredInside) {
					vertexCapacity -= 1 ;
					registerInside(targetVertex);
				}
			}
		} else {
			if ((sourceVertex.getX() < left) || (right < sourceVertex.getX()) || (sourceVertex.getY() < top) || 
					(bottom < sourceVertex.getY())){
				sourceIn = false;
			}
			if ((targetVertex.getX() < left) || (right < targetVertex.getX()) || (targetVertex.getY() < top) || 
					(bottom < targetVertex.getY())){
				targetIn = false;
			}
			if (sourceIn && targetIn && (sourceVertex.getDegree() > secondMinDegreeVertex.getDegree()) && 
					(targetVertex.getDegree() > secondMinDegreeVertex.getDegree())) {
				addVertex(sourceVertex);
				addVertex(targetVertex);
				if (edgeCapacity > 0) addEdge(wrapper);
				if (!sourceIsRegisteredInside && !targetIsRegisteredInside) {
					removeVertex(secondMinDegreeVertex);
					removeVertex(minDegreeVertex);
					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
					updateMinDegreeVertices(newVertices);
				} else if (!sourceIsRegisteredInside) {
					removeVertex(minDegreeVertex);
					registerInside(sourceVertex);
				} else if (!targetIsRegisteredInside) {
					removeVertex(minDegreeVertex);
					registerInside(targetVertex);
				}
			} else if (sourceIn && !(targetIn) && edgeCapacity > 0 &&
					(sourceVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside)) {
				addVertex(sourceVertex);
				addVertex(targetVertex);
				addEdge(wrapper);
				if (!sourceIsRegisteredInside) {
					removeVertex(minDegreeVertex);
					registerInside(sourceVertex);
				}
			} else if (targetIn && !(sourceIn) && edgeCapacity > 0 && 
					(targetVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside)) {
				addVertex(sourceVertex);
				addVertex(targetVertex);
				addEdge(wrapper);
				if (!targetIsRegisteredInside) {
					removeVertex(minDegreeVertex);
					registerInside(targetVertex);
				}
			}
		}											
	}
	
	public void addWrapperCollectionLayout(List<WrapperGVD> wrapperCollection) {
		Iterator<WrapperGVD> iter = wrapperCollection.iterator();
		while (iter.hasNext()) addWrapperLayout(iter.next());
	}
	
	public void addWrapperLayout(WrapperGVD wrapper) {
		//if in zoomIn3 or pan3: cancel flinkjob if still running, close socket and reopen for next step and move to next step!
			if (vertexCapacity <= 0 && edgeCapacity <= 0) {
				try {
					JobIdsWithStatusOverview jobs = api.getJobs();
					List<JobIdWithStatus> list = jobs.getJobs();
					Iterator<JobIdWithStatus> iter = list.iterator();
					JobIdWithStatus job;
					while (iter.hasNext()) {
						job = iter.next();
						if (job.getStatus() == StatusEnum.RUNNING) {
							api.terminateJob(job.getId(), "cancel");
							break;
						}
					}
				} catch (ApiException e) {
					System.out.println("Job was cancelled by server application.");
				}
			}
		if (wrapper.getEdgeLabel().equals("identityEdge")) {
			addWrapperIdentity(wrapper.getSourceVertex());
		} else {
			addNonIdentityWrapperLayout(wrapper);
		}
	}
	
	private void addNonIdentityWrapperLayout(WrapperGVD wrapper) {
		VertexGVD sourceVertex = wrapper.getSourceVertex();
		VertexGVD targetVertex = wrapper.getTargetVertex();
		String sourceId = sourceVertex.getIdGradoop();
		String targetId = targetVertex.getIdGradoop();
		VertexGVD sourceLayouted = null;
		VertexGVD targetLayouted = null;
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
			if (vertexCapacity > 0) {
				addVertex(targetVertex);
				if (!targetIsRegisteredInside) {
					updateMinDegreeVertex(targetVertex);
					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
					vertexCapacity -= 1;
				}
				if (edgeCapacity > 0) addEdge(wrapper);
			} else if (targetVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside) {
				addVertex(targetVertex);
				if (edgeCapacity > 0) addEdge(wrapper);
				if (!targetIsRegisteredInside) {
					removeVertex(minDegreeVertex);
					registerInside(targetVertex);
				}
			}

		} else if (targetLayouted != null) {
			if (vertexCapacity > 0) {
				addVertex(sourceVertex);
				if (!sourceIsRegisteredInside) {
					updateMinDegreeVertex(sourceVertex);
					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					vertexCapacity -= 1;
				}
				if (edgeCapacity > 0) addEdge(wrapper);
			} else {
				if (sourceVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside) {
					addVertex(sourceVertex);
					if (edgeCapacity > 0) addEdge(wrapper);
					if (!sourceIsRegisteredInside) {
						removeVertex(minDegreeVertex);
						registerInside(sourceVertex);
					}
				}
			}
		}
		
		//Both nodes do not have coordinates. Then both nodes necessarily need to be layouted inside
		else {
			if (vertexCapacity > 1) {
				addVertex(sourceVertex);
				if (!sourceIsRegisteredInside){
					updateMinDegreeVertex(sourceVertex);
					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					vertexCapacity -= 1;
				}
				addVertex(targetVertex);
				if (!targetIsRegisteredInside){
					updateMinDegreeVertex(targetVertex);
					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
					vertexCapacity -= 1;
				}
				if (edgeCapacity > 0) addEdge(wrapper);
			} else if (vertexCapacity == 1) {
				boolean sourceAdmission = false;
				boolean targetAdmission = false;
				if (sourceVertex.getDegree() > targetVertex.getDegree()) {
					addVertex(sourceVertex);
					sourceAdmission = true;
					if (targetVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside) {
						addVertex(targetVertex);
						targetAdmission = true;
						if (edgeCapacity > 0) addEdge(wrapper);
					}
				} else {
					addVertex(targetVertex);
					targetAdmission = true;
					if (sourceVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside) {
						addVertex(sourceVertex);
						sourceAdmission = true;
						if (edgeCapacity > 0) addEdge(wrapper);
					}
				}
				if (!sourceIsRegisteredInside && sourceAdmission && !targetIsRegisteredInside && targetAdmission) {
					removeVertex(minDegreeVertex);
					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
					updateMinDegreeVertices(newVertices);
				} else if (!sourceIsRegisteredInside && sourceAdmission) {
					registerInside(sourceVertex);
				} else if (!targetIsRegisteredInside && targetAdmission) {
					registerInside(targetVertex);
				}
				vertexCapacity -= 1 ;
			} else {
				if ((sourceVertex.getDegree() > secondMinDegreeVertex.getDegree()) && 
						(targetVertex.getDegree() > secondMinDegreeVertex.getDegree())) {
					addVertex(sourceVertex);
					addVertex(targetVertex);
					if (edgeCapacity > 0) addEdge(wrapper);
					if (!sourceIsRegisteredInside && !targetIsRegisteredInside) {
						removeVertex(secondMinDegreeVertex);
						removeVertex(minDegreeVertex);
						newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
						newVertices.put(targetVertex.getIdGradoop(), targetVertex);
						updateMinDegreeVertices(newVertices);
					} else if (!sourceIsRegisteredInside) {
						removeVertex(minDegreeVertex);
						registerInside(sourceVertex);
					} else if (!targetIsRegisteredInside) {
						removeVertex(minDegreeVertex);
						registerInside(targetVertex);
					}
				} else if (sourceVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside) {
					addVertex(sourceVertex);
					if (!sourceIsRegisteredInside) {
						removeVertex(minDegreeVertex);
						registerInside(sourceVertex);
					}
				} else if (targetVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside) {
					addVertex(targetVertex);
					if (!targetIsRegisteredInside) {
						removeVertex(minDegreeVertex);
						registerInside(targetVertex);
					}
				}
			}
		}
	}
	  
	private boolean addVertex(VertexGVD vertex) {
		String sourceId = vertex.getIdGradoop();
		if (!(globalVertices.containsKey(sourceId))) {
			Map<String,Object> map = new HashMap<String,Object>();
			map.put("incidence", (int) 1);
			map.put("vertex", vertex);
			globalVertices.put(sourceId, map);
			if (layout) {
				server.sendToAll("addVertexServer;" + vertex.getIdGradoop() + ";" + vertex.getX() + ";" + 
				vertex.getY() + ";" + vertex.getLabel() + ";" + vertex.getDegree() + ";" + vertex.getZoomLevel());
				sentToClientInSubStep = true;
			} else {
				if (layoutedVertices.containsKey(vertex.getIdGradoop())) {
					VertexGVD layoutedVertex = layoutedVertices.get(vertex.getIdGradoop());
					server.sendToAll("addVertexServer;" + vertex.getIdGradoop() + ";" + layoutedVertex.getX() + ";" + layoutedVertex.getY() + ";" 
							+ vertex.getLabel() + ";" + vertex.getDegree() + ";" + vertex.getZoomLevel());
					sentToClientInSubStep = true;
				} else {
					server.sendToAll("addVertexServerToBeLayouted;" + vertex.getIdGradoop() + ";" + vertex.getLabel() + ";" + vertex.getDegree()
						+ ";" + vertex.getZoomLevel());
					sentToClientInSubStep = true;
				}
			}
			return true;
		} else {
			Map<String,Object> map = globalVertices.get(sourceId);
			map.put("incidence", (int) map.get("incidence") + 1);
			return false;
		}		
	}
	
	private void removeVertex(VertexGVD vertex) {	
		if (globalVertices.containsKey(vertex.getIdGradoop())) {
			newVertices.remove(vertex.getIdGradoop());
			globalVertices.remove(vertex.getIdGradoop());
			server.sendToAll("removeObjectServer;" + vertex.getIdGradoop());
			Iterator<WrapperGVD> iter = edges.values().iterator();
			while (iter.hasNext()) {
				WrapperGVD wrapper = iter.next();
				String sourceId = wrapper.getSourceIdGradoop();
				String targetId = wrapper.getTargetIdGradoop();
				String vertexId = vertex.getIdGradoop();
				if (sourceId.equals(vertexId) || targetId.equals(vertexId)) {
					edgeCapacity += 1;
					iter.remove();
				}
			}
		}
	}
	  
	private void addEdge(WrapperGVD wrapper) {
		edges.put(wrapper.getEdgeIdGradoop(), wrapper);
		edgeCapacity -= 1;
		server.sendToAll("addEdgeServer;" + wrapper.getEdgeIdGradoop() + ";" + wrapper.getSourceIdGradoop() + ";" + 
				wrapper.getTargetIdGradoop() + ";" + wrapper.getEdgeLabel());
		sentToClientInSubStep = true;
	}
	
	private void updateMinDegreeVertex(VertexGVD vertex) {
		if (vertex.getDegree() <= minDegreeVertex.getDegree()) {
			secondMinDegreeVertex = minDegreeVertex;
			minDegreeVertex = vertex;
		} else if (vertex.getDegree() <= secondMinDegreeVertex.getDegree()) {
			secondMinDegreeVertex = vertex;
		}		
	}

	private void updateMinDegreeVertices(Map<String, VertexGVD> map) {
		Collection<VertexGVD> collection = map.values();
		Iterator<VertexGVD> iter = collection.iterator();
		minDegreeVertex = iter.next();
		secondMinDegreeVertex = iter.next();
		if (secondMinDegreeVertex.getDegree() < minDegreeVertex.getDegree()) {
			VertexGVD temp = minDegreeVertex;
			minDegreeVertex = secondMinDegreeVertex;
			secondMinDegreeVertex = temp;
		}
		for (Map.Entry<String, VertexGVD> entry : map.entrySet()) {
			VertexGVD vertex = entry.getValue();
			if (vertex.getDegree() < minDegreeVertex.getDegree() && !vertex.getIdGradoop().equals(secondMinDegreeVertex.getIdGradoop())) {
				secondMinDegreeVertex = minDegreeVertex;
				minDegreeVertex = vertex;
			} else if (vertex.getDegree() < secondMinDegreeVertex.getDegree() && !vertex.getIdGradoop().equals(minDegreeVertex.getIdGradoop()))  {
				secondMinDegreeVertex = vertex;
			}
		}
	}
	
	private void registerInside(VertexGVD vertex) {
		newVertices.put(vertex.getIdGradoop(), vertex);
		if (newVertices.size() > 1) {
			updateMinDegreeVertices(newVertices);
		} else {
			minDegreeVertex = vertex;
		}
	}
	
	private boolean hasVisualizedNeighborsInside(VertexGVD vertex) {
		for (WrapperGVD wrapper : edges.values()) {
			String sourceId = wrapper.getSourceIdGradoop();
			String targetId = wrapper.getTargetIdGradoop();
			String vertexId = vertex.getIdGradoop();
			if (sourceId.equals(vertexId)) {
				if (innerVertices.containsKey(targetId)) return true;
			} else if (targetId.equals(vertexId)) {
				if (innerVertices.containsKey(sourceId)) return true;
			}
		}
		return false;
	}
	
	public void clearOperation(){
		System.out.println("Executing WrapperHandler.clearOperation()!");
		if (operation != "initial"){
			if (!layout) {
				for (VertexGVD vertex : newVertices.values()) {
					VertexGVD layoutedVertex = layoutedVertices.get(vertex.getIdGradoop());
					vertex.setX(layoutedVertex.getX());
					vertex.setY(layoutedVertex.getY());
				}
			}
			innerVertices.putAll(newVertices); 
			Iterator<Map.Entry<String, Map<String,Object>>> iter = globalVertices.entrySet().iterator();
			while (iter.hasNext()) {
				VertexGVD vertex = (VertexGVD) iter.next().getValue().get("vertex");
				String vertexId = vertex.getIdGradoop();
				if ((((vertex.getX() < left) || (right < vertex.getX()) || (vertex.getY() < top) || 
						(bottom < vertex.getY())) && !hasVisualizedNeighborsInside(vertex)) ||
							((vertex.getX() >= left) && (right >= vertex.getX()) && (vertex.getY() >= top) && (bottom >= vertex.getY()) 
									&& !innerVertices.containsKey(vertexId))) {
					server.sendToAll("removeObjectServer;" + vertexId);
					Iterator<WrapperGVD> edgesIterator = edges.values().iterator();
					while (edgesIterator.hasNext()) {
						WrapperGVD wrapper = edgesIterator.next();
						String sourceId = wrapper.getSourceIdGradoop();
						String targetId = wrapper.getTargetIdGradoop();
						if (sourceId.equals(vertexId) || targetId.equals(vertexId)) {
							edgeCapacity += 1;
							edgesIterator.remove();
						}
					}
					iter.remove();
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
				for (VertexGVD vertex : innerVertices.values()) {
					VertexGVD layoutedVertex = layoutedVertices.get(vertex.getIdGradoop());
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
		Set<String> visualizedVertices = new HashSet<String>();
		for (Map.Entry<String, VertexGVD> entry : innerVertices.entrySet()) visualizedVertices.add(entry.getKey());
		Set<String> visualizedWrappers = new HashSet<String>();
		for (Map.Entry<String, WrapperGVD> entry : edges.entrySet()) visualizedWrappers.add(entry.getKey());
		GraphUtil graphUtil =  server.getFlinkCore().getGraphUtil();
		graphUtil.setVisualizedVertices(visualizedVertices);
		graphUtil.setVisualizedWrappers(visualizedWrappers);
		server.sendToAll("enableMouse");
	}
	
	public void setModelPositions(Float topModel, Float rightModel, Float bottomModel, Float leftModel) {
		this.top = topModel;
		this.right = rightModel;
		this.bottom = bottomModel;
		this.left = leftModel;
	}

	public void setOperation(String operation) {
		this.operation = operation;
	}
	
	public void setMaxVertices(int maxVertices) {
		this.maxVertices = maxVertices;
		this.edgeCapacity = maxVertices;
	}

	public void setLayoutMode(boolean layoutMode) {
		System.out.println("Setting wrapperHandler layout Mode to " + layoutMode);
		this.layout = layoutMode;
	}

	public void resetLayoutedVertices() {
		this.layoutedVertices = new HashMap<String,VertexGVD>();
	}

	public Map<String, VertexGVD> getLayoutedVertices() {
		return this.layoutedVertices;
	}

	public Map<String, VertexGVD> getNewVertices() {
		return this.newVertices;
	}
	
	public void setSentToClientInSubStep(Boolean sent) {
		this.sentToClientInSubStep = sent;
	}
	
	public Boolean getSentToClientInSubStep() {
		return this.sentToClientInSubStep;
	}

	public Map<String, VertexGVD> getInnerVertices() {
		return this.innerVertices;
	}

	public void updateLayoutedVertices(List<String> list) {
    	for (String vertexData : list) {
    		String[] arrVertexData = vertexData.split(",");
    		String vertexId = arrVertexData[0];
    		int x = Math.round(Float.parseFloat(arrVertexData[1]));
    		int y = Math.round(Float.parseFloat(arrVertexData[2]));
    		int zoomLevel = Integer.parseInt(arrVertexData[3]);
			VertexGVD vertex = new VertexGVD(vertexId, x, y, zoomLevel);
			layoutedVertices.put(vertexId, vertex);
    	}
	}

	public int getCapacity() {
		return this.vertexCapacity;
	}
	
	public Map<String,Map<String,Object>> getGlobalVertices(){
		return this.globalVertices;
	}
}
