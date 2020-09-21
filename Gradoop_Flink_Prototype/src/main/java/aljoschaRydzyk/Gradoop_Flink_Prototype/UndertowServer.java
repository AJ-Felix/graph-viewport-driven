package aljoschaRydzyk.Gradoop_Flink_Prototype;

import io.undertow.Undertow;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import static io.undertow.Handlers.*;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class UndertowServer {
	
	private static FlinkCore flinkCore;
	
	private static ArrayList<WebSocketChannel> channels = new ArrayList<>();
    private static String webSocketListenPath = "/graphData";
    private static int webSocketListenPort = 8887;
    private static String webSocketHost = "localhost";
    
    private static Integer maxVertices = 10;
    
    private static String graphOperationLogic = "serverSide";
    
    private static Float viewportPixelX = (float) 1000;
    private static Float viewportPixelY = (float) 1000;
    private static float zoomLevel = 1;
    
//    private static transient GraphVis graphVis;
    
    
    private static Map<String,Map<String,Object>> globalVertices;
	private static Map<String,VertexCustom> innerVertices;
	private static Map<String,VertexCustom> newVertices;
	private static Map<String,VVEdgeWrapper> edges;
	private static String operation;
	private static Integer capacity;
	private static Float topModel;
	private static Float rightModel;
	private static Float bottomModel;
	private static Float leftModel;
	private static VertexCustom secondMinDegreeVertex;
	private static VertexCustom minDegreeVertex;    
//    private static FlinkApi api = new FlinkApi();
    
//    private static JobID jobId;
	
    public static void main(final String[] args) {
    	
//    	BasicConfigurator.configure();
    	PrintStream fileOut = null;
		try {
			fileOut = new PrintStream("/home/aljoscha/out.txt");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.setOut(fileOut);
    	
        Undertow server = Undertow.builder().addHttpListener(webSocketListenPort, webSocketHost)
                .setHandler(path().addPrefixPath(webSocketListenPath, websocket((exchange, channel) -> {
                    channels.add(channel);
                    channel.getReceiveSetter().set(getListener());
                    channel.resumeReceives();
                })).addPrefixPath("/", resource(new ClassPathResourceManager(UndertowServer.class.getClassLoader(),
                        UndertowServer.class.getPackage())).addWelcomeFiles("index.html")/*.setDirectoryListingEnabled(true)*/))
                .build();
        server.start();
        System.out.println("Server started!");
//        api.getApiClient().setBasePath("http://localhost:8081");  
//        flinkCore = new FlinkCore();
    }
    
//    private static MessageHeaders getMessageHeaders() {
//    	new MessageHeaders();
//		return null;
//    	
//    }
    
    /**
     * helper function to Undertow server
     */
    private static AbstractReceiveListener getListener() {
        return new AbstractReceiveListener() {
            @Override
            protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) {
                final String messageData = message.getData();
                for (WebSocketChannel session : channel.getPeerConnections()) {
                    System.out.println(messageData);
                    WebSockets.sendText(messageData, session, null);
                }
                if (messageData.equals("serverSideLogic")) {
                	graphOperationLogic = "serverSide";
                } else if (messageData.equals("clientSideLogic")) {
                	graphOperationLogic = "clientSide";
                } else if (messageData.startsWith("TestThread")){
                	TestThread thread = new TestThread("prototype");
            		thread.start();
                } else if (messageData.startsWith("edgeIdString")) {
                	String[] arrMessageData = messageData.split(";");
                	List<String> list = new ArrayList<String>(Arrays.asList(arrMessageData));
                	list.remove(0);
                	Set<String> visualizedWrappers = new HashSet<String>(list);
                	flinkCore.getGraphUtil().setVisualizedWrappers(visualizedWrappers);
                } else if (messageData.startsWith("vertexIdString")) {
                	String[] arrMessageData = messageData.split(";");
                	List<String> list = new ArrayList<String>(Arrays.asList(arrMessageData));
                	list.remove(0);
                	Set<String> visualizedVertices = new HashSet<String>(list);
                	flinkCore.getGraphUtil().setVisualizedVertices(visualizedVertices);
                } else if (messageData.startsWith("buildTopView")) {
                	flinkCore = new FlinkCore(graphOperationLogic);
                	String[] arrMessageData = messageData.split(";");
                	if (arrMessageData[1].equals("retract")) {
	        			flinkCore.initializeGradoopGraphUtil();
        				DataStream<Tuple2<Boolean, Row>> wrapperStream = flinkCore.buildTopViewRetract(maxVertices);
        				wrapperStream.print().setParallelism(1);
	        			if (graphOperationLogic.equals("serverSide")) {
		    				initializeGraphRepresentation();
	        				DataStream<Tuple2<Boolean,VVEdgeWrapper>> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperRetract()).setParallelism(1);
//	        				wrapperStreamWrapper.addSink(new FlinkRowPrintSinkRetract());
	        				wrapperStreamWrapper.addSink(new WrapperObjectSinkRetract()).setParallelism(1);
	        			} else {
	        				wrapperStream.addSink(new WrapperRetractSink());
	        			}
                	} else if (arrMessageData[1].equals("appendJoin")) {
        				flinkCore.initializeCSVGraphUtilJoin();
        				DataStream<Row> wrapperStream = flinkCore.buildTopViewAppendJoin(maxVertices);
        				if (graphOperationLogic.equals("serverSide")) {
        					initializeGraphRepresentation();
            				DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
		    				wrapperStreamWrapper.addSink(new WrapperObjectSinkAppend()).setParallelism(1);
        				} else {
            				wrapperStream.addSink(new WrapperAppendSink());
        				}
        			} else if (arrMessageData[1].contentEquals("adjacency")) {
        				flinkCore.initializeAdjacencyGraphUtil();
        				DataStream<Row> wrapperStream = flinkCore.buildTopViewAdjacency(maxVertices);
        				if (graphOperationLogic.equals("serverSide")) {
        					initializeGraphRepresentation();
        					DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
    	    				wrapperStreamWrapper.addSink(new WrapperObjectSinkAppend()).setParallelism(1);
    	    				wrapperStream.addSink(new FlinkRowStreamPrintSink()).setParallelism(1);
        				} else {
            				wrapperStream.addSink(new WrapperAppendSink());
        				}
        			}
                	try {
        				flinkCore.getFsEnv().execute();
        			} catch (Exception e) {
        				e.printStackTrace();
        			}
                	if (graphOperationLogic.equals("serverSide")) {
                		clearOperation();
                	}
                } else if (messageData.startsWith("zoom")) {
        			String[] arrMessageData = messageData.split(";");
        			Float xRenderPos = Float.parseFloat(arrMessageData[1]);
        			Float yRenderPos = Float.parseFloat(arrMessageData[2]);
        			zoomLevel = Float.parseFloat(arrMessageData[3]);
        			Float topModelPos = (- yRenderPos / zoomLevel);
        			Float leftModelPos = (- xRenderPos /zoomLevel);
        			Float bottomModelPos = (topModelPos + viewportPixelY / zoomLevel);
        			Float rightModelPos = (leftModelPos + viewportPixelX / zoomLevel);
					flinkCore.setTopModelPos(topModelPos);
					flinkCore.setRightModelPos(rightModelPos);
					flinkCore.setBottomModelPos(bottomModelPos);
					flinkCore.setLeftModelPos(leftModelPos);
        			DataStream<Row> wrapperStream = flinkCore.zoom(topModelPos, rightModelPos, bottomModelPos, leftModelPos);
                	if (graphOperationLogic.equals("clientSide")) {
                		wrapperStream.addSink(new WrapperAppendSink());
                	} else {
	        			if (messageData.startsWith("zoomIn")) {
		    				UndertowServer.setOperation("zoomIn");
	        			} else {
	        				UndertowServer.setOperation("zoomOut");
	        			}
	    				DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
	    				UndertowServer.prepareOperation(topModelPos, rightModelPos, bottomModelPos, leftModelPos);
	    				wrapperStreamWrapper.addSink(new WrapperObjectSinkAppend()).setParallelism(1);
                	}
					try {
						flinkCore.getFsEnv().execute();
					} catch (Exception e) {
						e.printStackTrace();
					}
					if (graphOperationLogic.equals("serverSide")) {
                		clearOperation();
                	}
    			} else if (messageData.startsWith("pan")) {
        			String[] arrMessageData = messageData.split(";");
        			Float topModelOld = flinkCore.gettopModelPos();
        			Float bottomModelOld = flinkCore.getBottomModelPos();
        			Float leftModelOld = flinkCore.getLeftModelPos();
        			Float rightModelOld = flinkCore.getRightModelPos();
        			Float xModelDiff = Float.parseFloat(arrMessageData[1]); 
        			Float yModelDiff = Float.parseFloat(arrMessageData[2]);
					DataStream<Row> wrapperStream = flinkCore.pan(topModelOld, rightModelOld, bottomModelOld, leftModelOld, xModelDiff, yModelDiff);
					Float topModelNew = topModelOld + yModelDiff;
					Float bottomModelNew = bottomModelOld + yModelDiff;
					Float leftModelNew = leftModelOld + xModelDiff;
					Float rightModelNew = rightModelOld + xModelDiff;
					flinkCore.setTopModelPos(topModelNew);
					flinkCore.setBottomModelPos(bottomModelNew);
					flinkCore.setLeftModelPos(leftModelNew);
					flinkCore.setRightModelPos(rightModelNew);
                	if (graphOperationLogic.equals("clientSide")) {
                		wrapperStream.addSink(new WrapperAppendSink());
                	} else {
                		DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
    					UndertowServer.setOperation("pan");
	    				UndertowServer.prepareOperation(topModelNew, rightModelNew, bottomModelNew, leftModelNew);
	    				wrapperStreamWrapper.addSink(new WrapperObjectSinkAppend()).setParallelism(1);
                	}
					try {
						flinkCore.getFsEnv().execute();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (graphOperationLogic.equals("serverSide")) {
                		clearOperation();
                	}
				} else if (messageData.equals("displayAll")) {
    		        flinkCore = new FlinkCore(graphOperationLogic);
        			flinkCore.initializeCSVGraphUtilJoin();
        			DataStream<Row> wrapperStream = flinkCore.displayAll();
					wrapperStream.addSink(new WrapperAppendSink());
					try {
						flinkCore.getFsEnv().execute();
					} catch (Exception e) {
						e.printStackTrace();
					}
					UndertowServer.sendToAll("fitGraph");
//					UndertowServer.sendToAll("layout");
    			} else if (messageData.startsWith("cancel")){
    				String[] arr = messageData.split(";");
    				String jobID = arr[1];
    				System.out.println("Cancelling " + jobID);
//    				try {
//						api.terminateJob(jobID, "cancel");
//					} catch (ApiException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
    				System.out.println("Terminated job");
    			}
            }
        };
    }

    /**
     * sends a message to the all connected web socket clients
     */
    public static void sendToAll(String message) {
        for (WebSocketChannel session : channels) {
            WebSockets.sendText(message, session, null);
        }
    }
    
    public static void initializeGraphRepresentation() {
		operation = "initial";
		globalVertices = new HashMap<String,Map<String,Object>>();
		innerVertices = new HashMap<String,VertexCustom>();
		newVertices = new HashMap<String,VertexCustom>();
		edges = new HashMap<String,VVEdgeWrapper>();
	}
	
	public static void setOperation(String operation) {
		UndertowServer.operation = operation;
	}
	
	public static void prepareOperation(Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		UndertowServer.topModel = topModel;
		UndertowServer.rightModel = rightModel;
		UndertowServer.bottomModel = bottomModel;
		UndertowServer.leftModel = leftModel;
		if (operation != "zoomOut"){
			for (Map.Entry<String, VVEdgeWrapper> entry : edges.entrySet()) {
				VVEdgeWrapper wrapper = entry.getValue();
				Integer sourceX = wrapper.getSourceX();
				Integer sourceY = wrapper.getSourceY();
				Integer targetX = wrapper.getTargetX();
				Integer targetY = wrapper.getTargetY();
				if (((sourceX < leftModel) || (rightModel < sourceX) || (sourceY < topModel) || (bottomModel < sourceY)) &&
						((targetX  < leftModel) || (rightModel < targetX ) || (targetY  < topModel) || (bottomModel < targetY))){
					UndertowServer.sendToAll("removeObjectServer;" + wrapper.getEdgeIdGradoop());
					System.out.println("Removing Object in prepareOperation, ID: " + wrapper.getEdgeIdGradoop());
				}
			}			
			System.out.println("innerVertices size before removing in prepareOperation: " + innerVertices.size());
			Iterator<Map.Entry<String,VertexCustom>> iter = innerVertices.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String,VertexCustom> entry = iter.next();
				VertexCustom vertex = entry.getValue();
				if ((vertex.getX() < leftModel) || (rightModel < vertex.getX()) || (vertex.getY() < topModel) || (bottomModel < vertex.getY())) {
					iter.remove();
					System.out.println("Removing Object in prepareOperation in innerVertices only, ID: " + vertex.getIdNumeric());
				}
			}
			System.out.println("innerVertices size after removing in prepareOPeration: " + innerVertices.size());
			capacity = maxVertices - innerVertices.size();
		} else {
			capacity = 0;
		}
		if (operation.equals("pan") || operation.equals("zoomOut")) {
			newVertices = innerVertices;
			innerVertices = new HashMap<String,VertexCustom>();
			System.out.println(newVertices.size());
		} else {
			newVertices = new HashMap<String,VertexCustom>();
		}
		System.out.println("Capacity after prepareOperation: " + capacity);
	}
	
	public static void addWrapper(VVEdgeWrapper wrapper) {
		if (operation.equals("initial")) {
			if (wrapper.getEdgeLabel().equals("identityEdge")) {
				addWrapperIdentityInitial(wrapper.getSourceVertex());
			} else {
				addNonIdentityWrapperInitial(wrapper);
			}
		} else {
			System.out.println("SourceIdNumeric: " + wrapper.getSourceIdNumeric());
			System.out.println("TargetIdNumeric: " + wrapper.getTargetIdNumeric());
			System.out.println("WrapperLabel: " + wrapper.getEdgeLabel());
			System.out.println("Size of innerVertices: " + innerVertices.size());
			System.out.println("Size of newVertices: " + newVertices.size());
			System.out.println("ID minDegreeVertex: " + minDegreeVertex.getIdNumeric());
			System.out.println("ID secondMinDegreeVertex: " + secondMinDegreeVertex.getIdNumeric());
			System.out.println("Capacity: " + capacity);
			if (wrapper.getEdgeLabel().equals("identityEdge")) {
				addWrapperIdentity(wrapper.getSourceVertex());
			} else {
				addNonIdentityWrapper(wrapper);
			}
		}
	}
	
	public static void removeWrapper(VVEdgeWrapper wrapper) {
		if (wrapper.getEdgeIdGradoop() != "identityEdge") {
			String targetId = wrapper.getTargetIdGradoop();
			int targetIncidence = (int) globalVertices.get(targetId).get("incidence");
			if (targetIncidence == 1) {
				globalVertices.remove(targetId);
				if (innerVertices.containsKey(targetId)) innerVertices.remove(targetId);
				if (newVertices.containsKey(targetId)) newVertices.remove(targetId);
				System.out.println("removing object in removeWrapper, ID: " + wrapper.getTargetIdNumeric());
				UndertowServer.sendToAll("removeObjectServer;" + wrapper.getTargetIdNumeric());
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
			UndertowServer.sendToAll("removeObjectServer;" + wrapper.getSourceIdNumeric());
			System.out.println("removing object in removeWrapper, ID: " + wrapper.getSourceIdNumeric());
		} else {
			globalVertices.get(sourceId).put("incidence", sourceIncidence - 1);
		}
	}
	
	private static void registerInside(VertexCustom vertex) {
		newVertices.put(vertex.getIdGradoop(), vertex);
		if (newVertices.size() > 1) {
			updateMinDegreeVertices(newVertices);
		} else {
			minDegreeVertex = vertex;
		}
	}
	
	
	private static void addNonIdentityWrapper(VVEdgeWrapper wrapper) {
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
			System.out.println("In addNonIdentityWrapper, capacity == 1, sourceID: " + sourceVertex.getIdNumeric() + ", sourceIn: " + sourceIn + 
					", targetID: " + targetVertex.getIdNumeric() + ", targetIn: " + targetIn);
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
			} else if (sourceIn && !(targetIn) && (sourceVertex.getDegree() > minDegreeVertex.getDegree() 
					|| newVertices.containsKey(sourceVertex.getIdGradoop()) || innerVertices.containsKey(sourceVertex.getIdGradoop()))) {
				addVertex(sourceVertex);
				addVertex(targetVertex);
				addEdge(wrapper);
				if (!sourceIsRegisteredInside) {
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					registerInside(sourceVertex);
				}
			} else if (targetIn && !(sourceIn) && (targetVertex.getDegree() > minDegreeVertex.getDegree()
					|| newVertices.containsKey(targetVertex.getIdGradoop()) || innerVertices.containsKey(targetVertex.getIdGradoop()))) {
				addVertex(sourceVertex);
				addVertex(targetVertex);
				addEdge(wrapper);
				if (!targetIsRegisteredInside) {
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					registerInside(targetVertex);
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
		System.out.println("in updateMinDegreeVertices");
		Collection<VertexCustom> collection = map.values();
		Iterator<VertexCustom> iter = collection.iterator();
		minDegreeVertex = iter.next();
		secondMinDegreeVertex = iter.next();
		System.out.println("Initial min degree vertices: " + minDegreeVertex.getIdNumeric() + " " + secondMinDegreeVertex.getIdNumeric());
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
		System.out.println("Final min degree vertices: " + minDegreeVertex.getIdNumeric() + " " + secondMinDegreeVertex.getIdNumeric());
	}

	private static void removeVertex(VertexCustom vertex) {	
		if (!globalVertices.containsKey(vertex.getIdGradoop())) {
			System.out.println("cannot remove vertex because not in vertexGlobalMap, id: " + vertex.getIdGradoop());
		} else {
			newVertices.remove(vertex.getIdGradoop());
			globalVertices.remove(vertex.getIdGradoop());
			UndertowServer.sendToAll("removeObjectServer;" + vertex.getIdNumeric());
			Map<String,String> vertexNeighborMap = flinkCore.getGraphUtil().getAdjMatrix().get(vertex.getIdGradoop());
			Iterator<Map.Entry<String, VVEdgeWrapper>> iter = edges.entrySet().iterator();
			while (iter.hasNext()) if (vertexNeighborMap.values().contains(iter.next().getKey())) iter.remove();
			System.out.println("Removing Obect in removeVertex, ID: " + vertex.getIdNumeric());
		}
	}

	private static void reduceNeighborIncidence(VertexCustom vertex) {
		Set<String> neighborIds = getNeighborhood(vertex);
		for (String neighbor : neighborIds) {
			if (globalVertices.containsKey(neighbor)) {
				Map<String,Object> map = globalVertices.get(neighbor);
				map.put("incidence", (int) map.get("incidence") - 1); 
			}
		}
	}

	private static void addWrapperIdentity(VertexCustom vertex) {
		System.out.println("In addWrapperIdentity");
		if (vertex.getIdNumeric() == 0) System.out.println("zero is in identity!");
		String vertexId = vertex.getIdGradoop();
		boolean vertexIsRegisteredInside = newVertices.containsKey(vertexId) || innerVertices.containsKey(vertexId);
		if (capacity > 0) {
			addVertex(vertex);
			if (!vertexIsRegisteredInside) {
				if (vertex.getIdNumeric() == 0) System.out.println("zero is in identity and added!");
				newVertices.put(vertex.getIdGradoop(), vertex);
				updateMinDegreeVertex(vertex);
				capacity -= 1;
			}
		} else {
			System.out.println("In addWrapperIdentity, declined capacity > 0");
			if (vertex.getDegree() > minDegreeVertex.getDegree()) {
				if (vertex.getIdNumeric() == 0) System.out.println("comparison works");
				addVertex(vertex);
				if (!vertexIsRegisteredInside) {
					if (vertex.getIdNumeric() == 0) System.out.println("zero is in identity and added!");
					reduceNeighborIncidence(minDegreeVertex);
					removeVertex(minDegreeVertex);
					registerInside(vertex);
				}
			} else {
				System.out.println("In addWrapperIdentity, declined vertexDegree > minDegreeVertexDegree");
			}
		}
	}

	public static void addWrapperIdentityInitial(VertexCustom vertex) {
		boolean added = addVertex(vertex);
		if (added) innerVertices.put(vertex.getIdGradoop(), vertex);
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
		System.out.println("In addVertex");
		String sourceId = vertex.getIdGradoop();
		if (!(globalVertices.containsKey(sourceId))) {
			Map<String,Object> map = new HashMap<String,Object>();
			map.put("incidence", (int) 1);
			map.put("vertex", vertex);
			globalVertices.put(sourceId, map);
				UndertowServer.sendToAll("addVertexServer;" + vertex.getIdNumeric() + ";" + vertex.getX() + ";" + vertex.getY());
			return true;
		} else {
			System.out.println("In addVertex, declined because ID contained in globalVertices");
			Map<String,Object> map = globalVertices.get(sourceId);
			map.put("incidence", (int) map.get("incidence") + 1);
			return false;
		}		
	}
	
	public static void addEdge(VVEdgeWrapper wrapper) {
		edges.put(wrapper.getEdgeIdGradoop(), wrapper);
		UndertowServer.sendToAll("addEdgeServer;" + wrapper.getEdgeIdGradoop() + ";" + wrapper.getSourceIdNumeric() + ";" + wrapper.getTargetIdNumeric());
	}
	
	public static void clearOperation(){
		System.out.println("in clear operation");
		if (operation != "initial"){
			for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) System.out.println("innerVertex " + entry.getValue().getIdNumeric());
			innerVertices.putAll(newVertices); 
			for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) System.out.println("innerVertex " + entry.getValue().getIdNumeric());
			System.out.println("global...");
			Iterator<Map.Entry<String, Map<String,Object>>> iter = globalVertices.entrySet().iterator();
			while (iter.hasNext()) {
				VertexCustom vertex = (VertexCustom) iter.next().getValue().get("vertex");
				if ((((vertex.getX() < leftModel) || (rightModel < vertex.getX()) || (vertex.getY() < topModel) || 
						(bottomModel < vertex.getY())) && !hasVisualizedNeighborsInside(vertex)) ||
							((vertex.getX() >= leftModel) && (rightModel >= vertex.getX()) && (vertex.getY() >= topModel) && (bottomModel >= vertex.getY()) 
									&& !innerVertices.containsKey(vertex.getIdGradoop()))) {
					System.out.println("removing in clear operation " + vertex.getIdNumeric());
					UndertowServer.sendToAll("removeObjectServer;" + vertex.getIdNumeric());
					iter.remove();
					Map<String,String> vertexNeighborMap = flinkCore.getGraphUtil().getAdjMatrix().get(vertex.getIdGradoop());
					Iterator<Map.Entry<String, VVEdgeWrapper>> edgesIterator = edges.entrySet().iterator();
					while (edgesIterator.hasNext()) if (vertexNeighborMap.values().contains(edgesIterator.next().getKey())) edgesIterator.remove();
				} 
			}
		} else {
			newVertices = innerVertices;
			if (newVertices.size() > 1) {
				updateMinDegreeVertices(newVertices);
			} else if (newVertices.size() == 1) {
				minDegreeVertex = newVertices.values().iterator().next();
			}
		}
		operation = null;
		System.out.println("before updatemindegreevertices in clear operation");
//		if (newVertices.size() > 1) {
//			updateMinDegreeVertices(newVertices);
//		} else if (newVertices.size() == 1) {
//			minDegreeVertex = newVertices.values().iterator().next();
//		}
		Set<String> visualizedVertices = new HashSet<String>();
		for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) visualizedVertices.add(entry.getKey());
		Set<String> visualizedWrappers = new HashSet<String>();
		for (Map.Entry<String, VVEdgeWrapper> entry : edges.entrySet()) visualizedWrappers.add(entry.getKey());
		GraphUtil graphUtil =  flinkCore.getGraphUtil();
		graphUtil.setVisualizedVertices(visualizedVertices);
		graphUtil.setVisualizedWrappers(visualizedWrappers);
		System.out.println("global size "+ globalVertices.size());
	}
	
	public static Set<String> getNeighborhood(VertexCustom vertex){
		Set<String> neighborIds = new HashSet<String>();
		Map<String,Map<String,String>> adjMatrix = flinkCore.getGraphUtil().getAdjMatrix();
		for (Map.Entry<String, String> entry : adjMatrix.get(vertex.getIdGradoop()).entrySet()) neighborIds.add(entry.getKey());
		return neighborIds;
	}
	
	public static boolean hasVisualizedNeighborsInside(VertexCustom vertex) {
		Map<String,Map<String,String>> adjMatrix = flinkCore.getGraphUtil().getAdjMatrix();
		for (Map.Entry<String, String> entry : adjMatrix.get(vertex.getIdGradoop()).entrySet()) if (innerVertices.containsKey(entry.getKey())) return true;
		return false;
	}
}
