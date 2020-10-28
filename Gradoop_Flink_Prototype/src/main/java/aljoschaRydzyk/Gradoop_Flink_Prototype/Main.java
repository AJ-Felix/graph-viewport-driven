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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import com.nextbreakpoint.flinkclient.api.ApiException;
import com.nextbreakpoint.flinkclient.api.FlinkApi;
import com.nextbreakpoint.flinkclient.model.JobIdWithStatus;
import com.nextbreakpoint.flinkclient.model.JobIdsWithStatusOverview;

public class Main {
	
	private static FlinkCore flinkCore;
	
	private static ArrayList<WebSocketChannel> channels = new ArrayList<>();
    private static String webSocketListenPath = "/graphData";
    private static int webSocketListenPort = 8887;
    private static String webSocketHost = "localhost";
    
    private static Integer maxVertices = 100;
    
    private static String graphOperationLogic = "serverSide";
    private static boolean layout = true;
    
    private static int operationStep;
    
    private static Float viewportPixelX = (float) 1000;
    private static Float viewportPixelY = (float) 1000;
    private static float zoomLevel = 1;    
    
    private static Map<String,Map<String,Object>> globalVertices;
	private static Map<String,VertexCustom> innerVertices;
	private static Map<String,VertexCustom> newVertices;
	private static Map<String,VVEdgeWrapper> edges;
	private static Map<String,VertexCustom> layoutedVertices;
	private static String operation;
	private static Integer capacity;
	private static Float topModel;
	private static Float rightModel;
	private static Float bottomModel;
	private static Float leftModel;
	private static Float xModelDiff;
	private static Float yModelDiff;
	private static VertexCustom secondMinDegreeVertex;
	private static VertexCustom minDegreeVertex;    
	
	private static boolean identityWrapperAdded;
	private static boolean nonIdentityWrapperAdded;
	
	private static Row latestRow;
    private static FlinkApi api = new FlinkApi();
    
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
                })).addPrefixPath("/", resource(new ClassPathResourceManager(Main.class.getClassLoader(),
                        Main.class.getPackage())).addWelcomeFiles("index.html")/*.setDirectoryListingEnabled(true)*/))
                .build();
        server.start();
        System.out.println("Server started!");
        api.getApiClient().setBasePath("http://localhost:8081");  
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
                } else if (messageData.equals("preLayout")) {
                	layoutedVertices = new HashMap<String,VertexCustom>();
                	layout = false;
                } else if (messageData.equals("postLayout")) {
                	layout = true;
                } else if (messageData.startsWith("layoutBaseString")) {
                	String[] arrMessageData = messageData.split(";");
                	List<String> list = new ArrayList<String>(Arrays.asList(arrMessageData));
                	list.remove(0);
//                	String operationAndStep = list.remove(0);
//                	System.out.println("operationAndStep" + operationAndStep);
                	for (VertexCustom vertex : innerVertices.values()) System.out.println(vertex.getIdGradoop());
                	for (String vertexData : list) {
                		String[] arrVertexData = vertexData.split(",");
                		String vertexId = arrVertexData[0];
                		Integer x = Integer.parseInt(arrVertexData[1]);
                		Integer y = Integer.parseInt(arrVertexData[2]);
            			VertexCustom vertex = new VertexCustom(vertexId, x, y);
            			layoutedVertices.put(vertexId, vertex);
//            			VertexCustom innerVertex = innerVertices.get(vertexId);
//            			innerVertex.setX(x);
//            			innerVertex.setY(y);
                	}
                	System.out.println("layoutedVertices size: ");
                	System.out.println(layoutedVertices.size());
                	if (operation.equals("initial")) {
                		if (graphOperationLogic.equals("serverSide")) {
                    		clearOperation();
                    	}
                	} else if (operation.startsWith("zoom")) {
                		if (operation.contains("In")) {
                			if (operationStep == 1) {
                				if (capacity > 0) {
                					zoomInLayoutFirstStep();
                				} else {
                					zoomInLayoutFourthStep();
                				}
                			} else if (operationStep == 2) {
                				if (capacity > 0) {
                					zoomInLayoutSecondStep();
                				} else {
                					zoomInLayoutFourthStep();
                				}
                			} else if (operationStep == 3) zoomInLayoutFourthStep();
                		}
                	} else if (operation.startsWith("pan")) {
                		System.out.println("operation step " + operationStep);
                		if (operationStep == 1) {
                			if (capacity > 0) {
                				panLayoutFirstStep();
                			} else {
                				panLayoutFifthStep();
                			}
                		} else if (operationStep == 2) {
                			if (capacity > 0) {
                				panLayoutThirdStep();
                			} else {
                				panLayoutFifthStep();
                			}
                		} else if (operationStep == 3) {
                			if (capacity > 0) {
                				panLayoutThirdStep();
                			} else {
                				panLayoutFifthStep();
                			}
                		} else if (operationStep == 4) panLayoutFifthStep();
                	}
                } else if (messageData.startsWith("maxVertices")) {
                	String[] arrMessageData = messageData.split(";");
                	maxVertices = Integer.parseInt(arrMessageData[1]);
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
                	flinkCore.setTopModelPos((float) 0);
                	flinkCore.setLeftModelPos((float) 0);
                	flinkCore.setRightModelPos((float) 4000);
                	flinkCore.setBottomModelPos((float) 4000);
                	String[] arrMessageData = messageData.split(";");
                	if (arrMessageData[1].equals("retract")) {
	        			flinkCore.initializeGradoopGraphUtil();
        				DataStream<Tuple2<Boolean, Row>> wrapperStream = flinkCore.buildTopViewRetract(maxVertices);
        				wrapperStream.print().setParallelism(1);
	        			if (graphOperationLogic.equals("serverSide")) {
		    				initializeGraphRepresentation();
	        				DataStream<Tuple2<Boolean,VVEdgeWrapper>> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperRetract()).setParallelism(1);
	        				wrapperStreamWrapper.addSink(new WrapperObjectSinkRetractInitial()).setParallelism(1);
	        			} else {
	        				wrapperStream.addSink(new WrapperRetractSink());
	        			}
                	} else if (arrMessageData[1].equals("appendJoin")) {
        				flinkCore.initializeCSVGraphUtilJoin();
        				DataStream<Row> wrapperStream = flinkCore.buildTopViewAppendJoin(maxVertices);
        				if (graphOperationLogic.equals("serverSide")) {
        					initializeGraphRepresentation();
        					DataStream<VVEdgeWrapper> wrapperStreamWrapper;
        					if (layout) {
                				wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
        					} else {
        						wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
        					}
		    				wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendInitial()).setParallelism(1);
        				} else {
            				wrapperStream.addSink(new WrapperAppendSink());
        				}
        			} else if (arrMessageData[1].contentEquals("adjacency")) {
        				flinkCore.initializeAdjacencyGraphUtil();
        				DataStream<Row> wrapperStream = flinkCore.buildTopViewAdjacency(maxVertices);
        				if (graphOperationLogic.equals("serverSide")) {
        					initializeGraphRepresentation();
        					DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
    	    				wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendInitial()).setParallelism(1);
        				} else {
            				wrapperStream.addSink(new WrapperAppendSink());
        				}
        			}
                	try {
        				flinkCore.getFsEnv().execute();
        			} catch (Exception e) {
        				e.printStackTrace();
        			}
                	if (graphOperationLogic.equals("serverSide") && (layout)) {
                		clearOperation();
                	}
                } else if (messageData.startsWith("zoom")) {
        			String[] arrMessageData = messageData.split(";");
        			Float xRenderPos = Float.parseFloat(arrMessageData[1]);
        			Float yRenderPos = Float.parseFloat(arrMessageData[2]);
        			zoomLevel = Float.parseFloat(arrMessageData[3]);
        			Float topModelNew = (- yRenderPos / zoomLevel);
        			Float leftModelNew = (- xRenderPos /zoomLevel);
        			Float bottomModelNew = (topModelNew + viewportPixelY / zoomLevel);
        			Float rightModelNew = (leftModelNew + viewportPixelX / zoomLevel);
        			Float topModelOld = flinkCore.gettopModelPos();
        			Float leftModelOld = flinkCore.getLeftModelPos();
        			Float bottomModelOld = flinkCore.getBottomModelPos();
        			Float rightModelOld = flinkCore.getRightModelPos();
					flinkCore.setTopModelPos(topModelNew);
					flinkCore.setRightModelPos(rightModelNew);
					flinkCore.setBottomModelPos(bottomModelNew);
					flinkCore.setLeftModelPos(leftModelNew);
					System.out.println("Zoom ... top, right, bottom, left:" + topModelNew + " " + rightModelNew + " "+ bottomModelNew + " " + leftModelNew);
					if (!layout) {
						if (messageData.startsWith("zoomIn")) {
//							System.out.println(layoutedVertices.size());
		    				Main.setOperation("zoomIn");
							System.out.println("in zoom in layout function");
							for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) {
								System.out.println(entry.getValue().getIdGradoop() + " " + entry.getValue().getX() + " " + entry.getValue().getY());
							}
							Main.prepareOperation(topModelNew, rightModelNew, bottomModelNew, leftModelNew);
//							Map<String,VertexCustom> innerVerticesCopy = new HashMap<String,VertexCustom>();
//							innerVerticesCopy.put("5c6ab3fd8e3627bbfb10de29", Custom("5c6ab3fd8e3627bbfb10de29", "forum", 0, 2873, 2358, (long) 121));
//							Set<String> layoutedVerticesCopy = new HashSet<String>();
//							layoutedVerticesCopy.add("5c6ab3fd8e3627bbfb10de29");
							zoomInLayoutFirstStep();
						} else {
	        				Main.setOperation("zoomOut");
							System.out.println("in zoom out layout function");
							Main.prepareOperation(topModelNew, rightModelNew, bottomModelNew, leftModelNew);
//							zoomOutLayoutFirstStep(topModelOld, rightModelOld, bottomModelOld, leftModelOld);
	        			}
					} else {
	        			DataStream<Row> wrapperStream = flinkCore.zoom(topModelNew, rightModelNew, bottomModelNew, leftModelNew);
	                	if (graphOperationLogic.equals("clientSide")) {
	                		wrapperStream.addSink(new WrapperAppendSink());
	                	} else {
		        			if (messageData.startsWith("zoomIn")) {
			    				Main.setOperation("zoomIn");
		        			} else {
		        				Main.setOperation("zoomOut");
		        			}
		    				DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
		    				Main.prepareOperation(topModelNew, rightModelNew, bottomModelNew, leftModelNew);
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
					}
    			} else if (messageData.startsWith("pan")) {
        			String[] arrMessageData = messageData.split(";");
        			Float topModelOld = flinkCore.gettopModelPos();
        			Float bottomModelOld = flinkCore.getBottomModelPos();
        			Float leftModelOld = flinkCore.getLeftModelPos();
        			Float rightModelOld = flinkCore.getRightModelPos();
        			xModelDiff = Float.parseFloat(arrMessageData[1]); 
        			yModelDiff = Float.parseFloat(arrMessageData[2]);
					Float topModelNew = topModelOld + yModelDiff;
					Float bottomModelNew = bottomModelOld + yModelDiff;
					Float leftModelNew = leftModelOld + xModelDiff;
					Float rightModelNew = rightModelOld + xModelDiff;
					flinkCore.setTopModelPos(topModelNew);
					flinkCore.setBottomModelPos(bottomModelNew);
					flinkCore.setLeftModelPos(leftModelNew);
					flinkCore.setRightModelPos(rightModelNew);
					if (!layout) {
	    				Main.setOperation("pan");
	    				Main.setOperationStep(1);
	    				Main.prepareOperation(topModelNew, rightModelNew, bottomModelNew, leftModelNew);
	    				panLayoutFirstStep();
					} else {
						DataStream<Row> wrapperStream = flinkCore.pan(xModelDiff, yModelDiff);
	                	if (graphOperationLogic.equals("clientSide")) {
	                		wrapperStream.addSink(new WrapperAppendSink());
	                	} else {
	                		DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
	    					Main.setOperation("pan");
		    				Main.prepareOperation(topModelNew, rightModelNew, bottomModelNew, leftModelNew);
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
					Main.sendToAll("fitGraph");
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
    
    private static void zoomInLayoutFirstStep() {
    	Main.setOperationStep(1);
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutFirstStep(layoutedVertices, innerVertices);
    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		wrapperStream.addSink(new CheckEmptySink());
		latestRow = null;
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (latestRow == null) zoomInLayoutSecondStep();
    }
    
    private static void zoomInLayoutSecondStep() {
    	Main.setOperationStep(2);
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutSecondStep(layoutedVertices, innerVertices);
//    	DataStream<Boolean> boolStream = wrapperStream.keyBy(value -> value.getField(1)).process(new TimeOut(5)).setParallelism(1);
    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		wrapperStream.addSink(new CheckEmptySink());
		latestRow = null;
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (latestRow == null) zoomInLayoutThirdStep();
    }
    
    private static void zoomInLayoutThirdStep() {
    	Main.setOperationStep(3);
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutThirdStep(layoutedVertices);
    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		identityWrapperAdded = true;
		nonIdentityWrapperAdded = true;
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		//ATTENTION: Moving to Fourth step has to be controlled by capacity while the third step is being executed. When capacity is reached, cancel third step 
		//and move to fourth step. Probably Job Api is necessary here...
    }
    
    private static void zoomInLayoutFourthStep() {
    	Main.setOperationStep(4);
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutFourthStep(layoutedVertices, innerVertices);
    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (graphOperationLogic.equals("serverSide")) {
    		clearOperation();
    	}
    }
    
    private static void panLayoutFirstStep() {
    	Main.setOperationStep(1);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutFirstStep(layoutedVertices, newVertices);
		DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		latestRow = null;
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (latestRow == null) panLayoutSecondStep();
    }
    
    private static void panLayoutSecondStep() {
    	Main.setOperationStep(2);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutSecondStep(layoutedVertices, newVertices);
    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		latestRow = null;
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (latestRow == null) panLayoutThirdStep();
    }
    
    private static void panLayoutThirdStep() {
    	Main.setOperationStep(3);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutThirdStep(layoutedVertices, newVertices);
    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		latestRow = null;
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (latestRow == null) panLayoutFourthStep();
    }
    
    private static void panLayoutFourthStep() {
    	Main.setOperationStep(4);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutFourthStep(layoutedVertices);
		DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		identityWrapperAdded = true;
		nonIdentityWrapperAdded = true;
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		//ATTENTION: Moving to Fifth step has to be controlled by capacity while the third step is being executed. When capacity is reached, cancel third step 
		//and move to fourth step. Probably Job Api is necessary here...
    }
    
    private static void panLayoutFifthStep() {
    	Main.setOperationStep(5);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutFifthStep(layoutedVertices, newVertices, xModelDiff, yModelDiff);
    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (graphOperationLogic.equals("serverSide")) {
    		clearOperation();
    	}
    }

    /**
     * sends a message to the all connected web socket clients
     */
    public static void sendToAll(String message) {
        for (WebSocketChannel session : channels) {
            WebSockets.sendText(message, session, null);
        }
    }
    
    private static void initializeGraphRepresentation() {
		operation = "initial";
		globalVertices = new HashMap<String,Map<String,Object>>();
		innerVertices = new HashMap<String,VertexCustom>();
		newVertices = new HashMap<String,VertexCustom>();
		edges = new HashMap<String,VVEdgeWrapper>();
	}
	
	private static void setOperation(String operation) {
		Main.operation = operation;
	}
	
	private static void setOperationStep(Integer step) {
		Main.operationStep = step;
	}
	
	private static void prepareOperation(Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		Main.topModel = topModel;
		Main.rightModel = rightModel;
		Main.bottomModel = bottomModel;
		Main.leftModel = leftModel;
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
		}
		capacity = maxVertices - innerVertices.size();
		if (operation.equals("pan") || operation.equals("zoomOut")) {
			newVertices = innerVertices;
			innerVertices = new HashMap<String,VertexCustom>();
			System.out.println(newVertices.size());
		} else {
			newVertices = new HashMap<String,VertexCustom>();
		}
		System.out.println("Capacity after prepareOperation: " + capacity);
	}
	
	public static void addWrapperInitial(VVEdgeWrapper wrapper) {
		if (wrapper.getEdgeLabel().equals("identityEdge")) {
			addWrapperIdentityInitial(wrapper.getSourceVertex());
		} else {
			addNonIdentityWrapperInitial(wrapper);
		}
	}
	
	public static void addWrapper(VVEdgeWrapper wrapper) {
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
	
	public static void addWrapperLayout(VVEdgeWrapper wrapper) {
		//if in zoomIn3 or pan4: cancel flinkjob and move to next step!
		if ((operationStep == 3 && operation == "zoomIn") || (operationStep == 4 && operation == "pan")) {
//			if (!identityWrapperAdded && ! nonIdentityWrapperAdded) {
			if (capacity == 0) {
				try {
					JobIdsWithStatusOverview jobs = api.getJobs();
					List<JobIdWithStatus> list = jobs.getJobs();
					String jobid = list.get(0).getId();
					api.terminateJob(jobid, "cancel");
				} catch (ApiException e) {
					// TODO Auto-generated catch block
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
	
	public static void removeWrapper(VVEdgeWrapper wrapper) {
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
				nonIdentityWrapperAdded = false;
			}
		}
	}
	
	private static void addNonIdentityWrapperLayout(VVEdgeWrapper wrapper) {
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
					nonIdentityWrapperAdded = false;
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
					nonIdentityWrapperAdded = false;
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
					nonIdentityWrapperAdded = false;
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

	private static void removeVertex(VertexCustom vertex) {	
		if (!globalVertices.containsKey(vertex.getIdGradoop())) {
			System.out.println("cannot remove vertex because not in vertexGlobalMap, id: " + vertex.getIdGradoop());
		} else {
			newVertices.remove(vertex.getIdGradoop());
			globalVertices.remove(vertex.getIdGradoop());
			Main.sendToAll("removeObjectServer;" + vertex.getIdGradoop());
			Map<String,String> vertexNeighborMap = flinkCore.getGraphUtil().getAdjMatrix().get(vertex.getIdGradoop());
			Iterator<Map.Entry<String, VVEdgeWrapper>> iter = edges.entrySet().iterator();
			while (iter.hasNext()) if (vertexNeighborMap.values().contains(iter.next().getKey())) iter.remove();
			System.out.println("Removing Obect in removeVertex, ID: " + vertex.getIdGradoop());
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
			System.out.println("In addWrapperIdentity, declined capacity > 0");
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
	
	private static void addWrapperIdentityLayout(VertexCustom vertex) {
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
				identityWrapperAdded = false;
				System.out.println("identityWrapper not Added!");
			}
		}
	}

	private static void addWrapperIdentityInitial(VertexCustom vertex) {
		boolean added = addVertex(vertex);
		if (added) innerVertices.put(vertex.getIdGradoop(), vertex);
	}
	
	private static void addNonIdentityWrapperInitial(VVEdgeWrapper wrapper) {
		VertexCustom sourceVertex = wrapper.getSourceVertex();
		VertexCustom targetVertex = wrapper.getTargetVertex();
		boolean addedSource = addVertex(sourceVertex);
		if (addedSource) innerVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
		boolean addedTarget = addVertex(targetVertex);
		if (addedTarget) innerVertices.put(targetVertex.getIdGradoop(), targetVertex);
		addEdge(wrapper);
	}
	
	private static boolean addVertex(VertexCustom vertex) {
		System.out.println("In addVertex");
		String sourceId = vertex.getIdGradoop();
		if (!(globalVertices.containsKey(sourceId))) {
			Map<String,Object> map = new HashMap<String,Object>();
			map.put("incidence", (int) 1);
			map.put("vertex", vertex);
			globalVertices.put(sourceId, map);
			if (layout) {
				Main.sendToAll("addVertexServer;" + vertex.getIdGradoop() + ";" + vertex.getX() + ";" + vertex.getY() + ";" + vertex.getIdNumeric());
			} else {
				if (layoutedVertices.containsKey(vertex.getIdGradoop())) {
					VertexCustom layoutedVertex = layoutedVertices.get(vertex.getIdGradoop());
					Main.sendToAll("addVertexServer;" + vertex.getIdGradoop() + ";" + layoutedVertex.getX() + ";" + layoutedVertex.getY() + ";" 
							+ vertex.getIdNumeric());
				} else {
					Main.sendToAll("addVertexServerToBeLayouted;" + vertex.getIdGradoop() + ";" + vertex.getDegree() + ";" + vertex.getIdNumeric());
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
	
	private static void addEdge(VVEdgeWrapper wrapper) {
		edges.put(wrapper.getEdgeIdGradoop(), wrapper);
		Main.sendToAll("addEdgeServer;" + wrapper.getEdgeIdGradoop() + ";" + wrapper.getSourceIdGradoop() + ";" + wrapper.getTargetIdGradoop());
	}
	
	private static void clearOperation(){
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
					Map<String,String> vertexNeighborMap = flinkCore.getGraphUtil().getAdjMatrix().get(vertex.getIdGradoop());
					Iterator<Map.Entry<String, VVEdgeWrapper>> edgesIterator = edges.entrySet().iterator();
					while (edgesIterator.hasNext()) if (vertexNeighborMap.values().contains(edgesIterator.next().getKey())) edgesIterator.remove();
				} 
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
		System.out.println("before updatemindegreevertices in clear operation");
		Set<String> visualizedVertices = new HashSet<String>();
		for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) visualizedVertices.add(entry.getKey());
		Set<String> visualizedWrappers = new HashSet<String>();
		for (Map.Entry<String, VVEdgeWrapper> entry : edges.entrySet()) visualizedWrappers.add(entry.getKey());
		GraphUtil graphUtil =  flinkCore.getGraphUtil();
		graphUtil.setVisualizedVertices(visualizedVertices);
		graphUtil.setVisualizedWrappers(visualizedWrappers);
		System.out.println("global size "+ globalVertices.size());
	}
	
	private static Set<String> getNeighborhood(VertexCustom vertex){
		Set<String> neighborIds = new HashSet<String>();
		Map<String,Map<String,String>> adjMatrix = flinkCore.getGraphUtil().getAdjMatrix();
		for (Map.Entry<String, String> entry : adjMatrix.get(vertex.getIdGradoop()).entrySet()) neighborIds.add(entry.getKey());
		return neighborIds;
	}
	
	private static boolean hasVisualizedNeighborsInside(VertexCustom vertex) {
		Map<String,Map<String,String>> adjMatrix = flinkCore.getGraphUtil().getAdjMatrix();
		for (Map.Entry<String, String> entry : adjMatrix.get(vertex.getIdGradoop()).entrySet()) if (innerVertices.containsKey(entry.getKey())) return true;
		return false;
	}
	
	public static void latestRow(Row element) {
		latestRow = element;
	}
}
