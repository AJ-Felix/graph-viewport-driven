package aljoschaRydzyk.Gradoop_Flink_Prototype;

import static io.undertow.Handlers.path;
import static io.undertow.Handlers.websocket;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.types.Row;

import com.nextbreakpoint.flinkclient.api.FlinkApi;

import io.undertow.Undertow;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

public class Server implements Serializable{
public static FlinkCore flinkCore;
	
	public ArrayList<WebSocketChannel> channels = new ArrayList<>();
    private String webSocketListenPath = "/graphData";
    private int webSocketListenPort = 8897;
    private String webSocketHost = "localhost";
    
    private Integer maxVertices = 100;
    
//    private String graphOperationLogic;
    private boolean layout = true;
    
    private int operationStep;
    
    private Float viewportPixelX = (float) 1000;
    private Float viewportPixelY = (float) 1000;
    
	private String operation;
	private Integer capacity;  
	
	public boolean sentToClientInSubStep;
//	private Row latestRow;
    private FlinkApi api = new FlinkApi();
    private WrapperHandler wrapperHandler;
    private FlinkResponseHandler flinkResponseHandler;
    
    private static Server server = null;
    
    public static Server getInstance() {
    	if (server == null) server = new Server();
		return server;
    }

    private Server () {
    	System.out.println("executing server constructor");
	}
    
    public void initializeServerFunctionality() {
    	Undertow server = Undertow.builder()
    			.addHttpListener(webSocketListenPort, webSocketHost)
                .setHandler(path()
                	.addPrefixPath(webSocketListenPath, websocket((exchange, channel) -> {
	                    channels.add(channel);
	                    channel.getReceiveSetter().set(getListener());
	                    channel.resumeReceives();
	                }))
//                	.addPrefixPath("/", resource(new ClassPathResourceManager(Main.class.getClassLoader(),
//                        Main.class.getPackage())).addWelcomeFiles("index.html")/*.setDirectoryListingEnabled(true)*/)
            	).build();
    	server.start();
        System.out.println("Server started!");
        api.getApiClient().setBasePath("http://localhost:8081");  
//        graphOperationLogic = "serverSide";
        
        
    }
    
    public void initializeFlinkAndGraphRep() {
//    	flinkCore = new FlinkCore(graphOperationLogic);
    	flinkCore = new FlinkCore();
  		wrapperHandler = WrapperHandler.getInstance();
  		wrapperHandler.initializeGraphRepresentation();
  		
  		//initialize FlinkResponseHandler
        flinkResponseHandler = new FlinkResponseHandler(wrapperHandler);
		flinkResponseHandler.listen();
    }
    
    private AbstractReceiveListener getListener() {
        return new AbstractReceiveListener() {
            @Override
            protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) {
                final String messageData = message.getData();
                for (WebSocketChannel session : channel.getPeerConnections()) {
                    System.out.println(messageData);
                    WebSockets.sendText(messageData, session, null);
                }
                if (messageData.equals("preLayout")) {
//                	layoutedVertices = new HashMap<String,VertexCustom>();
                	wrapperHandler.resetLayoutedVertices();
                	setLayoutMode(false);
                } else if (messageData.equals("postLayout")) {
                	setLayoutMode(true);
                } else if (messageData.startsWith("layoutBaseString")) {
                	String[] arrMessageData = messageData.split(";");
                	List<String> list = new ArrayList<String>(Arrays.asList(arrMessageData));
                	list.remove(0);
//                	for (VertexCustom vertex : innerVertices.values()) System.out.println(vertex.getIdGradoop());
                	wrapperHandler.updateLayoutedVertices(list);
                	nextSubStep();
                } else if (messageData.startsWith("maxVertices")) {
                	String[] arrMessageData = messageData.split(";");
                	maxVertices = Integer.parseInt(arrMessageData[1]);
                	wrapperHandler.setMaxVertices(maxVertices);
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
                  	Float topModel = (float) 0;
                	Float rightModel = (float) 4000;
                	Float bottomModel = (float) 4000;
                	Float leftModel = (float) 0;
                	setModelPositions(topModel, rightModel, bottomModel, leftModel);
                	setOperation("initial");
                	String[] arrMessageData = messageData.split(";");
                	if (arrMessageData[1].equals("retract")) {
                		buildTopViewRetract();        			
                	} else if (arrMessageData[1].equals("appendJoin")) {
                		buildTopViewAppendJoin();	
        			} else if (arrMessageData[1].contentEquals("adjacency")) {
        				buildTopViewAdjacency();
        			}
                	try {
        				flinkCore.getFsEnv().execute();
        			} catch (Exception e) {
        				e.printStackTrace();
        			}
                	if (layout) wrapperHandler.clearOperation();
                } else if (messageData.startsWith("zoom")) {
        			String[] arrMessageData = messageData.split(";");
        			Float xRenderPosition = Float.parseFloat(arrMessageData[1]);
        			Float yRenderPosition = Float.parseFloat(arrMessageData[2]);
        			Float zoomLevel = Float.parseFloat(arrMessageData[3]);
        			Float topModel = (- yRenderPosition / zoomLevel);
        			Float leftModel = (- xRenderPosition /zoomLevel);
        			Float bottomModel = (topModel + viewportPixelY / zoomLevel);
        			Float rightModel = (leftModel + viewportPixelX / zoomLevel);
        			Float topModelOld = flinkCore.getTopModel();
        			Float leftModelOld = flinkCore.getLeftModel();
        			Float bottomModelOld = flinkCore.getBottomModel();
        			Float rightModelOld = flinkCore.getRightModel();
        			setModelPositions(topModel, rightModel, bottomModel, leftModel);
					flinkCore.setModelPositionsOld(topModelOld, rightModelOld, bottomModelOld, leftModelOld);
					System.out.println("Zoom ... top, right, bottom, left:" + topModel + " " + rightModel + " "+ bottomModel + " " + leftModel);
					flinkResponseHandler.setOperation("zoom");
					wrapperHandler.prepareOperation();
					if (!layout) {
						flinkResponseHandler.setVerticesHaveCoordinates(false);
						if (messageData.startsWith("zoomIn")) {
		    				setOperation("zoomIn");
							System.out.println("in zoom in layout function");
//							for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) {
//								System.out.println(entry.getValue().getIdGradoop() + " " + entry.getValue().getX() + " " + entry.getValue().getY());
//							}
							zoomInLayoutFirstStep();
						} else {
	        				setOperation("zoomOut");
							System.out.println("in zoom out layout function");
							zoomOutLayoutFirstStep();
	        			}
					} else {
						if (messageData.startsWith("zoomIn")) {
		    				setOperation("zoomIn");
	        			} else {
	        				setOperation("zoomOut");
	        			}
						zoom();
					}
    			} else if (messageData.startsWith("pan")) {
        			String[] arrMessageData = messageData.split(";");
        			Float topModelOld = flinkCore.getTopModel();
        			Float bottomModelOld = flinkCore.getBottomModel();
        			Float leftModelOld = flinkCore.getLeftModel();
        			Float rightModelOld = flinkCore.getRightModel();
        			Float xModelDiff = Float.parseFloat(arrMessageData[1]); 
        			Float yModelDiff = Float.parseFloat(arrMessageData[2]);
        			Float topModel = topModelOld + yModelDiff;
        			Float bottomModel = bottomModelOld + yModelDiff;
        			Float leftModel = leftModelOld + xModelDiff;
        			Float rightModel = rightModelOld + xModelDiff;
					flinkCore.setModelPositionsOld(topModelOld, rightModelOld, bottomModelOld, leftModelOld);
					setModelPositions(topModel, rightModel, bottomModel, leftModel);
					System.out.println("Pan ... top, right, bottom, left:" + topModel + " " + rightModel + " "+ bottomModel + " " + leftModel);
					setOperation("pan");
    				wrapperHandler.prepareOperation();
    				flinkResponseHandler.setOperation("pan");
					if (!layout) {
	    				flinkResponseHandler.setVerticesHaveCoordinates(false);
	    				panLayoutFirstStep();
					} else {
						flinkResponseHandler.setVerticesHaveCoordinates(true);
						pan();
					}
				} 
            }
        };
    }
    
    private void buildTopViewRetract() {
    	flinkCore.initializeGradoopGraphUtil();
		DataStream<Tuple2<Boolean, Row>> wrapperStream = flinkCore.buildTopViewRetract(maxVertices);
//		if (graphOperationLogic.equals("serverSide")) {
			wrapperHandler.initializeGraphRepresentation();
			flinkResponseHandler.setOperation("initialRetract");
			DataStream<String> wrapperLine;
			if (layout) {
				flinkResponseHandler.setVerticesHaveCoordinates(true);
				wrapperLine = wrapperStream.map(new WrapperMapLineRetract());
			} else {
				flinkResponseHandler.setVerticesHaveCoordinates(false);
				wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinatesRetract());
			}
			wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
//			wrapperLine.addSink(new SocketClientSink<Tuple2<Boolean,String>>("localhost", 8898, 
//					new TypeInformationSerializationSchema<>(
//			        wrapperLine.getType(), 
//			        flinkCore.getFsEnv().getConfig()), 3));
			
			//local execution
//			DataStream<Tuple2<Boolean,VVEdgeWrapper>> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperRetract()).setParallelism(1);
//			wrapperStreamWrapper.addSink(new WrapperObjectSinkRetractInitial()).setParallelism(1);
//		} else {
//			wrapperStream.addSink(new WrapperRetractSink());
//		}
    }
    
    private void buildTopViewAppendJoin() {
    	flinkCore.initializeCSVGraphUtilJoin();
		DataStream<Row> wrapperStream = flinkCore.buildTopViewAppendJoin(maxVertices);
//		if (graphOperationLogic.equals("serverSide")) {
			wrapperHandler.initializeGraphRepresentation();
			flinkResponseHandler.setOperation("initialAppend");
			DataStream<String> wrapperLine;
			if (layout) {
				flinkResponseHandler.setVerticesHaveCoordinates(true);
				wrapperLine = wrapperStream.map(new WrapperMapLine());
			} else {
				flinkResponseHandler.setVerticesHaveCoordinates(false);
				wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
			}
			wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
			
			//local execution
//			DataStream<VVEdgeWrapper> wrapperStreamWrapper;
//			if (layout) {
//				wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
//			} else {
//				wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//			}
//			wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendInitial()).setParallelism(1);
			
//		} else {
//			wrapperStream.addSink(new WrapperAppendSink());
//		}
    }
    
    private void buildTopViewAdjacency() {
    	flinkCore.initializeAdjacencyGraphUtil();
		wrapperHandler.initializeGraphRepresentation();
		flinkResponseHandler.setOperation("initialAppend");
		DataStream<Row> wrapperStream = flinkCore.buildTopViewAdjacency(maxVertices);
//		if (graphOperationLogic.equals("serverSide")) {
//			
			DataStream<String> wrapperLine;
			if (layout) {
				flinkResponseHandler.setVerticesHaveCoordinates(true);
				wrapperLine = wrapperStream.map(new WrapperMapLine());
			} else {
				flinkResponseHandler.setVerticesHaveCoordinates(false);
				wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
			}
			wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
			
			//local execution
//			DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
//			wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendInitial());
			
//		} else {
//			wrapperStream.addSink(new WrapperAppendSink());
//		}
    }
    
    private void zoom() {
    	DataStream<Row> wrapperStream = flinkCore.zoom();
//    	if (graphOperationLogic.equals("clientSide")) {
//    		wrapperStream.addSink(new WrapperAppendSink());
//    	} else {
			
			flinkResponseHandler.setVerticesHaveCoordinates(true);
			System.out.println("executing zoom on server class");		    				
			DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLine());
			wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
			
			//local execution
//			DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
//			wrapperStreamWrapper.addSink(new WrapperObjectSinkAppend()).setParallelism(1);
//    	}
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
//    	if (graphOperationLogic.equals("serverSide")) {
//    		clearOperation();
    		wrapperHandler.clearOperation();
//    	}
    }
    
    private void pan() {
    	DataStream<Row> wrapperStream = flinkCore.pan();
//    	if (graphOperationLogic.equals("clientSide")) {
//    		wrapperStream.addSink(new WrapperAppendSink());
//    	} else {
			
			DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLine());
			wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
			
		//local execution
//    		DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
//			wrapperStreamWrapper.addSink(new WrapperObjectSinkAppend()).setParallelism(1);
		
//    	}
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//    	if (graphOperationLogic.equals("serverSide")) {
//    		clearOperation();
    		wrapperHandler.clearOperation();
//    	}
    }
    
    private void nextSubStep() {
    	if (operation.equals("initial")) {
//    		if (graphOperationLogic.equals("serverSide")) {
//    			clearOperation();
        		wrapperHandler.clearOperation();
//        	}
    	} else if (operation.startsWith("zoom")) {
    		if (operation.contains("In")) {
    			if (operationStep == 1) {
    				if (capacity > 0) {
    					zoomInLayoutSecondStep();
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
    		} else {
    			if (operationStep == 1) zoomOutLayoutSecondStep();
    		}
    	} else if (operation.startsWith("pan")) {
    		if (operationStep == 1) {
    			if (capacity > 0) {
    				panLayoutSecondStep();
    			} else {
    				panLayoutFourthStep();
    			}
    		} else if (operationStep == 2) {
    			if (capacity > 0) {
    				panLayoutSecondStep();
    			} else {
    				panLayoutFourthStep();
    			}
    		} else if (operationStep == 3) panLayoutFourthStep();
    	}
    }

	private void zoomInLayoutFirstStep() {
    	setOperationStep(1);
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutFirstStep(wrapperHandler.getLayoutedVertices(), 
    			wrapperHandler.getInnerVertices());	
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
    	wrapperHandler.setSentToClientInSubStep(false);
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (flinkResponseHandler.getLine() == "empty" || wrapperHandler.getSentToClientInSubStep() == false) zoomInLayoutSecondStep();

    	//local execution
//    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		wrapperStream.addSink(new CheckEmptySink());
//		latestRow = null;
//		Main.sentToClientInSubStep = false;
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		if (latestRow == null || Main.sentToClientInSubStep == false) zoomInLayoutSecondStep();
    }
    
    private void zoomInLayoutSecondStep() {
    	setOperationStep(2);
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutSecondStep(wrapperHandler.getLayoutedVertices(), 
    			wrapperHandler.getInnerVertices(), wrapperHandler.getNewVertices());
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
    	wrapperHandler.setSentToClientInSubStep(false);
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (flinkResponseHandler.getLine() == "empty" || wrapperHandler.getSentToClientInSubStep() == false) zoomInLayoutThirdStep();
    	
    	//local execution
//    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		wrapperStream.addSink(new CheckEmptySink());
//		latestRow = null;
//		Main.sentToClientInSubStep = false;
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		if (latestRow == null || Main.sentToClientInSubStep == false) zoomInLayoutThirdStep();
    }
    
    private  void zoomInLayoutThirdStep() {
    	setOperationStep(3);
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutThirdStep(wrapperHandler.getLayoutedVertices());
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
    	wrapperHandler.setSentToClientInSubStep(false);
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (wrapperHandler.getSentToClientInSubStep() == false) zoomInLayoutFourthStep();
    	
		//local execution
//    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		Main.sentToClientInSubStep = false;
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		if (Main.sentToClientInSubStep == false) zoomInLayoutFourthStep();
    }
    
    private void zoomInLayoutFourthStep() {
    	setOperationStep(4);
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutFourthStep(wrapperHandler.getLayoutedVertices(),
    			wrapperHandler.getInnerVertices(), wrapperHandler.getNewVertices());
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
    	wrapperHandler.clearOperation();
    	
    	//local execution
//    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
    	
//		if (graphOperationLogic.equals("serverSide")) {
//    		handler.clearOperation();
//    		clearOperation();
//    	}
    }
    
    private void zoomOutLayoutFirstStep() {
    	setOperationStep(1);
		DataStream<Row> wrapperStream = flinkCore.zoomOutLayoutFirstStep(wrapperHandler.getLayoutedVertices());
		DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
    	wrapperHandler.setSentToClientInSubStep(false);
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (flinkResponseHandler.getLine() == "empty" || 
				wrapperHandler.getSentToClientInSubStep() == false) wrapperHandler.clearOperation();
		
		//local execution
//    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		wrapperStream.addSink(new CheckEmptySink());
//		latestRow = null;
//		sentToClientInSubStep = false;
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		if ((sentToClientInSubStep == false || latestRow == null) 
////				&& graphOperationLogic.equals("serverSide")
//				) {
//			handler.clearOperation();
////			clearOperation();
//		}
    }
    
    private void zoomOutLayoutSecondStep() {
    	setOperationStep(2);
		DataStream<Row> wrapperStream = flinkCore.zoomOutLayoutSecondStep(wrapperHandler.getLayoutedVertices(), 
				wrapperHandler.getNewVertices());
		DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		wrapperHandler.clearOperation();
		
		//local execution
//    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
////		if (graphOperationLogic.equals("serverSide")) {
////			clearOperation(); 
//			handler.clearOperation();
////		}
    }
    
    
    private void panLayoutFirstStep() {
    	setOperationStep(1);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutFirstStep(wrapperHandler.getLayoutedVertices(), 
    			wrapperHandler.getNewVertices());
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
    	wrapperHandler.setSentToClientInSubStep(false);
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}		
		if (flinkResponseHandler.getLine() == "empty" || wrapperHandler.getSentToClientInSubStep() == false) panLayoutSecondStep();

    	//local execution
//    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		wrapperStream.addSink(new CheckEmptySink());
//		latestRow = null;
//		Main.sentToClientInSubStep = false;
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		if (latestRow == null || Main.sentToClientInSubStep == false) panLayoutSecondStep();
    }
    
    private void panLayoutSecondStep() {
    	setOperationStep(2);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutSecondStep(wrapperHandler.getLayoutedVertices(), 
    			wrapperHandler.getNewVertices());
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
    	wrapperHandler.setSentToClientInSubStep(false);
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}		
		if (flinkResponseHandler.getLine() == "empty" || wrapperHandler.getSentToClientInSubStep() == false) panLayoutThirdStep();
		
    	//local execution
//    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		wrapperStream.addSink(new CheckEmptySink());
//		latestRow = null;
//		Main.sentToClientInSubStep = false;
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		if (latestRow == null || Main.sentToClientInSubStep == false) panLayoutThirdStep();
    }
    
    private void panLayoutThirdStep() {
    	setOperationStep(3);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutThirdStep(wrapperHandler.getLayoutedVertices());
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
    	wrapperHandler.setSentToClientInSubStep(false);
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}		
		if (wrapperHandler.getSentToClientInSubStep() == false) panLayoutFourthStep();
		
    	//local execution
//		DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		Main.sentToClientInSubStep = false;
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		if (Main.sentToClientInSubStep == false) panLayoutFourthStep();
    }
    
    private void panLayoutFourthStep() {
    	setOperationStep(4);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutFourthStep(wrapperHandler.getLayoutedVertices(), 
    			wrapperHandler.getNewVertices());
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}		
		wrapperHandler.clearOperation();

//    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
////		if (graphOperationLogic.equals("serverSide")) {
////    		clearOperation();
//			handler.clearOperation();
////    	}
    }

    /**
     * sends a message to the all connected web socket clients
     */
    public void sendToAll(String message) {
        for (WebSocketChannel session : channels) {
            WebSockets.sendText(message, session, null);
        }
    }
	  
//    private void initializeGraphRepresentation() {
//    	System.out.println("initializing grpah representation");
//		operation = "initial";
//		globalVertices = new HashMap<String,Map<String,Object>>();
//		innerVertices = new HashMap<String,VertexCustom>();
//		newVertices = new HashMap<String,VertexCustom>();
//		edges = new HashMap<String,VVEdgeWrapper>();
//	}
	
	private void setOperation(String operation) {
		wrapperHandler.setOperation(operation);
		this.operation = operation;
	}
	
	private void setOperationStep(Integer step) {
		wrapperHandler.setOperationStep(operationStep);
		operationStep = step;
	}
	
    private void setLayoutMode(Boolean layoutMode) {
		wrapperHandler.setLayoutMode(layoutMode);	
		layout = layoutMode;
	}
    
    private void setModelPositions(Float topModel, Float rightModel, Float bottomModel, Float leftModel) {
    	flinkCore.setModelPositions(topModel, rightModel, bottomModel, leftModel);
    	wrapperHandler.setModelPositions(topModel, rightModel, bottomModel, leftModel);
    }
	
//	public void latestRow(Row element) {
//		latestRow = element;
//	}
}
