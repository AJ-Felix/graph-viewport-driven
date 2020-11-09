package aljoschaRydzyk.Gradoop_Flink_Prototype;

import static io.undertow.Handlers.path;
import static io.undertow.Handlers.resource;
import static io.undertow.Handlers.websocket;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.types.Row;

import com.nextbreakpoint.flinkclient.api.FlinkApi;

import io.undertow.Undertow;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.websockets.WebSocketConnectionCallback;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedBinaryMessage;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import io.undertow.websockets.spi.WebSocketHttpExchange;

public class Server implements Serializable{
public static FlinkCore flinkCore;
	
	public ArrayList<WebSocketChannel> channels = new ArrayList<>();
    private String webSocketListenPath = "/graphData";
    private int webSocketListenPort = 8897;
    private int webSocketListenPort2 = 8898;
    private String webSocketHost = "localhost";
    
    private Integer maxVertices = 100;
    
    private String graphOperationLogic;
    private boolean layout = true;
    
    private int operationStep;
    
    private Float viewportPixelX = (float) 1000;
    private Float viewportPixelY = (float) 1000;
    private float zoomLevel = 1;    
    
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
	
	public boolean sentToClientInSubStep;
	private Row latestRow;
    private FlinkApi api = new FlinkApi();
    private WrapperHandler handler;
    
//    private static final class InstanceHolder {
//    	static final Server INSTANCE = new Server();
//    }
    
//    public static Server getInstance () {
//		return InstanceHolder.INSTANCE;
//	}
    
    private static Server server = null;
    
    public static Server getInstance() {
    	if (server == null) server = new Server();
		return server;
    }

    private Server () {
    	System.out.println("executing server constructor");
//    	Undertow server = Undertow.builder().addHttpListener(webSocketListenPort, webSocketHost)
//                .setHandler(path().addPrefixPath(webSocketListenPath, websocket((exchange, channel) -> {
//                    channels.add(channel);
//                    channel.getReceiveSetter().set(getListener());
//                    channel.resumeReceives();
//                })).addPrefixPath("/", resource(new ClassPathResourceManager(Main.class.getClassLoader(),
//                        Main.class.getPackage())).addWelcomeFiles("index.html")/*.setDirectoryListingEnabled(true)*/))
//                .build();
//    	server.start();
//        System.out.println("Server started!");
//        api.getApiClient().setBasePath("http://localhost:8081");  
//        graphOperationLogic = "serverSide";
//        
//      //initialize graph representation
//  		handler = WrapperHandler.getInstance();
	}
    
    public void initializeServerFunctionality() {
    	Undertow server = Undertow.builder()
    			.addHttpListener(webSocketListenPort, webSocketHost)
//    			.addHttpListener(webSocketListenPort2, webSocketHost)
//                .setHandler(websocket(new WebSocketConnectionCallback() {
//
//					@Override
//					public void onConnect(WebSocketHttpExchange exchange, WebSocketChannel channel) {
//						channels.add(channel);
//						channel.getReceiveSetter().set(getListener2());
//						channel.resumeReceives();
//						System.out.println("connected to socket2!");
//					}
//                	
//                }))
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
        graphOperationLogic = "serverSide";
        
      //initialize graph representation
  		handler = WrapperHandler.getInstance();
  		handler.initializeGraphRepresentation();
    }
    
    public void initializeServerFunctionality2() {
    	Undertow server = Undertow.builder()
    			.addHttpListener(webSocketListenPort2, webSocketHost)
                .setHandler(
                		path().addPrefixPath("/", websocket((exchange, channel) -> {
	                    channels.add(channel);
	                    System.out.println("connted to socket2");
	                    channel.getReceiveSetter().set(getListener2());
	                    channel.resumeReceives();
	                }))
//                		new HttpHandler() {
//                            @Override
//                            public void handleRequest(HttpServerExchange exchange) throws Exception {
//                            	System.out.println("handled request");
//                                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
//                                exchange.getResponseSender().send("Hello World");
//                            }
//                        }
//                		websocket(new WebSocketConnectionCallback() {
//
//					@Override
//					public void onConnect(WebSocketHttpExchange exchange, WebSocketChannel channel) {
//						channels.add(channel);
//						channel.getReceiveSetter().set(getListener2());
//						channel.resumeReceives();
//						System.out.println("connected to socket2!");
//					}
//            
//                })
                		)
            	.build();
    	server.start();
    }
    
    private AbstractReceiveListener getListener2() {
    	return new AbstractReceiveListener() {
    		@Override
    		protected void onFullTextMessage(final WebSocketChannel channel, BufferedTextMessage message) throws IOException {
    			final String messageData = message.getData();
                for (WebSocketChannel session : channel.getPeerConnections()) {
                    System.out.println(messageData);
                    WebSockets.sendText(messageData, session, null);
                }
    	    }
    		
    		@Override
    		protected void onFullBinaryMessage(final WebSocketChannel channel, BufferedBinaryMessage message) throws IOException {
    			System.out.println(message.equals("sdfsdf"));
    			message.getData().free();
    	    }
    	};
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
                	for (VertexCustom vertex : innerVertices.values()) System.out.println(vertex.getIdGradoop());
                	for (String vertexData : list) {
                		String[] arrVertexData = vertexData.split(",");
                		String vertexId = arrVertexData[0];
                		Integer x = Integer.parseInt(arrVertexData[1]);
                		Integer y = Integer.parseInt(arrVertexData[2]);
            			VertexCustom vertex = new VertexCustom(vertexId, x, y);
            			if (layoutedVertices.containsKey(vertexId)) System.out.println("vertex already in layoutedVertices!!!");
            			layoutedVertices.put(vertexId, vertex);
                	}
                	System.out.println("layoutedVertices size: ");
                	System.out.println(layoutedVertices.size());
                	if (operation.equals("initial")) {
                		if (graphOperationLogic.equals("serverSide")) {
//                			clearOperation();
                    		handler.clearOperation();
                    	}
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
                			if (operationStep == 1) zoomOutLayoutSecondStep(topModelOld, rightModelOld, bottomModelOld, leftModelOld);
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
	        			if (graphOperationLogic.equals("serverSide")) {
		    				initializeGraphRepresentation();
		    				
		    				//local execution
//	        				DataStream<Tuple2<Boolean,VVEdgeWrapper>> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperRetract()).setParallelism(1);
//	        				wrapperStreamWrapper.addSink(new WrapperObjectSinkRetractInitial()).setParallelism(1);
	        			} else {
	        				wrapperStream.addSink(new WrapperRetractSink());
	        			}
                	} else if (arrMessageData[1].equals("appendJoin")) {
        				flinkCore.initializeCSVGraphUtilJoin();
        				DataStream<Row> wrapperStream = flinkCore.buildTopViewAppendJoin(maxVertices);
        				if (graphOperationLogic.equals("serverSide")) {
        					initializeGraphRepresentation();
        					
        					DataStream<String> wrapperLine;
        					if (layout) {
	    	    				wrapperLine = wrapperStream.map(new WrapperMapLine());
        					} else {
        						wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
        					}
    	    				wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema(), 3));
        					
        					//local execution
//        					DataStream<VVEdgeWrapper> wrapperStreamWrapper;
//        					if (layout) {
//                				wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
//        					} else {
//        						wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//        					}
//		    				wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendInitial()).setParallelism(1);
    	    				
        				} else {
            				wrapperStream.addSink(new WrapperAppendSink());
        				}
        			} else if (arrMessageData[1].contentEquals("adjacency")) {
        				flinkCore.initializeAdjacencyGraphUtil();
    					initializeGraphRepresentation();
        				DataStream<Row> wrapperStream = flinkCore.buildTopViewAdjacency(maxVertices);
        				if (graphOperationLogic.equals("serverSide")) {
        					
        					DataStream<String> wrapperLine;
        					if (layout) {
	    	    				wrapperLine = wrapperStream.map(new WrapperMapLine());
        					} else {
        						wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
        					}
    	    				wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
    	    				
        					//local execution
//        					DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
//        					wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendInitial());
        					
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
//                		clearOperation();
                		handler.clearOperation();
                	}
                } else if (messageData.startsWith("zoom")) {
        			String[] arrMessageData = messageData.split(";");
        			Float xRenderPos = Float.parseFloat(arrMessageData[1]);
        			Float yRenderPos = Float.parseFloat(arrMessageData[2]);
        			zoomLevel = Float.parseFloat(arrMessageData[3]);
        			topModel = (- yRenderPos / zoomLevel);
        			leftModel = (- xRenderPos /zoomLevel);
        			bottomModel = (topModel + viewportPixelY / zoomLevel);
        			rightModel = (leftModel + viewportPixelX / zoomLevel);
        			topModelOld = flinkCore.gettopModelPos();
        			leftModelOld = flinkCore.getLeftModelPos();
        			bottomModelOld = flinkCore.getBottomModelPos();
        			rightModelOld = flinkCore.getRightModelPos();
					flinkCore.setTopModelPos(topModel);
					flinkCore.setRightModelPos(rightModel);
					flinkCore.setBottomModelPos(bottomModel);
					flinkCore.setLeftModelPos(leftModel);
					System.out.println("Zoom ... top, right, bottom, left:" + topModel + " " + rightModel + " "+ bottomModel + " " + leftModel);
					if (!layout) {
						if (messageData.startsWith("zoomIn")) {
		    				setOperation("zoomIn");
		    				handler.setOperation("zoomIn");
							System.out.println("in zoom in layout function");
							for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) {
								System.out.println(entry.getValue().getIdGradoop() + " " + entry.getValue().getX() + " " + entry.getValue().getY());
							}
							handler.prepareOperation();
							zoomInLayoutFirstStep();
						} else {
	        				setOperation("zoomOut");
		    				handler.setOperation("zoomOut");
							System.out.println("in zoom out layout function");
							handler.prepareOperation();
							zoomOutLayoutFirstStep(topModelOld, rightModelOld, bottomModelOld, leftModelOld);
	        			}
					} else {
	        			DataStream<Row> wrapperStream = flinkCore.zoom(topModel, rightModel, bottomModel, leftModel);
	        			wrapperStream.print();
//	                	if (graphOperationLogic.equals("clientSide")) {
//	                		wrapperStream.addSink(new WrapperAppendSink());
//	                	} else {
		        			if (messageData.startsWith("zoomIn")) {
			    				setOperation("zoomIn");
			    				handler.setOperation("zoomIn");
		        			} else {
		        				setOperation("zoomOut");
			    				handler.setOperation("zoomOut");
		        			}
		    				handler.setModelPositions(topModel, rightModel, bottomModel, leftModel);
		    				System.out.println("executing zoom on server class");
		    				handler.prepareOperation();
//		    				Main.flinkResponseHandler.listen();
		    				
		    				DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLine());
		    				wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
		    				
		    				//local execution
//		    				DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
//		    				wrapperStreamWrapper.addSink(new WrapperObjectSinkAppend()).setParallelism(1);
//	                	}
	                	try {
							flinkCore.getFsEnv().execute();
						} catch (Exception e) {
							e.printStackTrace();
						}
//	                	if (graphOperationLogic.equals("serverSide")) {
//	                		clearOperation();
	                		handler.clearOperation();
//	                	}
					}
    			} else if (messageData.startsWith("pan")) {
        			String[] arrMessageData = messageData.split(";");
        			topModelOld = flinkCore.gettopModelPos();
        			bottomModelOld = flinkCore.getBottomModelPos();
        			leftModelOld = flinkCore.getLeftModelPos();
        			rightModelOld = flinkCore.getRightModelPos();
        			xModelDiff = Float.parseFloat(arrMessageData[1]); 
        			yModelDiff = Float.parseFloat(arrMessageData[2]);
					topModel = topModelOld + yModelDiff;
					bottomModel = bottomModelOld + yModelDiff;
					leftModel = leftModelOld + xModelDiff;
					rightModel = rightModelOld + xModelDiff;
					flinkCore.setTopModelPos(topModel);
					flinkCore.setBottomModelPos(bottomModel);
					flinkCore.setLeftModelPos(leftModel);
					flinkCore.setRightModelPos(rightModel);
					System.out.println("Pan ... top, right, bottom, left:" + topModel + " " + rightModel + " "+ bottomModel + " " + leftModel);
					setOperation("pan");
					handler.setOperation("pan");
    				handler.setModelPositions(topModel, rightModel, bottomModel, leftModel);
    				handler.prepareOperation();
					if (!layout) {
	    				setOperationStep(1);
	    				panLayoutFirstStep();
					} else {
						DataStream<Row> wrapperStream = flinkCore.pan(xModelDiff, yModelDiff);
//	                	if (graphOperationLogic.equals("clientSide")) {
//	                		wrapperStream.addSink(new WrapperAppendSink());
//	                	} else {
							
							DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLine());
							wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
							
						//local execution
//	                		DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
//	    					wrapperStreamWrapper.addSink(new WrapperObjectSinkAppend()).setParallelism(1);
						
//	                	}
	                	try {
							flinkCore.getFsEnv().execute();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
//	                	if (graphOperationLogic.equals("serverSide")) {
//	                		clearOperation();
	                		handler.clearOperation();
//	                	}
					}
				} 
            }
        };
    }
    
    private void zoomInLayoutFirstStep() {
    	setOperationStep(1);
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutFirstStep(layoutedVertices, innerVertices);
    	
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
    	handler.sentToClientInSubStep = false;
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (Main.flinkResponseHandler.line == "empty" || handler.sentToClientInSubStep == false) zoomInLayoutSecondStep();

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
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutSecondStep(layoutedVertices, innerVertices, newVertices);
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
    	handler.sentToClientInSubStep = false;
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (Main.flinkResponseHandler.line == "empty" || handler.sentToClientInSubStep == false) zoomInLayoutThirdStep();
    	
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
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutThirdStep(layoutedVertices);
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
    	handler.sentToClientInSubStep = false;
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (Main.flinkResponseHandler.line == "empty" || handler.sentToClientInSubStep == false) zoomInLayoutFourthStep();
    	
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
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutFourthStep(layoutedVertices, innerVertices, newVertices);
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>("localhost", 8898, new SimpleStringSchema()));
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
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
    
    private void zoomOutLayoutFirstStep(Float topModelOld, Float rightModelOld, Float bottomModelOld, Float leftModelOld) {
    	setOperationStep(1);
		DataStream<Row> wrapperStream = flinkCore.zoomOutLayoutFirstStep(layoutedVertices,
				topModelOld, rightModelOld, bottomModelOld, leftModelOld);
    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		wrapperStream.addSink(new CheckEmptySink());
		latestRow = null;
		sentToClientInSubStep = false;
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if ((sentToClientInSubStep == false || latestRow == null) && graphOperationLogic.equals("serverSide")) {
			handler.clearOperation();
//			clearOperation();
		}
    }
    
    private void zoomOutLayoutSecondStep(Float topModelOld, Float rightModelOld, Float bottomModelOld, Float leftModelOld) {
    	setOperationStep(2);
		DataStream<Row> wrapperStream = flinkCore.zoomOutLayoutSecondStep(layoutedVertices, newVertices,
				topModelOld, rightModelOld, bottomModelOld, leftModelOld);
    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (graphOperationLogic.equals("serverSide")) {
//			clearOperation(); 
			handler.clearOperation();
		}
    }
    
    
    private void panLayoutFirstStep() {
    	setOperationStep(1);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutFirstStep(layoutedVertices, newVertices);
    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		wrapperStream.addSink(new CheckEmptySink());
		latestRow = null;
		Main.sentToClientInSubStep = false;
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (latestRow == null || Main.sentToClientInSubStep == false) panLayoutSecondStep();
    }
    
    private void panLayoutSecondStep() {
    	setOperationStep(2);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutSecondStep(layoutedVertices, newVertices);
    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		wrapperStream.addSink(new CheckEmptySink());
		latestRow = null;
		Main.sentToClientInSubStep = false;
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (latestRow == null || Main.sentToClientInSubStep == false) panLayoutThirdStep();
    }
    
    private void panLayoutThirdStep() {
    	setOperationStep(3);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutThirdStep(layoutedVertices);
		DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		Main.sentToClientInSubStep = false;
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (Main.sentToClientInSubStep == false) panLayoutFourthStep();
		//ATTENTION: Moving to Fifth step has to be controlled by capacity while the third step is being executed. When capacity is reached, cancel third step 
		//and move to fourth step. Probably Job Api is necessary here...
    }
    
    private void panLayoutFourthStep() {
    	setOperationStep(4);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutFourthStep(layoutedVertices, newVertices, xModelDiff, yModelDiff);
    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (graphOperationLogic.equals("serverSide")) {
//    		clearOperation();
			handler.clearOperation();
    	}
    }

    /**
     * sends a message to the all connected web socket clients
     */
    public void sendToAll(String message) {
        for (WebSocketChannel session : channels) {
            WebSockets.sendText(message, session, null);
        }
    }
	  
    private void initializeGraphRepresentation() {
    	System.out.println("initializing grpah representation");
		operation = "initial";
		globalVertices = new HashMap<String,Map<String,Object>>();
		innerVertices = new HashMap<String,VertexCustom>();
		newVertices = new HashMap<String,VertexCustom>();
		edges = new HashMap<String,VVEdgeWrapper>();
	}
	
	private void setOperation(String operation) {
		this.operation = operation;
	}
	
	private void setOperationStep(Integer step) {
		operationStep = step;
	}
	
	public void latestRow(Row element) {
		latestRow = element;
	}
}
