package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.FileNotFoundException;
import java.io.PrintStream;

public class Main {
	
    private static Server server;
	
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
		
		//initialize Server
		server = Server.getInstance();
		server.initializeServerFunctionality();
		server.initializeFlinkAndGraphRep();
	
		

//		System.out.println(stringBuilder.toString());
//		server.initializeServerFunctionality2();
    	
//        Undertow server = Undertow.builder().addHttpListener(webSocketListenPort, webSocketHost)
//                .setHandler(path().addPrefixPath(webSocketListenPath, websocket((exchange, channel) -> {
//                    channels.add(channel);
//                    channel.getReceiveSetter().set(getListener());
//                    channel.resumeReceives();
//                })).addPrefixPath("/", resource(new ClassPathResourceManager(Main.class.getClassLoader(),
//                        Main.class.getPackage())).addWelcomeFiles("index.html")/*.setDirectoryListingEnabled(true)*/))
//                .build();
//        server.start();
//        System.out.println("Server started!");
//        api.getApiClient().setBasePath("http://localhost:8081");  
    }
    
    /**
     * helper function to Undertow server
     */
//    private static AbstractReceiveListener getListener() {
//        return new AbstractReceiveListener() {
//            @Override
//            protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) {
//                final String messageData = message.getData();
//                for (WebSocketChannel session : channel.getPeerConnections()) {
//                    System.out.println(messageData);
//                    WebSockets.sendText(messageData, session, null);
//                }
//                if (messageData.equals("serverSideLogic")) {
//                	graphOperationLogic = "serverSide";
//                } else if (messageData.equals("clientSideLogic")) {
//                	graphOperationLogic = "clientSide";
//                } else if (messageData.equals("preLayout")) {
//                	layoutedVertices = new HashMap<String,VertexCustom>();
//                	layout = false;
//                } else if (messageData.equals("postLayout")) {
//                	layout = true;
//                } else if (messageData.startsWith("layoutBaseString")) {
//                	String[] arrMessageData = messageData.split(";");
//                	List<String> list = new ArrayList<String>(Arrays.asList(arrMessageData));
//                	list.remove(0);
//                	for (VertexCustom vertex : innerVertices.values()) System.out.println(vertex.getIdGradoop());
//                	for (String vertexData : list) {
//                		String[] arrVertexData = vertexData.split(",");
//                		String vertexId = arrVertexData[0];
//                		Integer x = Integer.parseInt(arrVertexData[1]);
//                		Integer y = Integer.parseInt(arrVertexData[2]);
//            			VertexCustom vertex = new VertexCustom(vertexId, x, y);
//            			if (layoutedVertices.containsKey(vertexId)) System.out.println("vertex already in layoutedVertices!!!");
//            			layoutedVertices.put(vertexId, vertex);
//                	}
//                	System.out.println("layoutedVertices size: ");
//                	System.out.println(layoutedVertices.size());
//                	if (operation.equals("initial")) {
//                		if (graphOperationLogic.equals("serverSide")) {
////                			clearOperation();
//                    		handler.clearOperation();
//                    	}
//                	} else if (operation.startsWith("zoom")) {
//                		if (operation.contains("In")) {
//                			if (operationStep == 1) {
//                				if (capacity > 0) {
//                					zoomInLayoutSecondStep();
//                				} else {
//                					zoomInLayoutFourthStep();
//                				}
//                			} else if (operationStep == 2) {
//                				if (capacity > 0) {
//                					zoomInLayoutSecondStep();
//                				} else {
//                					zoomInLayoutFourthStep();
//                				}
//                			} else if (operationStep == 3) zoomInLayoutFourthStep();
//                		} else {
//                			if (operationStep == 1) zoomOutLayoutSecondStep(topModelOld, rightModelOld, bottomModelOld, leftModelOld);
//                		}
//                	} else if (operation.startsWith("pan")) {
//                		if (operationStep == 1) {
//                			if (capacity > 0) {
//                				panLayoutSecondStep();
//                			} else {
//                				panLayoutFourthStep();
//                			}
//                		} else if (operationStep == 2) {
//                			if (capacity > 0) {
//                				panLayoutSecondStep();
//                			} else {
//                				panLayoutFourthStep();
//                			}
//                		} else if (operationStep == 3) panLayoutFourthStep();
//                	}
//                } else if (messageData.startsWith("maxVertices")) {
//                	String[] arrMessageData = messageData.split(";");
//                	maxVertices = Integer.parseInt(arrMessageData[1]);
//                } else if (messageData.startsWith("edgeIdString")) {
//                	String[] arrMessageData = messageData.split(";");
//                	List<String> list = new ArrayList<String>(Arrays.asList(arrMessageData));
//                	list.remove(0);
//                	Set<String> visualizedWrappers = new HashSet<String>(list);
//                	flinkCore.getGraphUtil().setVisualizedWrappers(visualizedWrappers);
//                } else if (messageData.startsWith("vertexIdString")) {
//                	String[] arrMessageData = messageData.split(";");
//                	List<String> list = new ArrayList<String>(Arrays.asList(arrMessageData));
//                	list.remove(0);
//                	Set<String> visualizedVertices = new HashSet<String>(list);
//                	flinkCore.getGraphUtil().setVisualizedVertices(visualizedVertices);
//                } else if (messageData.startsWith("buildTopView")) {
//                	flinkCore = new FlinkCore(graphOperationLogic);
//                	flinkCore.setTopModelPos((float) 0);
//                	flinkCore.setLeftModelPos((float) 0);
//                	flinkCore.setRightModelPos((float) 4000);
//                	flinkCore.setBottomModelPos((float) 4000);
//                	String[] arrMessageData = messageData.split(";");
//                	if (arrMessageData[1].equals("retract")) {
//	        			flinkCore.initializeGradoopGraphUtil();
//        				DataStream<Tuple2<Boolean, Row>> wrapperStream = flinkCore.buildTopViewRetract(maxVertices);
//        				wrapperStream.print().setParallelism(1);
//	        			if (graphOperationLogic.equals("serverSide")) {
//		    				initializeGraphRepresentation();
//	        				DataStream<Tuple2<Boolean,VVEdgeWrapper>> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperRetract()).setParallelism(1);
//	        				wrapperStreamWrapper.addSink(new WrapperObjectSinkRetractInitial()).setParallelism(1);
//	        			} else {
//	        				wrapperStream.addSink(new WrapperRetractSink());
//	        			}
//                	} else if (arrMessageData[1].equals("appendJoin")) {
//        				flinkCore.initializeCSVGraphUtilJoin();
//        				DataStream<Row> wrapperStream = flinkCore.buildTopViewAppendJoin(maxVertices);
//        				if (graphOperationLogic.equals("serverSide")) {
//        					initializeGraphRepresentation();
//        					DataStream<VVEdgeWrapper> wrapperStreamWrapper;
//        					if (layout) {
//                				wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
//        					} else {
//        						wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//        					}
//		    				wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendInitial()).setParallelism(1);
//        				} else {
//            				wrapperStream.addSink(new WrapperAppendSink());
//        				}
//        			} else if (arrMessageData[1].contentEquals("adjacency")) {
//        				flinkCore.initializeAdjacencyGraphUtil();
//    					initializeGraphRepresentation();
//        				DataStream<Row> wrapperStream = flinkCore.buildTopViewAdjacency(maxVertices);
//        				if (graphOperationLogic.equals("serverSide")) {
//        					DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
//    	    				wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendInitial()).setParallelism(1);
//    	    				wrapperStream.addSink(new FlinkRowStreamPrintSink());
//    	    				wrapperStreamWrapper.print();
//        				} else {
//            				wrapperStream.addSink(new WrapperAppendSink());
//        				}
//        			}
//                	try {
//        				flinkCore.getFsEnv().execute();
//        			} catch (Exception e) {
//        				e.printStackTrace();
//        			}
//                	if (graphOperationLogic.equals("serverSide") && (layout)) {
////                		clearOperation();
//                		handler.clearOperation();
//                	}
//                } else if (messageData.startsWith("zoom")) {
//        			String[] arrMessageData = messageData.split(";");
//        			Float xRenderPos = Float.parseFloat(arrMessageData[1]);
//        			Float yRenderPos = Float.parseFloat(arrMessageData[2]);
//        			zoomLevel = Float.parseFloat(arrMessageData[3]);
//        			topModel = (- yRenderPos / zoomLevel);
//        			leftModel = (- xRenderPos /zoomLevel);
//        			bottomModel = (topModel + viewportPixelY / zoomLevel);
//        			rightModel = (leftModel + viewportPixelX / zoomLevel);
//        			topModelOld = flinkCore.gettopModelPos();
//        			leftModelOld = flinkCore.getLeftModelPos();
//        			bottomModelOld = flinkCore.getBottomModelPos();
//        			rightModelOld = flinkCore.getRightModelPos();
//					flinkCore.setTopModelPos(topModel);
//					flinkCore.setRightModelPos(rightModel);
//					flinkCore.setBottomModelPos(bottomModel);
//					flinkCore.setLeftModelPos(leftModel);
//					System.out.println("Zoom ... top, right, bottom, left:" + topModel + " " + rightModel + " "+ bottomModel + " " + leftModel);
//					if (!layout) {
//						if (messageData.startsWith("zoomIn")) {
////							System.out.println(layoutedVertices.size());
//		    				Main.setOperation("zoomIn");
//							System.out.println("in zoom in layout function");
//							for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) {
//								System.out.println(entry.getValue().getIdGradoop() + " " + entry.getValue().getX() + " " + entry.getValue().getY());
//							}
////							Main.prepareOperation();
//							handler.prepareOperation();
////							Map<String,VertexCustom> innerVerticesCopy = new HashMap<String,VertexCustom>();
////							innerVerticesCopy.put("5c6ab3fd8e3627bbfb10de29", Custom("5c6ab3fd8e3627bbfb10de29", "forum", 0, 2873, 2358, (long) 121));
////							Set<String> layoutedVerticesCopy = new HashSet<String>();
////							layoutedVerticesCopy.add("5c6ab3fd8e3627bbfb10de29");
//							zoomInLayoutFirstStep();
//						} else {
//	        				Main.setOperation("zoomOut");
//							System.out.println("in zoom out layout function");
////							prepareOperation();
//							handler.prepareOperation();
//							zoomOutLayoutFirstStep(topModelOld, rightModelOld, bottomModelOld, leftModelOld);
//	        			}
//					} else {
//	        			DataStream<Row> wrapperStream = flinkCore.zoom(topModel, rightModel, bottomModel, leftModel);
//	                	if (graphOperationLogic.equals("clientSide")) {
//	                		wrapperStream.addSink(new WrapperAppendSink());
//	                	} else {
//		        			if (messageData.startsWith("zoomIn")) {
//			    				Main.setOperation("zoomIn");
//		        			} else {
//		        				Main.setOperation("zoomOut");
//		        			}
//		    				DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
////		    				prepareOperation();
//		    				handler.prepareOperation();
//		    				wrapperStreamWrapper.addSink(new WrapperObjectSinkAppend()).setParallelism(1);
//	                	}
//	                	try {
//							flinkCore.getFsEnv().execute();
//						} catch (Exception e) {
//							e.printStackTrace();
//						}
//	                	if (graphOperationLogic.equals("serverSide")) {
////	                		clearOperation();
//	                		handler.clearOperation();
//	                	}
//					}
//    			} else if (messageData.startsWith("pan")) {
//        			String[] arrMessageData = messageData.split(";");
//        			topModelOld = flinkCore.gettopModelPos();
//        			bottomModelOld = flinkCore.getBottomModelPos();
//        			leftModelOld = flinkCore.getLeftModelPos();
//        			rightModelOld = flinkCore.getRightModelPos();
//        			xModelDiff = Float.parseFloat(arrMessageData[1]); 
//        			yModelDiff = Float.parseFloat(arrMessageData[2]);
//					topModel = topModelOld + yModelDiff;
//					bottomModel = bottomModelOld + yModelDiff;
//					leftModel = leftModelOld + xModelDiff;
//					rightModel = rightModelOld + xModelDiff;
//					flinkCore.setTopModelPos(topModel);
//					flinkCore.setBottomModelPos(bottomModel);
//					flinkCore.setLeftModelPos(leftModel);
//					flinkCore.setRightModelPos(rightModel);
//					System.out.println("Pan ... top, right, bottom, left:" + topModel + " " + rightModel + " "+ bottomModel + " " + leftModel);
//					if (!layout) {
//	    				Main.setOperation("pan");
//	    				Main.setOperationStep(1);
////	    				prepareOperation();
//	    				handler.prepareOperation();
//	    				panLayoutFirstStep();
//					} else {
//						DataStream<Row> wrapperStream = flinkCore.pan(xModelDiff, yModelDiff);
//	                	if (graphOperationLogic.equals("clientSide")) {
//	                		wrapperStream.addSink(new WrapperAppendSink());
//	                	} else {
//	                		DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppend());
//	    					Main.setOperation("pan");
////		    				prepareOperation();
//		    				handler.prepareOperation();
//	    					wrapperStreamWrapper.addSink(new WrapperObjectSinkAppend()).setParallelism(1);
//	                	}
//	                	try {
//							flinkCore.getFsEnv().execute();
//						} catch (Exception e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//	                	if (graphOperationLogic.equals("serverSide")) {
////	                		clearOperation();
//	                		handler.clearOperation();
//	                	}
//					}
//				} else if (messageData.equals("displayAll")) {
//    		        flinkCore = new FlinkCore(graphOperationLogic);
//        			flinkCore.initializeCSVGraphUtilJoin();
//        			DataStream<Row> wrapperStream = flinkCore.displayAll();
//					wrapperStream.addSink(new WrapperAppendSink());
//					try {
//						flinkCore.getFsEnv().execute();
//					} catch (Exception e) {
//						e.printStackTrace();
//					}
//					Main.sendToAll("fitGraph");
////					UndertowServer.sendToAll("layout");
//    			}
//            }
//        };
//    }
    
//    private static void zoomInLayoutFirstStep() {
//    	Main.setOperationStep(1);
//    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutFirstStep(layoutedVertices, innerVertices);
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
//    }
//    
//    private static void zoomInLayoutSecondStep() {
//    	Main.setOperationStep(2);
//    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutSecondStep(layoutedVertices, innerVertices, newVertices);
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
//    }
//    
//    private static void zoomInLayoutThirdStep() {
//    	Main.setOperationStep(3);
//    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutThirdStep(layoutedVertices);
//    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		Main.sentToClientInSubStep = false;
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		if (Main.sentToClientInSubStep == false) zoomInLayoutFourthStep();
//		//ATTENTION: Moving to Fourth step has to be controlled by capacity while the third step is being executed. When capacity is reached, cancel third step 
//		//and move to fourth step. Probably Job Api is necessary here...
//    }
//    
//    private static void zoomInLayoutFourthStep() {
//    	Main.setOperationStep(4);
//    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutFourthStep(layoutedVertices, innerVertices, newVertices);
//    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		if (graphOperationLogic.equals("serverSide")) {
//    		handler.clearOperation();
////    		clearOperation();
//    	}
//    }
//    
//    private static void zoomOutLayoutFirstStep(Float topModelOld, Float rightModelOld, Float bottomModelOld, Float leftModelOld) {
//    	Main.setOperationStep(1);
//		DataStream<Row> wrapperStream = flinkCore.zoomOutLayoutFirstStep(layoutedVertices,
//				topModelOld, rightModelOld, bottomModelOld, leftModelOld);
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
//		if ((sentToClientInSubStep == false || latestRow == null) && graphOperationLogic.equals("serverSide")) {
//			handler.clearOperation();
////			clearOperation();
//		}
//    }
//    
//    private static void zoomOutLayoutSecondStep(Float topModelOld, Float rightModelOld, Float bottomModelOld, Float leftModelOld) {
//    	Main.setOperationStep(2);
//		DataStream<Row> wrapperStream = flinkCore.zoomOutLayoutSecondStep(layoutedVertices, newVertices,
//				topModelOld, rightModelOld, bottomModelOld, leftModelOld);
//    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		if (graphOperationLogic.equals("serverSide")) {
////			clearOperation(); 
//			handler.clearOperation();
//		}
//    }
//    
//    
//    private static void panLayoutFirstStep() {
//    	Main.setOperationStep(1);
//    	DataStream<Row> wrapperStream = flinkCore.panLayoutFirstStep(layoutedVertices, newVertices);
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
//    }
//    
//    private static void panLayoutSecondStep() {
//    	Main.setOperationStep(2);
//    	DataStream<Row> wrapperStream = flinkCore.panLayoutSecondStep(layoutedVertices, newVertices);
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
//    }
//    
//    private static void panLayoutThirdStep() {
//    	Main.setOperationStep(3);
//    	DataStream<Row> wrapperStream = flinkCore.panLayoutThirdStep(layoutedVertices);
//		DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		Main.sentToClientInSubStep = false;
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		if (Main.sentToClientInSubStep == false) panLayoutFourthStep();
//		//ATTENTION: Moving to Fifth step has to be controlled by capacity while the third step is being executed. When capacity is reached, cancel third step 
//		//and move to fourth step. Probably Job Api is necessary here...
//    }
//    
//    private static void panLayoutFourthStep() {
//    	Main.setOperationStep(4);
//    	DataStream<Row> wrapperStream = flinkCore.panLayoutFourthStep(layoutedVertices, newVertices, xModelDiff, yModelDiff);
//    	DataStream<VVEdgeWrapper> wrapperStreamWrapper = wrapperStream.map(new WrapperMapVVEdgeWrapperAppendNoLayout());
//		wrapperStreamWrapper.addSink(new WrapperObjectSinkAppendLayout()).setParallelism(1);
//		try {
//			flinkCore.getFsEnv().execute();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		if (graphOperationLogic.equals("serverSide")) {
////    		clearOperation();
//			handler.clearOperation();
//    	}
//    }
//
//    /**
//     * sends a message to the all connected web socket clients
//     */
//    public static void sendToAll(String message) {
//        for (WebSocketChannel session : channels) {
//            WebSockets.sendText(message, session, null);
//        }
//    }
    
//    private static void initializeGraphRepresentation() {
//    	System.out.println("initializing grpah representation");
//		operation = "initial";
//		globalVertices = new HashMap<String,Map<String,Object>>();
//		innerVertices = new HashMap<String,VertexCustom>();
//		newVertices = new HashMap<String,VertexCustom>();
//		edges = new HashMap<String,VVEdgeWrapper>();
//	}
//	
//	private static void setOperation(String operation) {
//		Main.operation = operation;
//	}
//	
//	private static void setOperationStep(Integer step) {
//		Main.operationStep = step;
//	}
//	
//	public static void latestRow(Row element) {
//		latestRow = element;
//	}
	
//	private static void prepareOperation(){
//		System.out.println("top, right, bottom, left:" + topModel + " " + rightModel + " "+ bottomModel + " " + leftModel);
//		if (operation != "zoomOut"){
//			for (Map.Entry<String, VVEdgeWrapper> entry : edges.entrySet()) {
//				VVEdgeWrapper wrapper = entry.getValue();
//				System.out.println("globalVertices in prepareOperation: ");
//				System.out.println(wrapper.getSourceVertex().getIdGradoop());
//				System.out.println((VertexCustom) globalVertices.get(wrapper.getSourceVertex().getIdGradoop()).get("vertex") == null);
//				System.out.println(wrapper.getTargetVertex().getIdGradoop());
//				System.out.println((VertexCustom) globalVertices.get(wrapper.getTargetVertex().getIdGradoop()).get("vertex") == null);
//				Integer sourceX;
//				Integer sourceY;
//				Integer targetX;
//				Integer targetY;
//				if (layout) {
//					sourceX = wrapper.getSourceX();
//					sourceY = wrapper.getSourceY();
//					targetX = wrapper.getTargetX();
//					targetY = wrapper.getTargetY();
//				} else {
//					VertexCustom sourceVertex = (VertexCustom) layoutedVertices.get(wrapper.getSourceVertex().getIdGradoop());
//					sourceX = sourceVertex.getX();
//					sourceY = sourceVertex.getY();
//					VertexCustom targetVertex = (VertexCustom) layoutedVertices.get(wrapper.getTargetVertex().getIdGradoop());
//					targetX = targetVertex.getX();
//					targetY = targetVertex.getY();
//				}
//				if (((sourceX < leftModel) || (rightModel < sourceX) || (sourceY < topModel) || (bottomModel < sourceY)) &&
//						((targetX  < leftModel) || (rightModel < targetX ) || (targetY  < topModel) || (bottomModel < targetY))){
//					Main.sendToAll("removeObjectServer;" + wrapper.getEdgeIdGradoop());
//					System.out.println("Removing Object in prepareOperation, ID: " + wrapper.getEdgeIdGradoop());
//				}
//			}			
//			System.out.println("innerVertices size before removing in prepareOperation: " + innerVertices.size());
//			Iterator<Map.Entry<String,VertexCustom>> iter = innerVertices.entrySet().iterator();
//			while (iter.hasNext()) {
//				Map.Entry<String,VertexCustom> entry = iter.next();
//				VertexCustom vertex = entry.getValue();
//				System.out.println("VertexID: " + vertex.getIdGradoop() + ", VertexX: " + vertex.getX() + ", VertexY: "+ vertex.getY());
//				if ((vertex.getX() < leftModel) || (rightModel < vertex.getX()) || (vertex.getY() < topModel) || (bottomModel < vertex.getY())) {
//					iter.remove();
//					System.out.println("Removing Object in prepareOperation in innerVertices only, ID: " + vertex.getIdGradoop());
//				}
//			}
//			System.out.println("innerVertices size after removing in prepareOPeration: " + innerVertices.size());
//			//this is necessary in case the (second)minDegreeVertex will get deleted in the clear up step befor
//			if (innerVertices.size() > 1) {
//				updateMinDegreeVertices(innerVertices);
//			} else if (innerVertices.size() == 1) {
//				minDegreeVertex = innerVertices.values().iterator().next();
//			}
//		}
//		capacity = maxVertices - innerVertices.size();
//		if (operation.equals("pan") || operation.equals("zoomOut")) {
//			newVertices = innerVertices;
//			for (String vId : newVertices.keySet()) System.out.println("prepareOperation, newVertices key: " + vId);
//			innerVertices = new HashMap<String,VertexCustom>();
//			System.out.println(newVertices.size());
//		} else {
//			newVertices = new HashMap<String,VertexCustom>();
//		}
//		System.out.println("Capacity after prepareOperation: " + capacity);
//	}
//	
//	public static void addWrapperInitial(VVEdgeWrapper wrapper) {
//		if (wrapper.getEdgeLabel().equals("identityEdge")) {
//			addWrapperIdentityInitial(wrapper.getSourceVertex());
//		} else {
//			addNonIdentityWrapperInitial(wrapper);
//		}
//	}
//	
//	public static void addWrapper(VVEdgeWrapper wrapper) {
//		System.out.println("SourceIdNumeric: " + wrapper.getSourceIdNumeric());
//		System.out.println("SourceIdGradoop: " + wrapper.getSourceIdGradoop());
//		System.out.println("TargetIdNumeric: " + wrapper.getTargetIdNumeric());
//		System.out.println("TargetIdGradoop: " + wrapper.getTargetIdGradoop());
//		System.out.println("WrapperLabel: " + wrapper.getEdgeLabel());
//		System.out.println("Size of innerVertices: " + innerVertices.size());
//		System.out.println("Size of newVertices: " + newVertices.size());
//		System.out.println("ID Gradoop minDegreeVertex: " + minDegreeVertex.getIdGradoop());
//		System.out.println("ID Gradoop secondMinDegreeVertex: " + secondMinDegreeVertex.getIdGradoop());
//		System.out.println("Capacity: " + capacity);
//		if (wrapper.getEdgeLabel().equals("identityEdge")) {
//			addWrapperIdentity(wrapper.getSourceVertex());
//		} else {
//			addNonIdentityWrapper(wrapper);
//		}
//	}
//	
//	public static void addWrapperLayout(VVEdgeWrapper wrapper) {
//		//if in zoomIn3 or pan3: cancel flinkjob and move to next step!
//		if (operationStep == 3 && ((operation == "zoomIn") || (operation == "pan"))) {
//			if (capacity == 0) {
//				try {
//					JobIdsWithStatusOverview jobs = api.getJobs();
//					List<JobIdWithStatus> list = jobs.getJobs();
//					String jobid = list.get(0).getId();
//					System.out.println("flink api job list size: " + list.size());
//					api.terminateJob(jobid, "cancel");
//				} catch (ApiException e) {
//					e.printStackTrace();
//				}
//			}
//		} 
//		System.out.println("SourceIdNumeric: " + wrapper.getSourceIdNumeric());
//		System.out.println("SourceIdGradoop: " + wrapper.getSourceIdGradoop());
//		System.out.println("TargetIdNumeric: " + wrapper.getTargetIdNumeric());
//		System.out.println("TargetIdGradoop: " + wrapper.getTargetIdGradoop());
//		System.out.println("WrapperLabel: " + wrapper.getEdgeLabel());
//		System.out.println("Size of innerVertices: " + innerVertices.size());
//		System.out.println("Size of newVertices: " + newVertices.size());
//		System.out.println("ID Gradoop minDegreeVertex: " + minDegreeVertex.getIdGradoop());
//		System.out.println("ID Gradoop secondMinDegreeVertex: " + secondMinDegreeVertex.getIdGradoop());
//		System.out.println("Capacity: " + capacity);
//		if (wrapper.getEdgeLabel().equals("identityEdge")) {
//			addWrapperIdentityLayout(wrapper.getSourceVertex());
//		} else {
//			addNonIdentityWrapperLayout(wrapper);
//		}
//	}
//	
//	public static void removeWrapper(VVEdgeWrapper wrapper) {
//		if (wrapper.getEdgeIdGradoop() != "identityEdge") {
//			String targetId = wrapper.getTargetIdGradoop();
//			int targetIncidence = (int) globalVertices.get(targetId).get("incidence");
//			if (targetIncidence == 1) {
//				globalVertices.remove(targetId);
//				if (innerVertices.containsKey(targetId)) innerVertices.remove(targetId);
//				if (newVertices.containsKey(targetId)) newVertices.remove(targetId);
//				System.out.println("removing object in removeWrapper, ID: " + wrapper.getTargetIdGradoop());
//				Main.sendToAll("removeObjectServer;" + wrapper.getTargetIdGradoop());
//			} else {
//				globalVertices.get(targetId).put("incidence", targetIncidence - 1);
//			}
//		}
//		String sourceId = wrapper.getSourceIdGradoop();
//		int sourceIncidence = (int) globalVertices.get(sourceId).get("incidence");
//		if (sourceIncidence == 1) {
//			globalVertices.remove(sourceId);
//			if (innerVertices.containsKey(sourceId)) innerVertices.remove(sourceId);
//			if (newVertices.containsKey(sourceId)) newVertices.remove(sourceId);
//			Main.sendToAll("removeObjectServer;" + wrapper.getSourceIdGradoop());
//			System.out.println("removing object in removeWrapper, ID: " + wrapper.getSourceIdGradoop());
//		} else {
//			globalVertices.get(sourceId).put("incidence", sourceIncidence - 1);
//		}
//		edges.remove(wrapper.getEdgeIdGradoop());
//	}
//	
//	private static void registerInside(VertexCustom vertex) {
//		newVertices.put(vertex.getIdGradoop(), vertex);
//		if (newVertices.size() > 1) {
//			updateMinDegreeVertices(newVertices);
//		} else {
//			minDegreeVertex = vertex;
//		}
//	}
//	
//	private static void addNonIdentityWrapper(VVEdgeWrapper wrapper) {
//		VertexCustom sourceVertex = wrapper.getSourceVertex();
//		VertexCustom targetVertex = wrapper.getTargetVertex();
//		String sourceId = sourceVertex.getIdGradoop();
//		String targetId = targetVertex.getIdGradoop();
//		boolean sourceIsRegisteredInside = newVertices.containsKey(sourceId) || innerVertices.containsKey(sourceId);
//		boolean targetIsRegisteredInside = newVertices.containsKey(targetId) || innerVertices.containsKey(targetId);
//		if (capacity > 1) {
//			addVertex(sourceVertex);
//			if ((sourceVertex.getX() >= leftModel) && (rightModel >= sourceVertex.getX()) && (sourceVertex.getY() >= topModel) && 
//					(bottomModel >= sourceVertex.getY()) && !sourceIsRegisteredInside){
//				updateMinDegreeVertex(sourceVertex);
//				newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
//				capacity -= 1;
//			}
//			addVertex(targetVertex);
//			if ((targetVertex.getX() >= leftModel) && (rightModel >= targetVertex.getX()) && (targetVertex.getY() >= topModel) 
//					&& (bottomModel >= targetVertex.getY()) && !targetIsRegisteredInside){
//				updateMinDegreeVertex(targetVertex);
//				newVertices.put(targetVertex.getIdGradoop(), targetVertex);
//				capacity -= 1;
//			}
//			addEdge(wrapper);
//		} else if (capacity == 1){
//			boolean sourceIn = true;
//			boolean targetIn = true;
//			if ((sourceVertex.getX() < leftModel) || (rightModel < sourceVertex.getX()) || (sourceVertex.getY() < topModel) || 
//					(bottomModel < sourceVertex.getY())){
//				sourceIn = false;
//			}
//			if ((targetVertex.getX() < leftModel) || (rightModel < targetVertex.getX()) || (targetVertex.getY() < topModel) || 
//					(bottomModel < targetVertex.getY())){
//				targetIn = false;
//			}
//			System.out.println("In addNonIdentityWrapper, capacity == 1, sourceID: " + sourceVertex.getIdGradoop() + ", sourceIn: " + sourceIn + 
//					", targetID: " + targetVertex.getIdGradoop() + ", targetIn: " + targetIn);
//			if (sourceIn && targetIn) {
//				boolean sourceAdmission = false;
//				boolean targetAdmission = false;
//				if (sourceVertex.getDegree() > targetVertex.getDegree()) {
//					addVertex(sourceVertex);
//					sourceAdmission = true;
//					if (targetVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside) {
//						addVertex(targetVertex);
//						targetAdmission = true;
//						addEdge(wrapper);
//					}
//				} else {
//					addVertex(targetVertex);
//					targetAdmission = true;
//					if (sourceVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside) {
//						addVertex(sourceVertex);
//						sourceAdmission = true;
//						addEdge(wrapper);
//					}
//				}
//				if (!sourceIsRegisteredInside && sourceAdmission && !targetIsRegisteredInside && targetAdmission) {
//					reduceNeighborIncidence(minDegreeVertex);
//					removeVertex(minDegreeVertex);
//					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
//					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
//					updateMinDegreeVertices(newVertices);
//				} else if (!sourceIsRegisteredInside && sourceAdmission) {
//					registerInside(sourceVertex);
//				} else if (!targetIsRegisteredInside && targetAdmission) {
//					registerInside(targetVertex);
//				}
//				capacity -= 1 ;
//			} else if (sourceIn) {
//				addVertex(sourceVertex);
//				addVertex(targetVertex);
//				addEdge(wrapper);
//				if (!sourceIsRegisteredInside) {
//					capacity -= 1 ;
//					registerInside(sourceVertex);
//				}
//			} else if (targetIn) {
//				addVertex(targetVertex);
//				addVertex(sourceVertex);
//				addEdge(wrapper);
//				if (!targetIsRegisteredInside) {
//					capacity -= 1 ;
//					registerInside(targetVertex);
//				}
//			}
//		} else {
//			boolean sourceIn = true;
//			boolean targetIn = true;
//			if ((sourceVertex.getX() < leftModel) || (rightModel < sourceVertex.getX()) || (sourceVertex.getY() < topModel) || 
//					(bottomModel < sourceVertex.getY())){
//				sourceIn = false;
//			}
//			if ((targetVertex.getX() < leftModel) || (rightModel < targetVertex.getX()) || (targetVertex.getY() < topModel) || 
//					(bottomModel < targetVertex.getY())){
//				targetIn = false;
//			}
//			if (sourceIn && targetIn && (sourceVertex.getDegree() > secondMinDegreeVertex.getDegree()) && 
//					(targetVertex.getDegree() > secondMinDegreeVertex.getDegree())) {
//				addVertex(sourceVertex);
//				addVertex(targetVertex);
//				addEdge(wrapper);
//				if (!sourceIsRegisteredInside && !targetIsRegisteredInside) {
//					reduceNeighborIncidence(minDegreeVertex);
//					reduceNeighborIncidence(secondMinDegreeVertex);
//					removeVertex(secondMinDegreeVertex);
//					removeVertex(minDegreeVertex);
//					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
//					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
//					updateMinDegreeVertices(newVertices);
//				} else if (!sourceIsRegisteredInside) {
//					reduceNeighborIncidence(minDegreeVertex);
//					removeVertex(minDegreeVertex);
//					registerInside(sourceVertex);
//				} else if (!targetIsRegisteredInside) {
//					reduceNeighborIncidence(minDegreeVertex);
//					removeVertex(minDegreeVertex);
//					registerInside(targetVertex);
//				}
//			} else if (sourceIn && !(targetIn) && (sourceVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside)) {
//				addVertex(sourceVertex);
//				addVertex(targetVertex);
//				addEdge(wrapper);
//				if (!sourceIsRegisteredInside) {
//					reduceNeighborIncidence(minDegreeVertex);
//					removeVertex(minDegreeVertex);
//					registerInside(sourceVertex);
//				}
//			} else if (targetIn && !(sourceIn) && (targetVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside)) {
//				addVertex(sourceVertex);
//				addVertex(targetVertex);
//				addEdge(wrapper);
//				if (!targetIsRegisteredInside) {
//					reduceNeighborIncidence(minDegreeVertex);
//					removeVertex(minDegreeVertex);
//					registerInside(targetVertex);
//				}
//			} else {
//				System.out.println("nonIdentityWrapper not Added!");
//			}
//		}
//	}
//	
//	private static void addNonIdentityWrapperLayout(VVEdgeWrapper wrapper) {
//		//checken welche Knoten bereits Koordinaten haben. Es muss NICHT immer mindestens ein Knoten bereits Koordinaten haben! (siehe zoomIn3 und pan4)
//		//Wenn beide Koordinaten haben, dann kann mit ihnen wie gewöhnlich verfahren werden
//		//Wenn nur ein Knoten Koordinaten hat, dann muss der andere inside sein und damit wird die Kapazität um mindestens 1 verringert
//			//TODO
//		VertexCustom sourceVertex = wrapper.getSourceVertex();
//		VertexCustom targetVertex = wrapper.getTargetVertex();
//		String sourceId = sourceVertex.getIdGradoop();
//		String targetId = targetVertex.getIdGradoop();
//		VertexCustom sourceLayouted = null;
//		VertexCustom targetLayouted = null;
//		boolean sourceIsRegisteredInside = newVertices.containsKey(sourceId) || innerVertices.containsKey(sourceId);
//		boolean targetIsRegisteredInside = newVertices.containsKey(targetId) || innerVertices.containsKey(targetId);
//		if (layoutedVertices.containsKey(sourceVertex.getIdGradoop())) sourceLayouted = layoutedVertices.get(sourceVertex.getIdGradoop());
//		if (layoutedVertices.containsKey(targetVertex.getIdGradoop())) targetLayouted = layoutedVertices.get(targetVertex.getIdGradoop());
//		
//		//Both Nodes have coordinates and can be treated as usual
//		if (sourceLayouted != null && targetLayouted != null) {
//			sourceVertex.setX(sourceLayouted.getX());
//			sourceVertex.setY(sourceLayouted.getY());
//			targetVertex.setX(targetLayouted.getX());
//			targetVertex.setY(targetLayouted.getY());
//			addNonIdentityWrapper(wrapper);
//		}
//		
//		//Only one node has coordinates, then this node is necessarily already visualized and the other node necessarily needs to be layouted inside
//		else if (sourceLayouted != null) {
//			if (capacity > 0) {
//				addVertex(targetVertex);
//				if (!targetIsRegisteredInside) {
//					updateMinDegreeVertex(targetVertex);
//					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
//					capacity -= 1;
//				}
//				addEdge(wrapper);
//			} else {
//				if (targetVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside) {
//					addVertex(targetVertex);
//					addEdge(wrapper);
//					if (!targetIsRegisteredInside) {
//						reduceNeighborIncidence(minDegreeVertex);
//						removeVertex(minDegreeVertex);
//						registerInside(targetVertex);
//					}
//				} else {
//					System.out.println("nonIdentityWrapper not Added!");
//				}
//			}
//		} else if (targetLayouted != null) {
//			if (capacity > 0) {
//				addVertex(sourceVertex);
//				if (!sourceIsRegisteredInside) {
//					updateMinDegreeVertex(sourceVertex);
//					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
//					capacity -= 1;
//				}
//				addEdge(wrapper);
//			} else {
//				if (sourceVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside) {
//					addVertex(sourceVertex);
//					addEdge(wrapper);
//					if (!sourceIsRegisteredInside) {
//						reduceNeighborIncidence(minDegreeVertex);
//						removeVertex(minDegreeVertex);
//						registerInside(sourceVertex);
//					}
//				} else {
//					System.out.println("nonIdentityWrapper not Added!");
//				}
//			}
//		}
//		
//		//Both nodes do not have coordinates. Then both nodes necessarily need to be layouted inside
//		else {
//			if (capacity > 1) {
//				addVertex(sourceVertex);
//				if (!sourceIsRegisteredInside){
//					updateMinDegreeVertex(sourceVertex);
//					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
//					capacity -= 1;
//				}
//				addVertex(targetVertex);
//				if (!targetIsRegisteredInside){
//					updateMinDegreeVertex(targetVertex);
//					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
//					capacity -= 1;
//				}
//				addEdge(wrapper);
//			} else if (capacity == 1) {
//				boolean sourceAdmission = false;
//				boolean targetAdmission = false;
//				if (sourceVertex.getDegree() > targetVertex.getDegree()) {
//					addVertex(sourceVertex);
//					sourceAdmission = true;
//					if (targetVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside) {
//						addVertex(targetVertex);
//						targetAdmission = true;
//						addEdge(wrapper);
//					}
//				} else {
//					addVertex(targetVertex);
//					targetAdmission = true;
//					if (sourceVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside) {
//						addVertex(sourceVertex);
//						sourceAdmission = true;
//						addEdge(wrapper);
//					}
//				}
//				if (!sourceIsRegisteredInside && sourceAdmission && !targetIsRegisteredInside && targetAdmission) {
//					reduceNeighborIncidence(minDegreeVertex);
//					removeVertex(minDegreeVertex);
//					newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
//					newVertices.put(targetVertex.getIdGradoop(), targetVertex);
//					updateMinDegreeVertices(newVertices);
//				} else if (!sourceIsRegisteredInside && sourceAdmission) {
//					registerInside(sourceVertex);
//				} else if (!targetIsRegisteredInside && targetAdmission) {
//					registerInside(targetVertex);
//				}
//				capacity -= 1 ;
//			} else {
//				if ((sourceVertex.getDegree() > secondMinDegreeVertex.getDegree()) && 
//						(targetVertex.getDegree() > secondMinDegreeVertex.getDegree())) {
//					addVertex(sourceVertex);
//					addVertex(targetVertex);
//					addEdge(wrapper);
//					if (!sourceIsRegisteredInside && !targetIsRegisteredInside) {
//						reduceNeighborIncidence(minDegreeVertex);
//						reduceNeighborIncidence(secondMinDegreeVertex);
//						removeVertex(secondMinDegreeVertex);
//						removeVertex(minDegreeVertex);
//						newVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
//						newVertices.put(targetVertex.getIdGradoop(), targetVertex);
//						updateMinDegreeVertices(newVertices);
//					} else if (!sourceIsRegisteredInside) {
//						reduceNeighborIncidence(minDegreeVertex);
//						removeVertex(minDegreeVertex);
//						registerInside(sourceVertex);
//					} else if (!targetIsRegisteredInside) {
//						reduceNeighborIncidence(minDegreeVertex);
//						removeVertex(minDegreeVertex);
//						registerInside(targetVertex);
//					}
//				} else if (sourceVertex.getDegree() > minDegreeVertex.getDegree() || sourceIsRegisteredInside) {
//					addVertex(sourceVertex);
//					if (!sourceIsRegisteredInside) {
//						reduceNeighborIncidence(minDegreeVertex);
//						removeVertex(minDegreeVertex);
//						registerInside(sourceVertex);
//					}
//				} else if (targetVertex.getDegree() > minDegreeVertex.getDegree() || targetIsRegisteredInside) {
//					addVertex(targetVertex);
//					if (!targetIsRegisteredInside) {
//						reduceNeighborIncidence(minDegreeVertex);
//						removeVertex(minDegreeVertex);
//						registerInside(targetVertex);
//					}
//				} else {
//					System.out.println("nonIdentityWrapper not Added!");
//				}
//			}
//		}
//	}
//	
//	private static void updateMinDegreeVertex(VertexCustom vertex) {
//		//CHANGE: < to <=
//		if (vertex.getDegree() <= minDegreeVertex.getDegree()) {
//			secondMinDegreeVertex = minDegreeVertex;
//			minDegreeVertex = vertex;
//		} else if (vertex.getDegree() <= secondMinDegreeVertex.getDegree()) {
//			secondMinDegreeVertex = vertex;
//		}		
//	}
//
//	private static void updateMinDegreeVertices(Map<String, VertexCustom> map) {
//		System.out.println("in updateMinDegreeVertices");
//		Collection<VertexCustom> collection = map.values();
//		Iterator<VertexCustom> iter = collection.iterator();
//		minDegreeVertex = iter.next();
//		secondMinDegreeVertex = iter.next();
//		System.out.println("Initial min degree vertices: " + minDegreeVertex.getIdGradoop()+ " " + secondMinDegreeVertex.getIdGradoop());
//		if (secondMinDegreeVertex.getDegree() < minDegreeVertex.getDegree()) {
//			VertexCustom temp = minDegreeVertex;
//			minDegreeVertex = secondMinDegreeVertex;
//			secondMinDegreeVertex = temp;
//		}
//		for (Map.Entry<String, VertexCustom> entry : map.entrySet()) {
//			VertexCustom vertex = entry.getValue();
//			if (vertex.getDegree() < minDegreeVertex.getDegree() && !vertex.getIdGradoop().equals(secondMinDegreeVertex.getIdGradoop())) {
//				secondMinDegreeVertex = minDegreeVertex;
//				minDegreeVertex = vertex;
//			} else if (vertex.getDegree() < secondMinDegreeVertex.getDegree() && !vertex.getIdGradoop().equals(minDegreeVertex.getIdGradoop()))  {
//				secondMinDegreeVertex = vertex;
//			}
//		}
//		System.out.println("Final min degree vertices: " + minDegreeVertex.getIdGradoop() + " " + secondMinDegreeVertex.getIdGradoop());
//	}
//
//	private static void removeVertex(VertexCustom vertex) {	
//		if (!globalVertices.containsKey(vertex.getIdGradoop())) {
//			System.out.println("cannot remove vertex because not in vertexGlobalMap, id: " + vertex.getIdGradoop());
//		} else {
//			newVertices.remove(vertex.getIdGradoop());
//			globalVertices.remove(vertex.getIdGradoop());
//			Main.sendToAll("removeObjectServer;" + vertex.getIdGradoop());
//			Map<String,String> vertexNeighborMap = flinkCore.getGraphUtil().getAdjMatrix().get(vertex.getIdGradoop());
//			Iterator<Map.Entry<String, VVEdgeWrapper>> iter = edges.entrySet().iterator();
//			while (iter.hasNext()) if (vertexNeighborMap.values().contains(iter.next().getKey())) iter.remove();
//			System.out.println("Removing Obect in removeVertex, ID: " + vertex.getIdGradoop());
//		}
//	}
//
//	private static void reduceNeighborIncidence(VertexCustom vertex) {
//		Set<String> neighborIds = getNeighborhood(vertex);
//		for (String neighbor : neighborIds) {
//			if (globalVertices.containsKey(neighbor)) {
//				Map<String,Object> map = globalVertices.get(neighbor);
//				map.put("incidence", (int) map.get("incidence") - 1); 
//			}
//		}
//	}
//
//	private static void addWrapperIdentity(VertexCustom vertex) {
//		System.out.println("In addWrapperIdentity");
//		String vertexId = vertex.getIdGradoop();
//		boolean vertexIsRegisteredInside = newVertices.containsKey(vertexId) || innerVertices.containsKey(vertexId);
//		if (capacity > 0) {
//			addVertex(vertex);
//			if (!vertexIsRegisteredInside) {
//				newVertices.put(vertex.getIdGradoop(), vertex);
//				updateMinDegreeVertex(vertex);
//				capacity -= 1;
//			}
//		} else {
//			System.out.println("In addWrapperIdentity, declined capacity < 0");
//			if (vertex.getDegree() > minDegreeVertex.getDegree()) {
//				addVertex(vertex);
//				if (!vertexIsRegisteredInside) {
//					reduceNeighborIncidence(minDegreeVertex);
//					removeVertex(minDegreeVertex);
//					registerInside(vertex);
//				}
//			} else {
//				System.out.println("In addWrapperIdentity, declined vertexDegree > minDegreeVertexDegree");
//			}
//		}
//	}
//	
//	private static void addWrapperIdentityLayout(VertexCustom vertex) {
//		System.out.println("In addWrapperIdentityLayout");
//		String vertexId = vertex.getIdGradoop();
//		boolean vertexIsRegisteredInside = newVertices.containsKey(vertexId) || innerVertices.containsKey(vertexId);
//		if (capacity > 0) {
//			addVertex(vertex);
//			if (!vertexIsRegisteredInside) {
//				newVertices.put(vertex.getIdGradoop(), vertex);
//				updateMinDegreeVertex(vertex);
//				capacity -= 1;
//			}
//		} else {
//			System.out.println("In addWrapperIdentityLayout, declined capacity > 0");
//			if (vertex.getDegree() > minDegreeVertex.getDegree()) {
//				addVertex(vertex);
//				if (!vertexIsRegisteredInside) {
//					reduceNeighborIncidence(minDegreeVertex);
//					removeVertex(minDegreeVertex);
//					registerInside(vertex);
//				}
//			} else {
//				System.out.println("In addWrapperIdentity, declined vertexDegree > minDegreeVertexDegree");
//				System.out.println("identityWrapper not Added!");
//			}
//		}
//	}
//
//	private static void addWrapperIdentityInitial(VertexCustom vertex) {
//		boolean added = addVertex(vertex);
//		if (added) innerVertices.put(vertex.getIdGradoop(), vertex);
//	}
//	
//	private static void addNonIdentityWrapperInitial(VVEdgeWrapper wrapper) {
//		VertexCustom sourceVertex = wrapper.getSourceVertex();
//		VertexCustom targetVertex = wrapper.getTargetVertex();
//		boolean addedSource = addVertex(sourceVertex);
//		if (addedSource) innerVertices.put(sourceVertex.getIdGradoop(), sourceVertex);
//		boolean addedTarget = addVertex(targetVertex);
//		if (addedTarget) innerVertices.put(targetVertex.getIdGradoop(), targetVertex);
//		addEdge(wrapper);
//	}
//	
//	private static boolean addVertex(VertexCustom vertex) {
//		System.out.println("In addVertex");
//		String sourceId = vertex.getIdGradoop();
//		System.out.println("globalVertices is null?: " + globalVertices);
//		if (!(globalVertices.containsKey(sourceId))) {
//			Map<String,Object> map = new HashMap<String,Object>();
//			map.put("incidence", (int) 1);
//			map.put("vertex", vertex);
//			globalVertices.put(sourceId, map);
//			if (layout) {
//				Main.sendToAll("addVertexServer;" + vertex.getIdGradoop() + ";" + vertex.getX() + ";" + vertex.getY() + ";" + vertex.getIdNumeric());
//				Main.sentToClientInSubStep = true;
//			} else {
//				if (layoutedVertices.containsKey(vertex.getIdGradoop())) {
//					VertexCustom layoutedVertex = layoutedVertices.get(vertex.getIdGradoop());
//					Main.sendToAll("addVertexServer;" + vertex.getIdGradoop() + ";" + layoutedVertex.getX() + ";" + layoutedVertex.getY() + ";" 
//							+ vertex.getIdNumeric());
//					Main.sentToClientInSubStep = true;
//				} else {
//					Main.sendToAll("addVertexServerToBeLayouted;" + vertex.getIdGradoop() + ";" + vertex.getDegree() + ";" + vertex.getIdNumeric());
//					Main.sentToClientInSubStep = true;
//				}
//			}
//			return true;
//		} else {
//			System.out.println("In addVertex, declined because ID contained in globalVertices");
//			Map<String,Object> map = globalVertices.get(sourceId);
//			map.put("incidence", (int) map.get("incidence") + 1);
//			return false;
//		}		
//	}
//	
//	private static void addEdge(VVEdgeWrapper wrapper) {
//		edges.put(wrapper.getEdgeIdGradoop(), wrapper);
//		Main.sendToAll("addEdgeServer;" + wrapper.getEdgeIdGradoop() + ";" + wrapper.getSourceIdGradoop() + ";" + wrapper.getTargetIdGradoop());
//		Main.sentToClientInSubStep = true;
//	}
//	
//	private static void clearOperation(){
//		System.out.println("in clear operation");
//		System.out.println(operation);
//		if (operation != "initial"){
//			for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) System.out.println("innerVertex " + entry.getValue().getIdGradoop());
//			if (!layout) {
//				for (VertexCustom vertex : newVertices.values()) {
//					VertexCustom layoutedVertex = layoutedVertices.get(vertex.getIdGradoop());
//					vertex.setX(layoutedVertex.getX());
//					vertex.setY(layoutedVertex.getY());
//				}
//			}
//			System.out.println("innerVertices size before put all: " + innerVertices.size());
//			innerVertices.putAll(newVertices); 
//			System.out.println("innerVertices size after put all: " + innerVertices.size());
//			for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) System.out.println("innerVertex after putAll" + entry.getValue().getIdGradoop() + " " 
//					+ entry.getValue().getX());
//			System.out.println("global...");
//			Iterator<Map.Entry<String, Map<String,Object>>> iter = globalVertices.entrySet().iterator();
//			while (iter.hasNext()) {
//				VertexCustom vertex = (VertexCustom) iter.next().getValue().get("vertex");
//				if ((((vertex.getX() < leftModel) || (rightModel < vertex.getX()) || (vertex.getY() < topModel) || 
//						(bottomModel < vertex.getY())) && !hasVisualizedNeighborsInside(vertex)) ||
//							((vertex.getX() >= leftModel) && (rightModel >= vertex.getX()) && (vertex.getY() >= topModel) && (bottomModel >= vertex.getY()) 
//									&& !innerVertices.containsKey(vertex.getIdGradoop()))) {
//					System.out.println("removing in clear operation " + vertex.getIdGradoop());
//					Main.sendToAll("removeObjectServer;" + vertex.getIdGradoop());
//					iter.remove();
//					Map<String,String> vertexNeighborMap = flinkCore.getGraphUtil().getAdjMatrix().get(vertex.getIdGradoop());
//					Iterator<Map.Entry<String, VVEdgeWrapper>> edgesIterator = edges.entrySet().iterator();
//					while (edgesIterator.hasNext()) if (vertexNeighborMap.values().contains(edgesIterator.next().getKey())) edgesIterator.remove();
//				} 
//			}
//			//this is necessary in case the (second)minDegreeVertex will get deleted in the clear up step before (e.g. in ZoomOut)
//			if (newVertices.size() > 1) {
//				updateMinDegreeVertices(newVertices);
//			} else if (newVertices.size() == 1) {
//				minDegreeVertex = newVertices.values().iterator().next();
//			}
//		} else {
//			if (!layout) {
//				for (VertexCustom vertex : innerVertices.values()) {
//					VertexCustom layoutedVertex = layoutedVertices.get(vertex.getIdGradoop());
//					System.out.println(layoutedVertices.get(vertex.getIdGradoop()).getIdGradoop() + layoutedVertex.getX());
//					vertex.setX(layoutedVertex.getX());
//					vertex.setY(layoutedVertex.getY());
//				}
//			}
//			newVertices = innerVertices;
//			if (newVertices.size() > 1) {
//				updateMinDegreeVertices(newVertices);
//			} else if (newVertices.size() == 1) {
//				minDegreeVertex = newVertices.values().iterator().next();
//			}
//		}
////		operation = null;
//		Set<String> visualizedVertices = new HashSet<String>();
//		for (Map.Entry<String, VertexCustom> entry : innerVertices.entrySet()) visualizedVertices.add(entry.getKey());
//		Set<String> visualizedWrappers = new HashSet<String>();
//		for (Map.Entry<String, VVEdgeWrapper> entry : edges.entrySet()) visualizedWrappers.add(entry.getKey());
//		GraphUtil graphUtil =  flinkCore.getGraphUtil();
//		graphUtil.setVisualizedVertices(visualizedVertices);
//		graphUtil.setVisualizedWrappers(visualizedWrappers);
//		System.out.println("global size "+ globalVertices.size());
////		for (Map.Entry<String, Map<String,Object>> map : globalVertices.entrySet()) {
////			VertexCustom vertex = (VertexCustom) map.getValue().get("vertex");
////			System.out.println("vertex id: "  + vertex.getIdGradoop());
////			System.out.println("vertex degree: " + vertex.getDegree());
////			System.out.println("vertex incidence: " + map.getValue().get("incidence"));
////			System.out.println(vertex.getDegree() + 1 >= (Integer) map.getValue().get("incidence"));
////		}
//	}
//	
//	private static Set<String> getNeighborhood(VertexCustom vertex){
//		Set<String> neighborIds = new HashSet<String>();
////		for (VVEdgeWrapper wrapper : edges.values()) {
////			String vertexId = vertex.getIdGradoop();
////			String sourceId = wrapper.getSourceIdGradoop();
////			String targetId = wrapper.getTargetIdGradoop();
////			if (sourceId.equals(vertexId)) neighborIds.add(targetId);
////			else if (targetId.equals(vertexId)) neighborIds.add(sourceId);
////		}
////		return neighborIds;
//		Map<String,Map<String,String>> adjMatrix = flinkCore.getGraphUtil().getAdjMatrix();
//		for (Map.Entry<String, String> entry : adjMatrix.get(vertex.getIdGradoop()).entrySet()) neighborIds.add(entry.getKey());
//		return neighborIds;
//	}
//	
//	private static boolean hasVisualizedNeighborsInside(VertexCustom vertex) {
////		for (VVEdgeWrapper wrapper : edges.values()) {
////			String vertexId = vertex.getIdGradoop();
////			String sourceId = wrapper.getSourceIdGradoop();
////			String targetId = wrapper.getTargetIdGradoop();
////			if (sourceId.equals(vertexId) && innerVertices.containsKey(targetId)) return true;
////			else if (targetId.equals(vertexId) && innerVertices.containsKey(sourceId)) return true;
////		}
////		return false;
//		Map<String,Map<String,String>> adjMatrix = flinkCore.getGraphUtil().getAdjMatrix();
//		for (Map.Entry<String, String> entry : adjMatrix.get(vertex.getIdGradoop()).entrySet()) if (innerVertices.containsKey(entry.getKey())) return true;
//		return false;
//	}
//	

}
