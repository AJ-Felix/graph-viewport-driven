package aljoschaRydzyk.viewportDrivenGraphStreaming;

import static io.undertow.Handlers.path;
import static io.undertow.Handlers.websocket;

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.Eval.Evaluator;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.FlinkCore;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperMapLine;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperMapLineNoCoordinates;
import io.undertow.Undertow;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

public class Server implements Serializable{
	
	private String clusterEntryPointAddress = "localhost";
	private String hdfsEntryPointAddress;
	private int hdfsEntryPointPort;
	private String graphFilesDirectory;
	private String gradoopGraphId;
	private boolean stream = true;
	private float topModelBorder = 0;
	private float rightModelBorder = 4000;
	private float bottomModelBorder = 4000; 
	private float leftModelBorder = 0;
	private int edgeCount = 200;
	private List<WrapperGVD> wrapperCollection;
	private FlinkCore flinkCore;
	public ArrayList<WebSocketChannel> channels = new ArrayList<>();
    private String webSocketListenPath = "/";
    private int webSocketListenPort = 8897;    
    private int maxVertices;
    private int vertexCountNormalizationFactor = 5000;
    private boolean layout = true;
    private int operationStep;
    private Float viewportPixelX;
    private Float viewportPixelY;
	private String operation;
	public boolean sentToClientInSubStep;
    private WrapperHandler wrapperHandler;
    private FlinkResponseHandler flinkResponseHandler;
    private String localMachinePublicIp4 = "localhost";
    private int flinkResponsePort = 8898;
	private int vertexZoomLevel;
	private boolean maxVerticesLock = false;
	private int parallelism = 4;
	
	private static Server INSTANCE;
	
	//evaluation
	private boolean eval;
	private Evaluator evaluator;

    public Server () {
    	System.out.println("executing server constructor");

	}
    public static Server getInstance() {
    	if (INSTANCE == null) {
    		INSTANCE = new Server();
    	}
    	return INSTANCE;
    }
    
    public void setEvaluation(boolean eval) {
    	this.eval = eval;
		System.out.println("Evaluator Mode enabled!");
    }
    
    public void initializeServerFunctionality() {
    	Undertow server = Undertow.builder()
    			.addHttpListener(webSocketListenPort, localMachinePublicIp4)
                .setHandler(path()
                	.addPrefixPath(webSocketListenPath, websocket((exchange, channel) -> {
	                    channels.add(channel);
	                    channel.getReceiveSetter().set(getListener());
	                    channel.resumeReceives();
	                }))
            	)
                .build();
    	server.start();
        System.out.println("Server started!");  
    }
    
    public void initializeHandlers() {
    	wrapperHandler = new WrapperHandler();
  		wrapperHandler.initializeGraphRepresentation(edgeCount);
  		wrapperHandler.initializeAPI(localMachinePublicIp4);
  		
  		//initialize FlinkResponseHandler
        flinkResponseHandler = new FlinkResponseHandler(wrapperHandler);
        flinkResponseHandler.start();
    }
    
    public void setPublicIp4Adress() throws SocketException {
    	Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()){
            NetworkInterface intFace = interfaces.nextElement();
            System.out.println(intFace);
            if (!intFace.isUp() || intFace.isLoopback() || intFace.isVirtual()) continue;
            Enumeration<InetAddress> addresses = intFace.getInetAddresses();
            while (addresses.hasMoreElements()){
                InetAddress address = addresses.nextElement();
                if (address.isLoopbackAddress()) continue;
                if (address instanceof Inet4Address) {
                	localMachinePublicIp4 = address.getHostAddress().toString();
                	System.out.println("adress :" + address.getHostAddress().toString());
                }
            }
            
            //debug 
//            localMachinePublicIp4 = "172.22.87.188";
            
            if (localMachinePublicIp4.equals("localhost")) System.out.println("Server address set to 'localhost' (default).");
        }
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
                if (messageData.startsWith("layoutMode")) {
                	if (messageData.split(";")[1].equals("true")) {
                    	setLayoutMode(true);
                	} else {
                    	setLayoutMode(false);
                    	wrapperHandler.resetLayoutedVertices();
                	}
                } else if (messageData.equals("resetWrapperHandler")) {
                	wrapperHandler.initializeGraphRepresentation(edgeCount);
                } else if (messageData.startsWith("clusterEntryAddress")) {
                	clusterEntryPointAddress = messageData.split(";")[1];
                } else if (messageData.startsWith("hDFSEntryAddress")) {
                	hdfsEntryPointAddress = messageData.split(";")[1];
                } else if (messageData.startsWith("hDFSEntryPointPort")) {
                	hdfsEntryPointPort = Integer.parseInt(messageData.split(";")[1]);
                } else if (messageData.startsWith("graphFolderDirectory")) {
                	graphFilesDirectory = messageData.split(";")[1];
                } else if (messageData.startsWith("gradoopGraphId")) {
                	gradoopGraphId = messageData.split(";")[1];
                } else if (messageData.startsWith("parallelism")) {	
                	parallelism = Integer.parseInt(messageData.split(";")[1]);
                } else if (messageData.startsWith("viewportSize")) {
            		String[] arrMessageData = messageData.split(";");
                	Float xRenderPos = Float.parseFloat(arrMessageData[1]);
                	Float yRenderPos = Float.parseFloat(arrMessageData[2]);
                	Float zoomLevel = Float.parseFloat(arrMessageData[3]);
                	float viewportPixelXOld = (float) 0;
                	float viewportPixelYOld = (float) 0;
                	if (!wrapperHandler.getGlobalVertices().isEmpty()) {
                		viewportPixelXOld = viewportPixelX;
                    	viewportPixelYOld = viewportPixelY;
                	}
                	viewportPixelX = Float.parseFloat(arrMessageData[4]);
                	viewportPixelY = Float.parseFloat(arrMessageData[5]); 
                	if (wrapperHandler.getGlobalVertices().isEmpty()) {
                		calculateInitialViewportSettings(topModelBorder, rightModelBorder, bottomModelBorder, leftModelBorder);
                	} else {
                    	Float topModel = - yRenderPos / zoomLevel;
                    	Float leftModel = - xRenderPos / zoomLevel;
                    	Float bottomModel = - yRenderPos / zoomLevel + viewportPixelY / zoomLevel;
                    	Float rightModel = -xRenderPos / zoomLevel + viewportPixelX / zoomLevel;
    					if (!maxVerticesLock) calculateMaxVertices(topModel, rightModel, bottomModel, leftModel, zoomLevel);
                		flinkCore.setModelPositionsOld();
            			setModelPositions(topModel, rightModel, bottomModel, leftModel);
            			if (viewportPixelX < viewportPixelXOld || viewportPixelY < viewportPixelYOld) {
    						setOperation("zoomIn");
    						wrapperHandler.prepareOperation();
    						if (layout) {
    							if (stream) zoomStream();
    							else zoomSet();
    						} else {								
    							System.out.println("in zoom in layout function");
    							if (stream) zoomInLayoutStep1Stream();
    							else  zoomInLayoutStep1Set();				
    						}
    					} else {
    						setOperation("zoomOut");
    						wrapperHandler.prepareOperation();
    						if (layout) {
    							if (stream) zoomStream();
    							else zoomSet();
    						}
    						else {
    							if (stream)	zoomOutLayoutStep1Stream();
    							else zoomOutLayoutStep1Set();
    						}
    					}
                  	} 
                } else if (messageData.startsWith("layoutBaseString")) {
                	String[] arrMessageData = messageData.split(";");
                	List<String> list = new ArrayList<String>(Arrays.asList(arrMessageData));
                	list.remove(0);
                	wrapperHandler.updateLayoutedVertices(list);
                	nextSubStep();
                } else if (messageData.startsWith("fit")) {
                	String[] arrMessageData = messageData.split(";");
                	Float xRenderPosition = Float.parseFloat(arrMessageData[1]);
        			Float yRenderPosition = Float.parseFloat(arrMessageData[2]);
        			Float zoomLevel = Float.parseFloat(arrMessageData[3]);
        			Float topModel = (- yRenderPosition / zoomLevel);
        			Float leftModel = (- xRenderPosition /zoomLevel);
        			Float bottomModel = (topModel + viewportPixelY / zoomLevel);
        			Float rightModel = (leftModel + viewportPixelX / zoomLevel);
        			flinkCore.setModelPositionsOld();
        			setModelPositions(topModel, rightModel, bottomModel, leftModel);
                } else if (messageData.startsWith("buildTopView")) {
                	if (hdfsEntryPointAddress == null) 
                		flinkCore = new FlinkCore(clusterEntryPointAddress, "file://" + graphFilesDirectory, 
                    			gradoopGraphId, parallelism);
                	else 
                		flinkCore = new FlinkCore(clusterEntryPointAddress, "hdfs://" + hdfsEntryPointAddress + ":" + 
                				String.valueOf(hdfsEntryPointPort) + graphFilesDirectory, gradoopGraphId, parallelism);
                	
                	setModelPositions(topModelBorder, rightModelBorder, bottomModelBorder, leftModelBorder);
                	setOperation("initial");
                	if (!layout) setOperationStep(1);
                	String[] arrMessageData = messageData.split(";");
                	if (arrMessageData[1].equals("CSV")) {
                    	setStreamBool(true);
                		buildTopViewCSV();
        			} else if (arrMessageData[1].equals("adjacency")) {
                    	setStreamBool(true);
        				buildTopViewAdjacency();
        			} else if (arrMessageData[1].equals("gradoop")) {
                    	setStreamBool(false);
        				buildTopViewGradoop();
        			}
                	setVertexZoomLevel(0);
                } else if (messageData.startsWith("zoom")) {
        			String[] arrMessageData = messageData.split(";");
        			Float xRenderPosition = Float.parseFloat(arrMessageData[1]);
        			Float yRenderPosition = Float.parseFloat(arrMessageData[2]);
        			Float zoomLevel = Float.parseFloat(arrMessageData[3]);
        			Float topModel = (- yRenderPosition / zoomLevel);
        			Float leftModel = (- xRenderPosition /zoomLevel);
        			Float bottomModel = (topModel + viewportPixelY / zoomLevel);
        			Float rightModel = (leftModel + viewportPixelX / zoomLevel);
        			flinkCore.setModelPositionsOld();
        			setModelPositions(topModel, rightModel, bottomModel, leftModel);
        			if (!maxVerticesLock) calculateMaxVertices(topModel, rightModel, bottomModel, leftModel, zoomLevel);
					if (messageData.startsWith("zoomIn")) {
						setOperation("zoomIn");
						setVertexZoomLevel(vertexZoomLevel + 1);
						wrapperHandler.prepareOperation();
						if (layout) {
							if (stream) zoomStream();
							else zoomSet();
						} else {								
							if (stream) zoomInLayoutStep1Stream();
							else  zoomInLayoutStep1Set();				
						}
					} else {
						setOperation("zoomOut");
						setVertexZoomLevel(vertexZoomLevel - 1);
						wrapperHandler.prepareOperation();
						if (layout) {
							if (stream) zoomStream();
							else zoomSet();
						} else {
							if (stream)	zoomOutLayoutStep1Stream();
							else zoomOutLayoutStep1Set();
						}
					}
    			} else if (messageData.startsWith("pan")) {
        			String[] arrMessageData = messageData.split(";");
        			float[] modelPositions = flinkCore.getModelPositions();
        			Float topModelOld = modelPositions[0];
        			Float rightModelOld = modelPositions[1];
        			Float bottomModelOld = modelPositions[2];
        			Float leftModelOld = modelPositions[3];
        			Float xModelDiff = Float.parseFloat(arrMessageData[1]); 
        			Float yModelDiff = Float.parseFloat(arrMessageData[2]);
        			float zoomLevel = Float.parseFloat(arrMessageData[3]);
        			Float topModel = topModelOld + yModelDiff;
        			Float bottomModel = bottomModelOld + yModelDiff;
        			Float leftModel = leftModelOld + xModelDiff;
        			Float rightModel = rightModelOld + xModelDiff;
					flinkCore.setModelPositionsOld();
					setModelPositions(topModel, rightModel, bottomModel, leftModel);
					if (!maxVerticesLock) calculateMaxVertices(topModel, rightModel, bottomModel, leftModel, zoomLevel);
					setOperation("pan");
    				wrapperHandler.prepareOperation();
					if (!layout) {
						if (stream) panLayoutStep1Stream();
						else panLayoutStep1Set();
					} else {
						if (stream)	panStream();
						else panSet();
					}
				} 
            }
        };
    }
    
    private void setVertexZoomLevel(int zoomLevel) {
		this.vertexZoomLevel = zoomLevel;
		flinkCore.setVertexZoomLevel(zoomLevel);	
	}
    
    private void buildTopViewGradoop() {
    	flinkCore.initializeGradoopGraphUtil();
		wrapperHandler.initializeGraphRepresentation(edgeCount);
    	DataSet<WrapperGVD> wrapperSet = flinkCore.buildTopViewGradoop(maxVertices);
    	wrapperCollection = executeSet(wrapperSet);
		wrapperHandler.addWrapperCollectionInitial(wrapperCollection);
		sendToAll("finalOperations");
		wrapperHandler.clearOperation();
    }
    
    private void buildTopViewCSV() {
    	flinkCore.initializeCSVGraphUtilJoin();
		DataStream<Row> wrapperStream = flinkCore.buildTopViewCSV(maxVertices);
		wrapperHandler.initializeGraphRepresentation(edgeCount);
		flinkResponseHandler.setOperation("initialAppend");
		DataStream<String> wrapperLine;
		if (layout) {
			flinkResponseHandler.setVerticesHaveCoordinates(true);
			wrapperLine = wrapperStream.map(new WrapperMapLine());
		} else {
			flinkResponseHandler.setVerticesHaveCoordinates(false);
			wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
		}
		wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, 
				new SimpleStringSchema(), 3, true)).setParallelism(1);	
		new FlinkExecutorThread(operation, flinkCore.getFsEnv(), eval).start();
    }
    
    private void buildTopViewAdjacency() {
    	flinkCore.initializeAdjacencyGraphUtil();
		wrapperHandler.initializeGraphRepresentation(edgeCount);
		flinkResponseHandler.setOperation("initialAppend");
		DataStream<Row> wrapperStream = flinkCore.buildTopViewAdjacency(maxVertices);
		DataStream<String> wrapperLine;
		if (layout) {
			flinkResponseHandler.setVerticesHaveCoordinates(true);
			wrapperLine = wrapperStream.map(new WrapperMapLine());
		} else {
			flinkResponseHandler.setVerticesHaveCoordinates(false);
			wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
		}
		wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, 
				new SimpleStringSchema())).setParallelism(1);
		new FlinkExecutorThread(operation, flinkCore.getFsEnv(), eval).start();
    }
    
    private void zoomSet() {
    	DataSet<WrapperGVD> wrapperSet = flinkCore.zoomSet();
    	setOperationHelper(wrapperSet);
		if (!wrapperHandler.getSentToClientInSubStep()) sendToAll("enableMouse");
		else wrapperHandler.clearOperation();

    }
    
    private void zoomStream() {
    	DataStream<Row> wrapperStream = flinkCore.zoomStream();
		flinkResponseHandler.setVerticesHaveCoordinates(true);
		DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLine());
		wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, 
				new SimpleStringSchema())).setParallelism(1);
		wrapperHandler.setSentToClientInSubStep(false);
		new FlinkExecutorThread(operation, flinkCore.getFsEnv(), eval).start();
    }
    
    private void panSet() {
    	DataSet<WrapperGVD> wrapperSet = flinkCore.panSet();
    	setOperationHelper(wrapperSet);
		if (!wrapperHandler.getSentToClientInSubStep()) sendToAll("enableMouse");
		else wrapperHandler.clearOperation();
    }
    
    private void panStream() {
		flinkResponseHandler.setVerticesHaveCoordinates(true);
    	DataStream<Row> wrapperStream = flinkCore.pan();	
		DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLine());
		wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, 
				new SimpleStringSchema())).setParallelism(1);
		wrapperHandler.setSentToClientInSubStep(false);
		new FlinkExecutorThread(operation, flinkCore.getFsEnv(), eval).start();
    }
    
    private void nextSubStep() {
    	System.out.println("in nextSubStep function");
    	System.out.println("operation: " + operation);
    	System.out.println("operationStep: " + operationStep);
    	if (operation.equals("initial")) {
            wrapperHandler.clearOperation();
    	} else if (operation.equals("zoomIn") || operation.equals("pan")) {
			if (operationStep == 1) {
				if (wrapperHandler.getCapacity() > 0) {
					if (stream)	layoutingStreamOperation(2);
					else layoutingSetOperation(2);
				} else {
					System.out.println("nextSubStep 1, 4");
					if (stream)	layoutingStreamOperation(4);
					else layoutingSetOperation(4);
				}
			} else if (operationStep == 2) {
				if (wrapperHandler.getCapacity() > 0) {
					if (stream)	layoutingStreamOperation(2);
					else layoutingSetOperation(2);
				} else {
					System.out.println("nextSubStep 2, 4");
					if (stream)	layoutingStreamOperation(4);
					else layoutingSetOperation(4);
				}
			} else if (operationStep == 3) {
				if (stream)	layoutingStreamOperation(4);
				else layoutingSetOperation(4);
			} else if (operationStep == 4) {
	            wrapperHandler.clearOperation();
			}
    	} else if (operation.equals("zoomOut")) {
    		if (operationStep == 1) {
				if (stream) layoutingStreamOperation(2);
				else layoutingSetOperation(2);
			} else {
				wrapperHandler.clearOperation();
			}
    	} 
    }
    
    private void layoutingSetOperation(int nextOperationStep) {
    	setOperationStep(nextOperationStep);
    	if (operation.equals("zoomIn") || operation.equals("pan")) {
    		if (nextOperationStep == 1) {
    	    	DataSet<WrapperGVD> wrapperSet = flinkCore.zoomInLayoutStep1Set(wrapperHandler.getLayoutedVertices(), wrapperHandler.getInnerVertices());
    	    	setOperationHelper(wrapperSet);
	        	if (!wrapperHandler.getSentToClientInSubStep()) {
	        		if (wrapperHandler.getCapacity() == 0) layoutingSetOperation(4);
	        		else layoutingSetOperation(2);
    	    	}
    		} else if (nextOperationStep == 2) {
    			DataSet<WrapperGVD> wrapperSet = flinkCore.zoomInLayoutStep2Set(wrapperHandler.getLayoutedVertices(), wrapperHandler.getInnerVertices(), 
    	    			wrapperHandler.getNewVertices());
    	    	setOperationHelper(wrapperSet);
	        	if (!wrapperHandler.getSentToClientInSubStep()) {
	        		if (wrapperHandler.getCapacity() == 0) layoutingSetOperation(4);
	        		else layoutingSetOperation(3);
	        	}
    		} else if (nextOperationStep == 3) {
    			DataSet<WrapperGVD> wrapperSet = flinkCore.zoomInLayoutStep3Set(wrapperHandler.getLayoutedVertices());
    	    	setOperationHelper(wrapperSet);
	        	if (!wrapperHandler.getSentToClientInSubStep()) layoutingSetOperation(4);
    		} else if (nextOperationStep == 4) {
    			DataSet<WrapperGVD> wrapperSet = flinkCore.zoomInLayoutStep4Set(wrapperHandler.getLayoutedVertices(),
    	    			wrapperHandler.getInnerVertices(), wrapperHandler.getNewVertices());
    	    	setOperationHelper(wrapperSet);
	        	if (!wrapperHandler.getSentToClientInSubStep()) {
	        		wrapperHandler.clearOperation();
	        		sendToAll("enableMouse");
	        	}
    		}
    	} else if (operation.equals("zoomOut")) {
    		if (nextOperationStep == 1) {
    	    	flinkResponseHandler.setVerticesHaveCoordinates(false);
    			DataSet<WrapperGVD> wrapperSet = flinkCore.zoomOutLayoutStep1Set(wrapperHandler.getLayoutedVertices());
    	    	setOperationHelper(wrapperSet);
	        	if (!wrapperHandler.getSentToClientInSubStep())	sendToAll("enableMouse");
    		} else if (nextOperationStep == 2) {
    			DataSet<WrapperGVD> wrapperSet = flinkCore.zoomOutLayoutStep2Set(wrapperHandler.getLayoutedVertices(), 
    					wrapperHandler.getNewVertices());
    	    	setOperationHelper(wrapperSet);
	        	if (!wrapperHandler.getSentToClientInSubStep()) {
	        		wrapperHandler.clearOperation();
	        		sendToAll("enableMouse");
	        	}
    		}
    	}
    }
    
    private void setOperationHelper(DataSet<WrapperGVD> wrapperSet) {
    	wrapperHandler.setSentToClientInSubStep(false);
    	wrapperCollection = executeSet(wrapperSet);
    	wrapperHandler.addWrapperCollectionLayout(wrapperCollection);
    }
    
    private void layoutingStreamOperation(int nextOperationStep) {
    	setOperationStep(nextOperationStep);
    	DataStream<Row> wrapperStream = null;
    	if (operation.equals("zoomIn")) {
    		if (nextOperationStep == 1) {
    			flinkResponseHandler.setVerticesHaveCoordinates(false);
    	    	wrapperStream = flinkCore.zoomInLayoutStep1Stream(wrapperHandler.getLayoutedVertices(), 
    	    			wrapperHandler.getInnerVertices());	
    		} else if (nextOperationStep == 2) {
    			wrapperStream = flinkCore.zoomInLayoutStep2Stream(wrapperHandler.getLayoutedVertices(), 
    	    			wrapperHandler.getInnerVertices(), wrapperHandler.getNewVertices());
    		} else if (nextOperationStep == 3) {
    	    	wrapperStream = flinkCore.zoomInLayoutThirdStep(wrapperHandler.getLayoutedVertices());
    		} else if (nextOperationStep == 4) {
    			wrapperStream = flinkCore.zoomInLayoutStep4Stream(wrapperHandler.getLayoutedVertices(),
    	    			wrapperHandler.getInnerVertices(), wrapperHandler.getNewVertices());
    		}
    	} else if (operation.equals("zoomOut")) {
    		if (nextOperationStep == 1) {
    	    	flinkResponseHandler.setVerticesHaveCoordinates(false);
    			wrapperStream = flinkCore.zoomOutLayoutStep1Stream(wrapperHandler.getLayoutedVertices());
    		} else if (nextOperationStep == 2) {
    			wrapperStream = flinkCore.zoomOutLayoutStep2Stream(wrapperHandler.getLayoutedVertices(), 
    					wrapperHandler.getNewVertices());
    		}
    	} else if (operation.equals("pan")) {
    		if (nextOperationStep == 1) {
    			flinkResponseHandler.setVerticesHaveCoordinates(false);
    	    	wrapperStream = flinkCore.panLayoutStep1Stream(wrapperHandler.getLayoutedVertices(), 
    	    			wrapperHandler.getNewVertices());
    		} else if (nextOperationStep == 2) {
    			wrapperStream = flinkCore.panLayoutStep2Stream(wrapperHandler.getLayoutedVertices(), 
    	    			wrapperHandler.getNewVertices());
    		} else if (nextOperationStep == 3) {
    	    	wrapperStream = flinkCore.panLayoutStep3Stream(wrapperHandler.getLayoutedVertices());
    		} else if (nextOperationStep == 4) {
    	    	wrapperStream = flinkCore.panLayoutStep4Stream(wrapperHandler.getLayoutedVertices(), 
    	    			wrapperHandler.getNewVertices());
    		}
    	}
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, 
    			new SimpleStringSchema())).setParallelism(1);
    	wrapperHandler.setSentToClientInSubStep(false);
		new FlinkExecutorThread(operation, flinkCore.getFsEnv(), eval).start();	
    }
    
    private void zoomInLayoutStep1Set() {}

	private void zoomInLayoutStep1Stream() {}
    
    private void zoomOutLayoutStep1Set() {}
    
    private void zoomOutLayoutStep1Stream() {}
 
    
    private void panLayoutStep1Set() {}
    
    private void panLayoutStep1Stream() { }

    /**
     * sends a message to the all connected web socket clients
     */
    public synchronized void sendToAll(String message) {
        for (WebSocketChannel session : channels) {
        	WebSockets.sendText(message, session, null);
        }
    }
	
	private void setOperation(String operation) {
		wrapperHandler.setOperation(operation);
		flinkResponseHandler.setOperation(operation);
		this.operation = operation;
		sendToAll("operation;" + operation);
	}
	
	private void setOperationStep(int step) {
		wrapperHandler.setOperationStep(step);
		operationStep = step;
		sendToAll("operationAndStep;" + operation + ";" + operationStep);
	}
	
    private void setLayoutMode(boolean layoutMode) {
		wrapperHandler.setLayoutMode(layoutMode);	
		layout = layoutMode;
	}
    
    private void setModelPositions(Float topModel, Float rightModel, Float bottomModel, Float leftModel) {
    	System.out.println("Setting model positions: top, right, bottom, left ..." + topModel + " " + rightModel + " " + bottomModel 
    			+ " " + leftModel);
    	flinkCore.setModelPositions(topModel, rightModel, bottomModel, leftModel);
    	wrapperHandler.setModelPositions(topModel, rightModel, bottomModel, leftModel);
    }
    
    public FlinkCore getFlinkCore() {
    	return this.flinkCore;
    }
    
    public void setStreamBool(Boolean stream) {
    	this.stream = stream;
    	flinkCore.setStreamBool(stream);
    }
    
    public int getMaxVertices() {
    	return maxVertices;
    }
    
    private void calculateInitialViewportSettings(float topBorder, float rightBorder, float bottomBorder, float leftBorder) {
    	float yRange = bottomBorder - topBorder;
    	float xRange = rightBorder - leftBorder;
		float xRenderPosition;
		float yRenderPosition;
		float zoomLevelCalc;
		if (viewportPixelX / xRange > viewportPixelY / yRange) {
			yRenderPosition = (float) 0;
			zoomLevelCalc = viewportPixelY / bottomBorder; 
			float xModelPixel = viewportPixelX / zoomLevelCalc;
			float xModelOffset = (xModelPixel - xRange) / 2;
			xRenderPosition = xModelOffset * zoomLevelCalc;
			if (!maxVerticesLock) maxVertices = (int) (viewportPixelY * (xRange * zoomLevelCalc) / vertexCountNormalizationFactor);
		} else {
			xRenderPosition = (float) 0;
			zoomLevelCalc = viewportPixelX / rightBorder; 
			float yModelPixel = viewportPixelY / zoomLevelCalc;
			float yModelOffset = (yModelPixel - yRange) / 2;
			yRenderPosition = yModelOffset * zoomLevelCalc;
			if (!maxVerticesLock) maxVertices = (int) (viewportPixelX * (yRange * zoomLevelCalc) / vertexCountNormalizationFactor);
		}
    	System.out.println("viewportSize, maxVertices after: " + maxVertices);
    	sendToAll("modelBorders;" + topModelBorder + ";" + rightModelBorder + ";" + bottomModelBorder + ";" + leftModelBorder);
		sendToAll("zoomAndPan;" + zoomLevelCalc + ";" + xRenderPosition + ";" + yRenderPosition);
		if (!maxVerticesLock) wrapperHandler.setMaxVertices(maxVertices);
    }
    
    private void calculateMaxVertices(float topModel, float rightModel, float bottomModel, float leftModel, float zoomLevel) {
    	topModel = Math.max(topModelBorder, topModel);
		rightModel = Math.min(rightModelBorder, rightModel);
		bottomModel = Math.min(bottomModelBorder, bottomModel);
		leftModel = Math.max(leftModelBorder, leftModel);
		System.out.println("calculateMaxVertices, top, right, bottom, left: " + 
				topModel + " " + rightModel + " " + bottomModel + " " + leftModel);
		float xPixelProportion = (rightModel - leftModel) * zoomLevel;
		float yPixelProportion = (bottomModel - topModel) * zoomLevel;
		System.out.println("xPixelProportion: " + xPixelProportion + " of total pixels: " + 
				viewportPixelX);
		System.out.println("yPixelProportion: " + yPixelProportion + " of total pixels: " + 
				viewportPixelY);
		maxVertices = (int) (viewportPixelY * (yPixelProportion / viewportPixelY) *
				viewportPixelX * (xPixelProportion / viewportPixelX) / vertexCountNormalizationFactor);
		wrapperHandler.setMaxVertices(maxVertices);
    }
    
    private List<WrapperGVD> executeSet(DataSet<WrapperGVD> wrapperSet) {
    	wrapperCollection = new ArrayList<WrapperGVD>();
		try {
			if (eval) wrapperCollection = evaluator.executeSet(operation, wrapperSet);
			else wrapperCollection = wrapperSet.collect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return wrapperCollection;
    }
    
    public void onLayoutingStreamJobTermination() {
    	if (wrapperHandler.getSentToClientInSubStep() || operation.equals("initial")){
    		sendToAll("finalOperations");
    	} else {
	    	if (operation.equals("zoomIn") || operation.equals("pan")) {
	    		if (operationStep == 1) {
    				if (wrapperHandler.getCapacity() == 0) sendToAll("enableMouse");
    				else layoutingStreamOperation(2);
	    		} else if (operationStep == 2) {
    				if (wrapperHandler.getCapacity() == 0) layoutingStreamOperation(4);
    				else layoutingStreamOperation(3);
	    		} else if (operationStep == 3) {
	    			layoutingStreamOperation(4);
	    		} else if (operationStep == 4) {
	    	    	sendToAll("finalOperations");
	    		}
	    	} else if (operation.equals("zoomOut")) {
	    		if (operationStep == 1) {
    	    		sendToAll("enableMouse");
	    		} else if (operationStep == 2) {
    	    		sendToAll("finalOperations");
	    		} 
	    	}
    	}
    }
    
    public void onIsLayoutedStreamJobtermination() {
    	if (wrapperHandler.getSentToClientInSubStep()) {
    		wrapperHandler.clearOperation();
			sendToAll("finalOperations");		
    	} else sendToAll("enableMouse");
    }
}
