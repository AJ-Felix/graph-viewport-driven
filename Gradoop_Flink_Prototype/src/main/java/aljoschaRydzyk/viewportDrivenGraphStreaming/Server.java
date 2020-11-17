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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperMapLine;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperMapLineNoCoordinates;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperMapLineNoCoordinatesRetract;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperMapLineRetract;
import io.undertow.Undertow;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;

public class Server implements Serializable{
	
	private FlinkCore flinkCore;
	public ArrayList<WebSocketChannel> channels = new ArrayList<>();
    private String webSocketListenPath = "/graphData";
    private int webSocketListenPort = 8897;    
    private Integer maxVertices = 100;
    private Integer vertexCountNormalizationFactor = 5000;
    private boolean layout = true;
    private int operationStep;
    private Float viewportPixelX = (float) 1000;
    private Float viewportPixelY = (float) 1000;
	private String operation;
	public boolean sentToClientInSubStep;
    private WrapperHandler wrapperHandler;
    private FlinkResponseHandler flinkResponseHandler;
    private String localMachinePublicIp4 = "localhost";
    private int flinkResponsePort = 8898;
    private String clusterEntryPointIp4 = "localhost";
    private int clusterEntryPointPort = 8081;
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
    			.addHttpListener(webSocketListenPort, localMachinePublicIp4)
                .setHandler(path()
                	.addPrefixPath(webSocketListenPath, websocket((exchange, channel) -> {
	                    channels.add(channel);
	                    channel.getReceiveSetter().set(getListener());
	                    channel.resumeReceives();
	                }))
            	).build();
    	server.start();
        System.out.println("Server started!");  
    }
    
    public void initializeHandlers() {
    	wrapperHandler = new WrapperHandler();
  		wrapperHandler.initializeGraphRepresentation();
  		wrapperHandler.initializeAPI(localMachinePublicIp4);
  		
  		//initialize FlinkResponseHandler
        flinkResponseHandler = new FlinkResponseHandler(wrapperHandler);
        flinkResponseHandler.start();
//		flinkResponseHandler.listen();
    }
    
    public void setPublicIp4Adresses(String clusterEntryPointIp4) throws SocketException {
    	this.clusterEntryPointIp4 = clusterEntryPointIp4;
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
                if (messageData.equals("preLayout")) {
                	wrapperHandler.resetLayoutedVertices();
                	setLayoutMode(false);
                } else if (messageData.equals("postLayout")) {
                	setLayoutMode(true);
                } else if (messageData.equals("resetWrapperHandler")) {
                	wrapperHandler.initializeGraphRepresentation();
                } else if (messageData.startsWith("fitted")) {
                	String[] arrMessageData = messageData.split(";");
                	Float xRenderPos = Float.parseFloat(arrMessageData[1]);
                	Float yRenderPos = Float.parseFloat(arrMessageData[2]);
                	Float zoomLevel = Float.parseFloat(arrMessageData[3]);
                	Float topModel = - yRenderPos / zoomLevel;
                	Float leftModel = - xRenderPos / zoomLevel;
                	Float bottomModel = - yRenderPos / zoomLevel + viewportPixelY / zoomLevel;
                	Float rightModel = -xRenderPos / zoomLevel + viewportPixelX / zoomLevel;
                	setModelPositions(topModel, rightModel, bottomModel, leftModel);
                } else if (messageData.startsWith("viewportSize")) {
                	String[] arrMessageData = messageData.split(";");
                	Float xRenderPos = Float.parseFloat(arrMessageData[1]);
                	Float yRenderPos = Float.parseFloat(arrMessageData[2]);
                	Float zoomLevel = Float.parseFloat(arrMessageData[3]);
                	viewportPixelX = Float.parseFloat(arrMessageData[4]);
                	viewportPixelY = Float.parseFloat(arrMessageData[5]);                	
                	Float topModel = - yRenderPos / zoomLevel;
                	Float leftModel = - xRenderPos / zoomLevel;
                	Float bottomModel = - yRenderPos / zoomLevel + viewportPixelY / zoomLevel;
                	Float rightModel = -xRenderPos / zoomLevel + viewportPixelX / zoomLevel;
                	Integer tempMaxVertices = maxVertices;
                	if (bottomModel - topModel > 4000) {
                		maxVertices = (int) (viewportPixelX * viewportPixelX /vertexCountNormalizationFactor);
                	} else if (rightModel - leftModel > 4000) {
                		maxVertices = (int) (viewportPixelY * viewportPixelY /vertexCountNormalizationFactor);
                	} else {
                    	maxVertices = (int) (viewportPixelX * viewportPixelY / vertexCountNormalizationFactor);
                	}
                	if (tempMaxVertices != maxVertices) {
                		//TODO: Resizing... Add or remove vertices, write new model coordinates to data structures
                		//ideally find out, how much resizing in x and y direction and add vertices accordingly
                	}
                	System.out.println("maxVertices derived from viewport: " + maxVertices);
                } else if (messageData.startsWith("layoutBaseString")) {
                	String[] arrMessageData = messageData.split(";");
                	List<String> list = new ArrayList<String>(Arrays.asList(arrMessageData));
                	list.remove(0);
                	wrapperHandler.updateLayoutedVertices(list);
                	nextSubStep();
                } else if (messageData.startsWith("maxVertices")) {
                	String[] arrMessageData = messageData.split(";");
                	maxVertices = Integer.parseInt(arrMessageData[1]);
                	wrapperHandler.setMaxVertices(maxVertices);
                } 
                else if (messageData.startsWith("buildTopView")) {
                	flinkCore = new FlinkCore(clusterEntryPointIp4, clusterEntryPointPort);
                  	Float topModel = (float) 0;
                	Float rightModel = (float) 4000;
                	Float bottomModel = (float) 4000;
                	Float leftModel = (float) 0;
                	setModelPositions(topModel, rightModel, bottomModel, leftModel);
                	setOperation("initial");
                	String[] arrMessageData = messageData.split(";");
                	System.out.println(arrMessageData[1]);
                	if (arrMessageData[1].equals("retract")) {
                		buildTopViewRetract();        			
                	} else if (arrMessageData[1].equals("appendJoin")) {
                		System.out.println("appendjoin??!?!?");
                		buildTopViewAppendJoin();
        			} else if (arrMessageData[1].contentEquals("adjacency")) {
        				buildTopViewAdjacency();
        			}
                	try {
        				flinkCore.getFsEnv().execute();
        			} catch (Exception e) {
        				e.printStackTrace();
        			}
                	Server.getInstance().sendToAll("fit");
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
					if (messageData.startsWith("zoomIn")) {
						setOperation("zoomIn");
						wrapperHandler.prepareOperation();
						if (layout) zoom();
						else {
							flinkResponseHandler.setVerticesHaveCoordinates(false);
							System.out.println("in zoom in layout function");
							zoomInLayoutFirstStep();
						}
					} else {
						setOperation("zoomOut");
						wrapperHandler.prepareOperation();
						if (layout) zoom();
						else {
							flinkResponseHandler.setVerticesHaveCoordinates(false);
							System.out.println("in zoom out layout function");
							zoomOutLayoutFirstStep();
						}
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
			wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));
    }
    
    private void buildTopViewAppendJoin() {
    	flinkCore.initializeCSVGraphUtilJoin();
		DataStream<Row> wrapperStream = flinkCore.buildTopViewAppendJoin(maxVertices);
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
			wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));	
    }
    
    private void buildTopViewAdjacency() {
    	flinkCore.initializeAdjacencyGraphUtil();
		wrapperHandler.initializeGraphRepresentation();
		flinkResponseHandler.setOperation("initialAppend");
		DataStream<Row> wrapperStream = flinkCore.buildTopViewAdjacency(maxVertices);
		wrapperStream.print();
			DataStream<String> wrapperLine;
			if (layout) {
				flinkResponseHandler.setVerticesHaveCoordinates(true);
				wrapperLine = wrapperStream.map(new WrapperMapLine());
			} else {
				flinkResponseHandler.setVerticesHaveCoordinates(false);
				wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
			}
			wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));
    }
    
    private void zoom() {
    	DataStream<Row> wrapperStream = flinkCore.zoom();
    	wrapperStream.print();
		flinkResponseHandler.setVerticesHaveCoordinates(true);
		System.out.println("executing zoom on server class");		    				
		DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLine());
		wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
    	wrapperHandler.clearOperation();

    }
    
    private void pan() {
    	DataStream<Row> wrapperStream = flinkCore.pan();			
		DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLine());
		wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	wrapperHandler.clearOperation();
    }
    
    private void nextSubStep() {
    	System.out.println("in nextSubStep function");
    	System.out.println("operation: " + operation);
    	System.out.println("operationStep: " + operationStep);
    	if (operation.equals("initial")) {
        	wrapperHandler.clearOperation();
    	} else if (operation.startsWith("zoom")) {
    		if (operation.contains("In")) {
    			if (operationStep == 1) {
    				if (wrapperHandler.getCapacity() > 0) {
    					zoomInLayoutSecondStep();
    				} else {
    					System.out.println("nextSubStep 1, 4");
    					zoomInLayoutFourthStep();
    				}
    			} else if (operationStep == 2) {
    				if (wrapperHandler.getCapacity() > 0) {
    					zoomInLayoutSecondStep();
    				} else {
    					System.out.println("nextSubStep 2, 4");
    					zoomInLayoutFourthStep();
    				}
    			} else if (operationStep == 3) zoomInLayoutFourthStep();
    		} else {
    			if (operationStep == 1) zoomOutLayoutSecondStep();
    		}
    	} else if (operation.startsWith("pan")) {
    		if (operationStep == 1) {
    			if (wrapperHandler.getCapacity() > 0) {
    				panLayoutSecondStep();
    			} else {
    				panLayoutFourthStep();
    			}
    		} else if (operationStep == 2) {
    			if (wrapperHandler.getCapacity() > 0) {
    				panLayoutSecondStep();
    			} else {
    				panLayoutFourthStep();
    			}
    		} else if (operationStep == 3) panLayoutFourthStep();
    	}
    }

	private void zoomInLayoutFirstStep() {
		System.out.println("zoomInfirst, operationStep: " + wrapperHandler.getOperationStep());
    	setOperationStep(1);
		System.out.println("zoomInfirst, operationStep: " + wrapperHandler.getOperationStep());
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutFirstStep(wrapperHandler.getLayoutedVertices(), 
    			wrapperHandler.getInnerVertices());	
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));
    	wrapperHandler.setSentToClientInSubStep(false);
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (flinkResponseHandler.getLine() == "empty" || wrapperHandler.getSentToClientInSubStep() == false) zoomInLayoutSecondStep();
    }
    
    private void zoomInLayoutSecondStep() {
    	setOperationStep(2);
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutSecondStep(wrapperHandler.getLayoutedVertices(), 
    			wrapperHandler.getInnerVertices(), wrapperHandler.getNewVertices());
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));
    	wrapperHandler.setSentToClientInSubStep(false);
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (flinkResponseHandler.getLine() == "empty" || wrapperHandler.getSentToClientInSubStep() == false) zoomInLayoutThirdStep();
    }
    
    private  void zoomInLayoutThirdStep() {
    	setOperationStep(3);
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutThirdStep(wrapperHandler.getLayoutedVertices());
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));
    	wrapperHandler.setSentToClientInSubStep(false);
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (wrapperHandler.getSentToClientInSubStep() == false) zoomInLayoutFourthStep();
    }
    
    private void zoomInLayoutFourthStep() {
    	setOperationStep(4);
    	DataStream<Row> wrapperStream = flinkCore.zoomInLayoutFourthStep(wrapperHandler.getLayoutedVertices(),
    			wrapperHandler.getInnerVertices(), wrapperHandler.getNewVertices());
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
    	wrapperHandler.clearOperation();
    }
    
    private void zoomOutLayoutFirstStep() {
    	setOperationStep(1);
		DataStream<Row> wrapperStream = flinkCore.zoomOutLayoutFirstStep(wrapperHandler.getLayoutedVertices());
		DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));
    	wrapperHandler.setSentToClientInSubStep(false);
    	try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (flinkResponseHandler.getLine() == "empty" || 
				wrapperHandler.getSentToClientInSubStep() == false) wrapperHandler.clearOperation();
    }
    
    private void zoomOutLayoutSecondStep() {
    	setOperationStep(2);
		DataStream<Row> wrapperStream = flinkCore.zoomOutLayoutSecondStep(wrapperHandler.getLayoutedVertices(), 
				wrapperHandler.getNewVertices());
		DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
		wrapperHandler.clearOperation();
    }
    
    
    private void panLayoutFirstStep() {
    	setOperationStep(1);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutFirstStep(wrapperHandler.getLayoutedVertices(), 
    			wrapperHandler.getNewVertices());
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));
    	wrapperHandler.setSentToClientInSubStep(false);
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}		
		if (flinkResponseHandler.getLine() == "empty" || wrapperHandler.getSentToClientInSubStep() == false) panLayoutSecondStep();
    }
    
    private void panLayoutSecondStep() {
    	setOperationStep(2);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutSecondStep(wrapperHandler.getLayoutedVertices(), 
    			wrapperHandler.getNewVertices());
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));
    	wrapperHandler.setSentToClientInSubStep(false);
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}		
		if (flinkResponseHandler.getLine() == "empty" || wrapperHandler.getSentToClientInSubStep() == false) panLayoutThirdStep();
    }
    
    private void panLayoutThirdStep() {
    	setOperationStep(3);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutThirdStep(wrapperHandler.getLayoutedVertices());
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));
    	wrapperHandler.setSentToClientInSubStep(false);
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}		
		if (wrapperHandler.getSentToClientInSubStep() == false) panLayoutFourthStep();
    }
    
    private void panLayoutFourthStep() {
    	setOperationStep(4);
    	DataStream<Row> wrapperStream = flinkCore.panLayoutFourthStep(wrapperHandler.getLayoutedVertices(), 
    			wrapperHandler.getNewVertices());
    	DataStream<String> wrapperLine = wrapperStream.map(new WrapperMapLineNoCoordinates());
    	wrapperLine.addSink(new SocketClientSink<String>(localMachinePublicIp4, flinkResponsePort, new SimpleStringSchema()));
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			e.printStackTrace();
		}		
		wrapperHandler.clearOperation();
    }

    /**
     * sends a message to the all connected web socket clients
     */
    public void sendToAll(String message) {
        for (WebSocketChannel session : channels) {
            WebSockets.sendText(message, session, null);
        }
    }
	
	private void setOperation(String operation) {
		wrapperHandler.setOperation(operation);
		this.operation = operation;
	}
	
	private void setOperationStep(Integer step) {
		wrapperHandler.setOperationStep(step);
		operationStep = step;
	}
	
    private void setLayoutMode(Boolean layoutMode) {
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
    
    public FlinkResponseHandler getFlinkResponseHandler() {
    	return this.flinkResponseHandler;
    }
}
