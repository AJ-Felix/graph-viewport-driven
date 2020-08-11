package aljoschaRydzyk.Gradoop_Flink_Prototype;

import io.undertow.Undertow;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import static io.undertow.Handlers.*;

import java.util.ArrayList;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import Temporary.Server;

public class UndertowServer {
	
	private static FlinkCore flinkCore;
	
	private static ArrayList<WebSocketChannel> channels = new ArrayList<>();
    private static String webSocketListenPath = "/graphData";
    private static int webSocketListenPort = 8887;
    private static String webSocketHost = "localhost";
    
    private static Float viewportPixelX = (float) 1000;
    private static Float viewportPixelY = (float) 1000;
    private static Float modelPixelX = (float) 4000;
    private static Float modelPixelY = (float) 4000;
    private static float zoomLevel = 1;
	
    public static void main(final String[] args) {
    	
//    	BasicConfigurator.configure();
//    	PrintStream fileOut = null;
//		try {
//			fileOut = new PrintStream("/home/aljoscha/out.txt");
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		System.setOut(fileOut);
    	
        Undertow server = Undertow.builder().addHttpListener(webSocketListenPort, webSocketHost)
                .setHandler(path().addPrefixPath(webSocketListenPath, websocket((exchange, channel) -> {
                    channels.add(channel);
                    channel.getReceiveSetter().set(getListener());
                    channel.resumeReceives();
                })).addPrefixPath("/", resource(new ClassPathResourceManager(Server.class.getClassLoader(),
                        Server.class.getPackage())).addWelcomeFiles("index.html")/*.setDirectoryListingEnabled(true)*/))
                .build();
        server.start();
        System.out.println("Server started!");
    }
    
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
                if (messageData.startsWith("buildTopView")) {
        			flinkCore = new FlinkCore();
        			String[] arrMessageData = messageData.split(";");
        			if (arrMessageData[1].equals("retract")) {
	        			flinkCore.initializeGradoopGraphUtil();
	        			DataStream<Tuple2<Boolean, Row>> wrapperStream = flinkCore.buildTopViewRetract();
	        			wrapperStream.addSink(new SinkFunction<Tuple2<Boolean,Row>>(){
	        				@Override 
	        				public void invoke(Tuple2<Boolean, Row> element, Context context) {
	        					String sourceIdNumeric = element.f1.getField(4).toString();
	        					String sourceX = element.f1.getField(6).toString();
	        					String sourceY = element.f1.getField(7).toString();
	        					String edgeIdGradoop = element.f1.getField(1).toString();
	        					String targetIdNumeric = element.f1.getField(10).toString();
	        					String targetX = element.f1.getField(12).toString();
	        					String targetY = element.f1.getField(13).toString();
	        					if (element.f0) {
	        						UndertowServer.sendToAll("addVertex;" + sourceIdNumeric + 
	        							";" + sourceX + ";" + sourceY);
	        						if (!edgeIdGradoop.equals("identityEdge")) {
	        						UndertowServer.sendToAll("addVertex;" + targetIdNumeric + 
	        							";" + targetX + ";" + targetY);
	        						UndertowServer.sendToAll("addEdge;" + edgeIdGradoop + 
	        							";" + sourceIdNumeric + ";" + targetIdNumeric);
	        						}
	        					} else if (!element.f0) {
	        							UndertowServer.sendToAll("removeVertex;" + sourceIdNumeric + 
	                							";" + sourceX + ";" + sourceY );
	            					if (!edgeIdGradoop.equals("identityEdge")) {
	        							UndertowServer.sendToAll("removeVertex;" + targetIdNumeric + 
	                							";" + targetX + ";" + targetY );
	        							UndertowServer.sendToAll("removeEdge" + edgeIdGradoop + 
	        							";" + sourceIdNumeric + ";" + targetIdNumeric);
	            					}
	        					}
	        				}
	        			});
        			} else if (arrMessageData[1].equals("appendJoin")) {
        				flinkCore.initializeCSVGraphUtilJoin();
        				DataStream<Row> wrapperStream = flinkCore.buildTopViewAppendJoin();
        				wrapperStream.addSink(new SinkFunction<Row>(){
	        				public void invoke(Row element, Context context) {
	        					String sourceIdNumeric = element.getField(2).toString();
	        					String sourceX = element.getField(4).toString();
	        					String sourceY = element.getField(5).toString();
	        					String edgeIdGradoop = element.getField(13).toString();
	        					String targetIdNumeric = element.getField(8).toString();
	        					String targetX = element.getField(10).toString();
	        					String targetY = element.getField(11).toString();
        						UndertowServer.sendToAll("addVertex;" + sourceIdNumeric + 
        							";" + sourceX + ";" + sourceY);
        						if (!edgeIdGradoop.equals("identityEdge")) {
        						UndertowServer.sendToAll("addVertex;" + targetIdNumeric + 
        							";" + targetX + ";" + targetY);
        						UndertowServer.sendToAll("addEdge;" + edgeIdGradoop + 
        							";" + sourceIdNumeric + ";" + targetIdNumeric);
	        					}
	        				}
	        			});
        			} else if (arrMessageData[1].equals("appendMap")) {
        				flinkCore.initializeCSVGraphUtilMap();
        				DataStream<Row> wrapperStream = flinkCore.buildTopViewAppendMap();
        				wrapperStream.addSink(new SinkFunction<Row>(){
	        				@Override
	        				public void invoke(Row element, Context context) {
	        					String sourceIdNumeric = element.getField(2).toString();
	        					String sourceX = element.getField(4).toString();
	        					String sourceY = element.getField(5).toString();
	        					String sourceDegree = element.getField(6).toString();
	        					String edgeIdGradoop = element.getField(13).toString();
	        					String targetIdNumeric = element.getField(8).toString();
	        					String targetX = element.getField(10).toString();
	        					String targetY = element.getField(11).toString();
	        					String targetDegree = element.getField(12).toString();
        						UndertowServer.sendToAll("addWrapper;" + sourceIdNumeric + 
        							";" + sourceX + ";" + sourceY + ";" + sourceDegree + ";" + targetIdNumeric + 
        							";" + targetX + ";" + targetY + ";" + targetDegree + ";" + edgeIdGradoop);
	        				}
	        			});
        			}
        			try {
        				flinkCore.getFsEnv().execute();
        			} catch (Exception e1) {
        				// TODO Auto-generated catch block
        				e1.printStackTrace();
        			}
        			UndertowServer.sendToAll("fitGraph");
                }
    			if (messageData.startsWith("zoomIn")) {
        			String[] arrMessageData = messageData.split(";");
        			Integer xRenderPos = Integer.parseInt(arrMessageData[1]);
        			Integer yRenderPos = Integer.parseInt(arrMessageData[2]);
        			zoomLevel = Float.parseFloat(arrMessageData[3]);
        			Integer topModelPos = (int) (- yRenderPos / zoomLevel);
        			Integer leftModelPos = (int) (- xRenderPos /zoomLevel);
        			Integer bottomModelPos = (int) (topModelPos + viewportPixelY / zoomLevel);
        			Integer rightModelPos = (int) (leftModelPos + viewportPixelX / zoomLevel);
					flinkCore.setTopModelPos(topModelPos);
					flinkCore.setRightModelPos(rightModelPos);
					flinkCore.setBottomModelPos(bottomModelPos);
					flinkCore.setLeftModelPos(leftModelPos);
        			DataStream<Row> wrapperStream = flinkCore.zoomIn(topModelPos, rightModelPos, bottomModelPos, leftModelPos);
					wrapperStream.addSink(new SinkFunction<Row>() {
	    				@Override 
	    				public void invoke(Row element, Context context) {
	    					String sourceIdNumeric = element.getField(2).toString();
        					String sourceX = element.getField(4).toString();
        					String sourceY = element.getField(5).toString();
        					String edgeIdGradoop = element.getField(13).toString();
        					String targetIdNumeric = element.getField(8).toString();
        					String targetX = element.getField(10).toString();
        					String targetY = element.getField(11).toString();
    						UndertowServer.sendToAll("addVertex;" + sourceIdNumeric + 
    							";" + sourceX + ";" + sourceY);
    						if (!edgeIdGradoop.equals("identityEdge")) {
    						UndertowServer.sendToAll("addVertex;" + targetIdNumeric + 
    							";" + targetX + ";" + targetY);
    						UndertowServer.sendToAll("addEdge;" + edgeIdGradoop + 
    							";" + sourceIdNumeric + ";" + targetIdNumeric);
        					}
	    				}
					});
					try {
						flinkCore.getFsEnv().execute();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
    			}
    			if (messageData.startsWith("pan")) {
        			String[] arrMessageData = messageData.split(";");
        			Integer topModelPos = flinkCore.gettopModelPos();
        			Integer bottomModelPos = flinkCore.getBottomModelPos();
        			Integer leftModelPos = flinkCore.getLeftModelPos();
        			Integer rightModelPos = flinkCore.getRightModelPos();
        			Integer xModelDiff = Integer.parseInt(arrMessageData[1]); 
					Integer yModelDiff = Integer.parseInt(arrMessageData[2]);
//					Integer xModelDiff = (int) -(xRenderDiff / zoomLevel);
//					Integer yModelDiff = (int) -(yRenderDiff / zoomLevel);
					DataStream<Row> wrapperStream = flinkCore.pan(topModelPos, rightModelPos, bottomModelPos, leftModelPos, xModelDiff, yModelDiff);
					flinkCore.setTopModelPos(topModelPos + yModelDiff);
					flinkCore.setBottomModelPos(bottomModelPos + yModelDiff);
					flinkCore.setLeftModelPos(leftModelPos + xModelDiff);
					flinkCore.setRightModelPos(rightModelPos + xModelDiff);
					wrapperStream.addSink(new SinkFunction<Row>() {
	    				@Override 
	    				public void invoke(Row element, Context context) {
	    					String sourceIdNumeric = element.getField(2).toString();
        					String sourceX = element.getField(4).toString();
        					String sourceY = element.getField(5).toString();
        					String edgeIdGradoop = element.getField(13).toString();
        					String targetIdNumeric = element.getField(8).toString();
        					String targetX = element.getField(10).toString();
        					String targetY = element.getField(11).toString();
    						UndertowServer.sendToAll("addVertex;" + sourceIdNumeric + 
    							";" + sourceX + ";" + sourceY);
    						if (!edgeIdGradoop.equals("identityEdge")) {
    						UndertowServer.sendToAll("addVertex;" + targetIdNumeric + 
    							";" + targetX + ";" + targetY);
    						UndertowServer.sendToAll("addEdge;" + edgeIdGradoop + 
    							";" + sourceIdNumeric + ";" + targetIdNumeric);
    						}
        				}
					});
					try {
						flinkCore.getFsEnv().execute();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
    			}
    			if (messageData.equals("displayAll")) {
        			flinkCore = new FlinkCore();
        			flinkCore.initializeCSVGraphUtilJoin();
        			DataStream<Row> wrapperStream = flinkCore.displayAll();
					wrapperStream.addSink(new SinkFunction<Row>() {
	    				@Override 
	    				public void invoke(Row element, Context context) {
	    					String sourceIdNumeric = element.getField(2).toString();
        					String sourceX = element.getField(4).toString();
        					String sourceY = element.getField(5).toString();
        					String edgeIdGradoop = element.getField(13).toString();
        					String targetIdNumeric = element.getField(8).toString();
        					String targetX = element.getField(10).toString();
        					String targetY = element.getField(11).toString();
    						UndertowServer.sendToAll("addVertex;" + sourceIdNumeric + 
    							";" + sourceX + ";" + sourceY);
    						if (!edgeIdGradoop.equals("identityEdge")) {
    						UndertowServer.sendToAll("addVertex;" + targetIdNumeric + 
    							";" + targetX + ";" + targetY);
    						UndertowServer.sendToAll("addEdge;" + edgeIdGradoop + 
    							";" + sourceIdNumeric + ";" + targetIdNumeric);
    						}
	    				}
					});
					try {
						flinkCore.getFsEnv().execute();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					UndertowServer.sendToAll("fitGraph");
//					UndertowServer.sendToAll("layout");
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
}
