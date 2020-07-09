package aljoschaRydzyk.Gradoop_Flink_Prototype;

import io.undertow.Undertow;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import static io.undertow.Handlers.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.apache.log4j.BasicConfigurator;

import Temporary.Server;

public class UndertowServer {
	
	private static FlinkCore flinkCore;
	
	private static ArrayList<WebSocketChannel> channels = new ArrayList<>();
    private static String webSocketListenPath = "/graphData";
    private static int webSocketListenPort = 8887;
    private static String webSocketHost = "localhost";
	
    public static void main(final String[] args) {
    	
//    	BasicConfigurator.configure();
    	
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
                if (messageData.equals("buildTopView")) {
        			flinkCore = new FlinkCore();
        			List<DataStream<Tuple2<Boolean, Row>>> graphStreams = flinkCore.buildTopView();
        			DataStream<Tuple2<Boolean, Row>> vertexStream = graphStreams.get(0);
        			DataStream<Tuple2<Boolean, Row>> edgeStream = graphStreams.get(1);
        			vertexStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
        				@Override 
        				public void invoke(Tuple2<Boolean, Row> element, @SuppressWarnings("rawtypes") Context context) {
        					if (element.f0) {
        						UndertowServer.sendToAll("addVertex;" + element.f1.getField(0).toString() + 
        							";" + element.f1.getField(1).toString() + ";" + element.f1.getField(2).toString() );
        					} else if (!element.f0) {
            					UndertowServer.sendToAll("removeVertex;" + element.f1.getField(0).toString() + 
            							";" + element.f1.getField(1).toString() + ";" + element.f1.getField(2).toString() );
        					}
        				}
        			});
        			try {
        				flinkCore.getFsEnv().execute();
        			} catch (Exception e1) {
        				// TODO Auto-generated catch block
        				e1.printStackTrace();
        			}
        			edgeStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {

        				private static final long serialVersionUID = -8999956705061275432L;
        				@Override 
        				public void invoke(Tuple2<Boolean, Row> element, @SuppressWarnings("rawtypes") Context context) {
        					if (element.f0) {
        						UndertowServer.sendToAll("addEdge;" + element.f1.getField(0) + 
        							";" + element.f1.getField(1) + ";" + element.f1.getField(2));
        					} 
        				}
        			});
        			try {
        				flinkCore.getFsEnv().execute();
        			} catch (Exception e1) {
        				// TODO Auto-generated catch block
        				e1.printStackTrace();
        			}
        			UndertowServer.sendToAll("fitGraph");
                }
    			if (messageData.equals("zoomTopLeftCorner")) {
//    				UndertowServer.sendToAll("clearGraph");
    				UndertowServer.sendToAll("removeSpatialSelection;0;2000;2000;0");
    				
    					@SuppressWarnings("rawtypes")
						List<DataStream> graphStreams = flinkCore.zoomIn(0, 2000, 2000, 0);
						@SuppressWarnings("unchecked")
						DataStream<Tuple5<String, String, String, String, String>> vertexStream = graphStreams.get(0);
						vertexStream.addSink(new SinkFunction<Tuple5<String, String, String, String, String>>() {

		    				private static final long serialVersionUID = -8999956705061275432L;
		    				@Override 
		    				public void invoke(Tuple5<String, String, String, String, String> element, @SuppressWarnings("rawtypes") Context context) {
		    					UndertowServer.sendToAll("addVertex;" + element.f2 + ";" + element.f3 + ";" + element.f4);
		    				}
						});
						try {
							flinkCore.getFsEnv().execute();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						@SuppressWarnings("unchecked")
						DataStream<Tuple2<Boolean, Row>> edgeStream = graphStreams.get(1);
						edgeStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {

		    				private static final long serialVersionUID = -8999956705061275432L;
		    				@Override 
		    				public void invoke(Tuple2<Boolean, Row> element, @SuppressWarnings("rawtypes") Context context) {
		    					UndertowServer.sendToAll("addEdge;" + element.f1.getField(0) + ";" + element.f1.getField(1) + ";" + element.f1.getField(2));
		    				}
						});
						try {
							flinkCore.getFsEnv().execute();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
	    				UndertowServer.sendToAll("fitGraph");
    			}
    			if (messageData.equals("panRight")) {
    				UndertowServer.sendToAll("removeSpatialSelection;0;2000;2000;500");
    				@SuppressWarnings("rawtypes")
					List<DataStream> graphStreams = flinkCore.panRight(0, 2000, 2000, 0, 0, 2500, 2000, 500);
					@SuppressWarnings("unchecked")
					DataStream<Tuple5<String, String, String, String, String>> vertexStream = graphStreams.get(0);
					vertexStream.addSink(new SinkFunction<Tuple5<String, String, String, String, String>>() {

	    				private static final long serialVersionUID = -8999956705061275432L;
	    				@Override 
	    				public void invoke(Tuple5<String, String, String, String, String> element, @SuppressWarnings("rawtypes") Context context) {
	    					UndertowServer.sendToAll("addVertex;" + element.f2 + ";" + element.f3 + ";" + element.f4);
	    				}
					});
					try {
						flinkCore.getFsEnv().execute();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					@SuppressWarnings("unchecked")
					DataStream<Tuple2<Boolean, Row>> edgeStream = graphStreams.get(1);
					edgeStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {

	    				private static final long serialVersionUID = -8999956705061275432L;
	    				@Override 
	    				public void invoke(Tuple2<Boolean, Row> element, @SuppressWarnings("rawtypes") Context context) {
	    					UndertowServer.sendToAll("addEdge;" + element.f1.getField(0) + ";" + element.f1.getField(1) + ";" + element.f1.getField(2));
	    				}
					});
					try {
						flinkCore.getFsEnv().execute();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
    				UndertowServer.sendToAll("fitGraph");
    			}
    			if (messageData.equals("displayAll")) {
        			flinkCore = new FlinkCore();
    				List<DataStream> graphStreams = flinkCore.displayAll();
    				@SuppressWarnings("unchecked")
					DataStream<Tuple5<String, String, String, String, String>> vertexStream = graphStreams.get(0);
					vertexStream.addSink(new SinkFunction<Tuple5<String, String, String, String, String>>() {

	    				private static final long serialVersionUID = -8999956705061275432L;
	    				@Override 
	    				public void invoke(Tuple5<String, String, String, String, String> element, @SuppressWarnings("rawtypes") Context context) {
	    					UndertowServer.sendToAll("addVertex;" + element.f2 + 
//	    							"," + element.f3 + "," + element.f4 + 
	    							";" + element.f3 + ";" + element.f4);
	    				}
					});
					try {
						flinkCore.getFsEnv().execute();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					@SuppressWarnings("unchecked")
					DataStream<Tuple5<String, String, String, String, String>> edgeStream = graphStreams.get(1);
					edgeStream.addSink(new SinkFunction<Tuple5<String, String, String, String, String>>() {

	    				private static final long serialVersionUID = -8999956705061275432L;
	    				@Override 
	    				public void invoke(Tuple5<String, String, String, String, String> element, @SuppressWarnings("rawtypes") Context context) {
	    					UndertowServer.sendToAll("addEdge;" + element.f2 + ";" + element.f3 + ";" + element.f4);
	    				}
					});
					try {
						flinkCore.getFsEnv().execute();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					UndertowServer.sendToAll("fitGraph");
					UndertowServer.sendToAll("layout");
    			}
    			if (messageData.startsWith("pan")){
    				String[] inputArray = messageData.split(";");
    				Integer xMouseMovement = Integer.parseInt(inputArray[1]);
    				Integer yMouseMovement = Integer.parseInt(inputArray[2]);
    				List<DataStream> graphStreams = flinkCore.pan(xMouseMovement, yMouseMovement);
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
