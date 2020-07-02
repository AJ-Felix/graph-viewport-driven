package aljoschaRydzyk.Gradoop_Flink_Prototype;

import io.undertow.Undertow;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.websockets.WebSocketConnectionCallback;
import io.undertow.websockets.WebSocketProtocolHandshakeHandler;
import io.undertow.websockets.core.AbstractReceiveListener;
import io.undertow.websockets.core.BufferedTextMessage;
import io.undertow.websockets.core.WebSocketChannel;
import io.undertow.websockets.core.WebSockets;
import io.undertow.websockets.extensions.PerMessageDeflateHandshake;
import io.undertow.websockets.spi.WebSocketHttpExchange;



import static io.undertow.Handlers.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.log4j.BasicConfigurator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class UndertowServer {
	
	private static FlinkCore flinkCore;
	private static ArrayList<DataStream> graph_data_streams;
	
	private static transient CytoGraph cytograph = new CytoGraph();
	private static transient CytoVertex cytovertex;
	
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
        			graph_data_streams = flinkCore.buildTopView();
        			DataStream<Tuple2<Boolean, Row>> stream_vertices = graph_data_streams.get(0);
        			DataStream<Tuple2<Boolean, Row>> stream_edges = graph_data_streams.get(1);
        			stream_vertices.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
        				@Override 
        				public void invoke(Tuple2<Boolean, Row> element, Context context) {
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
        			stream_edges.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {

        				private static final long serialVersionUID = -8999956705061275432L;
        				@Override 
        				public void invoke(Tuple2<Boolean, Row> element, Context context) {
        					if (element.f0) {
        						UndertowServer.sendToAll("addEdge;" + element.f1.getField(0).toString() + 
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
        			UndertowServer.sendToAll("fitGraph");
                }
    			if (messageData.equals("zoomTopLeftCorner")) {
    				UndertowServer.sendToAll("clearGraph");
    				try {
						DataStream<Tuple5<String, String, String, String, String>> dstream_vertices = flinkCore.zoomIn(graph_data_streams.get(2),0, 2000, 2000, 0);
						dstream_vertices.addSink(new SinkFunction<Tuple5<String, String, String, String, String>>() {

		    				private static final long serialVersionUID = -8999956705061275432L;
		    				@Override 
		    				public void invoke(Tuple5<String, String, String, String, String> element, Context context) {
//		    					System.out.println("THis is the sink" + element);
		    					UndertowServer.sendToAll("addVertex;" + element.f2 + ";" + element.f3 + ";" + element.f4);
		    				}
						});
						flinkCore.getFsEnv().execute();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
    				UndertowServer.sendToAll("fitGraph");
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
