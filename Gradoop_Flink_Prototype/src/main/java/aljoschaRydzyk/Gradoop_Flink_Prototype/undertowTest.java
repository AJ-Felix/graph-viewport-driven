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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class undertowTest {
	
	private static transient CytoGraph cytograph = new CytoGraph();
	private static transient CytoVertex cytovertex;
	
    private static ArrayList<WebSocketChannel> channels = new ArrayList<>();
    private static String webSocketListenPath = "/graphData";
    private static int webSocketListenPort = 8887;
    private static String webSocketHost = "localhost";
	
    public static void main(final String[] args) {
        // Demonstrates how to use Websocket Protocol Handshake to enable Per-message deflate
//        Undertow server = Undertow.builder()
//                .addHttpListener(8080, "localhost")
//                .setHandler(path()
//                        .addPrefixPath("/myapp",
//                            new WebSocketProtocolHandshakeHandler(new WebSocketConnectionCallback() {
//
//                              @Override
//                              public void onConnect(WebSocketHttpExchange exchange, WebSocketChannel channel) {
//                                channel.getReceiveSetter().set(new AbstractReceiveListener() {
//
//                                  @Override
//                                  protected void onFullTextMessage(WebSocketChannel channel, BufferedTextMessage message) {
//                                    WebSockets.sendText(message.getData(), channel, null);
//                                  }
//                                });
//                                channel.resumeReceives();
//                              }
//                            }).addExtension(new PerMessageDeflateHandshake(false, 6)))
//                        .addPrefixPath("/", resource(new ClassPathResourceManager(undertowTest.class.getClassLoader(), undertowTest.class.getPackage())).addWelcomeFiles("index.html")))
//                .build();
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
        			FlinkCore flinkCore = new FlinkCore();
        			List<DataStream<Tuple2<Boolean, Row>>> graph_data_streams = flinkCore.buildTopView();
        			DataStream<Tuple2<Boolean, Row>> stream_vertices = graph_data_streams.get(0);
        			DataStream<Tuple2<Boolean, Row>> stream_edges = graph_data_streams.get(1);
        			stream_vertices
//        			.process(new ProcessFunction<Tuple2<Boolean, Row>, CytoVertex>(){
        //
//        				@Override
//        				public void processElement(Tuple2<Boolean, Row> arg0,
//        						ProcessFunction<Tuple2<Boolean, Row>, CytoVertex>.Context arg1, Collector<CytoVertex> arg2)
//        						throws Exception {
//        					// TODO Auto-generated method stub
//        					
//        				}
//        				
//        			})
//        			.process(new ProcessFunction<Tuple2<Boolean, Row>, CytoGraph>() {
//        					@Override
//        					public void processElement(Tuple2<Boolean, Row> element,
//        							ProcessFunction<Tuple2<Boolean, Row>, CytoGraph>.Context context,
//        							Collector<CytoGraph> collector) throws Exception {
//        						if (element.f0) {
//        							cytograph.addVertex(element.f1.getField(0).toString(), 
//        								Integer.parseInt(element.f1.getField(1).toString()),
//        								Integer.parseInt(element.f1.getField(2).toString()));
//        							collector.collect(cytograph);
//        						}
//        					}
//        				})
//        			.addSink(new SinkFunction<CytoGraph>() {
//
//        				private static final long serialVersionUID = -8999956705061275432L;
//        				@Override 
//        				public void invoke(CytoGraph element, Context context) {
//        					undertowTest.sendToAll("{ group: 'nodes', data: { id: 'n0' }, position: { x: 100, y: 100 } }");
//        					undertowTest.sendToAll(element.toString());
//        				}
//        			});
        			.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {

        				private static final long serialVersionUID = -8999956705061275432L;
        				@Override 
        				public void invoke(Tuple2<Boolean, Row> element, Context context) {
        					if (element.f0) {
        						undertowTest.sendToAll("addVertex;" + element.f1.getField(0).toString() + 
        							";" + element.f1.getField(1).toString() + ";" + element.f1.getField(2).toString() );
        					} else if (!element.f0) {
            					undertowTest.sendToAll("removeVertex;" + element.f1.getField(0).toString() + 
            							";" + element.f1.getField(1).toString() + ";" + element.f1.getField(2).toString() );
        					}
        				}
        			});
        			stream_edges.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {

        				private static final long serialVersionUID = -8999956705061275432L;
        				@Override 
        				public void invoke(Tuple2<Boolean, Row> element, Context context) {
        					if (element.f0) {
        						undertowTest.sendToAll("addEdge;" + element.f1.getField(0).toString() + 
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
