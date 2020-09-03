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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class UndertowServer {
	
	private static FlinkCore flinkCore;
	
	private static ArrayList<WebSocketChannel> channels = new ArrayList<>();
    private static String webSocketListenPath = "/graphData";
    private static int webSocketListenPort = 8887;
    private static String webSocketHost = "localhost";
    
    private static Integer maxVertices = 10;
    
    private static Float viewportPixelX = (float) 1000;
    private static Float viewportPixelY = (float) 1000;
    private static float zoomLevel = 1;
    
//    private static FlinkApi api = new FlinkApi();
    
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
                })).addPrefixPath("/", resource(new ClassPathResourceManager(UndertowServer.class.getClassLoader(),
                        UndertowServer.class.getPackage())).addWelcomeFiles("index.html")/*.setDirectoryListingEnabled(true)*/))
                .build();
        server.start();
        System.out.println("Server started!");
//        api.getApiClient().setBasePath("http://localhost:8081");  
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
                if (messageData.startsWith("TestThread")){
                	TestThread thread = new TestThread("prototype");
            		thread.start();
                }
                if (messageData.startsWith("edgeIdString")) {
                	String[] arrMessageData = messageData.split(";");
                	List<String> list = new ArrayList<String>(Arrays.asList(arrMessageData));
                	list.remove(0);
                	Set<String> visualizedWrappers = new HashSet<String>(list);
                	flinkCore.getGraphUtil().setVisualizedWrappers(visualizedWrappers);
                }
                if (messageData.startsWith("vertexIdString")) {
                	String[] arrMessageData = messageData.split(";");
                	List<String> list = new ArrayList<String>(Arrays.asList(arrMessageData));
                	list.remove(0);
                	Set<String> visualizedVertices = new HashSet<String>(list);
                	flinkCore.getGraphUtil().setVisualizedVertices(visualizedVertices);
                }
                if (messageData.startsWith("buildTopView")) {
                	flinkCore = new FlinkCore();
                	String[] arrMessageData = messageData.split(";");
        				if (arrMessageData[1].equals("retract")) {
	        			flinkCore.initializeGradoopGraphUtil();
	        			DataStream<Tuple2<Boolean, Row>> wrapperStream = flinkCore.buildTopViewRetract(maxVertices);
	        			wrapperStream.addSink(new WrapperRetractSink());
        			} else if (arrMessageData[1].equals("appendJoin")) {
        				flinkCore.initializeCSVGraphUtilJoin();
        				DataStream<Row> wrapperStream = flinkCore.buildTopViewAppendJoin(maxVertices);
        				wrapperStream.addSink(new WrapperAppendSink());
        			} else if (arrMessageData[1].contentEquals("adjacency")) {
        				flinkCore.initializeAdjacencyGraphUtil();
        				DataStream<Row> wrapperStream = flinkCore.buildTopViewAdjacency(maxVertices);
        				wrapperStream.addSink(new WrapperAppendSink());
        			}
        			try {
        				flinkCore.getFsEnv().execute();
        			} catch (Exception e) {
        				e.printStackTrace();
        			}
                }
    			if (messageData.startsWith("zoom")) {
        			String[] arrMessageData = messageData.split(";");
        			Float xRenderPos = Float.parseFloat(arrMessageData[1]);
        			Float yRenderPos = Float.parseFloat(arrMessageData[2]);
        			zoomLevel = Float.parseFloat(arrMessageData[3]);
        			Float topModelPos = (- yRenderPos / zoomLevel);
        			Float leftModelPos = (- xRenderPos /zoomLevel);
        			Float bottomModelPos = (topModelPos + viewportPixelY / zoomLevel);
        			Float rightModelPos = (leftModelPos + viewportPixelX / zoomLevel);
					flinkCore.setTopModelPos(topModelPos);
					flinkCore.setRightModelPos(rightModelPos);
					flinkCore.setBottomModelPos(bottomModelPos);
					flinkCore.setLeftModelPos(leftModelPos);
        			DataStream<Row> wrapperStream = flinkCore.zoom(topModelPos, rightModelPos, bottomModelPos, leftModelPos);
					wrapperStream.addSink(new WrapperAppendSink());
					try {
						flinkCore.getFsEnv().execute();
					} catch (Exception e) {
						e.printStackTrace();
					}
    			}
    			if (messageData.startsWith("pan")) {
        			String[] arrMessageData = messageData.split(";");
        			Float topModelPos = flinkCore.gettopModelPos();
        			Float bottomModelPos = flinkCore.getBottomModelPos();
        			Float leftModelPos = flinkCore.getLeftModelPos();
        			Float rightModelPos = flinkCore.getRightModelPos();
        			Float xModelDiff = Float.parseFloat(arrMessageData[1]); 
        			Float yModelDiff = Float.parseFloat(arrMessageData[2]);
					DataStream<Row> wrapperStream = flinkCore.pan(topModelPos, rightModelPos, bottomModelPos, leftModelPos, xModelDiff, yModelDiff);
					flinkCore.setTopModelPos(topModelPos + yModelDiff);
					flinkCore.setBottomModelPos(bottomModelPos + yModelDiff);
					flinkCore.setLeftModelPos(leftModelPos + xModelDiff);
					flinkCore.setRightModelPos(rightModelPos + xModelDiff);
					wrapperStream.addSink(new WrapperAppendSink());
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
					wrapperStream.addSink(new WrapperAppendSink());
					try {
						flinkCore.getFsEnv().execute();
					} catch (Exception e) {
						e.printStackTrace();
					}
					UndertowServer.sendToAll("fitGraph");
//					UndertowServer.sendToAll("layout");
    			}
    			if (messageData.startsWith("cancel")){
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

    /**
     * sends a message to the all connected web socket clients
     */
    public static void sendToAll(String message) {
        for (WebSocketChannel session : channels) {
            WebSockets.sendText(message, session, null);
        }
    }
}
