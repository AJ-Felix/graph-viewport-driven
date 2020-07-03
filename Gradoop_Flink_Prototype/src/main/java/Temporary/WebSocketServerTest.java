package Temporary;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import aljoschaRydzyk.Gradoop_Flink_Prototype.FlinkCore;

public class WebSocketServerTest extends WebSocketServer {
	
	private static transient CytoGraph<Object, Object> cytograph = new CytoGraph<>();

	public WebSocketServerTest( int port ) throws UnknownHostException {
		super( new InetSocketAddress( port ) );
	}

	public WebSocketServerTest( InetSocketAddress address ) {
		super( address );
	}

	@Override
	public void onOpen( WebSocket conn, ClientHandshake handshake ) {
		conn.send("Welcome to the server!"); //This method sends a message to the new client
		broadcast( "new connection: " + handshake.getResourceDescriptor() ); //This method sends a message to all clients connected
		System.out.println( conn.getRemoteSocketAddress().getAddress().getHostAddress() + " entered the room!" );
	}

	@Override
	public void onClose( WebSocket conn, int code, String reason, boolean remote ) {
		broadcast( conn + " has left the room!" );
		System.out.println( conn + " has left the room!" );
	}

	@Override
	public void onMessage( WebSocket conn, String message ) {
		broadcast( message );
		System.out.println( conn + ": " + message );
		if (message.equals("buildTopView")) {
			
			ObjectMapper objmap = new ObjectMapper();
			VertexObject vertex = new VertexObject("true", "12334567", "300", "300", "198");
			String cytoAddString  = "{ group: 'nodes', data: { id: '" + vertex.getId() + "' }, position: { x: " + vertex.getX() + ", y: " + vertex.getY() + " } }";
			try {
				broadcast(objmap.writeValueAsString(vertex));
				broadcast(cytoAddString);
				broadcast("addVertex;" + cytoAddString);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			FlinkCore flinkCore = new FlinkCore();
			List<DataStream<Tuple2<Boolean, Row>>> graph_data_streams = flinkCore.buildTopView();
			DataStream<Tuple2<Boolean, Row>> stream_vertices = graph_data_streams.get(0);
			DataStream<Tuple2<Boolean, Row>> stream_edges = graph_data_streams.get(1);
			stream_vertices
//			.process(new ProcessFunction<Tuple2<Boolean, Row>, CytoVertex>(){
//
//				@Override
//				public void processElement(Tuple2<Boolean, Row> arg0,
//						ProcessFunction<Tuple2<Boolean, Row>, CytoVertex>.Context arg1, Collector<CytoVertex> arg2)
//						throws Exception {
//					// TODO Auto-generated method stub
//					
//				}
//				
//			})
			.process(new ProcessFunction<Tuple2<Boolean, Row>, String>() {
					@Override
					public void processElement(Tuple2<Boolean, Row> element,
							ProcessFunction<Tuple2<Boolean, Row>, String>.Context context,
							Collector<String> collector) throws Exception {
						if (element.f0) {
//							cytograph.addVertex(element.f1.getField(0).toString(), 
//								Integer.parseInt(element.f1.getField(1).toString()) +
//								Integer.parseInt(element.f1.getField(2).toString()));
							collector.collect("teststrngg");
						}
					}
				})
			.addSink(new SinkFunction<String>() {

				private static final long serialVersionUID = -8999956705061275432L;
				@Override 
				public void invoke(String element, Context context) {
					broadcast("{ group: 'nodes', data: { id: 'n0' }, position: { x: 100, y: 100 } }");
//					broadcast("addVertex;{ group: 'nodes', data: " + element.toString() + " }");
				}
			});
			try {
				flinkCore.getFsEnv().execute();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			//  { group: 'nodes', data: { id: 'n0' }, position: { x: 100, y: 100 } },
		}
	}
	@Override
	public void onMessage( WebSocket conn, ByteBuffer message ) {
		broadcast( message.array() );
		System.out.println( conn + ": " + message );
	}


	public static void main( String[] args ) throws InterruptedException , IOException {
		int port = 8887; // 843 flash policy port
		try {
			port = Integer.parseInt( args[ 0 ] );
		} catch ( Exception ex ) {
		}
		WebSocketServerTest s = new WebSocketServerTest( port );
		s.start();
		System.out.println( "ChatServer started on port: " + s.getPort() );

		BufferedReader sysin = new BufferedReader( new InputStreamReader( System.in ) );
		while ( true ) {
			String in = sysin.readLine();
			s.broadcast( in );
			if( in.equals( "exit" ) ) {
				s.stop(1000);
				break;
			}
		}
	}
	@Override
	public void onError( WebSocket conn, Exception ex ) {
		ex.printStackTrace();
		if( conn != null ) {
			// some errors like port binding failed may not be assignable to a specific websocket
		}
	}

	@Override
	public void onStart() {
		System.out.println("Server started!");
		setConnectionLostTimeout(0);
		setConnectionLostTimeout(100);
	}

}