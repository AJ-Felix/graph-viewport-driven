package aljoschaRydzyk.Gradoop_Flink_Prototype;


import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.types.Row;
import org.apache.log4j.BasicConfigurator;
import com.corundumstudio.socketio.*;
import com.corundumstudio.socketio.listener.*;


public class Server_SocketIO {

    public static void main(String[] args) throws Exception {
    	
    	BasicConfigurator.configure();
		
		com.corundumstudio.socketio.Configuration config = new com.corundumstudio.socketio.Configuration();
        config.setHostname("localhost");
        config.setPort(9092);

        final SocketIOServer server = new SocketIOServer(config);
		        
        server.addEventListener("buildTopView", String.class, new DataListener<String>() {

			@Override
			public void onData(SocketIOClient client, String data, AckRequest ackSender) throws Exception {
				FlinkCore flinkCore = new FlinkCore();
				List<DataStream<Tuple2<Boolean, Row>>> graph_data_streams = flinkCore.buildTopView();
				DataStream<Tuple2<Boolean, Row>> stream_vertices = graph_data_streams.get(0);
				DataStream<Tuple2<Boolean, Row>> stream_edges = graph_data_streams.get(1);
				Iterator<Tuple2<Boolean,Row>> iterator_vertices = DataStreamUtils.collect(stream_vertices);
				Iterator<Tuple2<Boolean,Row>> iterator_edges = DataStreamUtils.collect(stream_edges);
				iterator_vertices.forEachRemaining(vertex -> {
					System.out.println(vertex);
					if (vertex.f0) {
						server.getBroadcastOperations().sendEvent("addVertex", new VertexObject(vertex.f0.toString(), vertex.f1.getField(0).toString(), 
								vertex.f1.getField(1).toString(), vertex.f1.getField(2).toString(), vertex.f1.getField(3).toString()));
					} else if (vertex.f0) {
						server.getBroadcastOperations().sendEvent("removeVertex", new VertexObject(vertex.f0.toString(), vertex.f1.getField(0).toString(), 
								vertex.f1.getField(1).toString(), vertex.f1.getField(2).toString(), vertex.f1.getField(3).toString()));
					}
				});
				iterator_edges.forEachRemaining(edge -> {
					System.out.println(edge);
					if (edge.f0) {
					server.getBroadcastOperations().sendEvent("addEdge", new VertexObject(edge.f0.toString(), edge.f1.getField(0).toString(), 
							edge.f1.getField(1).toString(), edge.f1.getField(2).toString(), edge.f1.getField(3).toString()));
					} else if (edge.f0) {
					server.getBroadcastOperations().sendEvent("removeEdge", new VertexObject(edge.f0.toString(), edge.f1.getField(0).toString(), 
							edge.f1.getField(1).toString(), edge.f1.getField(2).toString(), edge.f1.getField(3).toString()));
					}
				});
			}
        });
				
				// process Function had serialization problem with Socket.IO
//				stream_vertices.process(new ProcessFunction<Tuple2<Boolean, Row>, Tuple2<Boolean, Row>>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public void processElement(Tuple2<Boolean, Row> element,
//							ProcessFunction<Tuple2<Boolean, Row>, Tuple2<Boolean, Row>>.Context context,
//							Collector<Tuple2<Boolean, Row>> collector) throws Exception {
//						System.out.println(element);
//						if (element.f0) {
//							server.getBroadcastOperations().sendEvent("addVertex", new VertexObject(element.f0.toString(), element.f1.getField(0).toString(), 
//									element.f1.getField(1).toString(), element.f1.getField(2).toString(), element.f1.getField(3).toString()));
//						} else if (element.f0) {
//							server.getBroadcastOperations().sendEvent("removeVertex", new VertexObject(element.f0.toString(), element.f1.getField(0).toString(), 
//									element.f1.getField(1).toString(), element.f1.getField(2).toString(), element.f1.getField(3).toString()));
//						}
//					}
//				});
//				stream_edges.process(new ProcessFunction<Tuple2<Boolean, Row>, Tuple2<Boolean, Row>>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public void processElement(Tuple2<Boolean, Row> element,
//							ProcessFunction<Tuple2<Boolean, Row>, Tuple2<Boolean, Row>>.Context context,
//							Collector<Tuple2<Boolean, Row>> collector) throws Exception {
//						if (element.f0) {
//							server.getBroadcastOperations().sendEvent("addEdge", new EdgeObject(element.f0.toString(), element.f1.getField(0).toString(), 
//									element.f1.getField(1).toString(), element.f1.getField(2).toString()));
//						} else if (element.f0) {
//							server.getBroadcastOperations().sendEvent("removeEdge", new EdgeObject(element.f0.toString(), element.f1.getField(0).toString(), 
//									element.f1.getField(1).toString(), element.f1.getField(2).toString()));
//						}
//					}
//				});
//			}
//        });

        server.start();
        System.out.println("Hit enter to stop:...");
        System.in.read();

        server.stop();
         
    }
}
