package Temporary;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;
import org.apache.flink.types.Row;
import org.java_websocket.server.WebSocketServer;
import org.java_websocket.WebSocket;



public class TheSinkFunction<String> implements SinkFunction<String>() {
	
	@Override 
	public void invoke(Tuple2<Boolean, Row> element, Context context){
		System.out.println("addVertex;{ group: 'nodes', data: { id: '" + 
				element.f1.getField(0).toString()  + "' }, position: { x: " + 
				element.f1.getField(1).toString() + ", y: " + element.f1.getField(2).toString() + " } }");
	}
}
