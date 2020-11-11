package Temporary;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;
import org.apache.flink.types.Row;

public class SecondServerSink implements SinkFunction<Row> {
	
	private final Server2 server2 = Server2.getInstance();
	
	@Override 
	public void invoke(Row element, @SuppressWarnings("rawtypes") Context context) {
//		Main.addWrapperInitial(element);
		System.out.println("new element in initial sink!");
//		WrapperHandler handler = WrapperHandler.getInstance();
		server2.sendToAll(element.toString());
	}
}
