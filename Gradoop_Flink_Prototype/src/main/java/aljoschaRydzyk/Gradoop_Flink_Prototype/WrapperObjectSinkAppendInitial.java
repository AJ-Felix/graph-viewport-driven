package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class WrapperObjectSinkAppendInitial implements SinkFunction<VVEdgeWrapper>{
	
//	private final WrapperHandler handler = WrapperHandler.getInstance();

	@Override 
	public void invoke(VVEdgeWrapper element, @SuppressWarnings("rawtypes") Context context) {
//		Main.addWrapperInitial(element);
		System.out.println("new element in initial sink!");
		WrapperHandler handler = WrapperHandler.getInstance();
		handler.addWrapperInitial(element);
	}
}
