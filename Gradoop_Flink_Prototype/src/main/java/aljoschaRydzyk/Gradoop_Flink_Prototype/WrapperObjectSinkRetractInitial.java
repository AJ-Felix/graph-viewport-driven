package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class WrapperObjectSinkRetractInitial implements SinkFunction<Tuple2<Boolean, VVEdgeWrapper>> {
	
	private final WrapperHandler handler = WrapperHandler.getInstance();

	@Override 
	public void invoke(Tuple2<Boolean, VVEdgeWrapper> element, @SuppressWarnings("rawtypes") Context context) {
		if (element.f0) {
//			Main.addWrapperInitial(element.f1);
			handler.addWrapperInitial(element.f1);
		} else {
//			Main.removeWrapper(element.f1);
			handler.removeWrapper(element.f1);
		}
	}
}
