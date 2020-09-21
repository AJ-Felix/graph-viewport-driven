package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;

public class WrapperObjectSinkRetract implements SinkFunction<Tuple2<Boolean, VVEdgeWrapper>> {
	@Override 
	public void invoke(Tuple2<Boolean, VVEdgeWrapper> element, @SuppressWarnings("rawtypes") Context context) {
		if (element.f0) {
			UndertowServer.addWrapper(element.f1);
		} else {
			UndertowServer.removeWrapper(element.f1);
		}
	}
}