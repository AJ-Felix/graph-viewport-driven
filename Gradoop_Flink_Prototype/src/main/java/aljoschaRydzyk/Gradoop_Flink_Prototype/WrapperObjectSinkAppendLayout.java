package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;

public class WrapperObjectSinkAppendLayout implements SinkFunction<VVEdgeWrapper>{
	@Override 
	public void invoke(VVEdgeWrapper element, @SuppressWarnings("rawtypes") Context context) {
		Main.addWrapperLayout(element);
	}
}
