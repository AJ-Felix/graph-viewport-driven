package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class WrapperObjectSinkAppendInitial implements SinkFunction<VVEdgeWrapper>{
	@Override 
	public void invoke(VVEdgeWrapper element, @SuppressWarnings("rawtypes") Context context) {
		Main.addWrapperInitial(element);
	}
}