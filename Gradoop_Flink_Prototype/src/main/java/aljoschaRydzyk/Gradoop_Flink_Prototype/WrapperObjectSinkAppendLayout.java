package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class WrapperObjectSinkAppendLayout implements SinkFunction<VVEdgeWrapper>{

	private final WrapperHandler handler = WrapperHandler.getInstance();

	@Override 
	public void invoke(VVEdgeWrapper element, @SuppressWarnings("rawtypes") Context context) {
//		Main.addWrapperLayout(element);
		handler.addWrapperLayout(element);
	}
}
