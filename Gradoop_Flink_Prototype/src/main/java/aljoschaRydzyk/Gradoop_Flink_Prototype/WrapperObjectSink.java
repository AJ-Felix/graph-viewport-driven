package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;
import org.apache.flink.types.Row;

public class WrapperObjectSink implements SinkFunction<VVEdgeWrapper>{
//	private GraphVis graphVis;
	
	public WrapperObjectSink() {
//		this.graphVis = graphVis;
	}
	
	@Override 
	public void invoke(VVEdgeWrapper element, @SuppressWarnings("rawtypes") Context context) {
		UndertowServer.addWrapper(element);
	}
}
