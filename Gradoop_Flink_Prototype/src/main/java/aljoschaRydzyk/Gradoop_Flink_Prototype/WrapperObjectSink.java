package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;
import org.apache.flink.types.Row;

public class WrapperObjectSink implements SinkFunction<VVEdgeWrapper>{
	private GraphVis graphVis;
	
	public WrapperObjectSink(GraphVis graphVis) {
		this.graphVis = graphVis;
	}
	
	@Override 
	public void invoke(VVEdgeWrapper element, @SuppressWarnings("rawtypes") Context context) {
		this.graphVis.addWrapper(element);
	}
}
