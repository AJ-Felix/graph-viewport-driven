package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class WrapperFilterTargetIsVisualized implements FilterFunction<Row> {
	private Map<String,VertexCustom> visualizedVertices;
	
	public WrapperFilterTargetIsVisualized(Map<String,VertexCustom> visualizedVertices) {
		this.visualizedVertices = visualizedVertices; 
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		return this.visualizedVertices.containsKey(value.getField(7));
	}

}
