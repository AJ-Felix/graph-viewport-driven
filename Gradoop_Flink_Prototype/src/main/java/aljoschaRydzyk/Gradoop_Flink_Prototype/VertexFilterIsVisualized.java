package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterIsVisualized implements FilterFunction<Row> {
	Map<String,VertexCustom> visualizedVertices;
	
	public VertexFilterIsVisualized(Map<String,VertexCustom>  visualizedVertices) {
		this.visualizedVertices = visualizedVertices;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		return this.visualizedVertices.containsKey(value.getField(1));
	}

}
