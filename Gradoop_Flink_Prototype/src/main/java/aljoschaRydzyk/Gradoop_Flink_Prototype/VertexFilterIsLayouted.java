package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterIsLayouted implements FilterFunction<Row> {
	private Map<String,VertexCustom> layoutedVertices;
	
	public VertexFilterIsLayouted (Map<String,VertexCustom> layoutedVertices) {
		this.layoutedVertices = layoutedVertices;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		return this.layoutedVertices.containsKey(value.getField(1));
	}
}
