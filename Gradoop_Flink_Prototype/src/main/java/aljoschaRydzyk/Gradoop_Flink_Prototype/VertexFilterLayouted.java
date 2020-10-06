package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterLayouted implements FilterFunction<Row> {
	private Set<String> layoutedVertices;
	
	public VertexFilterLayouted (Set<String> layoutedVertices) {
		this.layoutedVertices = layoutedVertices;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		return this.layoutedVertices.contains(value.getField(1));
	}
}
