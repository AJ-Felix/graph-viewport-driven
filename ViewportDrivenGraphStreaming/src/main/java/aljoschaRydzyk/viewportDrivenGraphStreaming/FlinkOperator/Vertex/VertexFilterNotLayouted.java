package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterNotLayouted implements FilterFunction<Row>{
	private Set<String> layoutedVertices;
	
	public VertexFilterNotLayouted (Set<String> layoutedVertices) {
		this.layoutedVertices = layoutedVertices;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		return !this.layoutedVertices.contains(value.getField(1).toString());
	}
}
