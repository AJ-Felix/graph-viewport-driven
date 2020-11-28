package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterNotVisualized implements FilterFunction<Row> {
	private Set<String> visualizedVertices;
	
	public VertexFilterNotVisualized(Set<String>  visualizedVertices) {
		this.visualizedVertices = visualizedVertices;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		return !this.visualizedVertices.contains(value.getField(1).toString());
	}
}
