package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.VertexGVD;

public class VertexFilterIsLayouted implements FilterFunction<Row> {
	private Map<String,VertexGVD> layoutedVertices;
	
	public VertexFilterIsLayouted (Map<String,VertexGVD> layoutedVertices) {
		this.layoutedVertices = layoutedVertices;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		return this.layoutedVertices.containsKey(value.getField(1));
	}
}