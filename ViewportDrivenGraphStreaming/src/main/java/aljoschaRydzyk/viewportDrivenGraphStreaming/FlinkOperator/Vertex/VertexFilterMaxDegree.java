package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterMaxDegree implements FilterFunction<Row>{
	private long numberVertices;
	
	public VertexFilterMaxDegree(long numberVertices) {
		this.numberVertices = numberVertices;
	}
	@Override
	public boolean filter(Row value) throws Exception {
		return (long) value.getField(2) < numberVertices;
	}
}
