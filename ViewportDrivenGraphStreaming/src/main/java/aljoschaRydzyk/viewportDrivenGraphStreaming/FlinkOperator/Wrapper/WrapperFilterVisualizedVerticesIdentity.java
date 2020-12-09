package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper;

import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class WrapperFilterVisualizedVerticesIdentity implements FilterFunction<Row> {
	private Set<String> visualizedVertices;
	
	public WrapperFilterVisualizedVerticesIdentity(Set<String> visualizedVertices) {
		this.visualizedVertices = visualizedVertices;
	}

	@Override
	public boolean filter(Row value) throws Exception {
		return !(visualizedVertices.contains(value.getField(1).toString()) && value.getField(16).equals("identityEdge"));
	}
}
