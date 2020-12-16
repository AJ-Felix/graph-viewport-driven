package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper;

import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class WrapperFilterNotVisualizedTrue implements FilterFunction<Row> {
	private Set<String> visualizedVertices;
	
	public WrapperFilterNotVisualizedTrue(Set<String>  visualizedVertices) {
		this.visualizedVertices = visualizedVertices;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		return !this.visualizedVertices.contains(value.getField(8).toString());
	}
}