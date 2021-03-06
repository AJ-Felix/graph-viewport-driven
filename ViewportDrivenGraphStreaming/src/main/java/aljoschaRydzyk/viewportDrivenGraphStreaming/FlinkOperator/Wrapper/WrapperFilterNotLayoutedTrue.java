package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper;

import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class WrapperFilterNotLayoutedTrue implements FilterFunction<Row> {
	private Set<String> layoutedVertices;
	
	public WrapperFilterNotLayoutedTrue (Set<String> layoutedVertices) {
		this.layoutedVertices = layoutedVertices;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		return !this.layoutedVertices.contains(value.getField(8).toString());
	}
}
