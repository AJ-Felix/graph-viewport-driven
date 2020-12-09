package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper;

import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class WrapperFilterVisualizedWrappers implements FilterFunction<Row> {
	private Set<String> visualizedWrappers;
	
	public WrapperFilterVisualizedWrappers(Set<String> visualizedWrappers) {
		this.visualizedWrappers = visualizedWrappers;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		System.out.println(value.getField(15).toString());
		System.out.println(!visualizedWrappers.contains(value.getField(15).toString()));
		return !visualizedWrappers.contains(value.getField(15).toString()); 
	}
}
