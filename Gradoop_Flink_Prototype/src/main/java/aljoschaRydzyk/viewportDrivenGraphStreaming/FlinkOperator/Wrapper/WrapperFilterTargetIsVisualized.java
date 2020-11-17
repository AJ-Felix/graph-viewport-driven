package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.VertexGVD;

public class WrapperFilterTargetIsVisualized implements FilterFunction<Row> {
	private Map<String,VertexGVD> visualizedVertices;
	
	public WrapperFilterTargetIsVisualized(Map<String,VertexGVD> visualizedVertices) {
		this.visualizedVertices = visualizedVertices; 
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		return this.visualizedVertices.containsKey(value.getField(7));
	}

}
