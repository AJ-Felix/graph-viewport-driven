package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;

public class VertexFilterNotVisualized implements FilterFunction<Row> {
	Map<String,VertexGVD> visualizedVertices;
	
	public VertexFilterNotVisualized(Map<String,VertexGVD>  visualizedVertices) {
		this.visualizedVertices = visualizedVertices;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		return !this.visualizedVertices.containsKey(value.getField(1).toString());
	}
}
