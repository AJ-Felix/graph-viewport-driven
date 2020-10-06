package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;

public class VertexIdFilterNotVisualized implements FilterFunction<String> {
	private Set<String> visualizedVertices;
	
	public VertexIdFilterNotVisualized (Set<String> visualizedVertices) {
		this.visualizedVertices = visualizedVertices;
	}
	
	@Override
	public boolean filter(String value) throws Exception {
		return !this.visualizedVertices.contains(value);
	}
}
