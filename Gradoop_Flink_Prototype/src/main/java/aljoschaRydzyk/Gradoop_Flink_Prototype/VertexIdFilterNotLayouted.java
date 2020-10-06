package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;

public class VertexIdFilterNotLayouted implements FilterFunction<String> {
	private Set<String> layoutedVertices;
	
	public VertexIdFilterNotLayouted (Set<String> layoutedVertices) {
		this.layoutedVertices = layoutedVertices;
	}
	
	@Override
	public boolean filter(String value) throws Exception {
		return !this.layoutedVertices.contains(value);
	}
}