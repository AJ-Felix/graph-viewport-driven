package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Map;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;

public class VertexFilterNotLayouted implements FilterFunction<Row>{
	private Map<String,VertexGVD> layoutedVertices;
	
	public VertexFilterNotLayouted (Map<String,VertexGVD> layoutedVertices) {
		this.layoutedVertices = layoutedVertices;
		for (String key : layoutedVertices.keySet()) 
			System.out.println("VertexFilterNotLayouted, Constructor: " + key);
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		for (String key : layoutedVertices.keySet()) 
			System.out.println("VertexFilterNotLayouted, Constructor: " + key);
		System.out.println("VertexFilterNotLayouted: " + 
			this.layoutedVertices.containsKey(value.getField(1).toString()) +
				value.getField(1));
		return !this.layoutedVertices.containsKey(value.getField(1).toString());
	}
}
