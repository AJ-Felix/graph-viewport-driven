package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.Map;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterLayouted implements FilterFunction<Row>{
	private Map<String,VertexCustom> layoutedVertices;
	
	public VertexFilterLayouted (Map<String,VertexCustom> layoutedVertices) {
		this.layoutedVertices = layoutedVertices;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		return this.layoutedVertices.containsKey(value.getField(1));
	}
}
