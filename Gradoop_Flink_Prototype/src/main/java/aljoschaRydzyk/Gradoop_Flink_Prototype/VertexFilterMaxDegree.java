package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterMaxDegree implements FilterFunction<Row>{
	private Integer numberVertices;
	
	public VertexFilterMaxDegree(Integer numberVertices) {
		this.numberVertices = numberVertices;
	}
	@Override
	public boolean filter(Row value) throws Exception {
		if ((Integer) value.getField(2) < numberVertices) return true;
		else return false;
	}
}
