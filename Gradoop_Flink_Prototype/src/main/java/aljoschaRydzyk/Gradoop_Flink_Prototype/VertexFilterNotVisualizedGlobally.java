package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.Map;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterNotVisualizedGlobally implements FilterFunction<Row> {
	Map<String, Map<String,Object>> visualizedVertices;
	
	public VertexFilterNotVisualizedGlobally(Map<String, Map<String,Object>>  visualizedVertices) {
		this.visualizedVertices = visualizedVertices;
	}
	@Override
	public boolean filter(Row value) throws Exception {
		return !this.visualizedVertices.containsKey(value.getField(1));
	}

}
