package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.Adjacency;

import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class VertexFlatMapMaxDegree implements FlatMapFunction<Row,Row>{
	private Map<String,Map<String,String>> adjMatrix;
	private Map<String,Row> wrapperMap;
	private int numberVertices;
	
	public VertexFlatMapMaxDegree(
			Map<String,Map<String,String>> adjMatrix,
			Map<String,Row> wrapperMap,
			int numberVertices
			) {
		this.adjMatrix = adjMatrix;
		this.wrapperMap = wrapperMap;
		this.numberVertices = numberVertices;
	}
	
	@Override
	public void flatMap(Row value, Collector<Row> out) throws Exception {
		String firstVertexIdGradoop = (String) value.getField(1);
		Long firstVertexIdNumeric = (Long) value.getField(2);
		Map<String,String> map = adjMatrix.get(firstVertexIdGradoop);
		for (String wrapperId : map.values()) {
			Row wrapper = wrapperMap.get(wrapperId);
			Long secondVertexIdNumeric;
			if (wrapper.getField(1).equals(firstVertexIdGradoop)) {
				secondVertexIdNumeric = (long) wrapper.getField(9);
			} else {
				secondVertexIdNumeric = (long) wrapper.getField(2);
			}
			if (secondVertexIdNumeric < numberVertices && firstVertexIdNumeric > secondVertexIdNumeric) {
				out.collect(wrapper);
			} 
		}
	}
}