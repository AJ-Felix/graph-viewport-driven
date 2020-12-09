package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.Adjacency;

import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class VertexFlatMapPanMaybeVis implements FlatMapFunction<Row,Row>{
	private Map<String,Map<String,String>> adjMatrix;
	private Map<String,Row> wrapperMap;
	private float topNew;
	private float rightNew;
	private float bottomNew;
	private float leftNew;

	public VertexFlatMapPanMaybeVis(Map<String, Map<String, String>> adjMatrix, Map<String, Row> wrapperMap,
			float topNew, float rightNew, float bottomNew, float leftNew) {
		this.adjMatrix = adjMatrix;
		this.wrapperMap = wrapperMap;
		this.topNew = topNew;
		this.rightNew = rightNew;
		this.bottomNew = bottomNew;
		this.leftNew = leftNew;
	}

	@Override
	public void flatMap(Row vertexId, Collector<Row> out) throws Exception {			
		String firstVertexId = (String) vertexId.getField(1);
		Map<String,String> map = adjMatrix.get(firstVertexId);
		for (String wrapperId : map.values()) {
			int secondVertexX;
			int secondVertexY;
			Row wrapper = wrapperMap.get(wrapperId);
			if (firstVertexId.equals(wrapper.getField(1))) {
				secondVertexX = (int) wrapper.getField(11);
				secondVertexY = (int) wrapper.getField(12);
			} else {
				secondVertexX = (int) wrapper.getField(4);
				secondVertexY = (int) wrapper.getField(5);
			}
			if ((leftNew <= secondVertexX) &&  (secondVertexX <= rightNew) && (topNew <= secondVertexY) && (secondVertexY <= bottomNew)) {
				out.collect(wrapper);
			} 
		}
	}
}
