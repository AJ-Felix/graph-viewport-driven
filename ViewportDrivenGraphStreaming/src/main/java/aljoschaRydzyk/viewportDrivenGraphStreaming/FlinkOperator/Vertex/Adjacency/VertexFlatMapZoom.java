package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.Adjacency;

import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class VertexFlatMapZoom implements FlatMapFunction<Row,Row>{
	private Map<String,Map<String,String>> adjMatrix;
	private Map<String,Row> wrapperMap;
	private float top;
	private float right;
	private float bottom;
	private float left;
	
	public VertexFlatMapZoom(
			Map<String,Map<String,String>> adjMatrix,
			Map<String,Row> wrapperMap,
			float top,
			float right,
			float bottom,
			float left) {
		this.adjMatrix = adjMatrix;
		this.wrapperMap = wrapperMap;
		this.top = top;
		this.right = right;
		this.bottom = bottom;
		this.left = left;
	}
	
	@Override
	public void flatMap(Row value, Collector<Row> out) throws Exception {
		String firstVertexId = (String) value.getField(1);
		Map<String,String> map = adjMatrix.get(firstVertexId);
		for (String wrapperId : map.values()) {
			Row wrapper = wrapperMap.get(wrapperId);
			String secondVertexId;
			int secondVertexX;
			int secondVertexY;
			if (firstVertexId.equals(wrapper.getField(1))) {
				secondVertexId = wrapper.getField(7).toString();
				secondVertexX = (int) wrapper.getField(11);
				secondVertexY = (int) wrapper.getField(12);
			} else {
				secondVertexId = wrapper.getField(1).toString();
				secondVertexX = (int) wrapper.getField(4);
				secondVertexY = (int) wrapper.getField(5);
			}
			if ((left > secondVertexX) ||  (secondVertexX > right) || (top > secondVertexY) || (secondVertexY > bottom)) {
				out.collect(wrapper);
			} else {
				if (firstVertexId.compareTo(secondVertexId) < 0) {
					out.collect(wrapper);
				}
			}
		}
	}
}
