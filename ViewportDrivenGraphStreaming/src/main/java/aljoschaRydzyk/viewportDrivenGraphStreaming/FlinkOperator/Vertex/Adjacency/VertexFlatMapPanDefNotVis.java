package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.Adjacency;

import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class VertexFlatMapPanDefNotVis implements FlatMapFunction<Row,Row>{
	private Map<String,Map<String,String>> adjMatrix;
	private Map<String,Row> wrapperMap;
	private float topNew;
	private float rightNew;
	private float bottomNew;
	private float leftNew;
	private float topOld;
	private float rightOld;
	private float bottomOld;
	private float leftOld;
	
	public VertexFlatMapPanDefNotVis(Map<String, Map<String, String>> adjMatrix, Map<String, Row> wrapperMap,
			float topNew, float rightNew, float bottomNew, float leftNew, float topOld, float rightOld, float bottomOld,
			float leftOld) {
		this.adjMatrix = adjMatrix;
		this.wrapperMap = wrapperMap;
		this.topNew = topNew;
		this.rightNew = rightNew;
		this.bottomNew = bottomNew;
		this.leftNew = leftNew;
		this.topOld = topOld;
		this.rightOld = rightOld;
		this.bottomOld = bottomOld;
		this.leftOld = leftOld;
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
			if (((leftOld > secondVertexX) || (secondVertexX > rightOld) || (topOld > secondVertexY) || (secondVertexY > bottomOld)) && 
					((leftNew > secondVertexX) || (secondVertexX > rightNew) || (topNew > secondVertexY) || (secondVertexY > bottomNew))) {
				out.collect(wrapper);
			} else {
				if (((leftOld > secondVertexX) || (secondVertexX > rightOld) || (topOld > secondVertexY) || (secondVertexY > bottomOld)) 
						&& (firstVertexId.compareTo(secondVertexId) < 0)) {
					out.collect(wrapper);
				}
			}
		}
	}
}