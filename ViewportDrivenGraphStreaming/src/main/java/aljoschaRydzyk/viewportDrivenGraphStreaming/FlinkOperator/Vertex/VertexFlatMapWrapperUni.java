package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class VertexFlatMapWrapperUni implements FlatMapFunction<Row,Tuple2<Boolean,Row>>{
	private Map<String,Map<String,String>> adjMatrix;
	private Map<String,Row> wrapperMap;

	public VertexFlatMapWrapperUni(Map<String,Map<String,String>> adjMatrix, Map<String,Row> layoutedVertices) {
		this.adjMatrix = adjMatrix;
		this.wrapperMap = layoutedVertices;
	}
	
	@Override
	public void flatMap(Row vertexRow, Collector<Tuple2<Boolean, Row>> out) throws Exception {
		String sourceId = vertexRow.getField(1).toString();
		for (String wrapperId : adjMatrix.get(sourceId).values()) {
			Row wrapper = wrapperMap.get(wrapperId);
			if (sourceId.equals(wrapper.getField(1).toString())) {
				if (sourceId.compareTo(wrapper.getField(8).toString()) > 0) out.collect(Tuple2.of(true, wrapper));
			} else {
				if (sourceId.compareTo(wrapper.getField(1).toString()) > 0) out.collect(Tuple2.of(false, wrapper));
			}
		}
	}
}
