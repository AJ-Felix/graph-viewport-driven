package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMEdge;

public class WrapperTupleComplexMapRow implements MapFunction<Tuple2<Tuple2<Row, EPGMEdge>,Row>,Row>{
	
	@Override
	public Row map(Tuple2<Tuple2<Row, EPGMEdge>, Row> tuple) throws Exception {
		EPGMEdge edge = tuple.f0.f1;
		Row sourceVertex = tuple.f0.f0;
		Row targetVertex = tuple.f1;
		Row vertices = Row.join(sourceVertex, Row.project(targetVertex, new int[] {1, 2, 3, 4, 5, 6, 7}));
		return Row.join(vertices, Row.of(edge.getId().toString(), edge.getLabel()));
	}
}
