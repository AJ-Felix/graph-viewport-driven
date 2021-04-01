package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.EPGMVertex;

public class VertexEPGMMapTupleDegreeComplex implements MapFunction<EPGMVertex, Tuple2<EPGMVertex,Long>> {
	
	@Override
	public Tuple2<EPGMVertex, Long> map(EPGMVertex vertex) throws Exception {
		return Tuple2.of(vertex, vertex.getPropertyValue("degree").getLong());
	}
}
