package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMVertex;

public class VertexTupleComplexMapRow implements MapFunction<Tuple2<Long,Tuple2<EPGMVertex,Long>>,Row> {
	private String graphId;
	private int zoomLevelSetSize;
	
	public VertexTupleComplexMapRow(String graphId, int zoomLevelSetSize) {
		this.graphId = graphId;
		this.zoomLevelSetSize = zoomLevelSetSize;
	}
	
	@Override
	public Row map(Tuple2<Long,Tuple2<EPGMVertex,Long>> tuple) throws Exception {
		EPGMVertex vertex = tuple.f1.f0;
		return Row.of(graphId, vertex.getId(), tuple.f0, vertex.getLabel(),
				vertex.getPropertyValue("X").getInt(), vertex.getPropertyValue("Y").getInt(),
				vertex.getPropertyValue("degree").getLong(), 
				Integer.parseInt(String.valueOf(tuple.f0)) / zoomLevelSetSize);
	}
}
