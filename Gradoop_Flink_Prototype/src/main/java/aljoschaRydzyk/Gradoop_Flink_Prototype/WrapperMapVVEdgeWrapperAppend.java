package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class WrapperMapVVEdgeWrapperAppend implements MapFunction<Row,VVEdgeWrapper> {
	@Override
	public VVEdgeWrapper map(Row value) throws Exception {
		VertexCustom sourceVertex = new VertexCustom(value.getField(1).toString(), value.getField(3).toString(), 
				(Integer) value.getField(2), (Integer) value.getField(4), (Integer) value.getField(5), (Long) value.getField(6));
		VertexCustom targetVertex = new VertexCustom(value.getField(7).toString(), value.getField(9).toString(), 
				(Integer) value.getField(8), (Integer) value.getField(10), (Integer) value.getField(11), (Long) value.getField(12));
		EdgeCustom edge = new EdgeCustom(value.getField(13).toString(), value.getField(14).toString(), value.getField(1).toString(), 
				value.getField(7).toString());
		return new VVEdgeWrapper(sourceVertex, targetVertex, edge);
	}
}
