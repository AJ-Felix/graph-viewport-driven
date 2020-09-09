package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

public class WrapperMapVVEdgeWrapperRetract implements MapFunction<Tuple2<Boolean,Row>,Tuple2<Boolean,VVEdgeWrapper>> {
	
	@Override
	public Tuple2<Boolean, VVEdgeWrapper> map(Tuple2<Boolean, Row> value) throws Exception {
		VertexCustom sourceVertex = new VertexCustom(value.f1.getField(1).toString(), value.f1.getField(3).toString(), 
				(Integer) value.f1.getField(2), (Integer) value.f1.getField(4), (Integer) value.f1.getField(5), (Long) value.f1.getField(6));
		VertexCustom targetVertex = new VertexCustom(value.f1.getField(7).toString(), value.f1.getField(9).toString(), 
				(Integer) value.f1.getField(8), (Integer) value.f1.getField(10), (Integer) value.f1.getField(11), (Long) value.f1.getField(12));
		EdgeCustom edge = new EdgeCustom(value.f1.getField(13).toString(), value.f1.getField(14).toString(), value.f1.getField(1).toString(), 
				value.f1.getField(7).toString());
		return Tuple2.of(value.f0, new VVEdgeWrapper(sourceVertex, targetVertex, edge));
	}

}
