package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.types.Row;

public class VerticesMapTuple implements MapFunction<Row,
	Tuple8<String,String,Long,String,Integer,Integer,Long,Integer>>{

	@Override
	public Tuple8<String, String, Long, String, Integer, Integer, Long, Integer> map(Row value)
			throws Exception {
		return Tuple8.of(	value.getField(0).toString(),
							value.getField(1).toString(),
							(long) value.getField(2),
							value.getField(3).toString(),
							(int) value.getField(4),
							(int) value.getField(5),
							(long) value.getField(6),
							(int) value.getField(7));
	}
}
