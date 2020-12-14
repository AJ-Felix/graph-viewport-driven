package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple17;
import org.apache.flink.types.Row;

public class WrapperMapTuple implements MapFunction<Row,Tuple17<
	String,
	String,Long,String,Integer,Integer,Long,Integer,
	String,Long,String,Integer,Integer,Long,Integer,
	String,String>>{

	@Override
	public Tuple17<
		String, 
		String, Long, String, Integer, Integer, Long, Integer, 
		String, Long, String, Integer, Integer, Long, Integer, 
		String, String> map(
			Row value) throws Exception {
		return Tuple17.of(
				value.getField(0).toString(),
				value.getField(1).toString(),
				(long) value.getField(2),
				value.getField(3).toString(),
				(int) value.getField(4),
				(int) value.getField(5),
				(long) value.getField(6),
				(int) value.getField(7),
				value.getField(8).toString(),
				(long) value.getField(9),
				value.getField(10).toString(),
				(int) value.getField(11),
				(int) value.getField(12),
				(long) value.getField(13),
				(int) value.getField(14),
				value.getField(15).toString(),
				value.getField(16).toString());
	}
}
