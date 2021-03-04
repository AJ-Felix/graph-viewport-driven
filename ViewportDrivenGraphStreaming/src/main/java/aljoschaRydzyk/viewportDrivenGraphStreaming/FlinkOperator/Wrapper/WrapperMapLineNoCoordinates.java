package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class WrapperMapLineNoCoordinates implements MapFunction<Row,String> {
	@Override
	public String map(Row value) throws Exception {
		return value.getField(0).toString() + "," +
			value.getField(1).toString() + "," +
			value.getField(2).toString() + "," +
			value.getField(3).toString() + "," +
			value.getField(6).toString() + "," +
			value.getField(7).toString() + "," +
			value.getField(8).toString() + "," +
			value.getField(9).toString() + "," +
			value.getField(10).toString() + "," +
			value.getField(13).toString() + "," +
			value.getField(14).toString() + "," +
			value.getField(15).toString() + "," +
			value.getField(16).toString() + 
			"\n";
	}
}
