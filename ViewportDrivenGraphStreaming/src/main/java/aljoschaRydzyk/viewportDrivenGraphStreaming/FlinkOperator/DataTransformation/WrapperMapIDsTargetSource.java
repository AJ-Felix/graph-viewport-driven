package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

public class WrapperMapIDsTargetSource implements MapFunction<Row, Tuple3<String,String,String>>{

	@Override
	public Tuple3<String, String, String> map(Row value) throws Exception {
		return Tuple3.of(
				value.getField(8).toString(), 
				value.getField(1).toString(),
				value.getField(15).toString());
	}
}