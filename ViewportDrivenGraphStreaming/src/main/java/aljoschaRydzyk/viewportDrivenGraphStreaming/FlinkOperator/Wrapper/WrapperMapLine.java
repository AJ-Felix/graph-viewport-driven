package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class WrapperMapLine implements MapFunction<Row,String>{
	@Override
	public String map(Row value) throws Exception {
		return value.toString() + "\n";
	}
}
