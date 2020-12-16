package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

public class WrapperFilterReverseDirection implements FilterFunction<Tuple2<Boolean,Row>>{

	@Override
	public boolean filter(Tuple2<Boolean,Row> value) throws Exception {
		return !value.f0;
	}
}