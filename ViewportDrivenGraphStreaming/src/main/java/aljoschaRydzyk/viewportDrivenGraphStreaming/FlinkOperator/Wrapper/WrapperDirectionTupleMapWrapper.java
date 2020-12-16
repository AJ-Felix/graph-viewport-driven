package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

public class WrapperDirectionTupleMapWrapper implements MapFunction<Tuple2<Boolean,Row>,Row>{

	@Override
	public Row map(Tuple2<Boolean, Row> value) throws Exception {
		return value.f1;
	}
}
