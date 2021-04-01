package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

public class WrapperTupleRowMapWrapperRow implements MapFunction<Tuple2<Tuple2<Row, Row>,Row>,Row>{
	@Override
	public Row map(Tuple2<Tuple2<Row, Row>, Row> tuple) throws Exception {
		return tuple.f0.f1;
	}
}
