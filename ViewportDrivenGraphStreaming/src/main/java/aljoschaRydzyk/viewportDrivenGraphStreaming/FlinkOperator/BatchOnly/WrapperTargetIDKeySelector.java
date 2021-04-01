package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

public class WrapperTargetIDKeySelector implements KeySelector<Tuple2<Row,Row>,String> {
	@Override
	public String getKey(Tuple2<Row, Row> tuple) throws Exception {
		return tuple.f1.getField(8).toString();
	}
}
