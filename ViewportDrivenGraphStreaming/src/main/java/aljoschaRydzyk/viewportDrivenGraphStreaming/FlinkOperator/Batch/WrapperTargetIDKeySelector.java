package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMEdge;

public class WrapperTargetIDKeySelector implements KeySelector<Tuple2<Row,Row>,String> {
	@Override
	public String getKey(Tuple2<Row, Row> tuple) throws Exception {
		return tuple.f1.getField(7).toString();
	}
}
