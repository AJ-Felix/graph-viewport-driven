package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMEdge;

public class EdgeTargetIDKeySelector implements KeySelector<Tuple2<Row,EPGMEdge>,String> {
	@Override
	public String getKey(Tuple2<Row, EPGMEdge> tuple) throws Exception {
		return tuple.f1.getTargetId().toString();
	}
}