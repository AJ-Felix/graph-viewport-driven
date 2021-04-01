package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.EPGMVertex;

public class VertexKeyselectorVertexLabel implements KeySelector<Tuple2<Long, Tuple2<EPGMVertex, Long>>,String>{

	@Override
	public String getKey(Tuple2<Long, Tuple2<EPGMVertex, Long>> value) throws Exception {
		return value.f1.f0.getLabel();
	}
}
