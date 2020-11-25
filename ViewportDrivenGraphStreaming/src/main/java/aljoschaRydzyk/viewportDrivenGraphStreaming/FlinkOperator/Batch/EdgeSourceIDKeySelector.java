package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.pojo.EPGMEdge;

public class EdgeSourceIDKeySelector implements KeySelector<EPGMEdge,String> {
	@Override
	public String getKey(EPGMEdge edge) throws Exception {
		return edge.getSourceId().toString();
	}
}
