package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMEdge;

public class WrapperSourceIDKeySelector implements KeySelector<Row,String> {
	@Override
	public String getKey(Row row) throws Exception {
		return row.getField(1).toString();
	}
}
