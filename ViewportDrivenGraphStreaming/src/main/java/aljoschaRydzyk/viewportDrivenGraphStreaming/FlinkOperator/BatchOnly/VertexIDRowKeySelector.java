package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.types.Row;

public class VertexIDRowKeySelector implements KeySelector<Row,String>{

	@Override
	public String getKey(Row row) throws Exception {
		return row.getField(1).toString();
	}

}
