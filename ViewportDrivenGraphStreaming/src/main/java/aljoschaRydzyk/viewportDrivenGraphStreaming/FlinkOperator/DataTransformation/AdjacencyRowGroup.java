package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.apache.flink.api.java.functions.KeySelector;

public class AdjacencyRowGroup implements KeySelector<AdjacencyRow, String>{

	@Override
	public String getKey(AdjacencyRow value) throws Exception {
		return value.getKey();
	}

}
