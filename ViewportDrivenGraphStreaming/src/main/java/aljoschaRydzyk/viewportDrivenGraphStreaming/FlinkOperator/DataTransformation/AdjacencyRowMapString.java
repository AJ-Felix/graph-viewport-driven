package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.apache.flink.api.common.functions.MapFunction;

public class AdjacencyRowMapString implements MapFunction<AdjacencyRow,String>{

	@Override
	public String map(AdjacencyRow adjaRow) throws Exception {
		return adjaRow.getKey() + ";" + adjaRow.getValue();
	}

}
