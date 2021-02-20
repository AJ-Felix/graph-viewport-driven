package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.apache.flink.api.common.functions.ReduceFunction;

public class AdjacencyRowReduce implements ReduceFunction<AdjacencyRow>{

	@Override
	public AdjacencyRow reduce(AdjacencyRow adjaRowMain, AdjacencyRow adjaRowAdd) throws Exception {
		adjaRowMain.setValue(adjaRowMain.getValue() + ";" + adjaRowAdd.getValue());
		return adjaRowMain;
	}

}
