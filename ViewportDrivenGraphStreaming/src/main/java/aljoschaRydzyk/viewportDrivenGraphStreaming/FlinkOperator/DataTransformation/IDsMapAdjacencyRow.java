package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class IDsMapAdjacencyRow implements MapFunction<Tuple3<String,String,String>, 
AdjacencyRow>{

	@Override
	public AdjacencyRow map(Tuple3<String, String, String> value) throws Exception {
		AdjacencyRow row = new AdjacencyRow(value.f0, value.f1 + "," + value.f2);
		return row;
	}

}
