package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class vertexFlatMapNeighbour implements FlatMapFunction<Row,String>{
	private Map<String,Map<String,String>> adjMatrix;
	
	public vertexFlatMapNeighbour (Map<String,Map<String,String>> adjMatrix) {
		this.adjMatrix = adjMatrix;
	}
	
	@Override
	public void flatMap(Row value, Collector<String> out) throws Exception {
		for (String neighbourId : this.adjMatrix.get(value.getField(1)).keySet()){
			out.collect(neighbourId);
		}
	}
}
