package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Partition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class StringMapAdjacencyMatrix implements MapFunction<String,Tuple2<String,Map<String,String>>>{
	
	@Override
	public Tuple2<String,Map<String,String>>
		map(String value){
		String[] cols = value.split(";");
		String firstVertexId = cols[0];
		cols = Arrays.copyOfRange(cols, 1, cols.length);
		Map<String,String> map = new HashMap<String,String>();
		for (String col : cols) {
			String[] entry = col.split(",");
			map.put(entry[0], entry[1]);
		}
		return Tuple2.of(firstVertexId, map);
	}		
}
