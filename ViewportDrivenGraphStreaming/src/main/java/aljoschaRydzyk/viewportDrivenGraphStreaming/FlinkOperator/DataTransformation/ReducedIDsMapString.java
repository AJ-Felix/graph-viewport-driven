package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class ReducedIDsMapString implements MapFunction<Tuple2<String, List<Tuple2<String, String>>>, 
String> {
	
	@Override
	public String map(Tuple2<String, List<Tuple2<String, String>>> value) throws Exception {
		String result = value.f0.toString();
		for (Tuple2<String,String> tuple2 : value.f1) result += ";" + tuple2.f0.toString() + "," + 
				tuple2.f1.toString();
		return result;
	}
}
