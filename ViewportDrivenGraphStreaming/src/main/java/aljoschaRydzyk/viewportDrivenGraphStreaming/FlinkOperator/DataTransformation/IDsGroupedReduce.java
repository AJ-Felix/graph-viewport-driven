package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class IDsGroupedReduce implements GroupReduceFunction<Tuple3<String, String, String>,
Tuple2<String,List<Tuple2<String,String>>>>{

	@Override
	public void reduce(Iterable<Tuple3<String, String, String>> values,
			Collector<Tuple2<String, List<Tuple2<String, String>>>> out) throws Exception {
		List<Tuple2<String,String>> list = new ArrayList<Tuple2<String,String>>();
		String key = null;
		for (Tuple3<String,String,String> tuple3 : values) {
			list.add(Tuple2.of(tuple3.f1, tuple3.f2));
			if (key == null) key = tuple3.f0.toString();
		}
		out.collect(Tuple2.of(key, list));
	}
}
