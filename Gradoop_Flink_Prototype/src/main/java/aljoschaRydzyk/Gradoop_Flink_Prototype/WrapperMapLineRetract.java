package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

public class WrapperMapLineRetract implements MapFunction<Tuple2<Boolean,Row>,String> {
	@Override
	public String map(Tuple2<Boolean, Row> value) throws Exception {
		return value.f1.toString() + "," + value.f0.toString() + "\n";
	}
}
