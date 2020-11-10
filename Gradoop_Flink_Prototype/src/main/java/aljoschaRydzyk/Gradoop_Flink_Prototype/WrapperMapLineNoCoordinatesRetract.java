package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

public class WrapperMapLineNoCoordinatesRetract implements MapFunction<Tuple2<Boolean,Row>,String> {
	@Override
	public String map(Tuple2<Boolean, Row> value) throws Exception {
		return  value.f1.getField(0).toString() + "," +
				value.f1.getField(1).toString() + "," + 
				value.f1.getField(2).toString() + "," +
				value.f1.getField(3).toString() + "," +
				value.f1.getField(6).toString() + "," +
				value.f1.getField(7).toString() + "," +
				value.f1.getField(8).toString() + "," +
				value.f1.getField(9).toString() + "," +
				value.f1.getField(12).toString() + "," +
				value.f1.getField(13).toString() + "," +
				value.f1.getField(14).toString() + "," +
				value.f0.toString() +
				"\n";
	}
}
