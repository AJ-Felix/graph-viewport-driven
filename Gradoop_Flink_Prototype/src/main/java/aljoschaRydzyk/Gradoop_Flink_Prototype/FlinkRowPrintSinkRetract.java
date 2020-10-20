package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

public class FlinkRowPrintSinkRetract implements SinkFunction<Tuple2<Boolean,Row>>{
	@Override 
	public void invoke(Tuple2<Boolean,Row> element, @SuppressWarnings("rawtypes") Context context) {
		System.out.println(element);
	}
}
