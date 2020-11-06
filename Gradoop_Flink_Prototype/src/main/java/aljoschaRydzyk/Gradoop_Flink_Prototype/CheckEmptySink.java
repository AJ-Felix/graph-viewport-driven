package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

public class CheckEmptySink implements SinkFunction<Row> {
	@Override 
	public void invoke(Row element, @SuppressWarnings("rawtypes") Context context) {
//		Main.latestRow(element);
	}
}
