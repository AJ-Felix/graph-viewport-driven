package aljoschaRydzyk.viewportDrivenGraphStreaming.Debug;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

public class FlinkRowStreamPrintSink implements SinkFunction<Row>{
	@Override 
	public void invoke(Row element, @SuppressWarnings("rawtypes") Context context) {
		System.out.println(element);
	}
}
