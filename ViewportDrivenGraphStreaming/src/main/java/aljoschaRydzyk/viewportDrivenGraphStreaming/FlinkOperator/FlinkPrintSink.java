package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class FlinkPrintSink implements SinkFunction<String> {
	@Override
	public void invoke(String s){
		System.out.println(s);
	}
}
