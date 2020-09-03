package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

public class WrapperAppendSink implements SinkFunction<Row>{
	@Override 
	public void invoke(Row element, @SuppressWarnings("rawtypes") Context context) {
		String sourceIdNumeric = element.getField(2).toString();
		String sourceX = element.getField(4).toString();
		String sourceY = element.getField(5).toString();
		String sourceDegree = element.getField(6).toString();
		String edgeIdGradoop = element.getField(13).toString();
		String edgeLabel = element.getField(14).toString();
		String targetIdNumeric = element.getField(8).toString();
		String targetX = element.getField(10).toString();
		String targetY = element.getField(11).toString();
		String targetDegree = element.getField(12).toString();
		UndertowServer.sendToAll("addWrapper;" + edgeIdGradoop + ";" + edgeLabel + ";" + sourceIdNumeric + ";" + sourceDegree + ";" +
				sourceX + ";" + sourceY + ";" + targetIdNumeric + ";" + targetDegree + ";" + targetX + ";" + targetY);
	}
}
