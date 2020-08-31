package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

public class WrapperRetractSink implements SinkFunction<Tuple2<Boolean,Row>>{
	@Override 
	public void invoke(Tuple2<Boolean, Row> element, Context context) {
		String sourceIdNumeric = element.f1.getField(2).toString();
		String sourceDegree = element.f1.getField(6).toString();
		String sourceX = element.f1.getField(4).toString();
		String sourceY = element.f1.getField(5).toString();
		String edgeIdGradoop = element.f1.getField(13).toString();
		String edgeLabel = element.f1.getField(14).toString();
		String targetIdNumeric = element.f1.getField(8).toString();
		String targetDegree = element.f1.getField(12).toString();
		String targetX = element.f1.getField(10).toString();
		String targetY = element.f1.getField(11).toString();
		if (element.f0) {
			UndertowServer.sendToAll("addWrapper;" + edgeIdGradoop + ";" + edgeLabel + ";" + sourceIdNumeric + ";" + sourceDegree + ";" +
					sourceX + ";" + sourceY + ";" + targetIdNumeric + ";" + targetDegree + ";" + targetX + ";" + targetY);
		} else if (!element.f0) {
			UndertowServer.sendToAll("removeWrapper;" + edgeIdGradoop + ";" + edgeLabel + ";" + sourceIdNumeric + ";" + sourceDegree + ";" +
					sourceX + ";" + sourceY + ";" + targetIdNumeric + ";" + targetDegree + ";" + targetX + ";" + targetY);
		}
	}
}
