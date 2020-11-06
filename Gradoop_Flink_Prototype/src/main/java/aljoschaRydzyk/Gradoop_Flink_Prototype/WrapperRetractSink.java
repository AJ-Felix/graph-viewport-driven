package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

public class WrapperRetractSink implements SinkFunction<Tuple2<Boolean,Row>>{
	@Override 
	public void invoke(Tuple2<Boolean, Row> element, @SuppressWarnings("rawtypes") Context context) {
		String sourceIdGradoop = element.f1.getField(1).toString();
		String sourceIdNumeric = element.f1.getField(2).toString();
		String sourceDegree = element.f1.getField(6).toString();
		String sourceX = element.f1.getField(4).toString();
		String sourceY = element.f1.getField(5).toString();
		String edgeIdGradoop = element.f1.getField(13).toString();
		String edgeLabel = element.f1.getField(14).toString();
		String targetIdGradoop = element.f1.getField(7).toString();
		String targetIdNumeric = element.f1.getField(8).toString();
		String targetDegree = element.f1.getField(12).toString();
		String targetX = element.f1.getField(10).toString();
		String targetY = element.f1.getField(11).toString();
		if (element.f0) {
//			Main.sendToAll("addWrapper;" + edgeIdGradoop + ";" + edgeLabel + ";" + sourceIdGradoop + ";" + sourceIdNumeric + ";" + sourceDegree + ";" +
//					sourceX + ";" + sourceY + ";" + targetIdGradoop + ";" + targetIdNumeric + ";" + targetDegree + ";" + targetX + ";" + targetY);
		} else if (!element.f0) {
//			Main.sendToAll("removeWrapper;" + edgeIdGradoop + ";" + edgeLabel + ";" + sourceIdGradoop + ";" + sourceDegree + ";" +
//					sourceX + ";" + sourceY + ";" + targetIdGradoop + ";" + targetIdNumeric + ";" + targetDegree + ";" + targetX + ";" + targetY);
		}
	}
}
