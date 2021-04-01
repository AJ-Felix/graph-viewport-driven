package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.EdgeVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperVDrive;

public class WrapperTupleRowMapWrapperVDrive implements MapFunction<Tuple2<Tuple2<Row, Row>,Row>,WrapperVDrive> {
	@Override
	public WrapperVDrive map(Tuple2<Tuple2<Row, Row>,Row> tuple) throws Exception {
		VertexVDrive sourceVertex = new VertexVDrive(
				tuple.f0.f1.getField(1).toString(),
				tuple.f0.f1.getField(3).toString(),
				(long) tuple.f0.f1.getField(2),
				(int) tuple.f0.f1.getField(4),
				(int) tuple.f0.f1.getField(5),
				(long) tuple.f0.f1.getField(6),
				(int) tuple.f0.f1.getField(7));
		VertexVDrive targetVertex = new VertexVDrive(
				tuple.f0.f1.getField(8).toString(),
				tuple.f0.f1.getField(10).toString(),
				(long) tuple.f0.f1.getField(9),
				(int) tuple.f0.f1.getField(11),
				(int) tuple.f0.f1.getField(12),
				(long) tuple.f0.f1.getField(13),
				(int) tuple.f0.f1.getField(14));
		EdgeVDrive edge = new EdgeVDrive(
				tuple.f0.f1.getField(15).toString(), 
				tuple.f0.f1.getField(16).toString(), 
				tuple.f0.f1.getField(1).toString(),
				tuple.f0.f1.getField(8).toString());
		WrapperVDrive wrapper = new WrapperVDrive(sourceVertex, targetVertex, edge);
		return wrapper;
	}
}
