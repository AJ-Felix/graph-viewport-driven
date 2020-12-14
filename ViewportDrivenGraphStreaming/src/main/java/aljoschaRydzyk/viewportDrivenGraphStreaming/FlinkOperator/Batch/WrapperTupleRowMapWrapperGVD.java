package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.EdgeGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;

public class WrapperTupleRowMapWrapperGVD implements MapFunction<Tuple2<Tuple2<Row, Row>,Row>,WrapperGVD> {
	@Override
	public WrapperGVD map(Tuple2<Tuple2<Row, Row>,Row> tuple) throws Exception {
		VertexGVD sourceVertex = new VertexGVD(
				tuple.f0.f1.getField(1).toString(),
				tuple.f0.f1.getField(3).toString(),
				(long) tuple.f0.f1.getField(2),
				(int) tuple.f0.f1.getField(4),
				(int) tuple.f0.f1.getField(5),
				(long) tuple.f0.f1.getField(6),
				(int) tuple.f0.f1.getField(7));
		VertexGVD targetVertex = new VertexGVD(
				tuple.f0.f1.getField(8).toString(),
				tuple.f0.f1.getField(10).toString(),
				(long) tuple.f0.f1.getField(9),
				(int) tuple.f0.f1.getField(11),
				(int) tuple.f0.f1.getField(12),
				(long) tuple.f0.f1.getField(13),
				(int) tuple.f0.f1.getField(14));
		EdgeGVD edge = new EdgeGVD(
				tuple.f0.f1.getField(15).toString(), 
				tuple.f0.f1.getField(16).toString(), 
				tuple.f0.f1.getField(1).toString(),
				tuple.f0.f1.getField(8).toString());
		WrapperGVD wrapper = new WrapperGVD(sourceVertex, targetVertex, edge);
		return wrapper;
	}
}
