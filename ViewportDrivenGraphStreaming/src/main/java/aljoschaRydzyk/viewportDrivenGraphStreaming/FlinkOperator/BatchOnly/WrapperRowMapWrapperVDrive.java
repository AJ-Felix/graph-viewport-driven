package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.EdgeVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperVDrive;

public class WrapperRowMapWrapperVDrive implements MapFunction<Row,WrapperVDrive> {
	@Override
	public WrapperVDrive map(Row row) throws Exception {
		VertexVDrive sourceVertex = new VertexVDrive(
				row.getField(1).toString(),
				row.getField(3).toString(),
				(long) row.getField(2),
				(int) row.getField(4),
				(int) row.getField(5),
				(long) row.getField(6),
				(int) row.getField(7));
		VertexVDrive targetVertex = new VertexVDrive(
				row.getField(8).toString(),
				row.getField(10).toString(),
				(long) row.getField(9),
				(int) row.getField(11),
				(int) row.getField(12),
				(long) row.getField(13),
				(int) row.getField(14));
		EdgeVDrive edge = new EdgeVDrive(
				row.getField(15).toString(), 
				row.getField(16).toString(), 
				row.getField(1).toString(),
				row.getField(8).toString());
		WrapperVDrive wrapper = new WrapperVDrive(sourceVertex, targetVertex, edge);
		return wrapper;
	}
}
