package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.EdgeVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperVDrive;

public class VertexMapIdentityWrapperVDrive implements MapFunction<Row,WrapperVDrive> {
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
				row.getField(1).toString(),
				row.getField(3).toString(),
				(long) row.getField(2),
				(int) row.getField(4),
				(int) row.getField(5),
				(long) row.getField(6),
				(int) row.getField(7));
		EdgeVDrive edge = new EdgeVDrive(
				"identityEdge",
				"identityEdge",
				row.getField(1).toString(), 
				row.getField(1).toString());
		WrapperVDrive wrapper = new WrapperVDrive(sourceVertex, targetVertex, edge);
		return wrapper;
	}
}
