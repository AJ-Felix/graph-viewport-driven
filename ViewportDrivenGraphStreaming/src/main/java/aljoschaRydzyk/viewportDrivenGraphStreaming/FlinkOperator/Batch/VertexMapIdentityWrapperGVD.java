package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.EdgeGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;

public class VertexMapIdentityWrapperGVD implements MapFunction<Row,WrapperGVD> {
	@Override
	public WrapperGVD map(Row row) throws Exception {
		VertexGVD sourceVertex = new VertexGVD(
				row.getField(1).toString(),
				row.getField(3).toString(),
				(long) row.getField(2),
				(int) row.getField(4),
				(int) row.getField(5),
				(long) row.getField(6),
				(int) row.getField(7));
		VertexGVD targetVertex = new VertexGVD(
				row.getField(1).toString(),
				row.getField(3).toString(),
				(long) row.getField(2),
				(int) row.getField(4),
				(int) row.getField(5),
				(long) row.getField(6),
				(int) row.getField(7));
		EdgeGVD edge = new EdgeGVD(
				"identityEdge",
				"identityEdge",
				row.getField(1).toString(), 
				row.getField(1).toString());
		WrapperGVD wrapper = new WrapperGVD(sourceVertex, targetVertex, edge);
		return wrapper;
	}
}
