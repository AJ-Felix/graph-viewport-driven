package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class VertexMapIdentityWrapper implements MapFunction<Row,Row>{
	@Override
	public Row map(Row value) throws Exception {
		Row row = Row.join(value, Row.project(value, new int[]{1, 2, 3, 4, 5, 6}));
		return Row.join(row, Row.of("identityEdge", "identityEdge"));
	}
}
