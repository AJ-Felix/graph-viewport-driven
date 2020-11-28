package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;

public class VertexFlatMapNotLayoutedUni implements FlatMapFunction<Row,String> {
	Map<String,Map<String,String>> adjMatrix;
//	Map<String,VertexGVD> layoutedVertices;
	Set<String> layoutedVertices;

	public VertexFlatMapNotLayoutedUni(Map<String,Map<String,String>> adjMatrix, 
//			Map<String,VertexGVD> layoutedVertices
			Set<String> layoutedVertices
			) {
		this.adjMatrix = adjMatrix;
		this.layoutedVertices = layoutedVertices;
	}
	
	@Override
	public void flatMap(Row value, Collector<String> out) throws Exception {
		String sourceId = value.getField(1).toString();
		System.out.println("In VertexFlatMapNotLayouted, sourceId: " + sourceId);
		for (Map.Entry<String, String> entry : adjMatrix.get(sourceId).entrySet()) {
			String targetId = entry.getKey();
//			if (!layoutedVertices.containsKey(targetId) && sourceId.compareTo(targetId) > 0) out.collect(entry.getValue());
			if (!layoutedVertices.contains(targetId) && sourceId.compareTo(targetId) > 0) out.collect(entry.getValue());
		}
	}
}
